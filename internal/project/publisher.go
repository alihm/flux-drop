package project

import (
	"context"
	"errors"

	"github.com/runonflux/flux-drop/internal/content"
)

type Publisher struct {
	Repository Repository
	DataRoot   string
}

// Publish consumes a staging tree. Installation/activation failures leave the
// reservation retryable: another replica may be completing the same operation,
// or the metadata commit may have succeeded after the response was lost.
// Expired reservations are reclaimed separately by RecoverExpired.
func (p *Publisher) Publish(ctx context.Context, actor Actor, request Reservation, staged *content.Staged) (Project, error) {
	defer staged.Discard()
	manifest, err := content.VerifyVersion(staged.Directory, staged.Digest)
	if err != nil {
		return Project{}, err
	}
	request.Digest = staged.Digest
	request.Bytes = 0
	for _, file := range manifest.Files {
		request.Bytes += file.Size
	}
	prepared, err := p.Repository.Reserve(ctx, actor, request)
	if err != nil {
		return Project{}, err
	}
	if prepared.Operation.State == "complete" {
		return prepared.Project, nil
	}
	if err := ctx.Err(); err != nil {
		return Project{}, err
	} // same idempotency key can resume
	marker := prepared.Project.InitialSlug
	if marker == "" {
		marker = prepared.Project.Slug
	}
	if err := staged.Install(p.DataRoot, prepared.Project.ID, marker); err != nil {
		return Project{}, errors.Join(ErrStorage, err)
	}
	return p.Repository.Activate(ctx, actor, prepared.Operation.ID)
}

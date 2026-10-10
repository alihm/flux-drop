package project

import (
	"context"
	"errors"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/password"
)

type Publisher struct {
	Repository Repository
	DataRoot   string
	Installer  ContentInstaller
	Published  func(Project)
}

// privateActivator is implemented by repositories that can make a new project
// live and password-protected in one transaction.
type privateActivator interface {
	ActivatePrivate(context.Context, Actor, string, string, int64) (Project, error)
}

// Publish consumes a staging tree. Installation/activation failures leave the
// reservation retryable: another replica may be completing the same operation,
// or the metadata commit may have succeeded after the response was lost.
// Expired reservations are reclaimed separately by RecoverExpired.
func (p *Publisher) Publish(ctx context.Context, actor Actor, request Reservation, staged *content.Staged) (Project, error) {
	defer staged.Discard()
	prepared, err := p.install(ctx, actor, request, staged)
	if err != nil {
		return Project{}, err
	}
	if prepared.Operation.State == "complete" {
		p.notify(prepared.Project)
		return prepared.Project, nil
	}
	result, err := p.Repository.Activate(ctx, actor, prepared.Operation.ID)
	if err == nil {
		p.notify(result)
	}
	return result, err
}

// PublishPrivate publishes a new project that is password-protected from its
// first activation; it is never publicly resolvable. The password is hashed
// before any quota is reserved so weak or busy requests fail early.
func (p *Publisher) PublishPrivate(ctx context.Context, actor Actor, request Reservation, staged *content.Staged, hasher *password.Hasher, secret string) (Project, error) {
	defer staged.Discard()
	activator, ok := p.Repository.(privateActivator)
	if !ok || hasher == nil || request.ProjectID != "" {
		return Project{}, ErrInvalid
	}
	hash, err := hasher.Create(ctx, secret)
	if err != nil {
		return Project{}, err
	}
	prepared, err := p.install(ctx, actor, request, staged)
	if err != nil {
		return Project{}, err
	}
	if prepared.Operation.State == "complete" {
		if !prepared.Project.Private {
			return Project{}, ErrConflict
		}
		p.notify(prepared.Project)
		return prepared.Project, nil
	}
	revision := prepared.Project.PolicyRevision + 1
	digest, err := password.Store(p.DataRoot, password.Record{ProjectID: prepared.Project.ID, PolicyRevision: revision, Hash: hash})
	if err != nil {
		return Project{}, errors.Join(ErrStorage, err)
	}
	result, err := activator.ActivatePrivate(ctx, actor, prepared.Operation.ID, digest, revision)
	if err == nil {
		p.notify(result)
	}
	return result, err
}

func (p *Publisher) notify(result Project) {
	if p.Published != nil {
		p.Published(result)
	}
}

// install verifies and reserves the staged version, then installs it unless the
// operation already completed.
func (p *Publisher) install(ctx context.Context, actor Actor, request Reservation, staged *content.Staged) (Prepared, error) {
	manifest, err := content.VerifyVersion(staged.Directory, staged.Digest)
	if err != nil {
		return Prepared{}, err
	}
	request.Files = len(manifest.Files)
	request.StorageManifest = &manifest
	request.Digest = staged.Digest
	request.Bytes = 0
	for _, file := range manifest.Files {
		request.Bytes += file.Size
	}
	prepared, err := p.Repository.Reserve(ctx, actor, request)
	if err != nil {
		return Prepared{}, err
	}
	if prepared.Operation.State == "complete" {
		return prepared, nil
	}
	if err := ctx.Err(); err != nil {
		return Prepared{}, err
	} // same idempotency key can resume
	marker := prepared.Project.InitialSlug
	if marker == "" {
		marker = prepared.Project.Slug
	}
	var installErr error
	if p.Installer != nil {
		installErr = p.Installer.Install(ctx, prepared, staged)
	} else {
		installErr = staged.Install(p.DataRoot, prepared.Project.ID, marker)
	}
	if err := installErr; err != nil {
		return Prepared{}, errors.Join(ErrStorage, err)
	}
	return prepared, nil
}

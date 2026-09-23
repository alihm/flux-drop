package project

import (
	"context"
	"errors"
	"math"

	"cloud.google.com/go/firestore"
	"github.com/runonflux/flux-drop/internal/password"
)

type PrivacyService struct {
	Repository Repository
	DataRoot   string
	Hasher     *password.Hasher
}

// Change verifies ownership before costly hashing. Only the transactional commit
// selects prepared metadata; a failed/stale attempt leaves an inert orphan file.
func (s *PrivacyService) Change(ctx context.Context, a Actor, id string, revision int64, private bool, secret string) (Project, error) {
	if revision < 1 || (!private && secret != "") {
		return Project{}, ErrInvalid
	}
	p, err := s.Repository.GetOwned(ctx, a, id)
	if err != nil {
		return Project{}, err
	}
	if p.Revision != revision || p.PendingOperation != "" || p.PolicyRevision == math.MaxInt64 {
		return Project{}, ErrConflict
	}
	digest := ""
	passwordRevision := int64(0)
	if private {
		hash, err := s.Hasher.Create(ctx, secret)
		if err != nil {
			return Project{}, err
		}
		passwordRevision = p.PolicyRevision + 1
		digest, err = password.Store(s.DataRoot, password.Record{ProjectID: id, PolicyRevision: passwordRevision, Hash: hash})
		if err != nil {
			return Project{}, errors.Join(ErrStorage, err)
		}
	}
	return s.Repository.SetPrivacy(ctx, a, id, revision, digest, passwordRevision)
}

// SetPrivacy is server-internal: digest must refer to a durably prepared password
// record. HTTP callers never supply hashes or policy revisions directly.
func (s *FirestoreRepository) SetPrivacy(ctx context.Context, a Actor, id string, revision int64, digest string, passwordRevision int64) (Project, error) {
	if !idRE.MatchString(id) || revision < 1 || (digest != "" && (!digestRE.MatchString(digest) || passwordRevision < 1)) || (digest == "" && passwordRevision != 0) {
		return Project{}, ErrInvalid
	}
	var result Project
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := read[Project](tx, s.ref("projects", id))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) || !p.Live(s.now()) {
			return ErrNotFound
		}
		if p.Revision != revision || p.PendingOperation != "" || p.PolicyRevision == math.MaxInt64 {
			return ErrConflict
		}
		if digest != "" && passwordRevision != p.PolicyRevision+1 {
			return ErrConflict
		}
		p.Private = digest != ""
		p.PasswordDigest = digest
		p.PasswordRevision = passwordRevision
		p.Revision++
		p.PolicyRevision++
		if err := tx.Set(s.ref("projects", id), p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

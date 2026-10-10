package project

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func (s *FirestoreRepository) revisionProject(tx *firestore.Transaction, a Actor, id string) (Project, error) {
	if err := s.authorize(tx, a); err != nil {
		return Project{}, err
	}
	p, err := read[Project](tx, s.ref("projects", id))
	if missing(err) {
		return p, ErrNotFound
	}
	if err != nil {
		return p, err
	}
	if !a.Owns(p.Owner) || !p.Live(s.now()) {
		return Project{}, ErrNotFound
	}
	return p, nil
}

func (s *FirestoreRepository) availableRevision(tx *firestore.Transaction, p Project, id string) (Operation, error) {
	op, err := read[Operation](tx, s.ref("operations", id))
	if missing(err) {
		return op, ErrNotFound
	}
	if err != nil {
		return op, err
	}
	if op.ID != id || op.ProjectID != p.ID || op.State != "complete" {
		return op, ErrNotFound
	}
	if _, err = tx.Get(s.ref("revision_removals", revisionIdentity(op))); err == nil {
		return op, ErrNotFound
	} else if !missing(err) {
		return op, err
	}
	if err = s.checkNotRetired(tx, VersionRef{p.ID, op.Digest}); err != nil {
		if errors.Is(err, ErrConflict) {
			return op, ErrNotFound
		}
		return op, err
	}
	return op, nil
}

func (s *FirestoreRepository) ListRevisions(ctx context.Context, a Actor, id, cursor string) ([]Revision, string, error) {
	if !idRE.MatchString(id) || cursor != "" && !digestRE.MatchString(cursor) {
		return nil, "", ErrInvalid
	}
	if _, err := s.GetOwned(ctx, a, id); err != nil {
		return nil, "", err
	}
	query := s.Client.Collection("drop_operations").Where("projectID", "==", id).OrderBy(firestore.DocumentID, firestore.Asc).Limit(21)
	if cursor != "" {
		query = query.StartAfter(cursor)
	}
	iter := query.Documents(ctx)
	defer iter.Stop()
	ids := []string{}
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, "", err
		}
		ids = append(ids, doc.Ref.ID)
	}
	next := ""
	if len(ids) > 20 {
		ids = ids[:20]
		next = ids[len(ids)-1]
	}
	var result []Revision
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		result = make([]Revision, 0, len(ids))
		p, err := s.revisionProject(tx, a, id)
		if err != nil {
			return err
		}
		for _, id := range ids {
			op, err := s.availableRevision(tx, p, id)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			created := op.PublishedAt
			if created.IsZero() {
				created = op.ExpiresAt.Add(-15 * time.Minute)
			}
			result = append(result, Revision{op.ID, op.Digest, op.Bytes, created, p.ActiveDigest == op.Digest && p.StorageGeneration == op.StorageGeneration})
		}
		return nil
	}, firestore.ReadOnly)
	return result, next, err
}

func (s *FirestoreRepository) SelectRevision(ctx context.Context, a Actor, id, version string, revision int64) (Project, error) {
	return s.changeRevision(ctx, a, id, version, revision, false)
}
func (s *FirestoreRepository) RemoveRevision(ctx context.Context, a Actor, id, version string, revision int64) (Project, error) {
	return s.changeRevision(ctx, a, id, version, revision, true)
}
func (s *FirestoreRepository) changeRevision(ctx context.Context, a Actor, id, version string, revision int64, remove bool) (Project, error) {
	if !idRE.MatchString(id) || !digestRE.MatchString(version) || revision < 1 {
		return Project{}, ErrInvalid
	}
	var result Project
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		p, err := s.revisionProject(tx, a, id)
		if err != nil {
			return err
		}
		if p.Revision != revision || p.PendingOperation != "" {
			return ErrConflict
		}
		op, err := s.availableRevision(tx, p, version)
		if err != nil {
			return err
		}
		if p.ActiveDigest == op.Digest && p.StorageGeneration == op.StorageGeneration {
			if remove {
				return ErrConflict
			}
			result = p
			return nil
		}
		if remove {
			// Firestore/local storage addresses versions by digest. Fence all
			// future installers before any physical cleanup can be considered.
			if err = tx.Set(s.ref("revision_removals", revisionIdentity(op)), map[string]any{"removedAt": s.now()}); err != nil {
				return err
			}
			if err = tx.Set(s.ref("retirements", retirementID(VersionRef{p.ID, op.Digest})), map[string]any{"removed": true}); err != nil {
				return err
			}
		} else {
			idx, err := read[digestRecord](tx, s.ref("digests", op.Digest))
			if err != nil && !missing(err) {
				return err
			}
			if idx.ProjectID != "" && idx.ProjectID != p.ID {
				return ErrConflict
			}
			old, err := read[digestRecord](tx, s.ref("digests", p.ActiveDigest))
			if err != nil && !missing(err) {
				return err
			}
			// All Firestore reads must precede the first write.
			if old.ProjectID == p.ID && p.ActiveDigest != op.Digest {
				if err = tx.Delete(s.ref("digests", p.ActiveDigest)); err != nil {
					return err
				}
			}
			if err = tx.Set(s.ref("digests", op.Digest), digestRecord{ProjectID: p.ID}); err != nil {
				return err
			}
			p.ActiveDigest, p.ActiveBytes, p.StorageGeneration = op.Digest, op.Bytes, op.StorageGeneration
			p.PolicyRevision++
		}
		p.Revision++
		p.UpdatedAt = s.now()
		if err = tx.Set(s.ref("projects", p.ID), p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

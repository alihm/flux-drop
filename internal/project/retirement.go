package project

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
)

// Retirement is a permanent metadata publication fence, not a filesystem lease
// or proof of reclamation. No worker currently consumes it to delete bytes.
type Retirement struct {
	Schema            int       `firestore:"schema"`
	ProjectID         string    `firestore:"projectID"`
	Digest            string    `firestore:"digest"`
	SourceOperationID string    `firestore:"sourceOperationID"`
	ProjectRevision   int64     `firestore:"projectRevision"`
	CreatedAt         time.Time `firestore:"createdAt"`
}

func retirementID(ref VersionRef) string { return hash([]string{ref.ProjectID, ref.Digest}) }

// Any existing record blocks publication, even if its contents are malformed.
func (s *FirestoreRepository) checkNotRetired(tx *firestore.Transaction, ref VersionRef) error {
	_, err := tx.Get(s.ref("retirements", retirementID(ref)))
	if missing(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return ErrConflict
}

// RetireVersion is an internal privileged maintenance primitive. It requires a
// known completed/aborted operation as provenance, a current project revision,
// no pending installer reservation, and a non-active target. It is intentionally
// not registered in an HTTP API, scheduler or CLI. Never delete its record to
// allow reuse: safe reuse will require a new storage generation.
func (s *FirestoreRepository) RetireVersion(ctx context.Context, ref VersionRef, sourceOperationID string, expectedRevision int64) (Retirement, error) {
	if !idRE.MatchString(ref.ProjectID) || !digestRE.MatchString(ref.Digest) || !digestRE.MatchString(sourceOperationID) || expectedRevision < 0 {
		return Retirement{}, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var result Retirement
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		existing, err := read[Retirement](tx, s.ref("retirements", retirementID(ref)))
		if err == nil {
			if existing.Schema != 1 || existing.ProjectID != ref.ProjectID || existing.Digest != ref.Digest || existing.SourceOperationID != sourceOperationID || existing.ProjectRevision != expectedRevision || existing.CreatedAt.IsZero() {
				return ErrConflict
			}
			result = existing
			return nil
		}
		if !missing(err) {
			return err
		}
		op, err := read[Operation](tx, s.ref("operations", sourceOperationID))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if op.ID != sourceOperationID || op.ProjectID != ref.ProjectID || op.Digest != ref.Digest || (op.State != "complete" && op.State != "aborted") {
			return ErrConflict
		}
		p, err := read[Project](tx, s.ref("projects", ref.ProjectID))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		now := s.now()
		if p.Revision != expectedRevision || retentionFinding(ref, p, now).Classification != "candidate" {
			return ErrConflict
		}
		result = Retirement{Schema: 1, ProjectID: ref.ProjectID, Digest: ref.Digest, SourceOperationID: sourceOperationID, ProjectRevision: p.Revision, CreatedAt: now}
		return tx.Create(s.ref("retirements", retirementID(ref)), result)
	})
	if err != nil {
		return Retirement{}, err
	}
	return result, nil
}

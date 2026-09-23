package project

import (
	"context"
	"log/slog"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

// ExpireProjects releases metadata quotas, never deletes replicated files.
// Pending updates are recovered first by RecoverExpired; until then their
// expired project is already inaccessible through Resolve.
func (s *FirestoreRepository) ExpireProjects(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 100 {
		return 0, ErrInvalid
	}
	iter := s.Client.Collection("drop_projects").Where("status", "==", "active").Where("expiresAt", "<=", s.now()).OrderBy("expiresAt", firestore.Asc).Limit(limit).Documents(ctx)
	defer iter.Stop()
	n := 0
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if err := s.expireProject(ctx, doc.Ref.ID); err != nil {
			return n, err
		}
		n++
	}
}

func (s *FirestoreRepository) expireProject(ctx context.Context, id string) error {
	if !idRE.MatchString(id) {
		return ErrInvalid
	}
	return s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		p, err := read[Project](tx, s.ref("projects", id))
		if missing(err) {
			return nil
		}
		if err != nil {
			return err
		}
		// Recheck after any concurrent claim, deletion or update. Never expire
		// account-owned projects even if malformed metadata carries a date.
		if p.Status != "active" || p.Owner.Kind != "anonymous" || p.ExpiresAt == nil || s.now().Before(*p.ExpiresAt) || p.PendingOperation != "" {
			return nil
		}
		q, err := s.readQuota(tx, p.Owner)
		if err != nil {
			return err
		}
		idx, err := read[digestRecord](tx, s.ref("digests", p.ActiveDigest))
		if err != nil && !missing(err) {
			return err
		}
		if q.Count <= 0 {
			return ErrConflict
		}
		if idx.ProjectID == p.ID {
			if err := tx.Delete(s.ref("digests", p.ActiveDigest)); err != nil {
				return err
			}
		}
		if err := tx.Set(s.ref("quotas", ownerKey(p.Owner)), quota{Count: q.Count - 1, Schema: q.Schema, ChargedBytes: q.ChargedBytes}); err != nil {
			return err
		}
		p.Status = "deleted"
		p.Revision++
		p.PolicyRevision++
		return tx.Set(s.ref("projects", id), p) // keep the slug tombstone
	})
}

// RunMaintenance performs bounded passes on each replica. Transactions make
// overlapping passes safe; cancellation stops the worker before clients close.
func (s *FirestoreRepository) RunMaintenance(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		pass, cancel := context.WithTimeout(ctx, 45*time.Second)
		_, recoveryErr := s.RecoverExpired(pass, 100)
		_, expiryErr := s.ExpireProjects(pass, 100)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if recoveryErr != nil {
			slog.Error("reservation recovery failed", "error", recoveryErr)
		}
		if expiryErr != nil {
			slog.Error("project expiry failed", "error", expiryErr)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

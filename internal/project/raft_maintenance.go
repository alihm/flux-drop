package project

import (
	"context"
	"errors"
	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/session"
	"log/slog"
	"strings"
	"time"
)

func (s *RaftRepository) expireProject(ctx context.Context, id string) error {
	if !idRE.MatchString(id) {
		return ErrInvalid
	}
	return s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
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
		idx, err := raftRead[digestRecord](tx, s.raftRef("digests", p.ActiveDigest))
		if err != nil && !raftMissing(err) {
			return err
		}
		if q.Count <= 0 {
			return ErrConflict
		}
		if idx.ProjectID == p.ID {
			if err := tx.Delete(s.raftRef("digests", p.ActiveDigest)); err != nil {
				return err
			}
		}
		if err := tx.Set(s.raftRef("quotas", ownerKey(p.Owner)), quota{Count: q.Count - 1, Schema: q.Schema, ChargedBytes: q.ChargedBytes}); err != nil {
			return err
		}
		p.Status = "deleted"
		p.Revision++
		p.PolicyRevision++
		return tx.Set(s.raftRef("projects", id), p) // keep the slug tombstone
	})
}

type metadataScanner interface {
	Scan(context.Context, string, string, int) (kv.Page, error)
}

// Maintain scans bounded candidate pages, then rechecks every mutation in a
// transaction. Scans alone never authorize cleanup or release a quota.
func (s *RaftRepository) Maintain(ctx context.Context, cursors map[string]string) error {
	scanner, ok := s.Store.Backend.(metadataScanner)
	if !ok || cursors == nil {
		return ErrInvalid
	}
	for _, prefix := range []string{"operations/", "projects/", "sessions/", "grants/", "unlock_budgets/"} {
		page, err := scanner.Scan(ctx, prefix, cursors[prefix], 100)
		if err != nil {
			return err
		}
		for key, record := range page.Records {
			id := strings.TrimPrefix(key, prefix)
			switch prefix {
			case "operations/":
				var op Operation
				if err := metadata.Decode(record.Value, &op); err != nil {
					return err
				}
				if op.State == "pending" && !s.now().Before(op.ExpiresAt) {
					if err := s.abort(ctx, nil, id, true); err != nil {
						return err
					}
				}
			case "projects/":
				var p Project
				if err := metadata.Decode(record.Value, &p); err != nil {
					return err
				}
				if p.Owner.Kind == "anonymous" && p.Status == "active" && p.ExpiresAt != nil && !s.now().Before(*p.ExpiresAt) {
					if err := s.expireProject(ctx, id); err != nil {
						return err
					}
				}
			default:
				eligible, err := expiredMetadata(prefix, record.Value, s.now())
				if err != nil {
					return err
				}
				if !eligible {
					continue
				}
				err = s.Store.Run(ctx, func(tx *metadata.Tx) error {
					// Re-read a typed record; the scan's version may already be obsolete.
					expired := false
					switch prefix {
					case "sessions/":
						var r session.Record
						err := tx.Get(key, &r)
						if errors.Is(err, metadata.ErrNotFound) {
							return nil
						}
						if err != nil {
							return err
						}
						expired = r.Revoked || !s.now().Before(r.ExpiresAt)
					case "grants/":
						var r Grant
						err := tx.Get(key, &r)
						if errors.Is(err, metadata.ErrNotFound) {
							return nil
						}
						if err != nil {
							return err
						}
						expired = !s.now().Before(r.ExpiresAt)
					case "unlock_budgets/":
						var r unlockBudget
						err := tx.Get(key, &r)
						if errors.Is(err, metadata.ErrNotFound) {
							return nil
						}
						if err != nil {
							return err
						}
						expired = !s.now().Before(r.ExpiresAt)
					}
					if expired {
						return tx.Delete(key)
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
		}
		cursors[prefix] = page.Next
	}
	return nil
}
func expiredMetadata(prefix string, raw []byte, now time.Time) (bool, error) {
	switch prefix {
	case "sessions/":
		var r session.Record
		if err := metadata.Decode(raw, &r); err != nil {
			return false, err
		}
		return r.Revoked || !now.Before(r.ExpiresAt), nil
	case "grants/":
		var r Grant
		if err := metadata.Decode(raw, &r); err != nil {
			return false, err
		}
		return !now.Before(r.ExpiresAt), nil
	case "unlock_budgets/":
		var r unlockBudget
		if err := metadata.Decode(raw, &r); err != nil {
			return false, err
		}
		return !now.Before(r.ExpiresAt), nil
	}
	return false, ErrInvalid
}
func (s *RaftRepository) RunMaintenance(ctx context.Context) {
	cursors := make(map[string]string)
	for ctx.Err() == nil {
		bounded, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := s.Maintain(bounded, cursors)
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Warn("metadata maintenance deferred")
		}
		timer := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

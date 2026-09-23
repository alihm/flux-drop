package project

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
)

type RaftRepository struct {
	Store                                *metadata.Store
	Now                                  func() time.Time
	AnonymousLimit, AccountLimit         int64
	AnonymousByteLimit, AccountByteLimit int64
}

func (s *RaftRepository) raftRef(collection, id string) string { return collection + "/" + id }
func raftMissing(err error) bool                               { return errors.Is(err, metadata.ErrNotFound) }

type raftTx struct{ tx *metadata.Tx }
type raftDocument struct {
	tx  *metadata.Tx
	key string
}

func (d raftDocument) DataTo(value any) error { return d.tx.Get(d.key, value) }
func (t *raftTx) Get(key string) (raftDocument, error) {
	err := t.tx.Get(key, nil)
	return raftDocument{t.tx, key}, err
}
func (t *raftTx) Delete(key string) error { return t.tx.Delete(key) }
func raftRead[T any](tx *raftTx, key string) (T, error) {
	var value T
	err := tx.tx.Get(key, &value)
	return value, err
}
func (s *RaftRepository) run(ctx context.Context, fn func(context.Context, *raftTx) error) error {
	return s.Store.Run(ctx, func(tx *metadata.Tx) error { return fn(ctx, &raftTx{tx}) })
}
func (s *RaftRepository) runContent(ctx context.Context, fn func(context.Context, *raftTx) error) error {
	return s.Store.RunContent(ctx, func(tx *metadata.Tx) error { return fn(ctx, &raftTx{tx}) })
}

// Only these reviewed changes are content-only. Comparing everything else
// means newly added project fields default to replicated when changed.
func contentOnlyProjectChange(old, next Project) bool {
	if old.Status == "deleted" || next.Status == "deleted" {
		return false
	}
	allowed := old
	allowed.ActiveDigest, allowed.ActiveBytes = next.ActiveDigest, next.ActiveBytes
	allowed.ChargedBytes, allowed.Revision = next.ChargedBytes, next.Revision
	allowed.PendingOperation = next.PendingOperation
	if old.Status == "reserved" && next.Status == "active" {
		allowed.Status = next.Status
		allowed.CreatedAt = next.CreatedAt
		// Initial activation establishes the anonymous project's expiry. It
		// must never remove the expiry (claim) or change an active expiry.
		if old.ExpiresAt != nil && next.ExpiresAt != nil {
			allowed.ExpiresAt = next.ExpiresAt
		}
	}
	return reflect.DeepEqual(allowed, next)
}

// ContentOnlyChange is also checked at the trusted coordinator boundary before
// honoring local acknowledgement. Creation is limited to a public reservation.
func ContentOnlyChange(old *Project, next Project) bool {
	if old == nil {
		return next.Status == "reserved" && !next.Private && next.PasswordDigest == "" && next.PasswordRevision == 0 && next.PolicyRevision == 1
	}
	return contentOnlyProjectChange(*old, next)
}
func (t *raftTx) Create(key string, value any) error {
	if err := t.tx.Get(key, nil); !raftMissing(err) {
		if err != nil {
			return err
		}
		return ErrConflict
	}
	return t.Set(key, value)
}

// Owner indexes are updated in the SAME transaction as project changes. They
// contain only live/reserved IDs, not an ever-growing history of tombstones.
type ownerIndex struct{ IDs []string }

func (t *raftTx) updateIndex(owner Owner, id string, add bool) error {
	key := "owner_projects/" + ownerKey(owner)
	var idx ownerIndex
	if err := t.tx.Get(key, &idx); err != nil && !raftMissing(err) {
		return err
	}
	pos := sort.SearchStrings(idx.IDs, id)
	if add && (pos == len(idx.IDs) || idx.IDs[pos] != id) {
		if len(idx.IDs) >= 1000 {
			return ErrQuota
		}
		idx.IDs = append(idx.IDs, id)
		sort.Strings(idx.IDs)
	} else if !add && pos < len(idx.IDs) && idx.IDs[pos] == id {
		idx.IDs = append(idx.IDs[:pos], idx.IDs[pos+1:]...)
	}
	return t.tx.Set(key, idx)
}
func (t *raftTx) Set(key string, value any) error {
	if strings.HasPrefix(key, "projects/") {
		p, ok := value.(Project)
		if !ok || key != "projects/"+p.ID {
			return ErrInvalid
		}
		var old Project
		err := t.tx.Get(key, &old)
		if err != nil && !raftMissing(err) {
			return err
		}
		if err == nil && !contentOnlyProjectChange(old, p) {
			t.tx.RequireReplication()
		}
		if raftMissing(err) && (p.Status != "reserved" || p.Private || p.PasswordDigest != "" || p.PasswordRevision != 0 || p.PolicyRevision != 1) {
			t.tx.RequireReplication()
		}
		if err == nil && old.Status != "deleted" && (old.Owner != p.Owner || p.Status == "deleted") {
			if err := t.updateIndex(old.Owner, p.ID, false); err != nil {
				return err
			}
		}
		if p.Status != "deleted" && (raftMissing(err) || old.Owner != p.Owner || old.Status == "deleted") {
			if err := t.updateIndex(p.Owner, p.ID, true); err != nil {
				return err
			}
		}
	}
	return t.tx.Set(key, value)
}

func (s *RaftRepository) ListOwned(ctx context.Context, a Actor, cursor string, limit int) ([]Project, string, error) {
	if limit < 1 || limit > 100 || (cursor != "" && !idRE.MatchString(cursor)) {
		return nil, "", ErrInvalid
	}
	var result []Project
	var next string
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		result = make([]Project, 0)
		next = ""
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		owners := []Owner{{"anonymous", a.AnonymousID}}
		if a.UID != "" {
			owners = append(owners, Owner{"firebase", a.UID})
		}
		ids := make(map[string]bool)
		for _, owner := range owners {
			idx, err := raftRead[ownerIndex](tx, "owner_projects/"+ownerKey(owner))
			if err != nil && !raftMissing(err) {
				return err
			}
			for _, id := range idx.IDs {
				if id > cursor {
					ids[id] = true
				}
			}
		}
		ordered := make([]string, 0, len(ids))
		for id := range ids {
			ordered = append(ordered, id)
		}
		sort.Strings(ordered)
		if len(ordered) > limit {
			ordered = ordered[:limit]
			next = ordered[len(ordered)-1]
		}
		for _, id := range ordered {
			p, err := raftRead[Project](tx, "projects/"+id)
			if err != nil {
				return err
			}
			if p.Live(s.now()) && a.Owns(p.Owner) {
				result = append(result, p)
			}
		}
		return nil
	})
	return result, next, err
}

var _ Repository = (*RaftRepository)(nil)
var _ UnlockRepository = (*RaftRepository)(nil)

package project

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func TestRaftMaintenanceRecoversAndExpires(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}, Now: func() time.Time { return now }}
	token, view, err := sessions.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a, err := ActorFrom(token, view)
	if err != nil {
		t.Fatal(err)
	}
	repo := &RaftRepository{Store: store, Now: func() time.Time { return now }}
	pending, err := repo.Reserve(ctx, a, Reservation{Key: "abandoned_operation", Name: "pending", Digest: strings.Repeat("a", 64), Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repo.Reserve(ctx, a, Reservation{Key: "completed_operation", Name: "active", Digest: strings.Repeat("b", 64), Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	p, err := repo.Activate(ctx, a, prepared.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(31 * 24 * time.Hour)
	if err = repo.Maintain(ctx, make(map[string]string)); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Resolve(ctx, p.Slug); !errors.Is(err, ErrNotFound) {
		t.Fatal("expired project served", err)
	}
	err = store.Run(ctx, func(tx *metadata.Tx) error {
		var op Operation
		if err := tx.Get("operations/"+pending.Operation.ID, &op); err != nil {
			return err
		}
		if op.State != "aborted" {
			t.Fatal("abandoned reservation not released")
		}
		var q quota
		if err := tx.Get("quotas/"+ownerKey(a.Owner()), &q); err != nil {
			return err
		}
		if q.Count != 0 || q.ChargedBytes != 2*versionCharge(10) {
			t.Fatal("quota accounting", q)
		}
		var idx ownerIndex
		if err := tx.Get("owner_projects/"+ownerKey(a.Owner()), &idx); err != nil {
			return err
		}
		if len(idx.IDs) != 0 {
			t.Fatal("stale owner index", idx)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(366 * 24 * time.Hour)
	if err = repo.Maintain(ctx, make(map[string]string)); err != nil {
		t.Fatal(err)
	}
	if _, err = sessions.Read(ctx, token); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("expired session retained", err)
	}
}

package project

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func TestTransferClaimAcrossSessions(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}, Now: func() time.Time { return now }}
	ownerToken, ownerView, err := sessions.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := ActorFrom(ownerToken, ownerView)
	repo := &RaftRepository{Store: store, Now: func() time.Time { return now }}
	prepared, err := repo.Reserve(ctx, owner, Reservation{Key: "transfer_one", Name: "agent-site", Digest: strings.Repeat("a", 64), Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	p, err := repo.Activate(ctx, owner, prepared.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	token, expires, err := repo.CreateTransfer(ctx, owner, p.ID, p.Revision)
	if err != nil || !expires.Equal(now.Add(transferLifetime)) {
		t.Fatal("issue", err)
	}
	otherToken, otherView, err := sessions.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := ActorFrom(otherToken, otherView)
	if _, err := repo.RedeemTransfer(ctx, other, token); !errors.Is(err, ErrForbidden) {
		t.Fatal("anonymous recipient accepted", err)
	}
	err = store.Run(ctx, func(tx *metadata.Tx) error {
		var record session.Record
		if err := tx.Get("sessions/"+other.SessionDigest, &record); err != nil {
			return err
		}
		record.UID = "recipient-google-uid"
		record.AuthUntil = now.Add(time.Hour)
		return tx.Set("sessions/"+other.SessionDigest, record)
	})
	if err != nil {
		t.Fatal(err)
	}
	other.UID = "recipient-google-uid"
	forged := token[:len(token)-1] + "A"
	if strings.HasSuffix(token, "A") {
		forged = token[:len(token)-1] + "B"
	}
	if _, err := repo.RedeemTransfer(ctx, other, forged); !errors.Is(err, ErrNotFound) {
		t.Fatal("forged transfer accepted", err)
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := repo.RedeemTransfer(ctx, other, token)
			if err == nil {
				if claimed.ExpiresAt != nil || claimed.Owner.ID != other.UID {
					t.Error("incorrect owner or expiry")
				}
				winners.Add(1)
			} else if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatal("transfer was not one-use", winners.Load())
	}
	if _, err := repo.GetOwned(ctx, owner, p.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("old owner retained management", err)
	}
	if _, err := repo.GetOwned(ctx, other, p.ID); err != nil {
		t.Fatal("new owner cannot manage", err)
	}
}

func TestTransferReissueRevocationAndExpiry(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}, Now: func() time.Time { return now }}
	token, view, _ := sessions.Create(ctx)
	owner, _ := ActorFrom(token, view)
	repo := &RaftRepository{Store: store, Now: func() time.Time { return now }}
	prepared, err := repo.Reserve(ctx, owner, Reservation{Key: "transfer_two", Digest: strings.Repeat("b", 64), Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	p, err := repo.Activate(ctx, owner, prepared.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := repo.CreateTransfer(ctx, owner, p.ID, p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := repo.CreateTransfer(ctx, owner, p.ID, p.Revision)
	if err != nil || first == second {
		t.Fatal("reissue", err)
	}
	if err := repo.RevokeTransfer(ctx, owner, p.ID, p.Revision); err != nil {
		t.Fatal(err)
	}
	if err := repo.RevokeTransfer(ctx, owner, p.ID, p.Revision); err != nil {
		t.Fatal("revoke is not idempotent", err)
	}
	third, _, err := repo.CreateTransfer(ctx, owner, p.ID, p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	otherToken, otherView, _ := sessions.Create(ctx)
	other, _ := ActorFrom(otherToken, otherView)
	err = store.Run(ctx, func(tx *metadata.Tx) error {
		var record session.Record
		if err := tx.Get("sessions/"+other.SessionDigest, &record); err != nil {
			return err
		}
		record.UID = "recipient"
		record.AuthUntil = now.Add(48 * time.Hour)
		return tx.Set("sessions/"+other.SessionDigest, record)
	})
	if err != nil {
		t.Fatal(err)
	}
	other.UID = "recipient"
	for _, old := range []string{first, second} {
		if _, err := repo.RedeemTransfer(ctx, other, old); !errors.Is(err, ErrNotFound) {
			t.Fatal("revoked token worked", err)
		}
	}
	now = now.Add(25 * time.Hour)
	if _, err := repo.RedeemTransfer(ctx, other, third); !errors.Is(err, ErrNotFound) {
		t.Fatal("expired token worked", err)
	}
}

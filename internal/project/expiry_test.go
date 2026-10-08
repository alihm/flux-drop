package project

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestOneWeekExpiryStartsAtActivationAndUpdatesDoNotExtendIt(t *testing.T) {
	repo, actor, _ := storageRepository(t, 1<<30)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	repo.Now = func() time.Time { return now }
	reserved, err := repo.Reserve(ctx, actor, Reservation{Key: "week_lifetime", Name: "week", Digest: strings.Repeat("a", 64), Bytes: 1, Files: 1})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(10 * time.Minute)
	p, err := repo.Activate(ctx, actor, reserved.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	expires := now.Add(7 * 24 * time.Hour)
	if p.ExpiresAt == nil || !p.ExpiresAt.Equal(expires) {
		t.Fatal("expiry is not one week from activation", p.ExpiresAt, expires)
	}
	now = now.Add(6 * 24 * time.Hour)
	updated, err := repo.Reserve(ctx, actor, Reservation{Key: "week_update", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: strings.Repeat("b", 64), Bytes: 1, Files: 1})
	if err != nil {
		t.Fatal(err)
	}
	p, err = repo.Activate(ctx, actor, updated.Operation.ID)
	if err != nil || p.ExpiresAt == nil || !p.ExpiresAt.Equal(expires) {
		t.Fatal("update extended expiry", p.ExpiresAt, err)
	}
	now = expires.Add(-time.Nanosecond)
	if _, err := repo.Resolve(ctx, p.Slug); err != nil {
		t.Fatal("expired early", err)
	}
	now = expires
	if _, err := repo.Resolve(ctx, p.Slug); !errors.Is(err, ErrNotFound) {
		t.Fatal("project served after one week", err)
	}
}

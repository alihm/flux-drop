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

func TestRaftProjectLifecycle(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	backend := &testmetadata.Backend{}
	store := &metadata.Store{Backend: backend}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
	token, view, err := sessions.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a, err := ActorFrom(token, view)
	if err != nil {
		t.Fatal(err)
	}
	repo := &RaftRepository{Store: store, Now: func() time.Time { return now }}
	prepared, err := repo.Reserve(ctx, a, Reservation{Key: "operation_one", Name: "hello", Digest: strings.Repeat("a", 64), Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	p, err := repo.Activate(ctx, a, prepared.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if p.Owner.ID != a.AnonymousID || p.ChargedBytes == 0 {
		t.Fatal("private durable fields lost", p)
	}
	list, _, err := repo.ListOwned(ctx, a, "", 100)
	if err != nil || len(list) != 1 {
		t.Fatal(list, err)
	}
	_, err = repo.Reserve(ctx, a, Reservation{Key: "operation_two", Name: "other", Digest: p.ActiveDigest, Bytes: 10})
	var duplicate *Duplicate
	if !errors.As(err, &duplicate) || duplicate.Slug != p.Slug {
		t.Fatal("duplicate detection", err)
	}
	oldSlug := p.Slug
	p, err = repo.Rename(ctx, a, p.ID, "renamed", p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if resolved, e := repo.Resolve(ctx, oldSlug); e != nil || resolved.Slug != p.Slug {
		t.Fatal("alias", resolved, e)
	}
	p, err = repo.SetPrivacy(ctx, a, p.ID, p.Revision, strings.Repeat("b", 64), p.PolicyRevision+1)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := repo.BeginUnlock(ctx, a, p.Slug)
	if err != nil || verified.PasswordDigest != p.PasswordDigest {
		t.Fatal("password metadata", verified, err)
	}
	grantToken, _, err := sessions.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	grantDigest, _ := session.Digest(grantToken)
	if _, err = repo.CompleteUnlock(ctx, a, verified, grantDigest); err != nil {
		t.Fatal(err)
	}
	if err = repo.ValidateGrant(ctx, a, p.ID, grantToken); err != nil {
		t.Fatal(err)
	}
	p, err = repo.SetPrivacy(ctx, a, p.ID, p.Revision, strings.Repeat("c", 64), p.PolicyRevision+1)
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.ValidateGrant(ctx, a, p.ID, grantToken); !errors.Is(err, ErrUnlockDenied) {
		t.Fatal("stale password grant accepted", err)
	}
	// Simulate a verified, bounded Google login in the same metadata transaction
	// store (signature verification is independently tested in session).
	err = store.Run(ctx, func(tx *metadata.Tx) error {
		var r session.Record
		if e := tx.Get("sessions/"+a.SessionDigest, &r); e != nil {
			return e
		}
		r.UID = "google-user"
		r.AuthUntil = now.Add(time.Hour)
		return tx.Set("sessions/"+a.SessionDigest, r)
	})
	if err != nil {
		t.Fatal(err)
	}
	a.UID = "google-user"
	p, err = repo.Claim(ctx, a, p.ID, p.Revision)
	if err != nil || p.ExpiresAt != nil {
		t.Fatal(p, err)
	}
	list, _, err = repo.ListOwned(ctx, a, "", 100)
	if err != nil || len(list) != 1 {
		t.Fatal("claim index", list, err)
	}
	if err = repo.Tombstone(ctx, a, p.ID, p.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Resolve(ctx, oldSlug); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted alias served", err)
	}
	list, _, err = repo.ListOwned(ctx, a, "", 100)
	if err != nil || len(list) != 0 {
		t.Fatal("deleted index", list, err)
	}
	backend.Failure = errors.New("quorum unavailable")
	if _, err = repo.Resolve(ctx, oldSlug); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatal("quorum loss misreported", err)
	}
}

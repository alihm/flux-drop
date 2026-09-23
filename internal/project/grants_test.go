package project

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/password"
	"github.com/runonflux/flux-drop/internal/session"
)

func TestFirestoreUnlockGrantLifecycle(t *testing.T) {
	r := testRepo(t)
	now := time.Now().UTC()
	r.Now = func() time.Time { return now }
	a := actor(t, r, "")
	visitor := actor(t, r, "")
	stranger := actor(t, r, "")
	pub := &Publisher{r, t.TempDir()}
	ctx := context.Background()
	p := publish(t, pub, a, "unlock-publish", "private grant page")
	privacy := &PrivacyService{Repository: r, DataRoot: pub.DataRoot, Hasher: password.NewHasher()}
	p, err := privacy.Change(ctx, a, p.ID, p.Revision, true, "a long private password")
	if err != nil {
		t.Fatal(err)
	}
	s := &UnlockService{Repository: r, DataRoot: pub.DataRoot, Hasher: password.NewHasher()}
	if _, _, err := s.Unlock(ctx, visitor, p.Slug, "a wrong private password"); !errors.Is(err, ErrUnlockDenied) {
		t.Fatal("wrong password", err)
	}
	token, g, err := s.Unlock(ctx, visitor, p.Slug, "a long private password")
	if err != nil {
		t.Fatal(err)
	}
	if g.ProjectID != p.ID || g.PolicyRevision != p.PolicyRevision || token == "" {
		t.Fatal("invalid grant")
	}
	if err := r.ValidateGrant(ctx, visitor, p.ID, token); err != nil {
		t.Fatal(err)
	}
	if err := r.ValidateGrant(ctx, stranger, p.ID, token); !errors.Is(err, ErrUnlockDenied) {
		t.Fatal("session binding failed", err)
	}
	if err := r.ValidateGrant(ctx, visitor, strings.Repeat("b", 32), token); !errors.Is(err, ErrUnlockDenied) {
		t.Fatal("project binding failed", err)
	}
	digest, _ := session.Digest(token)
	if _, err := r.ref("grants", digest).Get(ctx); err != nil {
		t.Fatal("missing hashed token record", err)
	}
	if _, err := r.ref("grants", token).Get(ctx); !missing(err) {
		t.Fatal("raw token persisted", err)
	}
	now = now.Add(time.Hour)
	if err := r.ValidateGrant(ctx, visitor, p.ID, token); !errors.Is(err, ErrUnlockDenied) {
		t.Fatal("expired grant accepted", err)
	}
	now = now.Add(-time.Hour)
	verified, err := r.BeginUnlock(ctx, visitor, p.Slug)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := privacy.Change(ctx, a, p.ID, p.Revision, true, "a changed private password")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ValidateGrant(ctx, visitor, p.ID, token); !errors.Is(err, ErrUnlockDenied) {
		t.Fatal("rotation did not revoke", err)
	}
	if _, err := r.CompleteUnlock(ctx, visitor, verified, strings.Repeat("c", 64)); !errors.Is(err, ErrUnlockDenied) {
		t.Fatal("stale verification issued grant", err)
	}
	token, _, err = s.Unlock(ctx, visitor, p.Slug, "a changed private password")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ref("sessions", visitor.SessionDigest).Update(ctx, []firestore.Update{{Path: "revoked", Value: true}}); err != nil {
		t.Fatal(err)
	}
	if err := r.ValidateGrant(ctx, visitor, p.ID, token); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("revoked session retained access", err)
	}
	if _, err := privacy.Change(ctx, a, p.ID, rotated.Revision, false, ""); err != nil {
		t.Fatal(err)
	}
}

func TestFirestoreUnlockSharedBudgets(t *testing.T) {
	r := testRepo(t)
	now := time.Now().UTC()
	r.Now = func() time.Time { return now }
	a := actor(t, r, "")
	pub := &Publisher{r, t.TempDir()}
	ctx := context.Background()
	p := publish(t, pub, a, "budget-publish", "budget page")
	privacy := &PrivacyService{Repository: r, DataRoot: pub.DataRoot, Hasher: password.NewHasher()}
	p, err := privacy.Change(ctx, a, p.ID, p.Revision, true, "a long private password")
	if err != nil {
		t.Fatal(err)
	}
	visitors := make([]Actor, 12)
	for i := range visitors {
		visitors[i] = actor(t, r, "")
	}
	var wg sync.WaitGroup
	results := make(chan error, len(visitors))
	for _, visitor := range visitors {
		wg.Add(1)
		go func(a Actor) { defer wg.Done(); _, err := r.BeginUnlock(ctx, a, p.Slug); results <- err }(visitor)
	}
	wg.Wait()
	close(results)
	ok, limited := 0, 0
	for err := range results {
		if err == nil {
			ok++
		} else if errors.Is(err, ErrUnlockLimited) {
			limited++
		} else {
			t.Fatal(err)
		}
	}
	if ok != 10 || limited != 2 {
		t.Fatal("shared limit bypass", ok, limited)
	}
	now = now.Add(time.Minute)
	if _, err := r.BeginUnlock(ctx, visitors[0], p.Slug); err != nil {
		t.Fatal("budget did not reset", err)
	}
	for i := 0; i < 9; i++ {
		if _, err := r.BeginUnlock(ctx, visitors[0], "unknown-abcdef"); !errors.Is(err, ErrUnlockDenied) {
			t.Fatal(err)
		}
	}
	if _, err := r.BeginUnlock(ctx, visitors[0], "unknown-abcdef"); !errors.Is(err, ErrUnlockLimited) {
		t.Fatal("unknown projects bypassed budget", err)
	}
}

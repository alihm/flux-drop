package project

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

type recordingDurability struct {
	*testmetadata.Backend
	last    kv.Durability
	commits int
}

func (b *recordingDurability) Commit(ctx context.Context, command kv.Transaction) error {
	b.last = command.Durability.Effective()
	b.commits++
	return b.Backend.Commit(ctx, command)
}

func TestProjectDurabilityLifecycle(t *testing.T) {
	ctx := context.Background()
	backend := &recordingDurability{Backend: &testmetadata.Backend{}}
	store := &metadata.Store{Backend: backend}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
	want := func(d kv.Durability, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if backend.last != d {
			t.Fatalf("durability %q, want %q", backend.last, d)
		}
	}
	token, view, err := sessions.Create(ctx)
	want(kv.Replicated, err)
	a, err := ActorFrom(token, view)
	if err != nil {
		t.Fatal(err)
	}
	repo := &RaftRepository{Store: store}
	r, err := repo.Reserve(ctx, a, Reservation{Key: "publish_one", Name: "hello", Digest: strings.Repeat("a", 64), Bytes: 10})
	want(kv.Local, err)
	p, err := repo.Activate(ctx, a, r.Operation.ID)
	want(kv.Local, err)
	p, err = repo.SetPrivacy(ctx, a, p.ID, p.Revision, strings.Repeat("b", 64), p.PolicyRevision+1)
	want(kv.Replicated, err)
	r, err = repo.Reserve(ctx, a, Reservation{Key: "update_one", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: strings.Repeat("c", 64), Bytes: 20})
	want(kv.Local, err)
	p, err = repo.Activate(ctx, a, r.Operation.ID)
	want(kv.Local, err)
	if !p.Private || p.PasswordDigest != strings.Repeat("b", 64) {
		t.Fatal("content update changed policy")
	}
	p, err = repo.Rename(ctx, a, p.ID, "renamed", p.Revision)
	want(kv.Replicated, err)
	err = store.Run(ctx, func(tx *metadata.Tx) error {
		var rec session.Record
		if err := tx.Get("sessions/"+a.SessionDigest, &rec); err != nil {
			return err
		}
		rec.UID = "google-user"
		rec.AuthUntil = time.Now().Add(time.Hour)
		return tx.Set("sessions/"+a.SessionDigest, rec)
	})
	want(kv.Replicated, err)
	a.UID = "google-user"
	p, err = repo.Claim(ctx, a, p.ID, p.Revision)
	want(kv.Replicated, err)
	want(kv.Replicated, repo.Tombstone(ctx, a, p.ID, p.Revision))
	_, _, err = sessions.Logout(ctx, token, view.Record.CSRF)
	want(kv.Replicated, err)
}

func TestContentTransactionPromotesPolicyChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Project)
	}{
		{"owner", func(p *Project) { p.Owner.ID = "different" }},
		{"owner-key", func(p *Project) { p.OwnerKey = "different" }},
		{"privacy", func(p *Project) { p.Private = true }},
		{"password", func(p *Project) { p.PasswordDigest = "different" }},
		{"password-revision", func(p *Project) { p.PasswordRevision++ }},
		{"policy", func(p *Project) { p.PolicyRevision++ }},
		{"delete", func(p *Project) { p.Status = "deleted" }},
		{"expiry", func(p *Project) { p.ExpiresAt = nil }},
		{"rename", func(p *Project) { p.Slug = "different" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			b := &recordingDurability{Backend: &testmetadata.Backend{}}
			s := &metadata.Store{Backend: b}
			repo := &RaftRepository{Store: s}
			expires := time.Now().Add(time.Hour)
			p := Project{ID: "one", Owner: Owner{"anonymous", "owner"}, OwnerKey: "owner", Slug: "original", Status: "active", ExpiresAt: &expires}
			if err := s.Run(ctx, func(tx *metadata.Tx) error { return tx.Set("projects/one", p) }); err != nil {
				t.Fatal(err)
			}
			err := repo.runContent(ctx, func(_ context.Context, tx *raftTx) error {
				next := p
				next.ActiveDigest = "updated-content"
				tc.change(&next)
				return tx.Set("projects/one", next)
			})
			if err != nil {
				t.Fatal(err)
			}
			if b.last != kv.Replicated {
				t.Fatal("policy change used content acknowledgement")
			}
		})
	}
}

package project

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func namingRepository(t *testing.T, firestore bool) (Repository, Actor, Actor) {
	t.Helper()
	if firestore {
		repo := testRepo(t)
		return repo, actor(t, repo, "owner"), actor(t, repo, "other")
	}
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
	makeActor := func() Actor {
		token, view, err := sessions.Create(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		a, err := ActorFrom(token, view)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	return &RaftRepository{Store: store}, makeActor(), makeActor()
}

func TestNamingAndWatermarkTransactions(t *testing.T) {
	for _, firestore := range []bool{false, true} {
		label := "raft"
		if firestore {
			label = "firestore"
		}
		t.Run(label, func(t *testing.T) {
			repo, a, other := namingRepository(t, firestore)
			ctx := context.Background()
			publish := func(key, name, digest string) Project {
				t.Helper()
				prepared, err := repo.Reserve(ctx, a, Reservation{Key: key, Name: name, Digest: digest, Bytes: 10})
				if err != nil {
					t.Fatal(err)
				}
				p, err := repo.Activate(ctx, a, prepared.Operation.ID)
				if err != nil {
					t.Fatal(err)
				}
				return p
			}
			first := publish("first_name", "my-site", "abcdef"+strings.Repeat("a", 58))
			if first.Slug != "my-site" || first.InitialSuffix != "" || first.WatermarkDisabled {
				t.Fatal(first)
			}
			second := publish("second_name", "my-site", "abcdef"+strings.Repeat("b", 58))
			third := publish("third_name", "my-site", "abcdef"+strings.Repeat("c", 58))
			if second.Slug != "my-site-abcdef" || third.Slug != "my-site-abcdefcccc" {
				t.Fatal("collision fallback", second.Slug, third.Slug)
			}
			replay, err := repo.Reserve(ctx, a, Reservation{Key: "second_name", Name: "my-site", Digest: second.ActiveDigest, Bytes: 10})
			if err != nil || replay.Project.ID != second.ID || replay.Project.Slug != second.Slug {
				t.Fatal("retry changed name", replay, err)
			}
			renamed, err := repo.Rename(ctx, a, first.ID, "clean-abcdef", first.Revision)
			if err != nil || renamed.Slug != "clean-abcdef" || renamed.InitialSuffix != "" {
				t.Fatal(renamed, err)
			}
			if alias, err := repo.Resolve(ctx, first.Slug); err != nil || alias.ID != first.ID {
				t.Fatal("alias lost", err)
			}
			if _, err := repo.SetWatermark(ctx, other, first.ID, renamed.Revision, false); !errors.Is(err, ErrNotFound) {
				t.Fatal("owner check", err)
			}
			disabled, err := repo.SetWatermark(ctx, a, first.ID, renamed.Revision, false)
			if err != nil || !disabled.WatermarkDisabled || disabled.Revision != renamed.Revision+1 || disabled.PolicyRevision != renamed.PolicyRevision || disabled.ActiveDigest != first.ActiveDigest || disabled.ChargedBytes != first.ChargedBytes {
				t.Fatal(disabled, err)
			}
			if _, err := repo.SetWatermark(ctx, a, first.ID, renamed.Revision, true); !errors.Is(err, ErrConflict) {
				t.Fatal("stale revision", err)
			}
			unchanged, err := repo.SetWatermark(ctx, a, first.ID, disabled.Revision, false)
			if err != nil || unchanged.Revision != disabled.Revision {
				t.Fatal("idempotent setting", unchanged, err)
			}
			update, err := repo.Reserve(ctx, a, Reservation{Key: "updated_site", ProjectID: first.ID, ExpectedRevision: disabled.Revision, Digest: strings.Repeat("d", 64), Bytes: 20})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.SetWatermark(ctx, a, first.ID, disabled.Revision, true); !errors.Is(err, ErrConflict) {
				t.Fatal("changed while upload pending", err)
			}
			updated, err := repo.Activate(ctx, a, update.Operation.ID)
			if err != nil || !updated.WatermarkDisabled {
				t.Fatal("upload lost preference", updated, err)
			}
			if err := repo.Tombstone(ctx, a, first.ID, updated.Revision); err != nil {
				t.Fatal(err)
			}
			replacement := publish("deleted_name", "clean-abcdef", strings.Repeat("e", 64))
			if replacement.Slug == "clean-abcdef" {
				t.Fatal("deleted URL hijacked")
			}
		})
	}
}

func TestConcurrentNameReservation(t *testing.T) {
	for _, firestore := range []bool{false, true} {
		label := "raft"
		if firestore {
			label = "firestore"
		}
		t.Run(label, func(t *testing.T) {
			repo, a, other := namingRepository(t, firestore)
			var wg sync.WaitGroup
			results := make(chan Prepared, 2)
			start := make(chan struct{})
			for i, actor := range []Actor{a, other} {
				wg.Add(1)
				go func(i int, a Actor) {
					defer wg.Done()
					<-start
					digest := strings.Repeat("a", 64)
					if i == 1 {
						digest = strings.Repeat("b", 64)
					}
					p, err := repo.Reserve(context.Background(), a, Reservation{Key: "concurrent_name", Name: "shared", Digest: digest, Bytes: 10})
					if err != nil {
						t.Error(err)
						return
					}
					results <- p
				}(i, actor)
			}
			close(start)
			wg.Wait()
			close(results)
			slugs := map[string]bool{}
			for p := range results {
				slugs[p.Project.Slug] = true
			}
			if len(slugs) != 2 || !slugs["shared"] {
				t.Fatal("name was not reserved atomically", slugs)
			}
		})
	}
}

func TestReservedRoutesGetSuffix(t *testing.T) {
	for _, name := range []string{"api", "admin", "agents", "unlock", "healthz", "readyz"} {
		slug, _, err := chooseSlug(name, strings.Repeat("a", 64), "id", func(string) (string, error) { return "", nil })
		if err != nil || slug != name+"-aaaaaa" {
			t.Fatal(name, slug, err)
		}
	}
}

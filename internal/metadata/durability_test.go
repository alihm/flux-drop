package metadata

import (
	"context"
	"errors"
	"testing"

	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

type durabilityBackend struct {
	*testmetadata.Backend
	commands []kv.Transaction
	fail     error
}

func (b *durabilityBackend) Commit(ctx context.Context, command kv.Transaction) error {
	b.commands = append(b.commands, command)
	if b.fail != nil {
		return b.fail
	}
	return b.Backend.Commit(ctx, command)
}

func TestDurabilityDefaultsAndMixedWrites(t *testing.T) {
	for _, tc := range []struct {
		name           string
		content        bool
		key            string
		deleted, force bool
		want           kv.Durability
	}{
		{"default-content", false, "projects/one", false, false, kv.Replicated},
		{"content-opt-in", true, "projects/one", false, false, kv.Local},
		{"mixed-session", true, "sessions/one", false, false, kv.Replicated},
		{"mixed-grant", true, "grants/one", false, false, kv.Replicated},
		{"unknown-collection", true, "future_policy/one", false, false, kv.Replicated},
		{"project-deletion", true, "projects/one", true, false, kv.Replicated},
		{"digest-retirement", true, "digests/one", true, false, kv.Local},
		{"explicit-promotion", true, "projects/one", false, true, kv.Replicated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &durabilityBackend{Backend: &testmetadata.Backend{}}
			store := &Store{Backend: backend}
			run := store.Run
			if tc.content {
				run = store.RunContent
			}
			err := run(context.Background(), func(tx *Tx) error {
				if tc.force {
					tx.RequireReplication()
				}
				if err := tx.Set("operations/one", "pending"); err != nil {
					return err
				}
				if tc.deleted {
					return tx.Delete(tc.key)
				}
				if err := tx.Set(tc.key, "value"); err != nil {
					return err
				}
				// Later content writes cannot downgrade a promoted transaction.
				return tx.Set("operations/one", "complete")
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(backend.commands) != 1 || backend.commands[0].Durability.Effective() != tc.want {
				t.Fatal(backend.commands)
			}
		})
	}
}

func TestContentCommitUnknownOutcomeNotReplayed(t *testing.T) {
	b := &durabilityBackend{Backend: &testmetadata.Backend{}, fail: context.DeadlineExceeded}
	s := &Store{Backend: b}
	err := s.RunContent(context.Background(), func(tx *Tx) error { return tx.Set("operations/one", "value") })
	if !errors.Is(err, context.DeadlineExceeded) || len(b.commands) != 1 {
		t.Fatal("indeterminate content commit replayed", err, len(b.commands))
	}
}

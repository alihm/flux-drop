package httpserver

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

type agentStartupBackend struct {
	metadata.Backend
	readFailures int
	ambiguous    bool
	writes       int
}

func (b *agentStartupBackend) Read(ctx context.Context, keys []string) (map[string]kv.Record, error) {
	if b.readFailures > 0 {
		b.readFailures--
		return nil, kv.ErrNotLeader
	}
	return b.Backend.Read(ctx, keys)
}

func (b *agentStartupBackend) Commit(ctx context.Context, tx kv.Transaction) error {
	if err := b.Backend.Commit(ctx, tx); err != nil {
		return err
	}
	b.writes++
	if b.ambiguous {
		b.ambiguous = false
		return context.DeadlineExceeded
	}
	return nil
}

func TestAgentEncryptionKeyWaitsForCoordinator(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		name := "coordinator startup"
		if ambiguous {
			name = "committed initialization with lost response"
		}
		t.Run(name, func(t *testing.T) {
			backend := &agentStartupBackend{Backend: &testmetadata.Backend{}, readFailures: 2, ambiguous: ambiguous}
			repo := &project.RaftRepository{Store: &metadata.Store{Backend: backend}}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			first, err := agentPrivateEncryptionKey(ctx, repo)
			if err != nil || len(first) != 32 {
				t.Fatal("initialization did not recover", len(first), err)
			}
			second, err := agentPrivateEncryptionKey(ctx, repo)
			if err != nil || !bytes.Equal(first, second) || backend.writes != 1 {
				t.Fatal("initialization overwrote the shared key", backend.writes, err)
			}
		})
	}
}

func TestAgentEncryptionKeyInitializationCancellation(t *testing.T) {
	backend := &agentStartupBackend{Backend: &testmetadata.Backend{}, readFailures: 1000}
	repo := &project.RaftRepository{Store: &metadata.Store{Backend: backend}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	key, err := agentPrivateEncryptionKey(ctx, repo)
	if !errors.Is(err, context.DeadlineExceeded) || key != nil || backend.writes != 0 {
		t.Fatal("unavailable coordinator must fail closed on cancellation", key, err)
	}
}

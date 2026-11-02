package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/runonflux/flux-drop/internal/metadata"
)

type plainReads struct{ metadata.Backend }

func TestTransactionReadPerformanceAndRevocation(t *testing.T) {
	g, n, s := newReadTestStore(t)
	ctx := context.Background()
	for _, row := range []struct {
		name    string
		backend metadata.Backend
	}{{"per-key-fences", plainReads{n}}, {"transaction-fence", n}} {
		store := &metadata.Store{Backend: row.backend}
		if err := store.Run(ctx, readPublicTestRecords); err != nil {
			t.Fatalf("%s: %v", row.name, err)
		}
	}
	for _, policy := range []string{"private", "deleted", "renamed"} {
		calls := 0
		denied := errors.New("policy changed")
		if err := s.Run(ctx, func(tx *metadata.Tx) error { return tx.Set("tests/a", "public") }); err != nil {
			t.Fatal(err)
		}
		err := s.Run(ctx, func(tx *metadata.Tx) error {
			calls++
			var value string
			if err := tx.Get("tests/a", &value); err != nil {
				return err
			}
			if value != "public" {
				return denied
			}
			return s.Run(ctx, func(other *metadata.Tx) error { return other.Set("tests/a", policy) })
		})
		if !errors.Is(err, denied) || calls != 2 {
			t.Fatalf("%s exposed stale policy: %d %v", policy, calls, err)
		}
	}
	// Subsequent keys and finish MUST NOT renew the initial fence.
	b := n.TransactionBackend()
	if _, err := b.Read(ctx, []string{"tests/a"}); err != nil {
		t.Fatal(err)
	}
	n.readMu.Lock()
	proof := n.readLease
	n.readMu.Unlock()
	if _, err := b.Read(ctx, []string{"tests/b"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Check(ctx, nil); err != nil {
		t.Fatal(err)
	}
	n.readMu.Lock()
	same := proof == n.readLease
	n.readMu.Unlock()
	if !same {
		t.Fatal("transaction fenced again")
	}
	follower := g.nodes[(g.leader(t, -1)+1)%3]
	if _, _, err := follower.readSnapshot(ctx, []string{"tests/a"}, nil); !errors.Is(err, ErrNotLeader) {
		t.Fatal("follower served stale metadata", err)
	}
}

// The latency comparison belongs in an opt-in benchmark: hundreds of fsynced
// barriers measure runner scheduling and snapshot I/O as well as read cost.
// Correctness tests above retain policy invalidation, fence reuse and refusal
// of follower reads, without making sustained throughput a CI prerequisite.
func BenchmarkTransactionReads(b *testing.B) {
	for _, plain := range []bool{true, false} {
		name := "transaction-fence"
		if plain {
			name = "per-key-fences"
		}
		b.Run(name, func(b *testing.B) {
			_, n, store := newReadTestStore(b)
			ctx := context.Background()
			if plain {
				store = &metadata.Store{Backend: plainReads{n}}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := store.Run(ctx, readPublicTestRecords); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

func readPublicTestRecords(tx *metadata.Tx) error {
	for _, key := range []string{"tests/a", "tests/b", "tests/c"} {
		var value string
		if err := tx.Get(key, &value); err != nil {
			return err
		}
		if value != "public" {
			return errors.New("read returned unexpected test value")
		}
	}
	return nil
}

func newReadTestStore(t testing.TB) (*testGroup, *Node, *metadata.Store) {
	t.Helper()
	// InmemTransport has no heartbeat fast path: its default 500ms RPC
	// timeout and the short generic test election timers mistake slow fsyncs
	// under -race for dead peers. This test exercises read consistency, not
	// failover latency. Allow scheduling/I/O headroom only in this fixture;
	// production authority proofs still expire at the unchanged 750ms bound.
	g := newTestGroupWithTiming(t, false, func(c *raft.Config) {
		tuneTest(c)
		c.HeartbeatTimeout = 3 * time.Second
		c.ElectionTimeout = 3 * time.Second
		c.LeaderLeaseTimeout = 2 * time.Second
	}, 3*time.Second)
	var n *Node
	var store *metadata.Store
	// Leader election and its first fsyncs may overlap. Seed all fixture rows
	// atomically and idempotently before testing transaction semantics; only
	// this setup is retried, never the revocation or follower-read assertions.
	// Match the longer election timers without changing other tests' deadlines.
	awaitFor(t, 20*time.Second, func() bool {
		for _, candidate := range g.nodes {
			if candidate.raft.State() != raft.Leader {
				continue
			}
			n = candidate
			store = &metadata.Store{Backend: n}
			if err := store.Run(context.Background(), func(tx *metadata.Tx) error {
				for _, key := range []string{"tests/a", "tests/b", "tests/c"} {
					if err := tx.Set(key, "public"); err != nil {
						return err
					}
				}
				return nil
			}); err == nil {
				return true
			}
		}
		return false
	})
	return g, n, store
}

package cluster

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
)

type plainReads struct{ metadata.Backend }

func TestTransactionReadPerformanceAndRevocation(t *testing.T) {
	g := newTestGroup(t)
	n := g.nodes[g.leader(t, -1)]
	ctx := context.Background()
	s := &metadata.Store{Backend: n}
	for _, key := range []string{"tests/a", "tests/b", "tests/c"} {
		if err := s.Run(ctx, func(tx *metadata.Tx) error { return tx.Set(key, "public") }); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []struct {
		name    string
		backend metadata.Backend
	}{{"per-key-fences", plainReads{n}}, {"transaction-fence", n}} {
		values := []time.Duration{}
		store := &metadata.Store{Backend: row.backend}
		for i := 0; i < 100; i++ {
			start := time.Now()
			if err := store.Run(ctx, func(tx *metadata.Tx) error {
				for _, key := range []string{"tests/a", "tests/b", "tests/c"} {
					var value string
					if err := tx.Get(key, &value); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			values = append(values, time.Since(start))
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		t.Logf("%s p50=%s p95=%s p99=%s", row.name, values[50], values[95], values[99])
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

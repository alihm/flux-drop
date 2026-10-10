package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
)

func TestLocalFollowerSnapshotSurvivesIsolationAndDetectsRestore(t *testing.T) {
	g := newTestGroup(t)
	leader := g.leader(t, -1)
	n := g.nodes[(leader+1)%3]
	ctx := context.Background()
	s := &metadata.Store{Backend: g.nodes[leader]}
	if e := s.Run(ctx, func(tx *metadata.Tx) error { return tx.Set("tests/local", "public") }); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		r, _, e := n.localSnapshot(ctx, []string{"tests/local"}, nil)
		if e == nil && r["tests/local"].Version != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not replicated", e)
		}
		time.Sleep(10 * time.Millisecond)
	}
	g.isolate((leader + 1) % 3)
	r, stamp, e := n.localSnapshot(ctx, []string{"tests/local"}, nil)
	if e != nil || r["tests/local"].Version == 0 {
		t.Fatal(r, e)
	}
	// No LastContact/age gate exists. The applied snapshot can serve forever.
	n.readMu.Lock()
	n.readLease = time.Time{}
	n.readMu.Unlock()
	if _, _, e := n.localSnapshot(ctx, []string{"tests/local"}, stamp); e != nil {
		t.Fatal("isolated follower consulted authority", e)
	}
	n.state.mu.Lock()
	n.state.generation++
	n.state.mu.Unlock()
	if _, _, e := n.localSnapshot(ctx, []string{"tests/local"}, stamp); !errors.Is(e, ErrConflict) {
		t.Fatal("restore stamp reused", e)
	}
	if _, _, e := n.readSnapshot(ctx, []string{"tests/local"}, nil); !errors.Is(e, ErrNotLeader) {
		t.Fatal("management used follower", e)
	}
}

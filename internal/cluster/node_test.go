package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

type testGroup struct {
	nodes      []*Node
	transports []*raft.InmemTransport
	configs    []Config
}

func tuneTest(c *raft.Config) {
	// Leave room for race-detector/Argon2 scheduling and fsync. The previous
	// 75ms lease caused unrelated CAS tests to lose their leader under load.
	c.HeartbeatTimeout = 750 * time.Millisecond
	c.ElectionTimeout = 750 * time.Millisecond
	c.LeaderLeaseTimeout = 500 * time.Millisecond
	c.CommitTimeout = 10 * time.Millisecond
	c.SnapshotThreshold = 32
	c.TrailingLogs = 8
}

func newTestGroup(t *testing.T) *testGroup {
	return newTestGroupMode(t, false)
}

func newTestGroupMode(t *testing.T, async bool) *testGroup {
	t.Helper()
	g := &testGroup{}
	initial := []Member{{ID: "one", Address: "127.0.0.1:18001"}, {ID: "two", Address: "127.0.0.1:18002"}, {ID: "three", Address: "127.0.0.1:18003"}}
	for _, m := range initial {
		_, transport := raft.NewInmemTransport(raft.ServerAddress(m.raftAddress()))
		g.transports = append(g.transports, transport)
		root := t.TempDir()
		if err := os.Chmod(root, 0700); err != nil {
			t.Fatal(err)
		}
		g.configs = append(g.configs, Config{ClusterID: testClusterID, Local: m, StateDir: root, ContentDir: t.TempDir(), InitialVoters: initial, AsyncContent: async})
	}
	for i, transport := range g.transports {
		for j, other := range g.transports {
			if i != j {
				transport.Connect(other.LocalAddr(), other)
			}
		}
	}
	for i, c := range g.configs {
		tune := tuneTest
		if async {
			tune = func(c *raft.Config) { c.SnapshotThreshold = 32; c.TrailingLogs = 8 }
		}
		node, err := start(c, g.transports[i], tune)
		if err != nil {
			t.Fatal(err)
		}
		g.nodes = append(g.nodes, node)
	}
	t.Cleanup(func() {
		for _, n := range g.nodes {
			_ = n.Close()
		}
	})
	return g
}

func await(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not converge")
}

func (g *testGroup) leader(t *testing.T, exclude int) int {
	t.Helper()
	found := -1
	await(t, func() bool {
		for i, n := range g.nodes {
			if i != exclude && n.raft.State() == raft.Leader {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				err := n.Ready(ctx)
				cancel()
				if err == nil {
					found = i
					return true
				}
			}
		}
		return false
	})
	return found
}

func (g *testGroup) isolate(index int) {
	g.transports[index].DisconnectAll()
	for i, tr := range g.transports {
		if i != index {
			tr.Disconnect(g.transports[index].LocalAddr())
		}
	}
}
func (g *testGroup) connect(index int) {
	for i, tr := range g.transports {
		if i != index {
			tr.Connect(g.transports[index].LocalAddr(), g.transports[index])
			g.transports[index].Connect(tr.LocalAddr(), tr)
		}
	}
}

func TestQuorumFailoverRejectsIsolatedOldLeader(t *testing.T) {
	g := newTestGroup(t)
	leader := g.leader(t, -1)
	ctx := context.Background()
	if err := g.nodes[leader].Commit(ctx, transaction("sessions/session1", 0, `{"revoked":false}`)); err != nil {
		t.Fatal(err)
	}
	before, err := g.nodes[leader].Read(ctx, []string{"sessions/session1"})
	if err != nil {
		t.Fatal(err)
	}
	g.isolate(leader)
	// Immediately after the partition the old node may still report Leader.
	// The read fence must prove quorum instead of trusting that role string.
	short, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	if _, err := g.nodes[leader].Read(short, []string{"sessions/session1"}); err == nil {
		t.Fatal("isolated leader authorized stale state")
	}
	cancel()
	replacement := g.leader(t, leader)
	if err := g.nodes[replacement].Commit(ctx, transaction("sessions/session1", before["sessions/session1"].Version, `{"revoked":true}`)); err != nil {
		t.Fatal(err)
	}
	short, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
	if err := g.nodes[leader].Commit(short, transaction("projects/rogue", 0, `{}`)); err == nil {
		t.Fatal("isolated old leader committed")
	}
	cancel()
	g.connect(leader)
	await(t, func() bool {
		return string(g.nodes[leader].state.read([]string{"sessions/session1"})["sessions/session1"].Value) == `{"revoked":true}`
	})
	current := g.leader(t, -1)
	if data, err := g.nodes[current].Read(ctx, []string{"projects/rogue"}); err != nil || len(data) != 0 {
		t.Fatal("uncommitted minority state survived", data, err)
	}
	// Losing every link must not trigger an automatic singleton bootstrap.
	for i := range g.nodes {
		g.isolate(i)
	}
	for _, n := range g.nodes {
		short, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		if err := n.Ready(short); err == nil {
			t.Fatal("ready without majority")
		}
		cancel()
	}
}

func TestConcurrentCASHasOneWinner(t *testing.T) {
	g := newTestGroup(t)
	n := g.nodes[g.leader(t, -1)]
	var wg sync.WaitGroup
	results := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results <- n.Commit(context.Background(), transaction("slugs/same", 0, fmt.Sprintf(`{"owner":%d}`, i)))
		}(i)
	}
	wg.Wait()
	close(results)
	won := 0
	for err := range results {
		if err == nil {
			won++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if won != 1 {
		t.Fatal("reservation conflict", won)
	}
}

func TestDurableRestartAndSnapshot(t *testing.T) {
	g := newTestGroup(t)
	leader := g.leader(t, -1)
	for i := 0; i < 40; i++ {
		if err := g.nodes[leader].Commit(context.Background(), transaction(fmt.Sprintf("projects/p%d", i), 0, `{"private":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.nodes[leader].raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	for _, n := range g.nodes {
		if err := n.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i, c := range g.configs {
		_, tr := raft.NewInmemTransport(raft.ServerAddress(c.Local.raftAddress()))
		g.transports[i] = tr
	}
	for i := range g.nodes {
		g.connect(i)
	}
	for i, c := range g.configs {
		n, err := start(c, g.transports[i], tuneTest)
		if err != nil {
			t.Fatal(err)
		}
		g.nodes[i] = n
	}
	n := g.nodes[g.leader(t, -1)]
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("projects/p%d", i)
		records, err := n.Read(context.Background(), []string{key})
		if err != nil || string(records[key].Value) != `{"private":true}` {
			t.Fatal(key, records, err)
		}
	}
	info, err := os.Stat(filepath.Join(n.config.StateDir, "raft.db"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("unsafe log permissions", err)
	}
}

func TestStorageRejectsReplicatedOrForeignIdentity(t *testing.T) {
	root := t.TempDir()
	_ = os.Chmod(root, 0700)
	c := Config{ClusterID: testClusterID, Local: Member{ID: "one", Address: "127.0.0.1:18001"}, StateDir: root, ContentDir: root}
	if err := prepareDirectory(c); err == nil {
		t.Fatal("replicated state allowed")
	}
	c.ContentDir = t.TempDir()
	if err := bindIdentity(c, false); err != nil {
		t.Fatal(err)
	}
	c.Local.ID = "two"
	if err := bindIdentity(c, true); err == nil {
		t.Fatal("copied identity accepted")
	}
	c.Local.ID = "one"
	c.ClusterID = strings.Repeat("b", 32)
	if err := bindIdentity(c, true); err == nil {
		t.Fatal("foreign cluster state accepted")
	}
	c.ClusterID = testClusterID
	c.InitialVoters = []Member{c.Local}
	if err := bindIdentity(c, false); err == nil {
		t.Fatal("lost history rebootstrap allowed")
	}
	if err := c.validate(); err == nil {
		t.Fatal("singleton bootstrap allowed")
	}
}

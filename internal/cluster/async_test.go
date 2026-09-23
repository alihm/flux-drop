package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

func contentTransaction(t *testing.T, id string, version uint64, p project.Project) Transaction {
	t.Helper()
	data, err := metadata.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	return Transaction{Schema: 1, Durability: kv.Local, Checks: []Check{{Key: "projects/" + id, Version: version}}, Writes: []Write{{Key: "projects/" + id, Value: data}}}
}
func awaitAsync(t *testing.T, n *Node) {
	t.Helper()
	await(t, func() bool { _, err := n.Read(context.Background(), []string{"health/ready"}); return err == nil })
}

func TestAsyncAcknowledgementAndSecurityBarrier(t *testing.T) {
	g := newTestGroupMode(t, true)
	leader := g.leader(t, -1)
	n := g.nodes[leader]
	awaitAsync(t, n)
	ctx := context.Background()
	p := project.Project{ID: "one", Status: "reserved", PolicyRevision: 1}
	if err := n.Commit(ctx, contentTransaction(t, p.ID, 0, p)); err != nil {
		t.Fatal(err)
	}
	r, err := n.Read(ctx, []string{"projects/one"})
	if err != nil {
		t.Fatal(err)
	}
	if r["projects/one"].Version == 0 {
		t.Fatal("local acknowledgement not readable")
	}
	p.Private = true
	p.PolicyRevision++
	// Deliberately mislabeled: the coordinator must upgrade this policy write.
	if err := n.Commit(ctx, contentTransaction(t, p.ID, r["projects/one"].Version, p)); err != nil {
		t.Fatal(err)
	}
	g.isolate(leader)
	replacement := g.leader(t, leader)
	awaitAsync(t, g.nodes[replacement])
	r, err = g.nodes[replacement].Read(ctx, []string{"projects/one"})
	if err != nil {
		t.Fatal(err)
	}
	var restored project.Project
	if err := metadata.Decode(r["projects/one"].Value, &restored); err != nil || !restored.Private || restored.PolicyRevision != 2 {
		t.Fatal("acknowledged security barrier lost", restored, err)
	}
	if _, err = n.Read(ctx, []string{"projects/one"}); err == nil {
		t.Fatal("old primary served stale authorization")
	}
}

func TestLocalContentCanBeAcknowledgedBeforeReplication(t *testing.T) {
	g := newTestGroupMode(t, true)
	leader := g.leader(t, -1)
	n := g.nodes[leader]
	awaitAsync(t, n)
	// Immediately after isolation a previously acquired authority window is
	// still valid. Content may succeed locally; security may not succeed.
	g.isolate(leader)
	p := project.Project{ID: "local", Status: "reserved", PolicyRevision: 1}
	start := time.Now()
	if err := n.Commit(context.Background(), contentTransaction(t, p.ID, 0, p)); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) >= authorityWindow {
		t.Fatal("content waited for quorum")
	}
	if n.state.read([]string{"projects/local"})["projects/local"].Version != 0 {
		t.Fatal("isolated content unexpectedly replicated")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.Commit(ctx, transaction("sessions/revoked", 0, `true`)); err == nil {
		t.Fatal("security write acknowledged without quorum")
	}
	replacement := g.leader(t, leader)
	awaitAsync(t, g.nodes[replacement])
	r, err := g.nodes[replacement].Read(context.Background(), []string{"projects/local"})
	if err != nil {
		t.Fatal(err)
	}
	if r["projects/local"].Version != 0 {
		t.Fatal("fixture did not exercise allowed content loss")
	}
}

func TestAsyncSnapshotsAndUnknownFields(t *testing.T) {
	g := newTestGroupMode(t, true)
	leader := g.leader(t, -1)
	n := g.nodes[leader]
	awaitAsync(t, n)
	tx := transaction("sessions/one", 0, `{"revoked":true}`)
	tx.Durability = kv.Local // unknown/protected collections never bypass replication
	if err := n.Commit(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	r, err := n.Read(context.Background(), []string{"sessions/one"})
	if err != nil {
		t.Fatal(err)
	}
	if r["sessions/one"].Version < 1<<32 {
		t.Fatal("generation-qualified revision missing")
	}
	if !json.Valid(r["sessions/one"].Value) {
		t.Fatal("invalid durable value")
	}
	tx.Revision = 99
	if err := n.Commit(context.Background(), tx); !errors.Is(err, ErrInvalid) {
		t.Fatal("caller selected revision", err)
	}
	// Exercise state snapshot round trip with versions exceeding Raft indices.
	snapshot, err := n.state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memorySink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}
	restored := newState(testClusterID)
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if restored.read([]string{"sessions/one"})["sessions/one"].Version != r["sessions/one"].Version {
		t.Fatal("snapshot changed version")
	}
}

func TestUncertainSecurityRecoveryWaitsForAllFutures(t *testing.T) {
	g := newTestGroupMode(t, true)
	leader := g.leader(t, -1)
	n := g.nodes[leader]
	awaitAsync(t, n)
	ctx := context.Background()
	if err := n.Commit(ctx, transaction("sessions/one", 0, `false`)); err != nil {
		t.Fatal(err)
	}
	r, err := n.Read(ctx, []string{"sessions/one"})
	if err != nil {
		t.Fatal(err)
	}
	w := n.async
	// Simulate a security future which reached durable consensus after its
	// requester stopped waiting, before updating the speculative view.
	w.mu.Lock()
	w.sequence++
	revision := w.term<<32 | w.sequence
	w.mu.Unlock()
	tx := transaction("sessions/one", r["sessions/one"].Version, `true`)
	tx.Revision = revision
	if err := n.commitRaft(ctx, tx); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failed = true
	w.outstanding = 1
	if err := w.ensure(ctx); !errors.Is(err, ErrNotLeader) {
		t.Fatal("recovered with an unsettled future", err)
	}
	w.outstanding = 0
	if err := w.ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if got := w.view.read([]string{"sessions/one"})["sessions/one"]; string(got.Value) != "true" || got.Version != revision {
		t.Fatal("recovered stale authorization", got)
	}
	if w.sequence != uint64(uint32(revision)) {
		t.Fatal("recovery reset same-term revision counter")
	}
}

func TestAuthorityWindowRejectsClockJumps(t *testing.T) {
	start := time.Now()
	deadline := start.Add(authorityWindow)
	for _, elapsed := range []time.Duration{-time.Second, authorityWindow, 2 * time.Second} {
		if authorityTimeValid(deadline, start.Add(elapsed)) {
			t.Fatal("invalid proof window accepted", elapsed)
		}
	}
	if !authorityTimeValid(deadline, start.Add(time.Millisecond)) {
		t.Fatal("fresh proof rejected")
	}
	// Stripping the monotonic component models a wall timestamp after suspend.
	if authorityTimeValid(deadline, start.Round(0).Add(time.Hour)) {
		t.Fatal("suspended proof accepted")
	}
}

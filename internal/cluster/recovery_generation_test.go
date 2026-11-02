package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestRecoveryGenerationStrandedLearnersResetAndRejoin(t *testing.T) {
	h := newChurnCluster(t)
	leader := h.leader()
	if err := h.step(); err != nil {
		t.Fatal(err)
	}
	learners := []*churnNode{h.add(nil), h.add(nil)}
	for _, learner := range learners {
		if err := h.enroll(learner); err != nil {
			t.Fatal(err)
		}
	}
	if err := leader.node.Commit(context.Background(), transaction("sessions/generation", 0, `{"uid":"owner"}`)); err != nil {
		t.Fatal(err)
	}
	for _, learner := range learners {
		await(t, func() bool { return learner.node.state.appliedIndex() >= leader.node.state.appliedIndex() })
	}
	for _, node := range h.nodes[:3] {
		if node != leader {
			h.stop(node)
		}
	}
	h.stop(leader)
	// A stored exact-index request is the only route to forced recovery.
	store := recoveryReadStore(t, leader.config)
	last, err := store.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	raw, _ := json.Marshal(singletonRequest{ClusterID: testClusterID, NodeID: leader.config.Local.ID, ExpectedLastIndex: last})
	if err := os.WriteFile(filepath.Join(leader.config.StateDir, "recover-singleton.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	h.start(leader)
	await(t, func() bool { return leader.node.raft.State() == raft.Leader && !leader.node.generationPending.Load() })
	generation, err := leader.node.recoveryGeneration()
	if err != nil || generation.Generation == 0 {
		t.Fatal(generation, err)
	}
	if !documentKey.MatchString(recoveryGenerationKey) {
		t.Fatal("generation key invalid")
	}
	// No singleton security write bypass was added.
	if err := leader.node.Commit(context.Background(), transaction("sessions/blocked-generation", 0, `true`)); err == nil {
		t.Fatal("singleton security write accepted")
	}
	for _, learner := range learners {
		await(t, func() bool { _, id := learner.node.raft.LeaderWithID(); return id == "" })
		leader.observer.Refresh(context.Background())
		learner.observer.Refresh(context.Background())
		peers, _ := learner.observer.Snapshot()
		var observed Observation
		for _, p := range peers {
			if p.Status.ID == leader.config.Local.ID {
				observed = p
			}
		}
		reply, err := learner.enrollment.exchangeRejoin(context.Background(), observed)
		if err != nil || reply.Result != "reset_required" {
			t.Fatal("stale history accepted", reply, err)
		}
		if !learner.enrollment.mayReset(context.Background()) {
			t.Fatal("stranded learner cannot reset")
		}
		h.stop(learner)
		if err := archiveSuperseded(learner.config, generation); err != nil {
			t.Fatal(err)
		}
		archives, _ := filepath.Glob(filepath.Join(learner.config.StateDir, "superseded-*"))
		if len(archives) != 1 {
			t.Fatal(archives)
		}
		if _, err := os.Stat(filepath.Join(archives[0], "raft.db")); err != nil {
			t.Fatal(err)
		}
		h.start(learner)
		if err := h.enroll(learner); err != nil {
			t.Fatal("empty reset node did not enroll", err)
		}
		await(t, func() bool { g, _ := learner.node.recoveryGeneration(); return g == generation })
	}
	h.awaitVoters(3)
	if err := leader.node.Commit(context.Background(), transaction("sessions/after-generation", 0, `true`)); err != nil {
		t.Fatal(err)
	}
	for _, learner := range learners {
		await(t, func() bool {
			return learner.node.state.read([]string{"sessions/after-generation"})["sessions/after-generation"].Version != 0
		})
		if learner.node.state.read([]string{"sessions/generation"})["sessions/generation"].Version == 0 {
			t.Fatal("metadata lost")
		}
	}
	// A normal restart must not create a second generation.
	h.stop(leader)
	h.start(leader)
	await(t, func() bool { g, _ := leader.node.recoveryGeneration(); return g == generation })
	time.Sleep(150 * time.Millisecond)
	if g, _ := leader.node.recoveryGeneration(); g != generation {
		t.Fatal("generation changed on normal restart")
	}
}

func TestRecoveryGenerationDiscardsLearnerUncommittedTail(t *testing.T) {
	h := newChurnCluster(t)
	leader := h.leader()
	h.step()
	learner := h.add(nil)
	if err := h.enroll(learner); err != nil {
		t.Fatal(err)
	}
	var survivor *churnNode
	for _, node := range h.nodes[:3] {
		if node != leader {
			if survivor == nil {
				survivor = node
			}
			h.stop(node)
		}
	}
	// Submit through the elected Raft leader's real Apply path while it is
	// losing quorum. The learner receives the tail, but no majority commits it.
	tail, _ := json.Marshal(transaction("cluster_trace/uncommitted-tail", 0, `true`))
	future := leader.node.raft.Apply(tail, 300*time.Millisecond)
	if err := future.Error(); err == nil {
		t.Fatal("minority unexpectedly committed tail")
	}
	target := future.Index()
	await(t, func() bool { last, _ := learner.node.store.LastIndex(); return last >= target && target > 0 })
	if learner.node.state.read([]string{"cluster_trace/uncommitted-tail"})["cluster_trace/uncommitted-tail"].Version != 0 {
		t.Fatal("uncommitted tail applied")
	}
	h.stop(leader)
	store := recoveryReadStore(t, survivor.config)
	last, _ := store.LastIndex()
	store.Close()
	raw, _ := json.Marshal(singletonRequest{ClusterID: testClusterID, NodeID: survivor.config.Local.ID, ExpectedLastIndex: last})
	if err := os.WriteFile(filepath.Join(survivor.config.StateDir, "recover-singleton.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	h.start(survivor)
	await(t, func() bool {
		return survivor.node.raft.State() == raft.Leader && !survivor.node.generationPending.Load()
	})
	await(t, func() bool { _, id := learner.node.raft.LeaderWithID(); return id == "" })
	survivor.observer.Refresh(context.Background())
	learner.observer.Refresh(context.Background())
	if err := learner.enrollment.request(context.Background()); err == nil {
		t.Fatal("old history admitted")
	}
	select {
	case reset := <-learner.node.control:
		if !reset {
			t.Fatal("wrong reset")
		}
	default:
		t.Fatal("reset not requested")
	}
	h.stop(learner)
	targetRecovery, _ := survivor.node.recoveryGeneration()
	if err := archiveSuperseded(learner.config, targetRecovery); err != nil {
		t.Fatal(err)
	}
	h.start(learner)
	if err := h.enroll(learner); err != nil {
		t.Fatal(err)
	}
	if learner.node.state.read([]string{"cluster_trace/uncommitted-tail"})["cluster_trace/uncommitted-tail"].Version != 0 {
		t.Fatal("discarded tail resurrected")
	}
}

func TestGenerationZeroCannotCertifyUnretiredAbsentHistory(t *testing.T) {
	h := newChurnCluster(t)
	leader := h.leader()
	h.step()
	node := h.add(nil)
	if err := h.enroll(node); err != nil {
		t.Fatal(err)
	}
	// An absent unretired history could have been stranded by a pre-generation
	// recovery. A generation-zero handshake alone cannot prove its lineage.
	if err := leader.node.RemoveMember(context.Background(), node.config.Local.ID); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { _, id := node.node.raft.LeaderWithID(); return id == "" })
	if err := h.enroll(node); err == nil {
		t.Fatal("uncertified legacy history rejoined")
	}
	c := leader.node.raft.GetConfiguration()
	if err := c.Error(); err != nil {
		t.Fatal(err)
	}
	for _, s := range c.Configuration().Servers {
		if string(s.ID) == node.config.Local.ID {
			t.Fatal("legacy history added")
		}
	}
}

func TestSupersededArchiveRetentionAndSymlinkRefusal(t *testing.T) {
	h := newChurnCluster(t)
	leader := h.leader()
	h.step()
	node := h.add(nil)
	if err := h.enroll(node); err != nil {
		t.Fatal(err)
	}
	notes := filepath.Join(node.config.StateDir, "superseded-notes")
	os.WriteFile(notes, []byte("operator notes"), 0600)
	for round := 0; round < 2; round++ {
		if err := leader.enrollment.retire(context.Background(), node.config.Local); err != nil {
			t.Fatal(err)
		}
		h.stop(node)
		if err := archiveSuperseded(node.config); err != nil {
			t.Fatal(err)
		}
		entries, _ := filepath.Glob(filepath.Join(node.config.StateDir, "superseded-*"))
		directories := 0
		for _, entry := range entries {
			if info, err := os.Lstat(entry); err == nil && info.IsDir() {
				directories++
			}
		}
		if directories != 1 {
			t.Fatal("superseded archives not bounded", entries)
		}
		if raw, err := os.ReadFile(notes); err != nil || string(raw) != "operator notes" {
			t.Fatal("unrelated operator file removed", err)
		}
		if err := secureAutomaticStateTree(node.config.StateDir); err != nil {
			t.Fatal(err)
		}
		h.start(node)
		if err := h.enroll(node); err != nil {
			t.Fatal(err)
		}
	}
	h.stop(node)
	directory := "superseded-" + recoveryTimestamp()
	if err := os.Symlink(t.TempDir(), filepath.Join(node.config.StateDir, directory)); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(resetIntent{ClusterID: testClusterID, NodeID: node.config.Local.ID, Directory: directory, Moving: true})
	os.WriteFile(filepath.Join(node.config.StateDir, "rejoin-reset.json"), raw, 0600)
	before, _ := os.ReadFile(filepath.Join(node.config.StateDir, "raft.db"))
	if err := resumeSuperseded(node.config); err == nil {
		t.Fatal("symlink archive accepted")
	}
	after, _ := os.ReadFile(filepath.Join(node.config.StateDir, "raft.db"))
	if !bytes.Equal(before, after) {
		t.Fatal("refused reset changed Raft state")
	}
}

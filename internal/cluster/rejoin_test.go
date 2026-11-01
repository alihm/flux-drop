package cluster

import (
	"context"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRetiredVoterAndLearnerRejoinWithState(t *testing.T) {
	h := newChurnCluster(t)
	leader := h.leader()
	// Register the bootstrap voters' real keys before the first failure.
	if err := h.step(); err != nil {
		t.Fatal(err)
	}
	spare := h.add(nil)
	if err := h.enroll(spare); err != nil {
		t.Fatal(err)
	}
	var victim *churnNode
	for _, n := range h.nodes[:3] {
		if n != leader {
			victim = n
			break
		}
	}
	for round := 0; round < 2; round++ {
		h.stop(victim)
		h.step()
		h.now = h.now.Add(3 * time.Minute)
		await(t, func() bool {
			h.step()
			c := leader.node.raft.GetConfiguration()
			c.Error()
			for _, s := range c.Configuration().Servers {
				if string(s.ID) == victim.config.Local.ID {
					return false
				}
			}
			return true
		})
		before := leader.node.raft.CurrentTerm()
		id := leader.config.Local.ID
		h.start(victim)
		if err := victim.observer.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return victim.enrollment.shouldRequestRejoin(true, h.now.Add(3*time.Minute)) })
		if err := h.enroll(victim); err != nil {
			t.Fatal("return refused", err)
		}
		await(t, func() bool { return victim.node.state.appliedIndex() >= leader.node.state.appliedIndex() })
		if h.leader().config.Local.ID != id || leader.node.raft.CurrentTerm() != before {
			t.Fatal("return disrupted leader")
		}
		if err := leader.node.Commit(context.Background(), transaction("sessions/return"+fmtInt(round), 0, `true`)); err != nil {
			t.Fatal(err)
		}
		// It is now a spare learner. Retire it again through failed health.
	}
}
func TestRetiredHigherTermWaitsWithoutDisruptingLeader(t *testing.T) {
	h := newChurnCluster(t)
	leader := h.leader()
	h.step()
	spare := h.add(nil)
	if err := h.enroll(spare); err != nil {
		t.Fatal(err)
	}
	h.stop(spare)
	h.step()
	h.now = h.now.Add(3 * time.Minute)
	await(t, func() bool {
		h.step()
		c := leader.node.raft.GetConfiguration()
		c.Error()
		for _, s := range c.Configuration().Servers {
			if string(s.ID) == spare.config.Local.ID {
				return false
			}
		}
		return true
	})
	// Hashicorp cannot cancel an already-started replication RPC on removal.
	// Let the production transport's five-second RPC bound drain before testing
	// admission of a node that has spent a long interval offline.
	time.Sleep(6 * time.Second)
	term := leader.node.raft.CurrentTerm()
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(spare.config.StateDir, "raft.db")})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetUint64([]byte("CurrentTerm"), term+100); err != nil {
		t.Fatal(err)
	}
	store.Close()
	h.start(spare)
	if err := h.enroll(spare); err == nil {
		t.Fatal("higher term was admitted into replication")
	}
	if h.leader() != leader || leader.node.raft.CurrentTerm() != term {
		t.Fatal("higher term disrupted leader")
	}
	if err := leader.node.Commit(context.Background(), transaction("sessions/highterm", 0, `true`)); err != nil {
		t.Fatal(err)
	}
}
func TestRetiredDifferentKeyAndCurrentLostVoterRefused(t *testing.T) {
	h := newChurnCluster(t)
	leader := h.leader()
	h.step()
	spare := h.add(nil)
	if err := h.enroll(spare); err != nil {
		t.Fatal(err)
	}
	h.stop(spare)
	h.step()
	h.now = h.now.Add(3 * time.Minute)
	await(t, func() bool {
		h.step()
		c := leader.node.raft.GetConfiguration()
		c.Error()
		for _, s := range c.Configuration().Servers {
			if string(s.ID) == spare.config.Local.ID {
				return false
			}
		}
		return true
	})
	spare.material = h.issue(spare.config.Local.ID)
	h.start(spare)
	if err := h.enroll(spare); err == nil {
		t.Fatal("retired key replacement accepted")
	}
	var victim *churnNode
	for _, n := range h.nodes[:3] {
		if n != leader {
			victim = n
			break
		}
	}
	h.stop(victim)
	victim.config.StateDir = t.TempDir()
	os.Chmod(victim.config.StateDir, 0700)
	victim.config.InitialVoters = nil
	h.start(victim)
	if err := h.enroll(victim); err == nil {
		t.Fatal("current voter lost history accepted")
	}
}

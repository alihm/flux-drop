package cluster

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

type excludeLocalPeers struct {
	source PeerSource
	self   netip.Addr
}

func (s excludeLocalPeers) Snapshot() ([]netip.AddrPort, bool) {
	all, fresh := s.source.Snapshot()
	var peers []netip.AddrPort
	for _, p := range all {
		if p.Addr() != s.self {
			peers = append(peers, p)
		}
	}
	return peers, fresh
}

func TestLeaderTransfersDoNotLatchOldPeerSnapshots(t *testing.T) {
	h := newChurnCluster(t)
	old := h.leader()
	var local *churnNode
	for _, n := range h.nodes {
		if n != old {
			local = n
			break
		}
	}
	observer, err := NewObserver(excludeLocalPeers{h.source, netip.MustParseAddrPort(local.config.Local.Address).Addr()}, testClusterID, "testapp", local.material.Client)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.client.CloseIdleConnections()
	observer.now = func() time.Time { return h.now }
	observer.setLocalStatus(local.node.Status)
	if err := observer.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	stale, _ := observer.Snapshot()
	if err := old.node.raft.LeadershipTransferToServer(raft.ServerID(local.config.Local.ID), raft.ServerAddress(local.config.Local.raftAddress())).Error(); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		return local.node.raft.State() == raft.Leader && local.node.Ready(context.Background()) == nil
	})
	manager := recoveryTestManager(h, local)
	manager.observer = observer
	// Re-reading old observations after this node becomes leader must not mix
	// its live role into the completed old refresh round, however often called.
	for i := 0; i < 4; i++ {
		manager.observeLeaderConflict(h.now)
		if manager.conflictDetected {
			t.Fatal("live local role combined with stale peer leader")
		}
	}
	// An ambiguous cached transition is resolved using current authenticated
	// probes, not an arbitrary leader choice or a permanent enrollment latch.
	current, err := observer.probe(context.Background(), netip.AddrPortFrom(netip.MustParseAddrPort(local.config.Local.Address).Addr(), h.statusPort))
	if err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	observer.observations = append(stale, current)
	observer.mu.Unlock()
	requester, err := newEnrollment(local.node, observer, RuntimeConfig{ContentDir: local.config.ContentDir, StatusPort: h.statusPort, EnrollmentCredential: h.credential}, local.material)
	if err != nil {
		t.Fatal(err)
	}
	defer requester.client.CloseIdleConnections()
	candidate, err := requester.leaderForRequest(context.Background())
	if err != nil || candidate.Status.ID != local.config.Local.ID {
		t.Fatal("transition blocked enrollment", candidate.Status.ID, err)
	}
	for i, target := range []*churnNode{old, h.nodes[2], local} {
		leader := h.leader()
		if leader != target {
			if err := leader.node.raft.LeadershipTransferToServer(raft.ServerID(target.config.Local.ID), raft.ServerAddress(target.config.Local.raftAddress())).Error(); err != nil {
				t.Fatal(err)
			}
		}
		await(t, func() bool {
			return target.node.raft.State() == raft.Leader && target.node.Ready(context.Background()) == nil
		})
		manager.observeLeaderConflict(h.now)
		if manager.conflictDetected {
			t.Fatal("transfer latched old round")
		}
		h.now = h.now.Add(autoRecoveryInterval)
		if err := observer.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		manager.observeLeaderConflict(h.now)
		if manager.conflictDetected {
			t.Fatal("normal leader transfer latched")
		}
		if err := target.node.Commit(context.Background(), transaction("sessions/normal-transfer"+fmtInt(i), 0, `true`)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLeaderConflictRoundContinuity(t *testing.T) {
	now := time.Now()
	var tracker leaderConflict
	two := []Observation{{Status: Status{ID: "a", Role: "Leader"}}, {Status: Status{ID: "b", Role: "Leader"}}}
	one := two[:1]
	tracker.observe(now, two, testClusterID)
	if tracker.latched {
		t.Fatal("one conflict round latched")
	}
	tracker.breakContinuity()
	tracker.observe(now.Add(15*time.Second), two, testClusterID)
	if tracker.latched {
		t.Fatal("failure did not break consecutive rounds")
	}
	tracker.observe(now.Add(30*time.Second), two, testClusterID)
	if !tracker.latched {
		t.Fatal("persistent leaders did not latch")
	}
	tracker.observe(now.Add(45*time.Second), one, testClusterID)
	// A long unobserved period cannot count as two minutes of single-leader rounds.
	tracker.observe(now.Add(5*time.Minute), one, testClusterID)
	if !tracker.latched {
		t.Fatal("sample gap cleared latch")
	}
	for i := 1; i <= 8; i++ {
		tracker.observe(now.Add(5*time.Minute+time.Duration(i)*15*time.Second), one, testClusterID)
		if i < 8 && !tracker.latched {
			t.Fatal("latch cleared before two minutes")
		}
	}
	if tracker.latched {
		t.Fatal("single leader did not clear latch")
	}
}

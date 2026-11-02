package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

func TestRecoveryViewTLSCompatibilityAndValidation(t *testing.T) {
	issue := materials(t)
	body := map[string]any{"nodeID": "peer", "clusterID": testClusterID, "lastLogIndex": 4, "lastLogTerm": 2, "hasLeader": false, "membershipRole": "learner", "discoveryViewHash": "", "evaluationState": "waiting", "recoveryMode": "observe", "protocol": 0, "futureExtension": true}
	code := 200
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code); json.NewEncoder(w).Encode(body) }))
	server.TLS = issue("peer").Server.Clone()
	server.StartTLS()
	defer server.Close()
	observer, err := NewObserver(&staticPeers{fresh: true}, testClusterID, "testapp", issue("caller").Client)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.client.CloseIdleConnections()
	address := netip.MustParseAddrPort(server.Listener.Addr().String())
	if _, err := observer.probeRecoveryView(context.Background(), address); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		key   string
		value any
	}{{"nodeID", "impostor"}, {"clusterID", "other"}, {"membershipRole", "candidate"}, {"recoveryMode", "enabled"}, {"lastLogTerm", 0}, {"hasLeader", "no"}} {
		old := body[test.key]
		body[test.key] = test.value
		if _, err := observer.probeRecoveryView(context.Background(), address); err == nil {
			t.Fatal("invalid known field accepted", test.key)
		}
		body[test.key] = old
	}
	code = 404
	if _, err := observer.probeRecoveryView(context.Background(), address); err == nil {
		t.Fatal("legacy 404 agreed to recovery")
	}
}

func TestManualLearnerRecoveryRequiresExplicitPermission(t *testing.T) {
	h := newChurnCluster(t)
	leader := h.leader()
	h.step()
	learner := h.add(nil)
	h.awaitEnrollment(learner)
	if err := leader.node.Commit(context.Background(), transaction("projects/learner-recovery", 0, `{"owner":"uid"}`)); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		return learner.node.state.read([]string{"projects/learner-recovery"})["projects/learner-recovery"].Version != 0
	})
	view, err := learner.node.RecoveryView()
	if err != nil || view.MembershipRole != "learner" || view.LastLogIndex == 0 {
		t.Fatal(view, err)
	}
	for _, n := range h.nodes[:3] {
		h.stop(n)
	}
	h.stop(learner)
	store := recoveryReadStore(t, learner.config)
	last, _ := store.LastIndex()
	store.Close()
	request := singletonRequest{ClusterID: testClusterID, NodeID: learner.config.Local.ID, ExpectedLastIndex: last}
	raw, _ := json.Marshal(request)
	path := filepath.Join(learner.config.StateDir, "recover-singleton.json")
	os.WriteFile(path, raw, 0600)
	before, _ := os.ReadFile(filepath.Join(learner.config.StateDir, "raft.db"))
	if n, err := StartTLS(learner.config, learner.config.Local.Address, learner.material); err == nil {
		n.Close()
		t.Fatal("implicit learner recovery")
	}
	after, _ := os.ReadFile(filepath.Join(learner.config.StateDir, "raft.db"))
	if !bytes.Equal(before, after) {
		t.Fatal("refused learner recovery changed Raft state")
	}
	request.AllowLearner = true
	raw, _ = json.Marshal(request)
	os.WriteFile(path, raw, 0600)
	h.start(learner)
	await(t, func() bool { return learner.node.raft.State() == raft.Leader && !learner.node.generationPending.Load() })
	if learner.node.state.read([]string{"projects/learner-recovery"})["projects/learner-recovery"].Version == 0 {
		t.Fatal("learner metadata lost")
	}
	if err := secureAutomaticStateTree(learner.config.StateDir); err != nil {
		t.Fatal(err)
	}
}

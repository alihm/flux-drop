package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func signedRecoveryMode(t *testing.T, n *churnNode, mode string) {
	t.Helper()
	_, err := ConfigureAutoRecovery(RuntimeConfig{ClusterID: n.config.ClusterID, ContentDir: n.config.ContentDir}, mode)
	if err != nil {
		t.Fatal(err)
	}
}
func recoveryTestManager(h *churnCluster, n *churnNode) *recoveryManager {
	return &recoveryManager{node: n.node, observer: n.observer, source: h.source, refresh: func(context.Context) error { return nil }, now: func() time.Time { return h.now }, window: autoRecoveryWindow, statusPort: h.statusPort}
}
func recoveryTestView(h *churnCluster, nodes ...*churnNode) {
	hash := discoveryView(h.source.addresses)
	for _, n := range nodes {
		n.node.recoveryDetails.Lock()
		n.node.recoveryDetails.view = hash
		n.node.recoveryDetails.mode = "observe"
		n.node.recoveryDetails.protocol = 1
		n.node.recoveryDetails.Unlock()
	}
}
func onlyListed(h *churnCluster, nodes ...*churnNode) {
	h.source.addresses = nil
	for _, n := range nodes {
		ip := netip.MustParseAddrPort(n.config.Local.Address).Addr()
		h.source.addresses = append(h.source.addresses, netip.AddrPortFrom(ip, h.statusPort))
	}
	recoveryTestView(h, nodes...)
}
func loseMajority(t *testing.T) (*churnCluster, *churnNode) {
	t.Helper()
	h := newChurnCluster(t)
	leader := h.leader()
	await(t, func() bool { return h.step() == nil })
	if err := leader.node.Commit(context.Background(), transaction("sessions/auto-recover", 0, `{"uid":"owner"}`)); err != nil {
		t.Fatal(err)
	}
	for _, n := range h.nodes[:3] {
		if n != leader {
			h.stop(n)
		}
	}
	await(t, func() bool {
		_, id := leader.node.raft.LeaderWithID()
		return id == "" && leader.node.raft.State() != raft.Leader
	})
	onlyListed(h, leader)
	return h, leader
}
func collectWindow(t *testing.T, h *churnCluster, m *recoveryManager) {
	t.Helper()
	for i := 0; i <= int(m.window/autoRecoveryInterval); i++ {
		if err := m.tick(context.Background()); err != nil {
			t.Fatalf("evidence sample %d failed: %v", i, err)
		}
		if i < int(m.window/autoRecoveryInterval) {
			h.now = h.now.Add(autoRecoveryInterval)
		}
	}
}
func TestAutoRecoveryPolicyAuthentication(t *testing.T) {
	t.Setenv(PassphraseEnv, strings.Repeat("policy-secret-", 4))
	c := RuntimeConfig{ClusterID: testClusterID, ContentDir: t.TempDir()}
	if status, err := ConfigureAutoRecovery(c, "status"); err != nil || status.Mode != "observe" || status.Valid {
		t.Fatal(status, err)
	}
	for _, mode := range []string{"off", "observe", "on"} {
		if status, err := ConfigureAutoRecovery(c, mode); err != nil || status.Mode != mode || !status.Valid {
			t.Fatal(status, err)
		}
	}
	path := filepath.Join(c.ContentDir, "auto-recovery.json")
	raw, _ := os.ReadFile(path)
	var policy autoRecoveryPolicy
	json.Unmarshal(raw, &policy)
	policy.Mode = "off"
	raw, _ = json.Marshal(policy)
	os.WriteFile(path, raw, 0600)
	if status, _ := ConfigureAutoRecovery(c, "status"); status.Mode != "observe" || status.Valid {
		t.Fatal("tampered policy trusted", status)
	}
	if _, err := ConfigureAutoRecovery(c, "automatic"); err == nil {
		t.Fatal("invalid mode accepted")
	}
}
func TestAutomaticRecoveryObserveAndOn(t *testing.T) {
	for _, mode := range []string{"observe", "on"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(PassphraseEnv, strings.Repeat("automatic-secret-", 3))
			h, node := loseMajority(t)
			signedRecoveryMode(t, node, mode)
			m := recoveryTestManager(h, node)
			collectWindow(t, h, m)
			if !m.reported || m.since.IsZero() {
				t.Fatal("complete window not reported")
			}
			request := filepath.Join(node.config.StateDir, "recover-singleton.json")
			if _, err := os.Lstat(request); !os.IsNotExist(err) {
				t.Fatal("evaluation mutated recovery state", err)
			}
			if mode == "observe" {
				if node.node.controlPending.Load() {
					t.Fatal("observe requested a restart")
				}
				return
			}
			select {
			case reset := <-node.node.control:
				if reset {
					t.Fatal("wrong restart action")
				}
			default:
				t.Fatal("winner did not request recovery")
			}
			stopped, err := m.performRecovery(context.Background())
			if !stopped || err == nil {
				t.Fatal(stopped, err)
			}
			h.stop(node)
			if _, err := os.Stat(request); err != nil {
				t.Fatal(err)
			}
			h.start(node)
			await(t, func() bool { return node.node.raft.State() == raft.Leader && !node.node.generationPending.Load() })
			if node.node.state.read([]string{"sessions/auto-recover"})["sessions/auto-recover"].Version == 0 {
				t.Fatal("metadata lost")
			}
			// Automatic actions reuse the tested manual backup/consume path.
			backups, _ := filepath.Glob(filepath.Join(node.config.StateDir, "pre-recovery-*"))
			if len(backups) != 1 {
				t.Fatal(backups)
			}
			if !recoveryBlocked(node.config, h.now) {
				t.Fatal("24-hour recovery cooldown missing")
			}
			for i := 0; i < 2; i++ {
				fresh := h.add(nil)
				h.awaitEnrollment(fresh)
			}
			h.awaitVoters(3)
			if err := node.node.Commit(context.Background(), transaction("sessions/rebuilt", 0, `true`)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Replace only the status listener: Raft and all stores/listeners remain real.
// Fault injection represents an authenticated older or disagreeing peer.
func recoveryFaultServer(t *testing.T, h *churnCluster, n *churnNode) *atomic.Int32 {
	t.Helper()
	n.server.Close()
	mode := &atomic.Int32{}
	normal := n.node.StatusHandler("testapp")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != recoveryViewPath || mode.Load() == 0 {
			normal.ServeHTTP(w, r)
			return
		}
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			http.Error(w, "forbidden", 403)
			return
		}
		switch mode.Load() {
		case 1:
			http.NotFound(w, r)
			return
		case 2:
			http.Error(w, "unavailable", 503)
			return
		}
		view, err := n.node.RecoveryView()
		if err != nil {
			http.Error(w, "unavailable", 503)
			return
		}
		switch mode.Load() {
		case 3:
			view.DiscoveryViewHash = strings.Repeat("f", 64)
		case 4:
			view.HasLeader = true
		case 5:
			view.Recovery = recoveryGeneration{Generation: 9999999999999, Lineage: strings.Repeat("a", 32)}
		case 6:
			view.Protocol = 0
		}
		json.NewEncoder(w).Encode(view)
	})
	server := httptest.NewUnstartedServer(handler)
	server.Listener.Close()
	ip := netip.MustParseAddrPort(n.config.Local.Address).Addr()
	var err error
	server.Listener, err = net.Listen("tcp", netip.AddrPortFrom(ip, h.statusPort).String())
	if err != nil {
		t.Fatal(err)
	}
	server.TLS = n.material.Server.Clone()
	server.StartTLS()
	n.server = server
	return mode
}

func TestAutomaticRecoveryGuardsResetEvidence(t *testing.T) {
	for _, mode := range []string{"observe", "on"} {
		t.Run(mode, func(t *testing.T) { testAutomaticRecoveryGuards(t, mode) })
	}
}
func testAutomaticRecoveryGuards(t *testing.T, mode string) {
	t.Setenv(PassphraseEnv, strings.Repeat("guard-secret-", 4))
	h := newChurnCluster(t)
	leader := h.leader()
	await(t, func() bool { return h.step() == nil })
	learner := h.add(nil)
	h.awaitEnrollment(learner)
	fault := recoveryFaultServer(t, h, learner)
	var lost []*churnNode
	for _, n := range h.nodes[:3] {
		if n != leader {
			lost = append(lost, n)
			h.stop(n)
		}
	}
	await(t, func() bool {
		_, id := leader.node.raft.LeaderWithID()
		return id == "" && leader.node.raft.State() != raft.Leader
	})
	await(t, func() bool { _, id := learner.node.raft.LeaderWithID(); return id == "" })
	onlyListed(h, leader, learner)
	signedRecoveryMode(t, leader, mode)
	m := recoveryTestManager(h, leader)
	if err := m.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		set   func()
		clear func()
	}{
		{"legacy-404", func() { fault.Store(1) }, func() { fault.Store(0) }},
		{"listed-unreachable", func() { fault.Store(2) }, func() { fault.Store(0) }},
		{"different-view", func() { fault.Store(3) }, func() { fault.Store(0) }},
		{"leader-appears", func() { fault.Store(4) }, func() { fault.Store(0) }},
		{"newer-generation", func() { fault.Store(5) }, func() { fault.Store(0) }},
		{"older-protocol", func() { fault.Store(6) }, func() { fault.Store(0) }},
		{"refresh-error", func() { m.refresh = func(context.Context) error { return errors.New("fetch failed") } }, func() { m.refresh = func(context.Context) error { return nil } }},
		{"stale", func() { h.source.fresh = false }, func() { h.source.fresh = true }},
		{"empty", func() { h.source.addresses = nil }, func() { onlyListed(h, leader, learner) }},
		{"self-absent", func() { onlyListed(h, learner) }, func() { onlyListed(h, leader, learner) }},
		{"malformed-address", func() { h.source.addresses = append(h.source.addresses, netip.AddrPort{}) }, func() { onlyListed(h, leader, learner) }},
		{"wrong-port", func() { a := h.source.addresses[0]; h.source.addresses[0] = netip.AddrPortFrom(a.Addr(), a.Port()+1) }, func() { onlyListed(h, leader, learner) }},
		{"duplicate-address", func() { h.source.addresses = append(h.source.addresses, h.source.addresses[0]) }, func() { onlyListed(h, leader, learner) }},
		{"voter-listed-but-down", func() {
			a := netip.MustParseAddrPort(lost[0].config.Local.Address)
			h.source.addresses = append(h.source.addresses, netip.AddrPortFrom(a.Addr(), h.statusPort))
			recoveryTestView(h, leader, learner)
		}, func() { onlyListed(h, leader, learner) }},
		{"missing-policy", func() { os.Remove(filepath.Join(leader.config.ContentDir, "auto-recovery.json")) }, func() { signedRecoveryMode(t, leader, mode) }},
		{"bad-policy", func() {
			os.WriteFile(filepath.Join(leader.config.ContentDir, "auto-recovery.json"), []byte(`{"mode":"on"}`), 0600)
		}, func() { signedRecoveryMode(t, leader, mode) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h.now = h.now.Add(autoRecoveryInterval)
			if err := m.tick(context.Background()); err != nil {
				t.Fatal("baseline", err)
			}
			test.set()
			h.now = h.now.Add(autoRecoveryInterval)
			if err := m.tick(context.Background()); err == nil {
				t.Fatal("guard failed to block")
			}
			if !m.since.IsZero() || m.node.controlPending.Load() {
				t.Fatal("guard retained evidence")
			}
			test.clear()
		})
	}
	// A missing voter that still answers authenticated direct probes blocks
	// recovery, even when the Flux list otherwise agrees. It is kept a follower
	// without leader contact by restarting only its status listener, with its
	// genuine TLS identity and durable metadata view.
	lostNode := lost[0]
	h.start(lostNode)
	onlyListed(h, leader, learner)
	h.now = h.now.Add(autoRecoveryInterval)
	if err := m.tick(context.Background()); err == nil || !m.since.IsZero() {
		t.Fatal("reachable absent voter permitted recovery")
	}
}

func TestSelfFencingModesAndFreshness(t *testing.T) {
	t.Setenv(PassphraseEnv, strings.Repeat("fencing-secret-", 3))
	h := newChurnCluster(t)
	leader := h.leader()
	other := h.nodes[0]
	if other == leader {
		other = h.nodes[1]
	}
	onlyListed(h, other)
	m := recoveryTestManager(h, leader)
	for _, mode := range []string{"observe", "on"} {
		signedRecoveryMode(t, leader, mode)
		for i := 0; i <= int(m.window/autoRecoveryInterval); i++ {
			_ = m.tick(context.Background())
			h.now = h.now.Add(autoRecoveryInterval)
		}
		if leader.node.selfFenced.Load() != (mode == "on") {
			t.Fatal("wrong authority fence", mode)
		}
	}
	if err := leader.node.Ready(context.Background()); !errors.Is(err, ErrNotLeader) {
		t.Fatal("fenced node ready", err)
	}
	if err := leader.node.Commit(context.Background(), transaction("sessions/fenced", 0, `true`)); !errors.Is(err, ErrNotLeader) {
		t.Fatal("fenced write", err)
	}
	// Failed/empty discovery never clears an engaged fence without evidence of
	// relisting; it also never starts or advances an absence window.
	m.refresh = func(context.Context) error { return errors.New("failed") }
	_ = m.tick(context.Background())
	if !leader.node.selfFenced.Load() {
		t.Fatal("failed fetch cleared fence")
	}
	m.refresh = func(context.Context) error { return nil }
	h.source.addresses = nil
	_ = m.tick(context.Background())
	if !leader.node.selfFenced.Load() {
		t.Fatal("empty list cleared fence")
	}
	onlyListed(h, leader, other)
	_ = m.tick(context.Background())
	if leader.node.selfFenced.Load() {
		t.Fatal("relisting did not clear fence")
	}
	// Relisting removes our fence immediately. Raft may independently have
	// elected another voter while this node withheld application authority.
	if elected := h.leader(); elected.node.Ready(context.Background()) != nil {
		t.Fatal("normal Raft authority not restored")
	}
	signedRecoveryMode(t, leader, "on")
	onlyListed(h, other)
	_ = m.tick(context.Background())
	h.now = h.now.Add(15 * time.Minute)
	_ = m.tick(context.Background())
	if leader.node.selfFenced.Load() {
		t.Fatal("unsampled timeout fenced node")
	}
}

func TestRecoveryWinnerOrdering(t *testing.T) {
	a := RecoveryView{NodeID: "a", MembershipRole: "voter", LastLogTerm: 2, LastLogIndex: 9}
	b := a
	b.NodeID = "b"
	aa := netip.MustParseAddrPort("127.0.0.1:8446")
	ba := netip.MustParseAddrPort("127.0.0.2:8446")
	if recoveryWinner(a, aa, b, ba) == recoveryWinner(b, ba, a, aa) {
		t.Fatal("tie did not choose one winner")
	}
	b.LastLogIndex = 10
	if !recoveryWinner(b, ba, a, aa) {
		t.Fatal("index ordering")
	}
	b.LastLogTerm = 1
	if !recoveryWinner(a, aa, b, ba) {
		t.Fatal("term ordering")
	}
	b.LastLogTerm = 100
	b.MembershipRole = "learner"
	if !recoveryWinner(a, aa, b, ba) {
		t.Fatal("surviving voter precedence")
	}
	a.MembershipRole = "learner"
	if !recoveryWinner(b, ba, a, aa) {
		t.Fatal("complete learner did not win")
	}
}

func TestAutomaticRecoveryChoosesCompleteLearnerAndResetsOther(t *testing.T) {
	t.Setenv(PassphraseEnv, strings.Repeat("learner-auto-secret-", 3))
	h := newChurnCluster(t)
	leader := h.leader()
	await(t, func() bool { return h.step() == nil })
	old := h.add(nil)
	complete := h.add(nil)
	for _, n := range []*churnNode{old, complete} {
		h.awaitEnrollment(n)
	}
	h.stop(old)
	await(t, func() bool {
		rows, err := leader.node.Read(context.Background(), []string{"projects/learner-tail"})
		if err != nil {
			return false
		}
		if row := rows["projects/learner-tail"]; row.Version != 0 {
			return string(row.Value) == `{"owner":"uid"}`
		}
		return leader.node.Commit(context.Background(), transaction("projects/learner-tail", 0, `{"owner":"uid"}`)) == nil
	})
	await(t, func() bool {
		return complete.node.state.read([]string{"projects/learner-tail"})["projects/learner-tail"].Version != 0
	})
	for _, n := range h.nodes[:3] {
		h.stop(n)
	}
	h.start(old)
	for _, n := range []*churnNode{old, complete} {
		await(t, func() bool { _, id := n.node.raft.LeaderWithID(); return id == "" })
	}
	onlyListed(h, old, complete)
	signedRecoveryMode(t, old, "on")
	signedRecoveryMode(t, complete, "on")
	a, b := recoveryTestManager(h, old), recoveryTestManager(h, complete)
	for i := 0; i <= int(autoRecoveryWindow/autoRecoveryInterval); i++ {
		if err := a.tick(context.Background()); err != nil {
			t.Fatal("older learner", err)
		}
		if err := b.tick(context.Background()); err != nil {
			t.Fatal("complete learner", err)
		}
		if i < int(autoRecoveryWindow/autoRecoveryInterval) {
			h.now = h.now.Add(autoRecoveryInterval)
		}
	}
	if old.node.controlPending.Load() || !complete.node.controlPending.Load() {
		t.Fatal("did not elect exactly one complete learner")
	}
	if stopped, err := b.performRecovery(context.Background()); !stopped || err == nil {
		t.Fatal(stopped, err)
	}
	h.stop(complete)
	h.start(complete)
	await(t, func() bool {
		return complete.node.raft.State() == raft.Leader && !complete.node.generationPending.Load()
	})
	complete.observer.Refresh(context.Background())
	old.observer.Refresh(context.Background())
	if err := old.enrollment.request(context.Background()); err == nil {
		t.Fatal("old generation admitted without reset")
	}
	select {
	case reset := <-old.node.control:
		if !reset {
			t.Fatal("wrong reset action")
		}
	default:
		t.Fatal("old learner did not request archive")
	}
	h.stop(old)
	target, _ := complete.node.recoveryGeneration()
	if err := archiveSuperseded(old.config, target); err != nil {
		t.Fatal(err)
	}
	h.start(old)
	h.awaitEnrollment(old)
	if old.node.state.read([]string{"projects/learner-tail"})["projects/learner-tail"].Version == 0 {
		t.Fatal("older learner retained its incomplete FSM")
	}
	fresh := h.add(nil)
	h.awaitEnrollment(fresh)
	h.awaitVoters(3)
}

func TestAutomaticRecoveryTwoSurvivingVotersChooseCompleteHistory(t *testing.T) {
	t.Setenv(PassphraseEnv, strings.Repeat("two-voters-secret-", 3))
	h := newChurnCluster(t)
	for len(h.nodes) < 5 {
		n := h.add(nil)
		h.awaitEnrollment(n)
	}
	await(t, func() bool { return h.step() == nil })
	h.now = h.now.Add(6 * time.Minute)
	h.awaitVoters(5)
	for _, node := range h.nodes {
		await(t, func() bool {
			c := node.node.raft.GetConfiguration()
			if c.Error() != nil {
				return false
			}
			v := 0
			for _, s := range c.Configuration().Servers {
				if s.Suffrage == raft.Voter {
					v++
				}
			}
			return v == 5
		})
	}
	leader := h.leader()
	var old *churnNode
	for _, n := range h.nodes {
		if n != leader {
			old = n
			break
		}
	}
	h.stop(old)
	if err := leader.node.Commit(context.Background(), transaction("sessions/complete-voter", 0, `true`)); err != nil {
		t.Fatal(err)
	}
	for _, n := range h.nodes {
		if n != leader && n != old {
			h.stop(n)
		}
	}
	await(t, func() bool {
		_, id := leader.node.raft.LeaderWithID()
		return id == "" && leader.node.raft.State() != raft.Leader
	})
	time.Sleep(6 * time.Second) // drain production transport RPCs from the old leader
	h.start(old)
	await(t, func() bool { _, id := old.node.raft.LeaderWithID(); return id == "" })
	onlyListed(h, leader, old)
	signedRecoveryMode(t, leader, "on")
	signedRecoveryMode(t, old, "on")
	a, b := recoveryTestManager(h, old), recoveryTestManager(h, leader)
	// Both listed survivors must answer each other; direct observation of self
	// is insufficient. Inject a network outage on the two status connections.
	oldObserver, newObserver := a.observer, b.observer
	for _, pair := range []struct {
		manager     *recoveryManager
		node, other *churnNode
	}{{a, old, leader}, {b, leader, old}} {
		observer, err := NewObserver(h.source, testClusterID, "testapp", pair.node.material.Client)
		if err != nil {
			t.Fatal(err)
		}
		defer observer.client.CloseIdleConnections()
		transport := observer.client.Transport.(*http.Transport)
		dial := transport.DialContext
		blocked := netip.AddrPortFrom(netip.MustParseAddrPort(pair.other.config.Local.Address).Addr(), h.statusPort).String()
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == blocked {
				return nil, errors.New("partition")
			}
			return dial(ctx, network, address)
		}
		pair.manager.observer = observer
		if err := pair.manager.tick(context.Background()); err == nil || !pair.manager.since.IsZero() || pair.node.node.controlPending.Load() {
			t.Fatal("partitioned survivor permitted recovery")
		}
	}
	a.observer = oldObserver
	b.observer = newObserver
	// Five voters cannot elect with only these two. Their shared, authenticated
	// discovery view permits only the more complete surviving voter to act.
	for i := 0; i <= int(autoRecoveryWindow/autoRecoveryInterval); i++ {
		if err := a.tick(context.Background()); err != nil {
			av, _ := old.node.RecoveryView()
			bv, _ := leader.node.RecoveryView()
			t.Fatalf("%v old=%+v complete=%+v", err, av, bv)
		}
		if err := b.tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		if i < int(autoRecoveryWindow/autoRecoveryInterval) {
			h.now = h.now.Add(autoRecoveryInterval)
		}
	}
	if old.node.controlPending.Load() || !leader.node.controlPending.Load() {
		t.Fatal("incomplete voter won recovery")
	}
}

func TestFinalRecoveryRecheckResetsWindow(t *testing.T) {
	t.Setenv(PassphraseEnv, strings.Repeat("recheck-secret-", 4))
	h, node := loseMajority(t)
	signedRecoveryMode(t, node, "on")
	m := recoveryTestManager(h, node)
	collectWindow(t, h, m)
	m.refresh = func(context.Context) error { return errors.New("final refresh failed") }
	if stopped, err := m.performRecovery(context.Background()); stopped || err == nil || !m.since.IsZero() {
		t.Fatal("failed final check retained eligibility", stopped, err)
	}
	if node.node.raft.State() == raft.Shutdown {
		t.Fatal("refused action stopped Raft")
	}
	// Atomic install must preserve an operator's competing request.
	path := filepath.Join(t.TempDir(), "recover-singleton.json")
	if err := writeNewRecoveryRequest(path, []byte("operator")); err != nil {
		t.Fatal(err)
	}
	if err := writeNewRecoveryRequest(path, []byte("automatic")); err == nil {
		t.Fatal("operator request overwritten")
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "operator" {
		t.Fatal(string(raw))
	}
}

func TestRecoveryCooldownAndPendingRequests(t *testing.T) {
	c := Config{StateDir: t.TempDir()}
	now := time.Now().UTC()
	if recoveryBlocked(c, now) {
		t.Fatal("empty state blocked")
	}
	for _, name := range []string{"recover-singleton.json", "restore-singleton.json", generationPendingFile} {
		path := filepath.Join(c.StateDir, name)
		os.WriteFile(path, []byte(`{}`), 0600)
		if !recoveryBlocked(c, now) {
			t.Fatal("pending operation did not block", name)
		}
		os.Remove(path)
	}
	stamp := now.Format("20060102T150405.000000000Z")
	os.WriteFile(filepath.Join(c.StateDir, "recover-singleton.done-"+stamp), nil, 0600)
	if !recoveryBlocked(c, now.Add(23*time.Hour)) {
		t.Fatal("cooldown too short")
	}
	if recoveryBlocked(c, now.Add(25*time.Hour)) {
		t.Fatal("cooldown never expires")
	}
}

func TestMultipleLeadersDisableRecoveryAndRejoin(t *testing.T) {
	t.Setenv(PassphraseEnv, strings.Repeat("split-brain-test-", 3))
	h := newChurnCluster(t)
	await(t, func() bool { return h.step() == nil })
	// Deliberately violate the operator runbook in the fixture: two independent
	// recoveries produce real sole-voter leaders on real TLS transports.
	for _, n := range h.nodes {
		h.stop(n)
	}
	for _, n := range h.nodes[:2] {
		store := recoveryReadStore(t, n.config)
		last, _ := store.LastIndex()
		store.Close()
		raw, _ := json.Marshal(singletonRequest{ClusterID: testClusterID, NodeID: n.config.Local.ID, ExpectedLastIndex: last})
		os.WriteFile(filepath.Join(n.config.StateDir, "recover-singleton.json"), raw, 0600)
		h.start(n)
		await(t, func() bool { return n.node.raft.State() == raft.Leader && !n.node.generationPending.Load() })
	}
	a, b := h.nodes[0], h.nodes[1]
	onlyListed(h, a, b)
	a.observer.Refresh(context.Background())
	signedRecoveryMode(t, a, "on")
	m := recoveryTestManager(h, a)
	if err := m.tick(context.Background()); err == nil || !m.conflictDetected {
		t.Fatal("two leaders not quarantined", err)
	}
	if err := a.enrollment.request(context.Background()); !errors.Is(err, ErrNotLeader) {
		t.Fatal("multiple leaders allowed rejoin", err)
	}
	if a.node.controlPending.Load() {
		t.Fatal("split brain requested automatic merge")
	}
}

func TestRecoveryEvidenceClocks(t *testing.T) {
	before := time.Now()
	if !recoverySampleGapValid(before, before.Add(autoRecoveryInterval)) {
		t.Fatal("valid cadence rejected")
	}
	for _, after := range []time.Time{before.Add(-time.Second), before.Add(time.Minute), time.Unix(before.Unix()+120, 0)} {
		if recoverySampleGapValid(before, after) {
			t.Fatal("clock gap retained evidence", after)
		}
	}
	if recoveryWindowElapsed(before, before.Add(autoRecoveryWindow)) != autoRecoveryWindow {
		t.Fatal("window clock mismatch")
	}
}

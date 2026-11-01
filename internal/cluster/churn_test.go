package cluster

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

type churnNode struct {
	node       *Node
	config     Config
	material   TLSMaterial
	server     *httptest.Server
	observer   *Observer
	enrollment *enrollment
}
type churnCluster struct {
	t                *testing.T
	nodes            []*churnNode
	source           *staticPeers
	issue            func(string) TLSMaterial
	now              time.Time
	port, statusPort uint16
	credential       string
}

func newChurnCluster(t *testing.T) *churnCluster {
	h := &churnCluster{t: t, source: &staticPeers{fresh: true}, issue: materials(t), now: time.Now(), port: uint16(recoveryPort(t)), statusPort: uint16(recoveryPort(t))}
	// Register cleanup before starting listeners, including partially failed starts.
	t.Cleanup(func() {
		for _, n := range h.nodes {
			if n.server != nil {
				n.server.Close()
			}
			if n.node != nil {
				n.node.Close()
			}
			if n.observer != nil {
				n.observer.client.CloseIdleConnections()
			}
			if n.enrollment != nil {
				n.enrollment.client.CloseIdleConnections()
			}
		}
	})
	key := make([]byte, 32)
	rand.Read(key)
	h.credential = filepath.Join(t.TempDir(), "admission")
	os.WriteFile(h.credential, []byte(base64.StdEncoding.EncodeToString(key)), 0600)
	var initial []Member
	for i := 0; i < 3; i++ {
		initial = append(initial, h.member(i))
	}
	// Provision identities/directories first, then start all real listeners
	// together. Sequential test provisioning must not race a two-node majority's
	// election against the third node's explicit bootstrap under slow disk I/O.
	for _, m := range initial {
		root := t.TempDir()
		os.Chmod(root, 0700)
		c := Config{ClusterID: testClusterID, Local: m, StateDir: root, ContentDir: t.TempDir(), InitialVoters: initial, AsyncContent: true}
		h.nodes = append(h.nodes, &churnNode{config: c, material: h.issue(m.ID)})
		a := netip.MustParseAddrPort(m.Address)
		h.source.addresses = append(h.source.addresses, netip.AddrPortFrom(a.Addr(), h.statusPort))
	}
	var starts sync.WaitGroup
	for _, n := range h.nodes {
		starts.Add(1)
		go func(n *churnNode) { defer starts.Done(); h.start(n) }(n)
	}
	starts.Wait()
	h.leader()
	return h
}

func (h *churnCluster) member(i int) Member {
	return Member{ID: fmt.Sprintf("churn%02d", i), Address: net.JoinHostPort(fmt.Sprintf("127.0.0.%d", 151+i), fmtInt(int(h.port)))}
}
func (h *churnCluster) add(initial []Member) *churnNode {
	h.t.Helper()
	m := h.member(len(h.nodes))
	root := h.t.TempDir()
	os.Chmod(root, 0700)
	c := Config{ClusterID: testClusterID, Local: m, StateDir: root, ContentDir: h.t.TempDir(), InitialVoters: initial, AsyncContent: true}
	n := &churnNode{config: c, material: h.issue(m.ID)}
	h.nodes = append(h.nodes, n)
	h.start(n)
	return n
}
func (h *churnCluster) start(n *churnNode) {
	h.t.Helper()
	var err error
	n.node, err = StartTLS(n.config, n.config.Local.Address, n.material)
	if err != nil {
		h.t.Fatal(err)
	}
	n.observer, err = NewObserver(h.source, testClusterID, "testapp", n.material.Client)
	if err != nil {
		h.t.Fatal(err)
	}
	n.enrollment, err = newEnrollment(n.node, n.observer, RuntimeConfig{ContentDir: n.config.ContentDir, StatusPort: h.statusPort, EnrollmentCredential: h.credential}, n.material)
	if err != nil {
		h.t.Fatal(err)
	}
	n.enrollment.now = func() time.Time { return h.now }
	n.enrollment.grace = 2 * time.Minute
	n.node.enrollment = n.enrollment
	s := httptest.NewUnstartedServer(n.node.StatusHandler("testapp"))
	s.Listener.Close()
	ip := netip.MustParseAddrPort(n.config.Local.Address).Addr()
	address := netip.AddrPortFrom(ip, h.statusPort)
	s.Listener, err = net.Listen("tcp", address.String())
	if err != nil {
		h.t.Fatal(err)
	}
	s.TLS = n.material.Server.Clone()
	s.StartTLS()
	n.server = s
	found := false
	for _, a := range h.source.addresses {
		if a == address {
			found = true
		}
	}
	if !found {
		h.source.addresses = append(h.source.addresses, address)
	}
}
func (h *churnCluster) stop(n *churnNode) {
	h.t.Helper()
	n.server.Close()
	n.observer.client.CloseIdleConnections()
	n.enrollment.client.CloseIdleConnections()
	n.server = nil
	n.node.Close()
	n.node = nil
}
func (h *churnCluster) leader() *churnNode {
	h.t.Helper()
	var leader *churnNode
	await(h.t, func() bool {
		for _, n := range h.nodes {
			if n.node != nil && n.node.raft.State() == raft.Leader && n.node.Ready(context.Background()) == nil {
				leader = n
				return true
			}
		}
		return false
	})
	return leader
}
func (h *churnCluster) enroll(n *churnNode) error {
	h.t.Helper()
	leader := h.leader()
	if err := leader.observer.Refresh(context.Background()); err != nil {
		return err
	}
	if err := n.observer.Refresh(context.Background()); err != nil {
		return err
	}
	return n.enrollment.request(context.Background())
}
func (h *churnCluster) step() error {
	n := h.leader()
	n.observer.Refresh(context.Background())
	return n.enrollment.reconcile(context.Background())
}
func (h *churnCluster) voters() int {
	c := h.leader().node.raft.GetConfiguration()
	if err := c.Error(); err != nil {
		h.t.Fatal(err)
	}
	v := 0
	for _, s := range c.Configuration().Servers {
		if s.Suffrage == raft.Voter {
			v++
		}
	}
	return v
}
func (h *churnCluster) awaitVoters(want int) {
	h.t.Helper()
	await(h.t, func() bool { h.step(); return h.voters() == want })
}

func TestDynamicVoterCountsAndBounds(t *testing.T) {
	h := newChurnCluster(t)
	for count := 3; count <= 9; count++ {
		if count > 3 {
			n := h.add(nil)
			if err := h.enroll(n); err != nil {
				t.Fatal(err)
			}
		}
		// Admission can precede catch-up; keep verifying the real TLS prefix.
		h.step()
		h.now = h.now.Add(6 * time.Minute)
		want := map[int]int{3: 3, 4: 3, 5: 5, 6: 5, 7: 7, 8: 7, 9: 7}[count]
		h.awaitVoters(want)
	}
	// Unadmitted, discovered nodes never contribute to eligibility.
	for len(h.nodes) < 16 {
		n := h.add(nil)
		if err := h.enroll(n); err != nil {
			t.Fatal(err)
		}
	}
	h.step()
	h.now = h.now.Add(6 * time.Minute)
	h.awaitVoters(7)
	extra := h.add(nil)
	if err := h.enroll(extra); err == nil {
		t.Fatal("17th member admitted")
	}
	configuration := h.leader().node.raft.GetConfiguration()
	configuration.Error()
	if len(configuration.Configuration().Servers) != maxMembers {
		t.Fatal("member cap violated")
	}
}
func TestDynamicWindowsAndFiveVoterFailureTolerance(t *testing.T) {
	h := newChurnCluster(t)
	for len(h.nodes) < 6 {
		n := h.add(nil)
		if err := h.enroll(n); err != nil {
			t.Fatal(err)
		}
	}
	h.step()
	h.now = h.now.Add(4 * time.Minute)
	h.step()
	if h.voters() != 3 {
		t.Fatal("raise window bypassed")
	}
	h.now = h.now.Add(2 * time.Minute)
	h.awaitVoters(5)
	leader := h.leader()
	configuration := leader.node.raft.GetConfiguration()
	configuration.Error()
	var failed []*churnNode
	for _, s := range configuration.Configuration().Servers {
		if s.Suffrage == raft.Voter && string(s.ID) != leader.config.Local.ID {
			for _, n := range h.nodes {
				if n.config.Local.ID == string(s.ID) && len(failed) < 2 {
					failed = append(failed, n)
				}
			}
		}
	}
	for _, n := range failed {
		h.stop(n)
	}
	if err := leader.node.Commit(context.Background(), transaction("sessions/twofailures", 0, `true`)); err != nil {
		t.Fatal("lost five-voter majority", err)
	}
	h.step()
	h.now = h.now.Add(time.Minute)
	h.step()
	if h.voters() != 5 {
		t.Fatal("failure grace bypassed")
	}
	h.now = h.now.Add(2 * time.Minute)
	// One spare replaces a failed voter immediately, without a lower window.
	await(t, func() bool {
		h.step()
		c := leader.node.raft.GetConfiguration()
		c.Error()
		for _, s := range c.Configuration().Servers {
			if string(s.ID) == failed[0].config.Local.ID {
				return false
			}
		}
		return true
	})
	for i := 0; i < 2; i++ {
		n := h.add(nil)
		if err := h.enroll(n); err != nil {
			t.Fatal(err)
		}
	}
	h.awaitVoters(5)
	if err := leader.node.Commit(context.Background(), transaction("sessions/replaced", 0, `true`)); err != nil {
		t.Fatal(err)
	}
}
func TestDynamicLowerWindowAndPolicyFloor(t *testing.T) {
	h := newChurnCluster(t)
	for len(h.nodes) < 5 {
		n := h.add(nil)
		if err := h.enroll(n); err != nil {
			t.Fatal(err)
		}
	}
	h.step()
	h.now = h.now.Add(6 * time.Minute)
	h.awaitVoters(5)
	leader := h.leader()
	var victim *churnNode
	for _, n := range h.nodes {
		if n != leader {
			victim = n
			break
		}
	}
	h.stop(victim)
	h.step()
	h.now = h.now.Add(14 * time.Minute)
	h.step()
	if h.voters() != 5 {
		t.Fatal("lower window bypassed")
	}
	// A short healthy sample restarts the entire lower window.
	h.start(victim)
	h.step()
	h.stop(victim)
	h.step()
	h.now = h.now.Add(14 * time.Minute)
	h.step()
	if h.voters() != 5 {
		t.Fatal("flapping did not reset window")
	}
	h.now = h.now.Add(2 * time.Minute)
	h.awaitVoters(4)
	// Healthy voters are never removed merely to make the count odd.
	h.now = h.now.Add(time.Hour)
	h.step()
	if h.voters() != 4 {
		t.Fatal("healthy voter retired")
	}
	records, err := leader.node.Read(context.Background(), []string{membershipPolicyKey})
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.node.Commit(context.Background(), transaction(membershipPolicyKey, records[membershipPolicyKey].Version, `{"voters":5}`)); err != nil {
		t.Fatal(err)
	}
	h.now = h.now.Add(time.Hour)
	h.step()
	if leader.enrollment.activeTarget < 5 {
		t.Fatal("policy floor ignored")
	}
}

func TestCommittedMembershipExcludesPendingConfiguration(t *testing.T) {
	h := newChurnCluster(t)
	leader := h.leader()
	h.step()
	spare := h.add(nil)
	for _, node := range h.nodes[:3] {
		if node != leader {
			h.stop(node)
		}
	}
	// Raft's real configuration-change path appends this entry but cannot
	// commit it with the other two voters down. No fake stores or FSM hooks.
	future := leader.node.raft.AddNonvoter(raft.ServerID(spare.config.Local.ID), raft.ServerAddress(spare.config.Local.raftAddress()), 0, time.Second)
	await(t, func() bool {
		latest := leader.node.raft.GetConfiguration()
		return latest.Error() == nil && len(latest.Configuration().Servers) == 4
	})
	committed, err := strconv.ParseUint(leader.node.raft.Stats()["commit_index"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	configuration, _, err := configurationAtIndex(leader.node.store, leader.node.snapshots, committed)
	if err != nil || len(configuration.Servers) != 3 {
		t.Fatal("uncommitted learner counted as eligible", configuration, err)
	}
	if err := future.Error(); err == nil {
		t.Fatal("minority committed a membership change")
	}
}

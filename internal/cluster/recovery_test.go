package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"
)

func recoveryPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.101:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

type recoveryTLSGroup struct {
	*testGroup
	issue func(string) TLSMaterial
}

// Recovery coverage never substitutes an in-memory transport: even refusal
// paths construct the same listener/transport and identity checks as production.
func newRecoveryTLSGroup(t *testing.T, async bool) *recoveryTLSGroup {
	t.Helper()
	issue := materials(t)
	g := &testGroup{}
	port := recoveryPort(t)
	members := []Member{}
	for i, id := range []string{"one", "two", "three"} {
		members = append(members, Member{ID: id, Address: net.JoinHostPort(fmt.Sprintf("127.0.0.%d", 111+i), fmtInt(port))})
	}
	for _, member := range members {
		root := t.TempDir()
		if err := os.Chmod(root, 0700); err != nil {
			t.Fatal(err)
		}
		config := Config{ClusterID: testClusterID, Local: member, StateDir: root, ContentDir: t.TempDir(), InitialVoters: members, AsyncContent: async}
		node, err := StartTLS(config, member.Address, issue(member.ID))
		if err != nil {
			for _, n := range g.nodes {
				n.Close()
			}
			t.Fatal(err)
		}
		g.configs = append(g.configs, config)
		g.nodes = append(g.nodes, node)
	}
	t.Cleanup(func() {
		for _, node := range g.nodes {
			node.Close()
		}
	})
	return &recoveryTLSGroup{testGroup: g, issue: issue}
}

func recoveryReadStore(t *testing.T, c Config) *raftboltdb.BoltStore {
	t.Helper()
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(c.StateDir, "raft.db"), BoltOptions: &bbolt.Options{ReadOnly: true, Timeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	return store
}
func writeSingleton(t *testing.T, c Config) {
	t.Helper()
	store := recoveryReadStore(t, c)
	index, err := store.LastIndex()
	store.Close()
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(singletonRequest{ClusterID: c.ClusterID, NodeID: c.Local.ID, ExpectedLastIndex: index})
	if err := os.WriteFile(filepath.Join(c.StateDir, "recover-singleton.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestSingletonRecoveryRealTLSAndReenrollment(t *testing.T) {
	issue := materials(t)
	port := recoveryPort(t)
	statusPort := recoveryPort(t)
	members := []Member{}
	for i, id := range []string{"survivor", "lost-two", "lost-three", "new-two", "new-three"} {
		members = append(members, Member{ID: id, Address: net.JoinHostPort("127.0.0."+[]string{"101", "102", "103", "104", "105"}[i], fmtInt(port))})
	}
	configs := make([]Config, 5)
	nodes := make([]*Node, 5)
	for i := range configs {
		dir := t.TempDir()
		os.Chmod(dir, 0700)
		configs[i] = Config{ClusterID: testClusterID, Local: members[i], StateDir: dir, ContentDir: t.TempDir(), AsyncContent: true}
		if i < 3 {
			configs[i].InitialVoters = members[:3]
		}
	}
	startNode := func(i int) {
		t.Helper()
		n, err := StartTLS(configs[i], members[i].Address, issue(members[i].ID))
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Close()
			}
		}
	}()
	for i := 0; i < 3; i++ {
		startNode(i)
	}
	leader := -1
	await(t, func() bool {
		for i := 0; i < 3; i++ {
			if nodes[i].raft.State() == raft.Leader {
				leader = i
				return true
			}
		}
		return false
	})
	ctx := context.Background()
	awaitAsync(t, nodes[leader])
	keys := []string{"sessions/owner", "projects/website", "owner_projects/owner", "cluster_policy/membership"}
	values := []string{`{"uid":"owner","authenticated":true}`, `{"id":"website","owner":"owner","slug":"hello"}`, `{"project":"website"}`, `{"voters":3}`}
	for i, key := range keys {
		if err := nodes[leader].Commit(ctx, transaction(key, 0, values[i])); err != nil {
			t.Fatal(err)
		}
	}
	await(t, func() bool { return nodes[0].state.read(keys)[keys[3]].Version != 0 })
	if err := (&automatic{node: nodes[0]}).recordGenesis(); err != nil {
		t.Fatal(err)
	}
	intent, _ := json.Marshal(configs[0].Local)
	if err := os.WriteFile(filepath.Join(configs[0].StateDir, "bootstrap-intent.json"), intent, 0600); err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{1, 2} {
		nodes[i].Close()
		nodes[i] = nil
	}
	await(t, func() bool { return nodes[0].raft.State() != raft.Leader })
	nodes[0].Close()
	nodes[0] = nil
	// No request: an ordinary restart must not form a singleton.
	startNode(0)
	time.Sleep(3 * time.Second)
	if nodes[0].raft.State() == raft.Leader {
		t.Fatal("implicit recovery")
	}
	nodes[0].Close()
	nodes[0] = nil
	writeSingleton(t, configs[0])
	startNode(0)
	await(t, func() bool { return nodes[0].raft.State() == raft.Leader })
	awaitAsync(t, nodes[0])
	for i, key := range keys {
		if string(nodes[0].state.read(keys)[key].Value) != values[i] {
			t.Fatalf("lost %s", key)
		}
	}
	if err := nodes[0].Commit(ctx, transaction("sessions/tooearly", 0, `true`)); err == nil {
		t.Fatal("singleton acknowledged security replication")
	}
	backups, _ := filepath.Glob(filepath.Join(configs[0].StateDir, "pre-recovery-*"))
	if len(backups) != 1 {
		t.Fatal(backups)
	}
	// A backup is genuinely usable Raft state with the original configuration.
	backupConfig := configs[0]
	backupConfig.StateDir = backups[0]
	store := recoveryReadStore(t, backupConfig)
	snapshots, err := raft.NewFileSnapshotStore(backups[0], 2, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := latestRecoveryConfiguration(store, snapshots)
	store.Close()
	if err != nil || len(configuration.Servers) != 3 {
		t.Fatal("unusable backup", configuration, err)
	}
	if err := secureAutomaticStateTree(configs[0].StateDir); err != nil {
		t.Fatal(err)
	}
	statusServers := []*httptest.Server{}
	defer func() {
		for _, s := range statusServers {
			s.Close()
		}
	}()
	peers := &staticPeers{fresh: true}
	observer, err := NewObserver(peers, testClusterID, "testapp", issue(members[0].ID).Client)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.client.CloseIdleConnections()
	credential := make([]byte, 32)
	rand.Read(credential)
	credentialPath := filepath.Join(t.TempDir(), "enrollment.key")
	if err := os.WriteFile(credentialPath, []byte(base64.StdEncoding.EncodeToString(credential)), 0600); err != nil {
		t.Fatal(err)
	}
	enrollmentConfig := RuntimeConfig{ContentDir: configs[0].ContentDir, StatusPort: uint16(statusPort), EnrollmentCredential: credentialPath}
	e, err := newEnrollment(nodes[0], observer, enrollmentConfig, issue(members[0].ID))
	if err != nil {
		t.Fatal(err)
	}
	defer e.client.CloseIdleConnections()
	nodes[0].enrollment = e
	leaderStatus := httptest.NewUnstartedServer(nodes[0].StatusHandler("testapp"))
	leaderStatus.Listener.Close()
	leaderStatus.Listener, err = net.Listen("tcp", net.JoinHostPort("127.0.0.101", fmtInt(statusPort)))
	if err != nil {
		t.Fatal(err)
	}
	leaderStatus.TLS = issue(members[0].ID).Server.Clone()
	leaderStatus.StartTLS()
	statusServers = append(statusServers, leaderStatus)

	for _, i := range []int{3, 4} {
		startNode(i)
		ip := netip.MustParseAddrPort(members[i].Address).Addr()
		server := httptest.NewUnstartedServer(nodes[i].StatusHandler("testapp"))
		server.Listener.Close()
		server.Listener, err = net.Listen("tcp", netip.AddrPortFrom(ip, uint16(statusPort)).String())
		if err != nil {
			t.Fatal(err)
		}
		server.TLS = issue(members[i].ID).Server.Clone()
		server.StartTLS()
		statusServers = append(statusServers, server)
		peers.addresses = append(peers.addresses, netip.AddrPortFrom(ip, uint16(statusPort)))
		if err := observer.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
		learnerSource := &staticPeers{fresh: true, addresses: []netip.AddrPort{netip.MustParseAddrPort(leaderStatus.Listener.Addr().String())}}
		learnerObserver, err := NewObserver(learnerSource, testClusterID, "testapp", issue(members[i].ID).Client)
		if err != nil {
			t.Fatal(err)
		}
		defer learnerObserver.client.CloseIdleConnections()
		if err := learnerObserver.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
		requester, err := newEnrollment(nodes[i], learnerObserver, enrollmentConfig, issue(members[i].ID))
		if err != nil {
			t.Fatal(err)
		}
		defer requester.client.CloseIdleConnections()
		if err := requester.request(ctx); err != nil {
			t.Fatal("authenticated enrollment", err)
		}
		cf := nodes[0].raft.GetConfiguration()
		if err := cf.Error(); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, s := range cf.Configuration().Servers {
			if string(s.ID) == members[i].ID {
				found = s.Suffrage == raft.Nonvoter
			}
		}
		if !found {
			t.Fatal("not admitted as learner")
		}
		await(t, func() bool {
			observer.Refresh(ctx)
			return e.reconcile(ctx) == nil && func() bool {
				cf := nodes[0].raft.GetConfiguration()
				cf.Error()
				for _, s := range cf.Configuration().Servers {
					if string(s.ID) == members[i].ID {
						return s.Suffrage == raft.Voter
					}
				}
				return false
			}()
		})
		if i == 3 {
			if err := nodes[0].Commit(ctx, transaction("sessions/replicated", 0, `true`)); err != nil {
				t.Fatal("replication never resumed", err)
			}
		}
	}
	if target, err := e.target(ctx); err != nil || target != 3 {
		t.Fatal("voter target changed", target, err)
	}
	// History/genesis do not veto recovered durable history, even a lost async tail.
	a := automatic{node: nodes[0], observer: observer}
	history, _ := json.Marshal(primaryHistory{Primary: "lost-two", HighestRevision: nodes[0].Status().DurableRevision + 100})
	os.WriteFile(filepath.Join(configs[0].StateDir, "primary-history.json"), history, 0600)
	if err := a.recordGenesis(); err != nil {
		t.Fatal(err)
	}
	if err := a.recordHistory(); err != nil {
		t.Fatal(err)
	}
	a.source = &staticPeers{fresh: true, addresses: append(append([]netip.AddrPort(nil), peers.addresses...), netip.MustParseAddrPort(leaderStatus.Listener.Addr().String()))}
	if err := a.tick(ctx, time.Now()); err != nil {
		t.Fatal("automatic history/genesis fenced recovered leader", err)
	}

	nodes[0].Close()
	nodes[0] = nil
	startNode(0)
	done, _ := filepath.Glob(filepath.Join(configs[0].StateDir, "recover-singleton.done-*"))
	after, _ := filepath.Glob(filepath.Join(configs[0].StateDir, "pre-recovery-*"))
	if len(done) != 1 || len(after) != 1 {
		t.Fatal("recovery repeated", done, after)
	}
	// Each of the three voters may be stopped in turn, leaving a working majority.
	for _, i := range []int{0, 3, 4} {
		nodes[i].Close()
		nodes[i] = nil
		await(t, func() bool {
			for _, j := range []int{0, 3, 4} {
				if nodes[j] != nil && nodes[j].raft.State() == raft.Leader {
					return nodes[j].Ready(ctx) == nil
				}
			}
			return false
		})
		startNode(i)
	}
	// Recover the saved copy itself, on an isolated address, proving it contains
	// usable original metadata rather than merely a parseable configuration.
	for i, node := range nodes {
		if node != nil {
			node.Close()
			nodes[i] = nil
		}
	}
	backupConfig.InitialVoters = nil
	backupConfig.Local.Address = net.JoinHostPort("127.0.0.106", fmtInt(recoveryPort(t)))
	writeSingleton(t, backupConfig)
	saved, err := StartTLS(backupConfig, backupConfig.Local.Address, issue(backupConfig.Local.ID))
	if err != nil {
		t.Fatal("pre-recovery copy unusable", err)
	}
	defer saved.Close()
	await(t, func() bool { return saved.raft.State() == raft.Leader })
	for i, key := range keys {
		if string(saved.state.read(keys)[key].Value) != values[i] {
			t.Fatal("backup copy lost", key)
		}
	}

}

func fmtInt(n int) string { return fmt.Sprintf("%d", n) }

func TestSingletonRecoveryRefusalsDoNotWriteStore(t *testing.T) {
	g := newRecoveryTLSGroup(t, false)
	leader := g.leader(t, -1)
	n := g.nodes[leader]
	if err := n.Commit(context.Background(), transaction("sessions/retained", 0, `true`)); err != nil {
		t.Fatal(err)
	}
	for _, node := range g.nodes {
		node.Close()
	}
	c := g.configs[leader]
	store := recoveryReadStore(t, c)
	index, _ := store.LastIndex()
	store.Close()
	valid, _ := json.Marshal(singletonRequest{ClusterID: c.ClusterID, NodeID: c.Local.ID, ExpectedLastIndex: index})
	cases := map[string][]byte{
		"wrong-case":    bytes.Replace(valid, []byte(`"clusterID"`), []byte(`"ClusterID"`), 1),
		"case-alias":    []byte(fmt.Sprintf(`{"clusterID":%q,"ClusterID":%q,"nodeID":%q,"expectedLastIndex":%d}`, c.ClusterID, c.ClusterID, c.Local.ID, index)),
		"missing-index": []byte(fmt.Sprintf(`{"clusterID":%q,"nodeID":%q}`, c.ClusterID, c.Local.ID)),
		"null-index":    []byte(fmt.Sprintf(`{"clusterID":%q,"nodeID":%q,"expectedLastIndex":null}`, c.ClusterID, c.Local.ID)), "cluster": bytes.Replace(valid, []byte(c.ClusterID), []byte(strings.Repeat("f", 32)), 1), "node": bytes.Replace(valid, []byte(c.Local.ID), []byte("other"), 1), "index": []byte(fmt.Sprintf(`{"clusterID":%q,"nodeID":%q,"expectedLastIndex":%d}`, c.ClusterID, c.Local.ID, index+1)), "malformed": []byte(`{"clusterID":`), "unknown": append(valid[:len(valid)-1], []byte(`,"unknown":true}`)...), "duplicate": []byte(fmt.Sprintf(`{"clusterID":%q,"clusterID":%q,"nodeID":%q,"expectedLastIndex":%d}`, c.ClusterID, c.ClusterID, c.Local.ID, index))}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			os.WriteFile(filepath.Join(c.StateDir, "recover-singleton.json"), data, 0600)
			before, _ := os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
			_, err := StartTLS(c, c.Local.Address, g.issue(c.Local.ID))
			if err == nil {
				t.Fatal("accepted invalid request")
			}
			after, _ := os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
			if !bytes.Equal(before, after) {
				t.Fatal("refusal modified raft.db")
			}
		})
	}
	path := filepath.Join(c.StateDir, "recover-singleton.json")
	os.Remove(path)
	target := filepath.Join(t.TempDir(), "request")
	os.WriteFile(target, valid, 0600)
	os.Symlink(target, path)
	before, _ := os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
	if _, err := StartTLS(c, c.Local.Address, g.issue(c.Local.ID)); err == nil {
		t.Fatal("followed symlink")
	}
	after, _ := os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
	if !bytes.Equal(before, after) {
		t.Fatal("symlink changed store")
	}
	os.Remove(path)
	// A foreign-owned request and an unsafe backup subtree also refuse before
	// a writable Bolt open. The operator's original request remains available.
	if os.Geteuid() == 0 {
		os.WriteFile(path, valid, 0600)
		if err := os.Chown(path, 65534, 65534); err != nil {
			t.Fatal(err)
		}
		before, _ = os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
		if _, err := StartTLS(c, c.Local.Address, g.issue(c.Local.ID)); err == nil {
			t.Fatal("foreign request accepted")
		}
		after, _ = os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
		if !bytes.Equal(before, after) {
			t.Fatal("foreign request modified store")
		}
		os.Chown(path, os.Geteuid(), os.Getegid())
		os.Remove(path)
	}
	os.WriteFile(path, valid, 0600)
	unsafe := filepath.Join(c.StateDir, "snapshots", "unsafe-link")
	if err := os.Symlink(target, unsafe); err != nil {
		t.Fatal(err)
	}
	before, _ = os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
	if _, err := StartTLS(c, c.Local.Address, g.issue(c.Local.ID)); err == nil {
		t.Fatal("unsafe backup source accepted")
	}
	after, _ = os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
	if !bytes.Equal(before, after) {
		t.Fatal("copy failure modified store")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("copy failure consumed request", err)
	}
	os.Remove(unsafe)
	os.Remove(path)
	// Even an absent snapshot directory must not be recreated by a refusal.
	savedSnapshots := filepath.Join(c.StateDir, "saved-snapshots")
	if err := os.Rename(filepath.Join(c.StateDir, "snapshots"), savedSnapshots); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(path, valid, 0600)
	before, _ = os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
	if _, err := StartTLS(c, c.Local.Address, g.issue(c.Local.ID)); err == nil {
		t.Fatal("missing backup source accepted")
	}
	after, _ = os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
	if !bytes.Equal(before, after) {
		t.Fatal("missing snapshot source changed store")
	}
	if _, err := os.Lstat(filepath.Join(c.StateDir, "snapshots")); !os.IsNotExist(err) {
		t.Fatal("refusal created snapshot directory", err)
	}
	if err := os.Rename(savedSnapshots, filepath.Join(c.StateDir, "snapshots")); err != nil {
		t.Fatal(err)
	}
	os.Remove(path)
	// Latest log configuration, not the bootstrap manifest, controls eligibility.
	st, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(c.StateDir, "raft.db")})
	if err != nil {
		t.Fatal(err)
	}
	st.StoreLog(&raft.Log{Index: index + 1, Term: 1, Type: raft.LogConfiguration, Data: raft.EncodeConfiguration(raft.Configuration{Servers: []raft.Server{{ID: raft.ServerID(c.Local.ID), Address: raft.ServerAddress(c.Local.raftAddress()), Suffrage: raft.Nonvoter}}})})
	st.Close()
	writeSingleton(t, c)
	before, _ = os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
	if _, err := StartTLS(c, c.Local.Address, g.issue(c.Local.ID)); err == nil {
		t.Fatal("recovered nonvoter")
	}
	after, _ = os.ReadFile(filepath.Join(c.StateDir, "raft.db"))
	if !bytes.Equal(before, after) {
		t.Fatal("nonvoter refusal modified store")
	}
}

func TestSingletonRecoverySnapshotOnly(t *testing.T) {
	g := newRecoveryTLSGroup(t, false)
	n := g.nodes[g.leader(t, -1)]
	if err := n.Commit(context.Background(), transaction("sessions/snapshot", 0, `{"retained":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := n.raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	for _, node := range g.nodes {
		node.Close()
	}
	c := n.config
	c.InitialVoters = nil
	c.Local.Address = net.JoinHostPort("127.0.0.101", fmtInt(recoveryPort(t)))
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(c.StateDir, "raft.db")})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	if err := store.DeleteRange(first, last); err != nil {
		t.Fatal(err)
	}
	store.Close()
	writeSingleton(t, c) // exactly zero log LastIndex, with an existing snapshot
	issue := g.issue
	node, err := StartTLS(c, c.Local.Address, issue(c.Local.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	await(t, func() bool { return node.raft.State() == raft.Leader })
	if string(node.state.read([]string{"sessions/snapshot"})["sessions/snapshot"].Value) != `{"retained":true}` {
		t.Fatal("snapshot-only recovery lost metadata")
	}
}

package cluster

import (
	"bytes"
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestPassphraseProvisioningIsStableAndNodeLocal(t *testing.T) {
	t.Setenv(caBundleEnv, "")
	t.Setenv("DROP_CLUSTER_ENROLLMENT_KEY", "")
	secret := strings.Repeat("private-test-only-", 3)
	content := t.TempDir()
	first := t.TempDir()
	second := t.TempDir()
	_ = os.Chmod(first, 0700)
	_ = os.Chmod(second, 0700)
	h := HostIdentity{App: "testapp", IP: netip.MustParseAddr("127.0.0.1")}
	one, err := provisionAutomatic(h, secret, first, content)
	if err != nil {
		t.Fatal(err)
	}
	two, err := provisionAutomatic(h, secret, second, content)
	if err != nil {
		t.Fatal(err)
	}
	if one.ClusterID != two.ClusterID || one.Local.ID == two.Local.ID {
		t.Fatal("cluster/node identity mismatch")
	}
	ca1, _ := os.ReadFile(one.CA)
	ca2, _ := os.ReadFile(two.CA)
	key1, _ := os.ReadFile(one.Key)
	key2, _ := os.ReadFile(two.Key)
	if !bytes.Equal(ca1, ca2) || bytes.Equal(key1, key2) {
		t.Fatal("CA not shared or leaf key not unique")
	}
	h.IP = netip.MustParseAddr("127.0.0.2")
	restarted, err := provisionAutomatic(h, secret, first, content)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Local.ID != one.Local.ID || restarted.Local.Address == one.Local.Address {
		t.Fatal("restart/IP change lost identity")
	}
	keyAgain, _ := os.ReadFile(one.Key)
	if !bytes.Equal(key1, keyAgain) {
		t.Fatal("restart changed key")
	}
	if _, err := provisionAutomatic(h, strings.Repeat("wrong-secret", 4), first, content); err == nil {
		t.Fatal("changed secret silently reset cluster")
	}
	for _, path := range []string{restarted.Certificate, restarted.Key, restarted.CA, restarted.CABundleFile} {
		if filepath.Dir(path) != first {
			t.Fatal("persistent secret outside local volume", path)
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("unsafe secret permissions", path, err)
		}
	}
}

func TestPassphraseRejectsWeakSecretAndExistingUnconfiguredState(t *testing.T) {
	if _, err := passphraseBundle("testapp", "weak"); err == nil {
		t.Fatal("weak secret accepted")
	}
	state := t.TempDir()
	_ = os.Chmod(state, 0700)
	if err := os.WriteFile(filepath.Join(state, "raft.db"), []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := provisionAutomatic(HostIdentity{App: "testapp", IP: netip.MustParseAddr("127.0.0.1")}, strings.Repeat("secret-", 8), state, t.TempDir()); err == nil {
		t.Fatal("existing state reset")
	}
}

func TestAutomaticSingletonWaitsForFreshSelfAndStableView(t *testing.T) {
	state := t.TempDir()
	_ = os.Chmod(state, 0700)
	c := Config{ClusterID: testClusterID, Local: Member{ID: "first", Address: "127.0.0.1:18001"}, StateDir: state, ContentDir: t.TempDir(), Automatic: true}
	_, transport := raft.NewInmemTransport(raft.ServerAddress(c.Local.raftAddress()))
	n, err := start(c, transport, tuneTest)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	source := &staticPeers{fresh: false}
	a := &automatic{node: n, source: source}
	n.auto = a
	ctx := context.Background()
	now := time.Now()
	if err := a.tick(ctx, now); err == nil || n.raft.LastIndex() != 0 {
		t.Fatal("stale discovery bootstrapped")
	}
	source.fresh = true
	if err := a.tick(ctx, now); err == nil || n.raft.LastIndex() != 0 {
		t.Fatal("empty discovery bootstrapped")
	}
	source.addresses = []netip.AddrPort{netip.MustParseAddrPort("127.0.0.2:8446")}
	if err := a.tick(ctx, now); err == nil || n.raft.LastIndex() != 0 {
		t.Fatal("missing self bootstrapped")
	}
	source.addresses = []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:8446")}
	if err := a.tick(ctx, now); err != nil {
		t.Fatal(err)
	}
	if n.raft.LastIndex() != 0 {
		t.Fatal("unstable view bootstrapped")
	}
	if err := a.tick(ctx, now.Add(31*time.Second)); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return n.raft.State() == raft.Leader })
	if _, err := os.Stat(filepath.Join(state, "bootstrap-intent.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(c.ContentDir, "cluster-genesis.json")); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticDoesNotRebootstrapExistingContent(t *testing.T) {
	state := t.TempDir()
	_ = os.Chmod(state, 0700)
	content := t.TempDir()
	if err := os.WriteFile(filepath.Join(content, "cluster-genesis.json"), []byte(`{"clusterID":"`+testClusterID+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	c := Config{ClusterID: testClusterID, Local: Member{ID: "replacement", Address: "127.0.0.1:18001"}, StateDir: state, ContentDir: content, Automatic: true}
	_, transport := raft.NewInmemTransport(raft.ServerAddress(c.Local.raftAddress()))
	n, err := start(c, transport, tuneTest)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	source := &staticPeers{fresh: true, addresses: []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:8446")}}
	a := &automatic{node: n, source: source}
	now := time.Now()
	_ = a.tick(context.Background(), now)
	if err := a.tick(context.Background(), now.Add(time.Minute)); err == nil || n.raft.LastIndex() != 0 {
		t.Fatal("known old deployment silently reinitialized")
	}
}

func TestAutomaticRejectsForeignContentAndSurvivingHistory(t *testing.T) {
	t.Setenv(caBundleEnv, "")
	t.Setenv("DROP_CLUSTER_ENROLLMENT_KEY", "")
	state, content := t.TempDir(), t.TempDir()
	_ = os.Chmod(state, 0700)
	if err := os.WriteFile(filepath.Join(content, "cluster-genesis.json"), []byte(`{"clusterID":"ffffffffffffffffffffffffffffffff"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := provisionAutomatic(HostIdentity{App: "testapp", IP: netip.MustParseAddr("127.0.0.1")}, strings.Repeat("private-history-test-", 3), state, content); err == nil {
		t.Fatal("foreign content initialized")
	}
	content = t.TempDir()
	if err := os.WriteFile(filepath.Join(state, "primary-history.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	c := Config{ClusterID: testClusterID, Local: Member{ID: "old", Address: "127.0.0.1:18001"}, StateDir: state, ContentDir: content, Automatic: true}
	_, transport := raft.NewInmemTransport(raft.ServerAddress(c.Local.raftAddress()))
	n, err := start(c, transport, tuneTest)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	source := &staticPeers{fresh: true, addresses: []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:8446")}}
	a := &automatic{node: n, source: source}
	now := time.Now()
	_ = a.tick(context.Background(), now)
	if err := a.tick(context.Background(), now.Add(time.Minute)); err == nil || n.raft.LastIndex() != 0 {
		t.Fatal("surviving primary history ignored")
	}
}

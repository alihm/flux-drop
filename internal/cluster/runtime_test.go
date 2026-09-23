package cluster

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/replica"
	"github.com/runonflux/flux-drop/internal/session"
)

func TestRuntimeManifestDefaultsAndValidation(t *testing.T) {
	raw := `{"app":"testapp","clusterID":"` + testClusterID + `","local":{"id":"one","address":"127.0.0.1:8445"},"selfIPs":["127.0.0.1"],"certificate":"/run/secrets/peer.crt","key":"/run/secrets/peer.key","ca":"/run/secrets/ca.crt"}`
	c, err := DecodeRuntimeConfig([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if c.StateDir != "/var/lib/drop-cluster" || c.ContentDir != "/data" || c.StatusPort != 8446 || len(c.InitialVoters) != 0 {
		t.Fatal(c)
	}
	for _, bad := range []string{raw + `{}`, strings.Replace(raw, `"app":`, `"unknown":`, 1), strings.Replace(raw, `"127.0.0.1:8445"`, `"evil.example:8445"`, 1), strings.Replace(raw, `"selfIPs":["127.0.0.1"]`, `"selfIPs":[]`, 1)} {
		if _, err := DecodeRuntimeConfig([]byte(bad)); err == nil {
			t.Fatal("accepted", bad)
		}
	}
	c.StatusListen = "0.0.0.0:8445"
	if err := c.validate(); err == nil {
		t.Fatal("listener collision accepted")
	}
	c.StatusListen = "0.0.0.0:8080"
	if err := c.validate(); err == nil {
		t.Fatal("public port accepted")
	}
}

func TestRuntimeManifestMustNotBeSynchronized(t *testing.T) {
	content := t.TempDir()
	state := t.TempDir()
	c := RuntimeConfig{App: "testapp", ClusterID: testClusterID, Local: Member{ID: "one", Address: "127.0.0.1:8445"}, ContentDir: content, StateDir: state, SelfIPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}, Certificate: "/run/secrets/peer.crt", Key: "/run/secrets/peer.key", CA: "/run/secrets/ca.crt"}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	inside := filepath.Join(content, "cluster.json")
	if err := os.WriteFile(inside, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuntimeConfig(inside); err == nil {
		t.Fatal("synchronized manifest accepted")
	}
	outside := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(outside, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuntimeConfig(outside); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(outside, 0666); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuntimeConfig(outside); err == nil {
		t.Fatal("writable manifest accepted")
	}
}

// Exercise the actual runtime constructor without contacting the public Flux
// API. Discovery parsing/refresh is separately covered in internal/replica.
func TestCoordinatorRuntimeLifecycle(t *testing.T) {
	issue := materials(t)
	configs := make([]RuntimeConfig, 3)
	var manifest []Member
	for i, id := range []string{"one", "two", "three"} {
		// Reserve then release loopback ports for the real runtime listeners.
		raftListener := listener(t)
		statusListener := listener(t)
		address := raftListener.Addr().String()
		statusAddress := statusListener.Addr().String()
		_ = raftListener.Close()
		_ = statusListener.Close()
		root := t.TempDir()
		_ = os.Chmod(root, 0700)
		secrets := t.TempDir()
		mat := issue(id)
		cert := mat.Client.Certificates[0]
		certPath, keyPath, caPath := filepath.Join(secrets, "peer.crt"), filepath.Join(secrets, "peer.key"), filepath.Join(secrets, "ca.crt")
		if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600); err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
			t.Fatal(err)
		}
		// Test certificate helper includes its CA as the second chain element.
		if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[1]}), 0600); err != nil {
			t.Fatal(err)
		}
		statusIP, _ := netip.ParseAddrPort(statusAddress)
		configs[i] = RuntimeConfig{App: "testapp", ClusterID: testClusterID, Local: Member{ID: id, Address: address}, StateDir: root, ContentDir: t.TempDir(), Listen: address, StatusListen: statusAddress, StatusPort: statusIP.Port(), SelfIPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}, Certificate: certPath, Key: keyPath, CA: caPath}
		manifest = append(manifest, configs[i].Local)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var runtimes []*Runtime
	for _, c := range configs {
		c.InitialVoters = manifest
		r, err := startRuntime(ctx, c, func(ctx context.Context, _ *replica.Discovery) { <-ctx.Done() })
		if err != nil {
			t.Fatal(err)
		}
		runtimes = append(runtimes, r)
		t.Cleanup(func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.Close(shutdown); err != nil {
				t.Error(err)
			}
		})
	}
	client, err := replica.NewPeerClient(issue("inspector").Client)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	leader := -1
	await(t, func() bool {
		for i, r := range runtimes {
			if r.Node.Status().Role == "Leader" && r.Node.Ready(context.Background()) == nil {
				leader = i
				return true
			}
		}
		return false
	})
	for i, c := range configs {
		// All three processes run on loopback with different test ports; production
		// nodes use distinct IPs and the same externally mapped status port.
		leaderStatus := netip.MustParseAddrPort(configs[leader].StatusListen)
		c.StatusPort = leaderStatus.Port()
		metadataClient, err := NewClient(c)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(metadataClient.Close)
		store := &metadata.Store{Backend: metadataClient}
		sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
		token, view, err := sessions.Create(ctx)
		if err != nil {
			t.Fatal("session through local coordinator/leader", err)
		}
		actor, err := project.ActorFrom(token, view)
		if err != nil {
			t.Fatal(err)
		}
		repo := &project.RaftRepository{Store: store}
		digest := strings.Repeat(string(rune('a'+i)), 64)
		prepared, err := repo.Reserve(ctx, actor, project.Reservation{Key: "rpc_operation", Name: "rpc", Digest: digest, Bytes: 10})
		if err != nil {
			t.Fatal("reserve through leader", err)
		}
		published, err := repo.Activate(ctx, actor, prepared.Operation.ID)
		if err != nil {
			t.Fatal("activate through leader", err)
		}
		if owned, e := repo.GetOwned(ctx, actor, published.ID); e != nil || owned.Owner.ID != actor.AnonymousID || owned.ChargedBytes == 0 {
			t.Fatal("durable private fields", owned, e)
		}
		if _, _, err := sessions.Logout(ctx, token, view.Record.CSRF); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.GetOwned(ctx, actor, published.ID); err == nil {
			t.Fatal("rotated session can still manage project")
		}
		if err := CheckHealth(ctx, c); err != nil {
			t.Fatal("healthy coordinator rejected", err)
		}
		wrongNode := c
		wrongNode.StatusListen = configs[(i+1)%len(configs)].StatusListen
		if err := CheckHealth(ctx, wrongNode); err == nil {
			t.Fatal("health check accepted another coordinator")
		}
		wrongCluster := c
		wrongCluster.ClusterID = "ffffffffffffffffffffffffffffffff"
		if err := CheckHealth(ctx, wrongCluster); err == nil {
			t.Fatal("health check accepted another cluster")
		}
		req, _ := http.NewRequest("GET", "https://"+c.StatusListen+StatusPath, nil)
		req.Header.Set("X-Drop-Cluster", testClusterID)
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var status Status
		err = json.NewDecoder(response.Body).Decode(&status)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != 200 || status.ID != c.Local.ID {
			t.Fatal(status, err)
		}
		req, _ = http.NewRequest("GET", "https://"+c.StatusListen+"/readyz", nil)
		req.Header.Set("X-Drop-Cluster", testClusterID)
		response, err = client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		want := 503
		if i == leader {
			want = 200
		}
		if response.StatusCode != want {
			t.Fatal("incorrect readiness", i, response.StatusCode)
		}
	}
	// Confirm listeners are actual TCP services, not in-memory transport hooks.
	conn, err := net.DialTimeout("tcp", configs[leader].Listen, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

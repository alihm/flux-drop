package cluster

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/analytics"
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
	// Match production: distinct IPs and common ports. A leader hint names a
	// member IP; overriding its status port with the initial leader's port on
	// one shared IP makes later elections route to the wrong TLS identity.
	raftReservation, statusReservation := listener(t), listener(t)
	raftPort := netip.MustParseAddrPort(raftReservation.Addr().String()).Port()
	statusPort := netip.MustParseAddrPort(statusReservation.Addr().String()).Port()
	_ = raftReservation.Close()
	_ = statusReservation.Close()
	for i, id := range []string{"one", "two", "three"} {
		ip := netip.MustParseAddr("127.0.0." + fmtInt(61+i))
		address := netip.AddrPortFrom(ip, raftPort).String()
		statusAddress := netip.AddrPortFrom(ip, statusPort).String()
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
		configs[i] = RuntimeConfig{App: "testapp", ClusterID: testClusterID, Local: Member{ID: id, Address: address}, StateDir: root, ContentDir: t.TempDir(), Listen: address, StatusListen: statusAddress, StatusPort: statusIP.Port(), SelfIPs: []netip.Addr{ip}, Certificate: certPath, Key: keyPath, CA: caPath}
		manifest = append(manifest, configs[i].Local)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var runtimes []*Runtime
	for _, c := range configs {
		c.InitialVoters = manifest
		r, err := startRuntimeWithSetup(ctx, c, func(ctx context.Context, _ *replica.Discovery) { <-ctx.Done() }, isolateRecoveryDiscovery)
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
		metadataClient, err := NewClient(c)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(metadataClient.Close)
		store := &metadata.Store{Backend: metadataClient}
		sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
		var token string
		var view session.View
		// Failed fixture allocations are disposable; publish only with the
		// final successfully acknowledged session, never an uncertain token.
		awaitRuntimeSetup(t, "session through local coordinator/leader", func() error {
			var err error
			token, view, err = sessions.Create(ctx)
			return err
		})
		actor, err := project.ActorFrom(token, view)
		if err != nil {
			t.Fatal(err)
		}
		repo := &project.RaftRepository{Store: store}
		// The session request cached a leader. Prove that authenticated routing
		// follows a real leadership transfer without changing client config.
		metadataClient.mu.Lock()
		initialLeader := metadataClient.leaderID
		metadataClient.mu.Unlock()
		leader = -1
		for j, candidate := range configs {
			if candidate.Local.ID == initialLeader {
				leader = j
			}
		}
		if leader < 0 {
			t.Fatal("session request did not cache an admitted leader", initialLeader)
		}
		next := (leader + 1) % len(runtimes)
		awaitFor(t, 15*time.Second, func() bool {
			if runtimes[next].Node.raft.State() == raft.Leader && runtimes[next].Node.Ready(ctx) == nil {
				return true
			}
			for _, current := range runtimes {
				if current.Node.raft.State() != raft.Leader || current.Node.Ready(ctx) != nil {
					continue
				}
				err := current.Node.raft.LeadershipTransferToServer(raft.ServerID(configs[next].Local.ID), raft.ServerAddress(configs[next].Local.raftAddress())).Error()
				if err != nil && !errors.Is(err, raft.ErrNotLeader) && !errors.Is(err, raft.ErrLeadershipLost) {
					t.Fatal("transfer cached leader", err)
				}
				break
			}
			return false
		})
		leader = next
		digest := strings.Repeat(string(rune('a'+i)), 64)
		var prepared project.Prepared
		awaitRuntimeSetup(t, "reserve through leader", func() error {
			var err error
			prepared, err = repo.Reserve(ctx, actor, project.Reservation{Key: "rpc_operation", Name: "rpc", Digest: digest, Bytes: 10})
			return err
		})
		var published project.Project
		awaitRuntimeSetup(t, "activate through leader", func() error {
			var err error
			published, err = repo.Activate(ctx, actor, prepared.Operation.ID)
			return err
		})
		metadataClient.mu.Lock()
		followed := metadataClient.leaderID == configs[leader].Local.ID
		metadataClient.mu.Unlock()
		if !followed {
			t.Fatal("client did not follow the new leader")
		}
		awaitRuntimeSetup(t, "read durable private fields", func() error {
			owned, e := repo.GetOwned(ctx, actor, published.ID)
			if e != nil {
				return e
			}
			if owned.Owner.ID != actor.AnonymousID || owned.ChargedBytes == 0 {
				t.Fatal("durable private fields", owned)
			}
			return nil
		})

		// Real mTLS local reads target this coordinator even when the client has
		// already cached another leader. They never poison management routing.
		await(t, func() bool {
			records, _, e := runtimes[i].Node.localSnapshot(ctx, []string{"projects/" + published.ID}, nil)
			if e != nil || records["projects/"+published.ID].Version == 0 {
				return false
			}
			var applied project.Project
			if e := metadata.Decode(records["projects/"+published.ID].Value, &applied); e != nil {
				t.Fatal("decode local project", e)
			}
			// Reserve already creates a record. The follower must have applied
			// Activate before a positive serving lookup can be asserted.
			return applied.Revision == published.Revision && applied.Status == "active" && applied.ActiveDigest == published.ActiveDigest
		})
		metadataClient.mu.Lock()
		cachedLeader := metadataClient.leaderID
		metadataClient.mu.Unlock()
		before := metadataClient.RPCMetrics()
		serving, e := repo.ResolveForServing(ctx, published.Slug)
		if e != nil || serving.ID != published.ID {
			t.Fatal("local serving RPC", serving, e)
		}
		after := metadataClient.RPCMetrics()
		if after["local_snapshot_local"]-before["local_snapshot_local"] != 2 {
			t.Fatal("public resolution exceeded two local RPCs", before, after)
		}
		if after["snapshot_read_leader"] != before["snapshot_read_leader"] {
			t.Fatal("positive local lookup contacted leader")
		}
		metadataClient.mu.Lock()
		sameLeader := cachedLeader == metadataClient.leaderID
		metadataClient.mu.Unlock()
		if !sameLeader {
			t.Fatal("local RPC poisoned leader routing")
		}
		// Page-view submission traverses the same real mTLS membership/routing
		// boundary, but never creates a metadata transaction or Raft record.
		now := time.Now().UTC()
		var hourly [24]uint64
		hourly[now.Hour()] = uint64(i + 1)
		snapshot := analytics.Snapshot{Node: c.Local.ID, Producer: strings.Repeat("a", 32), Day: analytics.Day(now).Format("2006-01-02"), Shard: strings.IndexByte("0123456789abcdef", published.ID[0]), Sequence: 1, UpdatedAt: now, Projects: map[string][24]uint64{published.ID: hourly}}
		awaitRuntimeSetup(t, "authenticated analytics submission", func() error { return metadataClient.SubmitPageViews(ctx, snapshot) })
		awaitRuntimeSetup(t, "analytics retry", func() error { return metadataClient.SubmitPageViews(ctx, snapshot) })
		awaitRuntimeSetup(t, "query analytics", func() error {
			result, e := metadataClient.PageViews(ctx, analytics.Query{ProjectID: published.ID, From: analytics.Day(now), To: analytics.Day(now).Add(24 * time.Hour), Interval: "hour"})
			if e != nil {
				return e
			}
			if result.PageViews != uint64(i+1) {
				t.Fatal("analytics RPC result", result)
			}
			return nil
		})
		snapshot.Node = "impersonated"
		if _, e := metadataClient.call(ctx, rpcRequest{Method: "analytics_put", Analytics: &snapshot}); e != ErrInvalid {
			t.Fatal("server accepted analytics node spoof", e)
		}
		if e := metadataClient.SubmitPageViews(ctx, snapshot); e != ErrInvalid {
			t.Fatal("analytics node spoof accepted", e)
		}
		logoutAttempted := false
		awaitRuntimeSetup(t, "logout", func() error {
			if logoutAttempted {
				// A lost acknowledgement must not blindly rotate twice. Confirm
				// the old record through the real RPC store before retrying.
				digest, _ := session.Digest(token)
				record, e := sessions.Store.Get(ctx, digest)
				if e != nil {
					return e
				}
				if record.Revoked {
					return nil
				}
			}
			logoutAttempted = true
			_, _, e := sessions.Logout(ctx, token, view.Record.CSRF)
			return e
		})
		awaitRuntimeSetup(t, "confirm revoked owner access", func() error {
			_, e := repo.GetOwned(ctx, actor, published.ID)
			if errors.Is(e, ErrNotLeader) {
				return e
			}
			if !errors.Is(e, session.ErrUnauthorized) {
				t.Fatal("rotated session did not get an authentication refusal", e)
			}
			return nil
		})
		// Both observation-only paths ignore pending operator requests, including
		// deliberately malformed ones that would fail a normal coordinator startup.
		for _, name := range []string{"recover-singleton.json", "restore-singleton.json"} {
			path := filepath.Join(c.StateDir, name)
			if err := os.WriteFile(path, []byte("pending operator review"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := LocalStatus(ctx, c); err != nil {
			t.Fatal("status attempted recovery", err)
		}
		if err := CheckHealth(ctx, c); err != nil {
			t.Fatal("healthy coordinator rejected", err)
		}
		for _, name := range []string{"recover-singleton.json", "restore-singleton.json"} {
			path := filepath.Join(c.StateDir, name)
			raw, err := os.ReadFile(path)
			if err != nil || string(raw) != "pending operator review" {
				t.Fatal("health/status consumed request", err)
			}
			os.Remove(path)
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
		awaitRuntimeSetup(t, "readiness", func() error {
			response, err := client.Do(req)
			if err != nil {
				return err
			}
			_ = response.Body.Close()
			want := 503
			if i == leader {
				want = 200
				if response.StatusCode == 503 {
					return ErrNotLeader
				}
			}
			if response.StatusCode != want {
				t.Fatal("incorrect readiness", i, response.StatusCode)
			}
			return nil
		})
	}
	// Confirm listeners are actual TCP services, not in-memory transport hooks.
	conn, err := net.DialTimeout("tcp", configs[leader].Listen, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

// Use only for disposable fixture allocation, read-only calls, or operations
// with explicit idempotency/confirmation. The 750ms authority proof may expire
// during a multi-RPC request, particularly around a leadership change. Only that
// fail-closed response is retried; identity errors and assertions fail at once.
func awaitRuntimeSetup(t *testing.T, operation string, run func() error) {
	t.Helper()
	awaitFor(t, 15*time.Second, func() bool {
		err := run()
		if err != nil && !errors.Is(err, ErrNotLeader) {
			t.Fatalf("%s: %v", operation, err)
		}
		return err == nil
	})
}

// Constructor tests keep the real stores/listeners while explicitly injecting
// discovery failure for the new observer, rather than contacting the Flux API.
func isolateRecoveryDiscovery(r *Runtime) {
	r.Node.recoveryManager.source = &staticPeers{fresh: false}
	r.Node.recoveryManager.refresh = func(context.Context) error { return replica.ErrDiscovery }
}

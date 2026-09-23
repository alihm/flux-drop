package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/replica"
)

func TestEnrollmentCredentialStorage(t *testing.T) {
	content := t.TempDir()
	secret := make([]byte, 32)
	_, _ = rand.Read(secret)
	encoded := []byte(base64.StdEncoding.EncodeToString(secret))
	path := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadEnrollmentCredential(path, content); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadEnrollmentCredential(path, content); err == nil {
		t.Fatal("readable secret accepted")
	}
	inside := filepath.Join(content, "credential")
	if err := os.WriteFile(inside, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadEnrollmentCredential(inside, content); err == nil {
		t.Fatal("replicated secret accepted")
	}
}

func TestPrivateEnrollmentAndReplacement(t *testing.T) {
	g := newTestGroup(t)
	leader := g.leader(t, -1)
	n := g.nodes[leader]
	ctx := context.Background()
	issue := materials(t)
	localAddress := netip.MustParseAddrPort(n.config.Local.Address)
	m := Member{ID: "replacement", Address: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.2"), localAddress.Port()).String()}
	_, tr := raft.NewInmemTransport(raft.ServerAddress(m.raftAddress()))
	root := t.TempDir()
	_ = os.Chmod(root, 0700)
	learner, err := start(Config{ClusterID: testClusterID, Local: m, StateDir: root, ContentDir: t.TempDir()}, tr, tuneTest)
	if err != nil {
		t.Fatal(err)
	}
	defer learner.Close()
	for _, other := range g.transports {
		other.Connect(tr.LocalAddr(), tr)
		tr.Connect(other.LocalAddr(), other)
	}
	status := httptest.NewUnstartedServer(learner.StatusHandler("testapp"))
	_ = status.Listener.Close()
	status.Listener, err = net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Fatal(err)
	}
	status.TLS = issue(m.ID).Server.Clone()
	status.StartTLS()
	defer status.Close()
	address := netip.MustParseAddrPort(status.Listener.Addr().String())
	source := &staticPeers{addresses: []netip.AddrPort{address}, fresh: true}
	observer, err := NewObserver(source, testClusterID, "testapp", issue(n.config.Local.ID).Client)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.client.CloseIdleConnections()
	if err = observer.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	e := &enrollment{node: n, observer: observer, port: address.Port(), missing: make(map[string]time.Time), now: func() time.Time { return now }, grace: 5 * time.Minute}
	_, _ = rand.Read(e.credential[:])
	n.enrollment = e
	server := httptest.NewUnstartedServer(n.StatusHandler("testapp"))
	server.TLS = issue(n.config.Local.ID).Server.Clone()
	server.StartTLS()
	defer server.Close()
	client, err := replica.NewPeerClient(issue(m.ID).Client)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	send := func(token string) int {
		data, _ := json.Marshal(m)
		r, _ := http.NewRequest(http.MethodPost, server.URL+enrollmentPath, bytes.NewReader(data))
		r.Header.Set("X-Drop-Cluster", testClusterID)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+token)
		res, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	if code := send(base64.StdEncoding.EncodeToString(make([]byte, 32))); code != 403 {
		t.Fatal("wrong credential", code)
	}
	if len(n.raft.GetConfiguration().Configuration().Servers) != 3 {
		t.Fatal("unauthorized membership change")
	}
	credential := base64.StdEncoding.EncodeToString(e.credential[:])
	if code := send(credential); code != 204 {
		t.Fatal("enrollment failed", code)
	}
	if code := send(credential); code != 204 {
		t.Fatal("learner admission not idempotent", code)
	}
	if err := e.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	for _, s := range n.raft.GetConfiguration().Configuration().Servers {
		if string(s.ID) == m.ID && s.Suffrage != raft.Nonvoter {
			t.Fatal("promoted before failure grace")
		}
	}
	now = now.Add(6 * time.Minute)
	await(t, func() bool {
		return e.reconcile(ctx) == nil && func() bool {
			for _, s := range n.raft.GetConfiguration().Configuration().Servers {
				if string(s.ID) == m.ID {
					return s.Suffrage == raft.Voter
				}
			}
			return false
		}()
	})
	if err := e.reconcile(ctx); err != nil {
		t.Fatal("retire old voter", err)
	}
	configuration := n.raft.GetConfiguration().Configuration()
	voters := 0
	for _, s := range configuration.Servers {
		if s.Suffrage == raft.Voter {
			voters++
		}
	}
	if voters != 3 {
		t.Fatal("replacement voter count", voters)
	}
	if code := send(credential); code == 204 {
		t.Fatal("existing voter identity allowed to reenroll lost local state")
	}
}

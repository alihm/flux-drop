package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/replica"
)

func materials(t *testing.T) func(string) TLSMaterial {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "cluster-test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	serial := int64(1)
	return func(id string) TLSMaterial {
		serial++
		pub, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		identity, _ := url.Parse("spiffe://flux-drop/testapp/" + id)
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"testapp.peer.flux-drop"}, URIs: []*url.URL{identity}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, key)
		if err != nil {
			t.Fatal(err)
		}
		client, server, err := replica.PeerTLS("testapp", id, tls.Certificate{Certificate: [][]byte{der, ca.Raw}, PrivateKey: private}, roots)
		if err != nil {
			t.Fatal(err)
		}
		return TLSMaterial{App: "testapp", Client: client, Server: server}
	}
}

func listener(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestTLSClusterElectionAndPinnedIdentity(t *testing.T) {
	issue := materials(t)
	listeners := []net.Listener{listener(t), listener(t), listener(t)}
	manifest := make([]Member, 3)
	for i, l := range listeners {
		manifest[i] = Member{ID: fmt.Sprintf("node%d", i), Address: l.Addr().String()}
	}
	var nodes []*Node
	var streams []*tlsStream
	for i, l := range listeners {
		root := t.TempDir()
		_ = os.Chmod(root, 0700)
		c := Config{ClusterID: testClusterID, Local: manifest[i], StateDir: root, ContentDir: t.TempDir(), InitialVoters: manifest}
		stream, err := newStream(l, c, issue(c.Local.ID))
		if err != nil {
			t.Fatal(err)
		}
		tr := raft.NewNetworkTransport(stream, 2, time.Second, io.Discard)
		n, err := start(c, tr, tuneTest)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, n)
		streams = append(streams, stream)
		t.Cleanup(func() { _ = n.Close() })
	}
	var leader *Node
	await(t, func() bool {
		for _, n := range nodes {
			if n.raft.State() == raft.Leader && n.Ready(context.Background()) == nil {
				leader = n
				return true
			}
		}
		return false
	})
	if err := leader.Commit(context.Background(), transaction("projects/tls", 0, `{"owner":"secure"}`)); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		for _, n := range nodes {
			if len(n.state.read([]string{"projects/tls"})) != 1 {
				return false
			}
		}
		return true
	})
	if conn, err := streams[0].Dial(raft.ServerAddress("wrong@"+manifest[1].Address), time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("accepted wrong destination certificate ID")
	}
	// A trusted certificate for the same app must not bridge different clusters.
	other := &tlsStream{material: streams[0].material, protocol: "flux-drop-raft/1/" + strings.Repeat("f", 32)}
	if conn, err := other.Dial(raft.ServerAddress(manifest[1].raftAddress()), time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("cross-cluster transport accepted")
	}
	noCertificate := streams[0].material.Client.Clone()
	noCertificate.Certificates = nil
	noCertificate.NextProtos = []string{streams[0].protocol}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", manifest[1].Address, noCertificate)
	if err == nil {
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		_, err = conn.Read(make([]byte, 1))
		_ = conn.Close()
		if err == nil {
			t.Fatal("unauthenticated Raft connection accepted")
		}
	}
}

type staticPeers struct {
	addresses []netip.AddrPort
	fresh     bool
}

func (s *staticPeers) Snapshot() ([]netip.AddrPort, bool) { return s.addresses, s.fresh }

func TestAuthenticatedStatusAndDiscoveryNeverChangeMembership(t *testing.T) {
	g := newTestGroup(t)
	n := g.nodes[g.leader(t, -1)]
	issue := materials(t)
	serverMaterial := issue(n.config.Local.ID)
	server := httptest.NewUnstartedServer(n.StatusHandler("testapp"))
	server.TLS = serverMaterial.Server.Clone()
	server.StartTLS()
	defer server.Close()
	address, err := netip.ParseAddrPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	source := &staticPeers{[]netip.AddrPort{address}, true}
	observer, err := NewObserver(source, testClusterID, "testapp", issue("inspector").Client)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.client.CloseIdleConnections()
	if err := observer.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	observations, fresh := observer.Snapshot()
	if !fresh || len(observations) != 1 || observations[0].Status.ID != n.config.Local.ID || observations[0].Status.QuorumVerified {
		t.Fatal(observations, fresh)
	}
	before := n.raft.GetConfiguration().Configuration()
	source.fresh = false
	if err := observer.Refresh(context.Background()); err == nil {
		t.Fatal("stale discovery accepted")
	}
	if _, fresh := observer.Snapshot(); fresh {
		t.Fatal("stale status retained")
	}
	after := n.raft.GetConfiguration().Configuration()
	if len(before.Servers) != 3 || len(after.Servers) != 3 {
		t.Fatal("discovery changed voters")
	}
	for _, tc := range []struct {
		path, cluster, cookie, method string
		status                        int
	}{
		{StatusPath, testClusterID, "", "GET", 200},
		{"/healthz", testClusterID, "", "GET", 200},
		{"/readyz", testClusterID, "", "GET", 200},
		{StatusPath, "wrong", "", "GET", 400},
		{StatusPath, testClusterID, "session=secret", "GET", 400},
		{StatusPath, testClusterID, "", "POST", 400},
		{StatusPath + "?secret=x", testClusterID, "", "GET", 400},
	} {
		r, _ := http.NewRequest(tc.method, server.URL+tc.path, nil)
		r.Header.Set("X-Drop-Cluster", tc.cluster)
		if tc.cookie != "" {
			r.Header.Set("Cookie", tc.cookie)
		}
		res, err := observer.client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != tc.status {
			t.Fatal(tc, res.StatusCode)
		}
	}
	w := httptest.NewRecorder()
	n.StatusHandler("testapp").ServeHTTP(w, httptest.NewRequest("GET", StatusPath, nil))
	if w.Code != 403 {
		t.Fatal("status exposed without TLS")
	}
}

func TestLearnerAdmissionCatchupPromotionAndReplacement(t *testing.T) {
	g := newTestGroup(t)
	leader := g.leader(t, -1)
	n := g.nodes[leader]
	if err := n.Commit(context.Background(), transaction("sessions/retained", 0, `{"revoked":true}`)); err != nil {
		t.Fatal(err)
	}
	learnerMember := Member{ID: "four", Address: "127.0.0.1:18004"}
	_, tr := raft.NewInmemTransport(raft.ServerAddress(learnerMember.raftAddress()))
	root := t.TempDir()
	_ = os.Chmod(root, 0700)
	learner, err := start(Config{ClusterID: testClusterID, Local: learnerMember, StateDir: root, ContentDir: t.TempDir()}, tr, tuneTest)
	if err != nil {
		t.Fatal(err)
	}
	defer learner.Close()
	for _, other := range g.transports {
		other.Connect(tr.LocalAddr(), tr)
		tr.Connect(other.LocalAddr(), other)
	}
	if err := n.AddLearner(context.Background(), learnerMember); err != nil {
		t.Fatal(err)
	}
	issue := materials(t)
	mat := issue("four")
	server := httptest.NewUnstartedServer(learner.StatusHandler("testapp"))
	server.TLS = mat.Server.Clone()
	server.StartTLS()
	defer server.Close()
	addr, _ := netip.ParseAddrPort(server.Listener.Addr().String())
	observer, err := NewObserver(&staticPeers{}, testClusterID, "testapp", issue("inspector").Client)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.client.CloseIdleConnections()
	await(t, func() bool {
		return string(learner.state.read([]string{"sessions/retained"})["sessions/retained"].Value) == `{"revoked":true}`
	})
	await(t, func() bool { return n.Promote(context.Background(), learnerMember, addr.Port(), observer) == nil })
	configuration := n.raft.GetConfiguration().Configuration()
	voters := 0
	for _, s := range configuration.Servers {
		if s.Suffrage == raft.Voter {
			voters++
		}
	}
	if voters != 4 {
		t.Fatal("learner not promoted", configuration)
	}
	remove := g.nodes[(leader+1)%3].config.Local.ID
	if err := n.RemoveMember(context.Background(), remove); err != nil {
		t.Fatal(err)
	}
	if err := n.RemoveMember(context.Background(), "four"); err == nil {
		t.Fatal("allowed shrinking below three voters")
	}
}

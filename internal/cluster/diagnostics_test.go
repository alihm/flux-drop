package cluster

import (
	"bytes"
	"context"
	"log/slog"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestClusterDiagnosticsAreRateLimitedAndObservational(t *testing.T) {
	g := newRecoveryTLSGroup(t, false)
	leader := g.leader(t, -1)
	survivor := g.nodes[leader]
	for i, n := range g.nodes {
		if i != leader {
			n.Close()
		}
	}
	await(t, func() bool { return survivor.raft.State() != raft.Leader })
	issue := g.issue
	observer, err := NewObserver(&staticPeers{fresh: true}, testClusterID, "testapp", issue(survivor.config.Local.ID).Client)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.client.CloseIdleConnections()
	runtime := Runtime{Node: survivor, Observer: observer, statusPort: 19998}
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(old)
	now := time.Now()
	state := diagnosticState{}
	runtime.diagnosticTick(context.Background(), &state, now)
	if logs.Len() != 0 {
		t.Fatal("early diagnostic", logs.String())
	}
	runtime.diagnosticTick(context.Background(), &state, now.Add(121*time.Second))
	output := logs.String()
	if !strings.Contains(output, `"level":"ERROR"`) || !strings.Contains(output, "unreachable_voters") {
		t.Fatal(output)
	}
	for i, c := range g.configs {
		if i != leader && !strings.Contains(output, c.Local.ID) {
			t.Fatal("missing unreachable voter", output)
		}
	}
	logs.Reset()
	runtime.diagnosticTick(context.Background(), &state, now.Add(122*time.Second))
	if logs.Len() != 0 {
		t.Fatal("unbounded alerts")
	}
	// No request file is created and the committed membership is unchanged.
	configuration := survivor.raft.GetConfiguration()
	if err := configuration.Error(); err != nil {
		t.Fatal(err)
	}
	if len(configuration.Configuration().Servers) != 3 {
		t.Fatal("diagnostics changed membership")
	}
	for _, name := range []string{"recover-singleton.json", "restore-singleton.json"} {
		if _, err := os.Stat(survivor.config.StateDir + "/" + name); !os.IsNotExist(err) {
			t.Fatal("diagnostic triggered recovery")
		}
	}
}

func TestEmptyNodeAdmissionDiagnosticAndStatus(t *testing.T) {
	issue := materials(t)
	c := Config{ClusterID: testClusterID, Local: Member{ID: "empty", Address: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.101"), uint16(recoveryPort(t))).String()}, StateDir: t.TempDir(), ContentDir: t.TempDir()}
	os.Chmod(c.StateDir, 0700)
	n, err := StartTLS(c, c.Local.Address, issue(c.Local.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(old)
	state := diagnosticState{}
	r := Runtime{Node: n}
	r.diagnosticTick(context.Background(), &state, n.started.Add(301*time.Second))
	if !strings.Contains(logs.String(), "five minutes for admission") {
		t.Fatal(logs.String())
	}
	logs.Reset()
	r.diagnosticTick(context.Background(), &state, n.started.Add(302*time.Second))
	if logs.Len() != 0 {
		t.Fatal("admission log not rate limited")
	}
	status := n.Status()
	if status.Voters != 0 || status.Learners != 0 || status.LastContactAgeSeconds != nil || status.LastSuccessfulBackup != nil {
		t.Fatal(status)
	}
	if n.raft.LastIndex() != 0 {
		t.Fatal("diagnostic bootstrapped empty state")
	}
}

func TestAuthenticatedStatusIncludesBackupAndMembership(t *testing.T) {
	g := newRecoveryTLSGroup(t, false)
	n := g.nodes[g.leader(t, -1)]
	if err := n.Commit(context.Background(), transaction("sessions/status", 0, `true`)); err != nil {
		t.Fatal(err)
	}
	if _, err := n.writeMetadataBackup(strings.Repeat("status-secret-", 4), ""); err != nil {
		t.Fatal(err)
	}
	status := n.Status()
	if status.Voters != 3 || status.Learners != 0 || status.LastSuccessfulBackup == nil || status.LastContactAgeSeconds == nil || *status.LastContactAgeSeconds != 0 {
		t.Fatal(status)
	}
	issue := g.issue
	server := httptest.NewUnstartedServer(n.StatusHandler("testapp"))
	server.TLS = issue(n.config.Local.ID).Server.Clone()
	server.StartTLS()
	defer server.Close()
	observer, err := NewObserver(&staticPeers{fresh: true}, testClusterID, "testapp", issue("observer").Client)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.client.CloseIdleConnections()
	address := netip.MustParseAddrPort(server.Listener.Addr().String())
	observation, err := observer.probe(context.Background(), address)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Status.Voters != 3 || observation.Status.LastSuccessfulBackup == nil {
		t.Fatal(observation)
	}
}

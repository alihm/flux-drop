package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/replica"
)

// This is the previous wire protocol, kept only in a test fixture to prove
// rolling upgrades. TLS and its actual RPC/listener remain real.
type legacyRaftStream struct {
	net.Listener
	local     Member
	material  TLSMaterial
	clusterID string
}

func (s *legacyRaftStream) Addr() net.Addr { return streamAddress(s.local.raftAddress()) }
func (s *legacyRaftStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	member, err := parseAddress(string(address))
	if err != nil {
		return nil, err
	}
	config := s.material.Client.Clone()
	config.NextProtos = []string{"flux-drop-raft/1/" + s.clusterID}
	verify := config.VerifyConnection
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if err := verify(state); err != nil {
			return err
		}
		if replica.PeerIdentity(state.PeerCertificates[0], s.material.App) != member.ID {
			return ErrInvalid
		}
		return nil
	}
	return (&tls.Dialer{NetDialer: &net.Dialer{Timeout: timeout}, Config: config}).DialContext(context.Background(), "tcp", member.Address)
}
func legacyRaftClient(t *testing.T, h *churnCluster) *raft.NetworkTransport {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.200:0")
	if err != nil {
		t.Fatal(err)
	}
	material := h.issue("legacy")
	server := material.Server.Clone()
	server.NextProtos = []string{"flux-drop-raft/1/" + testClusterID}
	stream := &legacyRaftStream{Listener: tls.NewListener(listener, server), local: Member{ID: "legacy", Address: listener.Addr().String()}, material: material, clusterID: testClusterID}
	transport := raft.NewNetworkTransport(stream, 1, time.Second, io.Discard)
	t.Cleanup(func() { transport.Close() })
	return transport
}
func TestRaftGenerationRejectsOldLeaderBeforeRaftBytes(t *testing.T) {
	t.Setenv(PassphraseEnv, strings.Repeat("transport-generation-", 3))
	h := newChurnCluster(t)
	oldLeader := h.leader()
	h.step()
	legacy := legacyRaftClient(t, h)
	header := raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("legacy"), Addr: []byte(legacy.LocalAddr())}
	var response raft.AppendEntriesResponse
	if err := legacy.AppendEntries(raft.ServerID(oldLeader.config.Local.ID), raft.ServerAddress(oldLeader.config.Local.raftAddress()), &raft.AppendEntriesRequest{RPCHeader: header, Term: 1}, &response); err != nil {
		t.Fatal("generation-zero legacy RPC stopped working", err)
	}
	var survivor *churnNode
	for _, n := range h.nodes {
		if n != oldLeader {
			survivor = n
			break
		}
	}
	h.stop(survivor)
	// Keep the old two-voter majority alive and advance its term through real
	// leadership transfers, so the returning old leader is strictly higher term.
	for i := 0; i < 3; i++ {
		leader := h.leader()
		if err := leader.node.raft.LeadershipTransfer().Error(); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { next := h.leader(); return next != leader })
	}
	oldLeader = h.leader()
	oldTerm := oldLeader.node.raft.CurrentTerm()
	writeSingleton(t, survivor.config)
	h.start(survivor)
	await(t, func() bool {
		return survivor.node.raft.State() == raft.Leader && !survivor.node.generationPending.Load()
	})
	recoveredTerm := survivor.node.raft.CurrentTerm()
	if oldTerm <= recoveredTerm {
		t.Fatal("fixture did not create a higher old term", oldTerm, recoveredTerm)
	}
	if err := survivor.node.Commit(context.Background(), transaction("cluster_trace/new-history", 0, `true`)); err != nil {
		t.Fatal(err)
	}
	if err := oldLeader.node.Commit(context.Background(), transaction("cluster_trace/old-history", 0, `true`)); err != nil {
		t.Fatal(err)
	}
	// A legacy higher-term RPC cannot bypass the new generation handshake.
	if err := legacy.AppendEntries(raft.ServerID(survivor.config.Local.ID), raft.ServerAddress(survivor.config.Local.raftAddress()), &raft.AppendEntriesRequest{RPCHeader: header, Term: oldTerm + 100}, &response); err == nil {
		t.Fatal("legacy old history reached recovered Raft")
	}
	time.Sleep(2 * time.Second) // permit old majority replication retries
	if survivor.node.raft.State() != raft.Leader || survivor.node.raft.CurrentTerm() != recoveredTerm {
		t.Fatal("old higher-term history changed recovered leader")
	}
	if survivor.node.state.read([]string{"cluster_trace/old-history"})["cluster_trace/old-history"].Version != 0 {
		t.Fatal("old history merged into recovered FSM")
	}
	// Fresh-list absence fences the still-live old majority's application writes.
	onlyListed(h, survivor)
	for _, n := range h.nodes {
		if n != survivor {
			signedRecoveryMode(t, n, "on")
			m := recoveryTestManager(h, n)
			for i := 0; i <= int(autoRecoveryWindow/autoRecoveryInterval); i++ {
				_ = m.tick(context.Background())
				h.now = h.now.Add(autoRecoveryInterval)
			}
			if !n.node.selfFenced.Load() {
				t.Fatal("old majority not fenced", n.config.Local.ID)
			}
		}
	}
	if err := oldLeader.node.Commit(context.Background(), transaction("sessions/old-write", 0, `true`)); !errors.Is(err, ErrNotLeader) {
		t.Fatal("old majority accepted application write", err)
	}
	// The current Flux cohort uses new identities and receives the new history.
	for i := 0; i < 2; i++ {
		fresh := h.add(nil)
		h.awaitEnrollment(fresh)
	}
	h.awaitVoters(3)
	if err := survivor.node.Commit(context.Background(), transaction("sessions/new-write", 0, `true`)); err != nil {
		t.Fatal(err)
	}
	for _, n := range h.nodes[3:] {
		await(t, func() bool {
			return n.node.state.read([]string{"sessions/new-write"})["sessions/new-write"].Version != 0
		})
	}
}

func TestRaftHelloBoundedAndValidated(t *testing.T) {
	// Framing consumes exactly the preface and cannot swallow a following RPC.
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	hello := raftHello{ClusterID: testClusterID, NodeID: "peer", Recovery: recoveryGeneration{}}
	done := make(chan error, 1)
	go func() {
		if err := writeRaftHello(a, hello); err != nil {
			done <- err
			return
		}
		_, err := a.Write([]byte{42})
		done <- err
	}()
	got, err := readRaftHello(b)
	if err != nil || got != hello {
		t.Fatal(got, err)
	}
	var next [1]byte
	if _, err := io.ReadFull(b, next[:]); err != nil || next[0] != 42 {
		t.Fatal("preface consumed RPC", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

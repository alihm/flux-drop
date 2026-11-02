package cluster

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"
)

// This is a TLS-authenticated transport preface, not an admission API. A Raft
// peer can otherwise reconnect without ever using enrollment. RecoverCluster
// breaks the old quorum intersection, so lineage must gate Raft bytes as well.
type raftHello struct {
	ClusterID string             `json:"clusterID"`
	NodeID    string             `json:"nodeID"`
	Recovery  recoveryGeneration `json:"recovery"`
	Empty     bool               `json:"empty"`
}
type raftBinding struct {
	ClusterID string             `json:"clusterID"`
	NodeID    string             `json:"nodeID"`
	Recovery  recoveryGeneration `json:"recovery"`
}
type raftGenerationGuard struct {
	node    *Node
	mu      sync.Mutex
	binding *raftBinding
}

const raftBindingFile = "raft-generation.json"

func newRaftGenerationGuard(n *Node) (*raftGenerationGuard, error) {
	g := &raftGenerationGuard{node: n}
	if _, err := os.Lstat(filepath.Join(n.config.StateDir, generationPendingFile)); err == nil {
		n.generationPending.Store(true)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var binding raftBinding
	_, err := readOperatorRequest(filepath.Join(n.config.StateDir, raftBindingFile), &binding, "clusterID", "nodeID", "recovery")
	if err == nil {
		if binding.ClusterID != n.config.ClusterID || binding.NodeID != n.config.Local.ID || !validRecoveryGeneration(binding.Recovery) {
			return nil, ErrInvalid
		}
		g.binding = &binding
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	// This journal was consumed only after moving the previous history aside.
	// An empty/newly loading history must remain bound to its authorized epoch
	// across a crash before the generation command/snapshot has applied.
	var reset resetIntent
	_, err = readOperatorRequest(filepath.Join(n.config.StateDir, "rejoin-reset.json"), &reset, "clusterID", "nodeID", "directory", "moving", "recovery")
	if err == nil && !reset.Moving {
		if reset.ClusterID != n.config.ClusterID || reset.NodeID != n.config.Local.ID || !validRecoveryGeneration(reset.Recovery) {
			return nil, ErrInvalid
		}
		if g.binding == nil || reset.Recovery.Generation > g.binding.Recovery.Generation || reset.Recovery.Generation == g.binding.Recovery.Generation && reset.Recovery != g.binding.Recovery {
			if err := g.persistLocked(reset.Recovery); err != nil {
				return nil, err
			}
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	// NewRaft may have restored a snapshot older than the generation command;
	// followers cannot apply that tail until reconnecting. Recover the transport
	// promise from authenticated local Raft history before admitting any byte.
	known, err := g.node.recoveryGeneration()
	if err != nil {
		return nil, err
	}
	first, err := n.store.FirstIndex()
	if err != nil {
		return nil, err
	}
	last, err := n.store.LastIndex()
	if err != nil {
		return nil, err
	}
	for index := last; index >= first && index > 0; index-- {
		var entry raft.Log
		if err := n.store.GetLog(index, &entry); err != nil {
			return nil, err
		}
		if entry.Type != raft.LogCommand {
			continue
		}
		var command Transaction
		if decodeStrict(entry.Data, &command) != nil || validateTransaction(command) != nil {
			continue
		}
		for _, write := range command.Writes {
			if write.Key == recoveryGenerationKey && !write.Delete {
				var value recoveryGeneration
				if decodeStrict(write.Value, &value) != nil || !validRecoveryGeneration(value) {
					return nil, ErrInvalid
				}
				if value.Generation > known.Generation {
					known = value
				}
			}
		}
	}
	if g.binding == nil && known.Generation > 0 || g.binding != nil && known.Generation > g.binding.Recovery.Generation {
		if err := g.persistLocked(known); err != nil {
			return nil, err
		}
	}
	return g, nil
}
func validRecoveryGeneration(value recoveryGeneration) bool {
	return value.Generation == 0 && value.Lineage == "" || value.Generation > 0 && len(value.Lineage) == 32
}
func (g *raftGenerationGuard) persistLocked(value recoveryGeneration) error {
	binding := raftBinding{g.node.config.ClusterID, g.node.config.Local.ID, value}
	raw, _ := json.Marshal(binding)
	if err := replaceCertificate(filepath.Join(g.node.config.StateDir, raftBindingFile), raw); err != nil {
		return err
	}
	g.binding = &binding
	return nil
}
func (g *raftGenerationGuard) effectiveLocked(current recoveryGeneration) recoveryGeneration {
	if g.binding != nil && (g.binding.Recovery.Generation > current.Generation || g.binding.Recovery.Generation == current.Generation && g.binding.Recovery != current) {
		return g.binding.Recovery
	}
	return current
}
func (g *raftGenerationGuard) effective() (recoveryGeneration, error) {
	current, err := g.node.recoveryGeneration()
	if err != nil {
		return current, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if current.Generation > 0 && (g.binding == nil || current.Generation > g.binding.Recovery.Generation) {
		if err := g.persistLocked(current); err != nil {
			return current, err
		}
	}
	return g.effectiveLocked(current), nil
}
func (g *raftGenerationGuard) hello() (raftHello, error) {
	if g.node.generationPending.Load() {
		return raftHello{}, ErrNotLeader
	}
	current, err := g.effective()
	if err != nil {
		return raftHello{}, err
	}
	return raftHello{ClusterID: g.node.config.ClusterID, NodeID: g.node.config.Local.ID, Recovery: current, Empty: g.node.raft.LastIndex() == 0 && g.node.state.appliedIndex() == 0}, nil
}
func (g *raftGenerationGuard) member(id string) bool {
	future := g.node.raft.GetConfiguration()
	if future.Error() != nil {
		return false
	}
	for _, server := range future.Configuration().Servers {
		if string(server.ID) == id {
			return true
		}
	}
	return false
}
func (g *raftGenerationGuard) authorize(peer raftHello) (recoveryGeneration, error) {
	if g.node.generationPending.Load() || peer.ClusterID != g.node.config.ClusterID || peer.NodeID == "" || !validRecoveryGeneration(peer.Recovery) {
		return recoveryGeneration{}, ErrNotLeader
	}
	current, err := g.node.recoveryGeneration()
	if err != nil {
		return current, err
	}
	empty := g.node.raft.LastIndex() == 0 && g.node.state.appliedIndex() == 0
	g.mu.Lock()
	if g.binding == nil && empty {
		if err := g.persistLocked(peer.Recovery); err != nil {
			g.mu.Unlock()
			return current, err
		}
	}
	own := g.effectiveLocked(current)
	g.mu.Unlock()
	if own == peer.Recovery {
		return own, nil
	}
	// Only an admitted empty recipient may receive a newer history. Its other
	// end must bind to our epoch before delivering any Raft byte to its FSM.
	if peer.Empty && peer.Recovery.Generation < own.Generation && g.member(peer.NodeID) {
		return own, nil
	}
	return own, ErrNotLeader
}

// Pin an empty node before requesting admission, so an unrelated old leader
// cannot win a race to initialize its Raft transport. Never retarget live data;
// a different existing binding requires the same journaled reset as old logs.
func (g *raftGenerationGuard) prepareEnrollment(ctx context.Context, e *enrollment, p Observation) error {
	address, err := netip.ParseAddrPort(p.Address)
	if err != nil {
		return err
	}
	peer, err := e.observer.probeRecoveryView(ctx, address)
	if err != nil {
		return nil
	} // legacy generation-zero leaders remain compatible
	if peer.NodeID != p.Status.ID {
		return ErrInvalid
	}
	if peer.EvaluationState == "generation-pending" {
		return ErrNotLeader
	}
	target := peer.Recovery
	if peer.TransportRecovery != nil {
		target = *peer.TransportRecovery
	}
	current, err := g.node.recoveryGeneration()
	if err != nil {
		return err
	}
	g.mu.Lock()
	own := g.effectiveLocked(current)
	if g.binding == nil && g.node.raft.LastIndex() == 0 && g.node.state.appliedIndex() == 0 {
		err = g.persistLocked(target)
		g.mu.Unlock()
		return err
	}
	g.mu.Unlock()
	if own != target && target.Generation >= own.Generation {
		if e.mayReset(ctx) && g.node.controlPending.CompareAndSwap(false, true) {
			copy := target
			g.node.resetRecovery.Store(&copy)
			g.node.control <- true
		}
		return ErrNotLeader
	}
	return nil
}

func writeRaftHello(conn net.Conn, hello raftHello) error {
	raw, err := json.Marshal(hello)
	if err != nil || len(raw) > 2048 {
		return ErrInvalid
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(raw)))
	if _, err := conn.Write(header[:]); err != nil {
		return err
	}
	_, err = conn.Write(raw)
	return err
}
func readRaftHello(conn net.Conn) (raftHello, error) {
	var hello raftHello
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return hello, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > 2048 {
		return hello, ErrInvalid
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(conn, raw); err != nil {
		return hello, err
	}
	if decodePeerJSON(raw, &hello) != nil {
		return hello, ErrInvalid
	}
	return hello, nil
}

type generationConn struct {
	net.Conn
	guard    *raftGenerationGuard
	expected recoveryGeneration
	memberID string
}

func (c *generationConn) valid() bool {
	if c.guard.node.generationPending.Load() {
		return false
	}
	current, err := c.guard.effective()
	return err == nil && current == c.expected && (c.memberID == "" || c.guard.node.raft.State() != raft.Leader || c.guard.member(c.memberID))
}
func (c *generationConn) Read(p []byte) (int, error) {
	if !c.valid() {
		c.Conn.Close()
		return 0, ErrNotLeader
	}
	n, err := c.Conn.Read(p)
	if !c.valid() {
		c.Conn.Close()
		return 0, ErrNotLeader
	}
	return n, err
}
func (c *generationConn) Write(p []byte) (int, error) {
	if !c.valid() {
		c.Conn.Close()
		return 0, ErrNotLeader
	}
	n, err := c.Conn.Write(p)
	if !c.valid() {
		c.Conn.Close()
		return 0, ErrNotLeader
	}
	return n, err
}

// NetworkTransport exposes the stream only inside this package; production and
// TLS tests both bind the guard before Raft starts exchanging protocol bytes.
type generationTransport struct {
	*raft.NetworkTransport
	stream *tlsStream
}

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"
)

const operationTimeout = 5 * time.Second

type Node struct {
	raft       *raft.Raft
	state      *state
	store      *raftboltdb.BoltStore
	transport  raft.Transport
	config     Config
	started    time.Time
	operations chan struct{}
	changeMu   sync.Mutex
	closeOnce  sync.Once
	closeErr   error
	enrollment *enrollment // configured before listeners/workers start
	async      *asyncWriter
	auto       *automatic
}

// start accepts a transport only internally; production callers use StartTLS.
// Tests use Raft's in-memory transport to simulate symmetric/asymmetric partitions.
func start(c Config, transport raft.Transport, tune func(*raft.Config)) (*Node, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if string(transport.LocalAddr()) != c.Local.raftAddress() {
		return nil, errors.New("transport local identity mismatch")
	}
	if err := prepareDirectory(c); err != nil {
		return nil, err
	}
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(c.StateDir, "raft.db"), BoltOptions: &bbolt.Options{Timeout: time.Second}})
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = store.Close()
		}
	}()
	logger := hclog.New(&hclog.LoggerOptions{Name: "drop-raft", Level: hclog.Warn, Output: os.Stderr})
	snapshots, err := raft.NewFileSnapshotStoreWithLogger(c.StateDir, 2, logger)
	if err != nil {
		return nil, err
	}
	existing, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		return nil, err
	}
	if err := bindIdentity(c, existing); err != nil {
		return nil, err
	}
	fsm := newState(c.ClusterID)
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(c.Local.ID)
	rc.HeartbeatTimeout = 2 * time.Second
	rc.ElectionTimeout = 2 * time.Second
	rc.LeaderLeaseTimeout = time.Second
	rc.CommitTimeout = 50 * time.Millisecond
	rc.Logger = logger
	rc.ShutdownOnRemove = false
	if tune != nil {
		tune(rc)
	}
	r, err := raft.NewRaft(rc, fsm, store, store, snapshots, transport)
	if err != nil {
		return nil, err
	}
	n := &Node{raft: r, state: fsm, store: store, transport: transport, config: c, started: time.Now(), operations: make(chan struct{}, 64)}
	if c.AsyncContent {
		n.async, err = newAsyncWriter(n)
		if err != nil {
			_ = r.Shutdown().Error()
			return nil, err
		}
	}
	if !existing && len(c.InitialVoters) > 0 {
		configuration := raft.Configuration{}
		for _, m := range c.InitialVoters {
			configuration.Servers = append(configuration.Servers, raft.Server{ID: raft.ServerID(m.ID), Address: raft.ServerAddress(m.raftAddress()), Suffrage: raft.Voter})
		}
		if err := r.BootstrapCluster(configuration).Error(); err != nil {
			_ = r.Shutdown().Error()
			if n.async != nil {
				n.async.close()
			}
			return nil, err
		}
	}
	ok = true
	return n, nil
}

// Identity is deliberately independent of the node's IP so it survives address
// changes, but may never be copied to another live replica.
func bindIdentity(c Config, hasState bool) error {
	path := filepath.Join(c.StateDir, "identity.json")
	want := struct{ ClusterID, NodeID string }{c.ClusterID, c.Local.ID}
	data, err := os.ReadFile(path)
	if err == nil {
		var got struct{ ClusterID, NodeID string }
		if decodeStrict(data, &got) != nil || got != want {
			return errors.New("node-local state belongs to another node or cluster")
		}
		if !hasState && len(c.InitialVoters) > 0 {
			return errors.New("refusing to rebootstrap existing node identity without Raft history")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	if hasState {
		return errors.New("Raft history has no matching node identity")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(file).Encode(want)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	dir, err := os.Open(c.StateDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.closeErr = n.raft.Shutdown().Error()
		if n.async != nil {
			n.async.close()
		}
		if closer, ok := n.transport.(raft.WithClose); ok {
			if err := closer.Close(); n.closeErr == nil {
				n.closeErr = err
			}
		}
		if err := n.store.Close(); n.closeErr == nil {
			n.closeErr = err
		}
	})
	return n.closeErr
}

func (n *Node) wait(ctx context.Context, submit func() raft.Future) error {
	// The permit remains held until the Raft future finishes, even if the
	// request is cancelled. A partition cannot create unbounded waiter goroutines.
	select {
	case n.operations <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		defer func() { <-n.operations }()
		if err := ctx.Err(); err != nil {
			done <- err
			return
		}
		done <- submit().Error()
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Node) fence(ctx context.Context) error {
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	if err := n.wait(ctx, n.raft.VerifyLeader); err != nil {
		return fmt.Errorf("quorum verification: %w", err)
	}
	return n.wait(ctx, func() raft.Future { return n.raft.Barrier(operationTimeout) })
}

// Read never returns a follower's potentially stale authorization metadata.
func (n *Node) Read(ctx context.Context, keys []string) (map[string]Record, error) {
	if len(keys) == 0 || len(keys) > maxChanges {
		return nil, ErrInvalid
	}
	for _, key := range keys {
		if !documentKey.MatchString(key) {
			return nil, ErrInvalid
		}
	}
	if n.async != nil {
		return n.async.read(ctx, keys)
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := n.fence(ctx); err != nil {
		return nil, err
	}
	return n.state.read(keys), nil
}

// Commit may return an indeterminate result on timeout/leadership loss. Callers
// must use checked/idempotent operations and re-read, never assume rollback.
func (n *Node) Commit(ctx context.Context, t Transaction) error {
	if t.Revision != 0 {
		return ErrInvalid
	}
	if n.async != nil {
		return n.async.commit(ctx, t)
	}
	return n.commitRaft(ctx, t)
}

func (n *Node) commitRaft(ctx context.Context, t Transaction) error {
	if err := validateTransaction(t); err != nil {
		return err
	}
	data, err := json.Marshal(t)
	if err != nil || len(data) > maxCommand {
		return ErrInvalid
	}
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	var future raft.ApplyFuture
	if err := n.wait(ctx, func() raft.Future { future = n.raft.Apply(data, operationTimeout); return future }); err != nil {
		return err
	}
	if result := future.Response(); result != nil {
		if err, ok := result.(error); ok {
			return err
		}
		return errors.New("invalid metadata result")
	}
	return nil
}

// Check validates a read set at one quorum-fenced point without appending a log
// entry. Applications use it to finish multi-read, read-only transactions.
func (n *Node) Check(ctx context.Context, checks []Check) error {
	keys := make([]string, len(checks))
	for i, c := range checks {
		keys[i] = c.Key
	}
	records, err := n.Read(ctx, keys)
	if err != nil {
		return err
	}
	for _, c := range checks {
		if records[c.Key].Version != c.Version {
			return ErrConflict
		}
	}
	return nil
}

var scanPrefix = regexp.MustCompile(`^[a-z][a-z0-9_]{0,47}/$`)

func (n *Node) Scan(ctx context.Context, prefix, cursor string, limit int) (kv.Page, error) {
	if !scanPrefix.MatchString(prefix) || limit < 1 || limit > 100 || (cursor != "" && (!documentKey.MatchString(cursor) || !strings.HasPrefix(cursor, prefix))) {
		return kv.Page{}, ErrInvalid
	}
	if n.async != nil {
		return n.async.scan(ctx, prefix, cursor, limit)
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := n.fence(ctx); err != nil {
		return kv.Page{}, err
	}
	n.state.mu.RLock()
	defer n.state.mu.RUnlock()
	keys := make([]string, 0)
	for key := range n.state.records {
		if strings.HasPrefix(key, prefix) && key > cursor {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	page := kv.Page{Records: make(map[string]Record)}
	if len(keys) > limit {
		keys = keys[:limit]
		page.Next = keys[len(keys)-1]
	}
	for _, key := range keys {
		r := n.state.records[key]
		r.Value = bytes.Clone(r.Value)
		page.Records[key] = r
	}
	return page, nil
}

// Ready means this node has just proved leader authority, not merely that its
// process is alive. Followers need forwarding before they can advertise readiness.
func (n *Node) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	return n.fence(ctx)
}

type Status struct {
	ClusterID     string `json:"clusterID"`
	ID            string `json:"id"`
	Role          string `json:"role"`
	LeaderID      string `json:"leaderID"`
	UptimeSeconds int64  `json:"uptimeSeconds"`
	Term          uint64 `json:"term"`
	AppliedIndex  uint64 `json:"appliedIndex"`
	StateIndex    uint64 `json:"stateIndex"`
	LastIndex     uint64 `json:"lastIndex"`
	// A status response is observational, NOT a quorum-backed readiness grant.
	QuorumVerified     bool   `json:"quorumVerified"`
	BootstrapView      string `json:"bootstrapView,omitempty"`
	DurableRevision    uint64 `json:"durableRevision,omitempty"`
	ReplicatedRevision uint64 `json:"replicatedRevision,omitempty"`
	HistoryDigest      string `json:"historyDigest,omitempty"`
}

func (n *Node) Status() Status {
	_, leader := n.raft.LeaderWithID()
	// AppliedIndex is queued to the FSM; StateIndex is fully consumed metadata.
	stateIndex := n.state.appliedIndex()
	s := Status{ClusterID: n.config.ClusterID, ID: n.config.Local.ID, Role: n.raft.State().String(), LeaderID: string(leader), UptimeSeconds: int64(time.Since(n.started) / time.Second), Term: n.raft.CurrentTerm(), StateIndex: stateIndex, AppliedIndex: n.raft.AppliedIndex(), LastIndex: n.raft.LastIndex()}
	if n.auto != nil {
		s.BootstrapView = n.auto.statusView()
	}
	n.state.mu.RLock()
	s.ReplicatedRevision = n.state.versionCeiling
	s.DurableRevision = s.ReplicatedRevision
	s.HistoryDigest = n.state.historyDigest
	n.state.mu.RUnlock()
	if n.async != nil {
		n.async.mu.Lock()
		if n.async.view != nil {
			s.DurableRevision = max(s.DurableRevision, n.async.view.versionCeiling)
		}
		n.async.mu.Unlock()
	}
	return s
}

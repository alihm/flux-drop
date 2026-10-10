package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/metadata"
)

type readStamp struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
}
type snapshotReader interface {
	readSnapshot(context.Context, []string, *readStamp) (map[string]Record, *readStamp, error)
}
type transactionBackend struct {
	metadata.Backend
	reader snapshotReader
	stamp  *readStamp
	legacy bool
}

func (n *Node) TransactionBackend() metadata.Backend {
	return &transactionBackend{Backend: n, reader: n}
}
func (c *Client) TransactionBackend() metadata.Backend {
	return &transactionBackend{Backend: c, reader: c}
}
func (b *transactionBackend) Read(ctx context.Context, keys []string) (map[string]Record, error) {
	if b.legacy {
		return b.Backend.Read(ctx, keys)
	}
	r, stamp, err := b.reader.readSnapshot(ctx, keys, b.stamp)
	// Rolling upgrades may route to an older leader. Fail back to its original
	// fenced protocol only before this transaction has read any snapshot bytes.
	if b.stamp == nil && errors.Is(err, ErrInvalid) {
		b.legacy = true
		return b.Backend.Read(ctx, keys)
	}
	if err == nil {
		b.stamp = stamp
	}
	return r, err
}
func (b *transactionBackend) Check(ctx context.Context, checks []Check) error {
	if b.legacy {
		return b.Backend.Check(ctx, checks)
	}
	_, _, err := b.reader.readSnapshot(ctx, nil, b.stamp)
	return err
}

// A transaction confirms the leader once, then reads one unchanged revision.
// The final check is local, never a second quorum round. Any intervening write
// (including privacy/deletion/rename) changes the index and retries the entire
// transaction. Thus a successful read linearizes after request arrival. Reads
// overlapping a later policy change may linearize first, as in any linearizable
// API; already transmitted bytes and browser caches cannot be recalled.
// Proofs never outlive the same-term authority window, which is shorter than
// the configured election timeout. Followers never serve metadata snapshots.
func (n *Node) readSnapshot(ctx context.Context, keys []string, stamp *readStamp) (map[string]Record, *readStamp, error) {
	if len(keys) > maxChanges || len(keys) == 0 && stamp == nil {
		return nil, nil, ErrInvalid
	}
	for _, key := range keys {
		if !documentKey.MatchString(key) {
			return nil, nil, ErrInvalid
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if n.async != nil {
		return n.async.readSnapshot(ctx, keys, stamp)
	}
	if stamp == nil {
		if err := n.fence(ctx); err != nil {
			return nil, nil, err
		}
	}
	n.readMu.Lock()
	valid := n.raft.State() == raft.Leader && n.raft.CurrentTerm() == n.readTerm && authorityTimeValid(n.readLease, time.Now())
	term := n.readTerm
	n.readMu.Unlock()
	if !valid {
		return nil, nil, ErrNotLeader
	}
	n.state.mu.RLock()
	defer n.state.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	current := &readStamp{Term: term, Index: n.state.index}
	if stamp != nil && *stamp != *current {
		return nil, nil, ErrConflict
	}
	return copyRecords(n.state.records, keys), current, nil
}
func (w *asyncWriter) readSnapshot(ctx context.Context, keys []string, stamp *readStamp) (map[string]Record, *readStamp, error) {
	// Existing views are immutable: writers replace the pointer under mu. Valid
	// authority reads take only a shared lock and never wait on ensure's network IO.
	w.mu.RLock()
	if w.term != w.node.raft.CurrentTerm() || w.failed || !w.authoritative() {
		w.mu.RUnlock()
		w.mu.Lock()
		err := w.ensure(ctx)
		w.mu.Unlock()
		if err != nil {
			return nil, nil, err
		}
		w.mu.RLock()
	}
	defer w.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if w.failed || !w.authoritative() {
		return nil, nil, ErrNotLeader
	}
	current := &readStamp{Term: w.term, Index: w.view.index}
	if stamp != nil && *stamp != *current {
		return nil, nil, ErrConflict
	}
	return w.view.read(keys), current, nil
}
func (c *Client) readSnapshot(ctx context.Context, keys []string, stamp *readStamp) (map[string]Record, *readStamp, error) {
	r, err := c.call(ctx, rpcRequest{Method: "snapshot_read", Keys: keys, Stamp: stamp})
	return r.Records, r.Stamp, err
}

func (b *transactionBackend) CoherentSnapshot() bool { return !b.legacy && b.stamp != nil }

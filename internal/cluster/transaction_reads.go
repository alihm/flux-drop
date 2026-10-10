package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/metadata"
)

type readStamp struct {
	Epoch      string `json:"epoch,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	Term       uint64 `json:"term"`
	Index      uint64 `json:"index"`
}
type snapshotReader interface {
	readSnapshot(context.Context, []string, *readStamp) (map[string]Record, *readStamp, error)
}
type transactionBackend struct {
	metadata.Backend
	reader snapshotReader
	stamp  *readStamp
	legacy bool
	local  bool
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
	if !b.local && b.stamp == nil && errors.Is(err, ErrInvalid) {
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

// Local snapshots deliberately have no replication-age gate. Applied follower
// state may remain stale indefinitely. Leaders must retain their speculative
// view authority rules; an unusable leader must not masquerade as a follower.
// Epoch includes process identity, view kind, term and restore generation, so
// restart/restore/election cannot validate an incompatible dependent read.
func (n *Node) localSnapshot(ctx context.Context, keys []string, stamp *readStamp) (map[string]Record, *readStamp, error) {
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
	if n.raft.State() == raft.Shutdown {
		return nil, nil, ErrNotLeader
	}
	epoch := n.localEpoch
	if n.raft.State() == raft.Leader {
		var base *readStamp
		if stamp != nil {
			copy := *stamp
			copy.Epoch = ""
			copy.Generation = 0
			base = &copy
		}
		records, current, err := n.readSnapshot(ctx, keys, base)
		if err != nil {
			return nil, nil, err
		}
		n.state.mu.RLock()
		current.Generation = n.state.generation
		n.state.mu.RUnlock()
		current.Epoch = epoch + "/leader"
		if stamp != nil && *stamp != *current {
			return nil, nil, ErrConflict
		}
		return records, current, nil
	}
	n.state.mu.RLock()
	defer n.state.mu.RUnlock()
	if n.raft.State() == raft.Leader {
		return nil, nil, ErrConflict
	}
	current := &readStamp{Epoch: epoch + "/applied", Generation: n.state.generation, Term: n.raft.CurrentTerm(), Index: n.state.index}
	if stamp != nil && *stamp != *current {
		return nil, nil, ErrConflict
	}
	return copyRecords(n.state.records, keys), current, nil
}

type localReader struct {
	node   *Node
	client *Client
}

func (r localReader) readSnapshot(ctx context.Context, keys []string, stamp *readStamp) (map[string]Record, *readStamp, error) {
	if r.node != nil {
		return r.node.localSnapshot(ctx, keys, stamp)
	}
	result, err := r.client.call(ctx, rpcRequest{Method: "local_snapshot", Keys: keys, Stamp: stamp})
	return result.Records, result.Stamp, err
}

type localBackend struct {
	metadata.Backend
	reader localReader
}

func (b localBackend) TransactionBackend() metadata.Backend {
	return &transactionBackend{Backend: b.Backend, reader: b.reader, local: true}
}
func (n *Node) ServingBackend() metadata.Backend   { return localBackend{n, localReader{node: n}} }
func (c *Client) ServingBackend() metadata.Backend { return localBackend{c, localReader{client: c}} }

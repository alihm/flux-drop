package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"go.etcd.io/bbolt"
)

const maxPendingContent = 64
const authorityWindow = 750 * time.Millisecond

var pendingBucket = []byte("pending-v1")

type proposal struct {
	command Transaction
	term    uint64
	done    chan error
}

// asyncWriter maintains a speculative, leader-only view. The fsynced queue is
// bounded; it is NOT replayed across leadership generations. Raft remains the
// ordered replication stream and durable security barrier. No follower serves
// this speculative view and no uncommitted security write is exposed through it.
type asyncWriter struct {
	node           *Node
	writeMu        sync.Mutex
	mu             sync.Mutex
	view           *state
	term, sequence uint64
	lease          time.Time
	leaseTerm      uint64
	failed         bool
	outstanding    int
	db             *bbolt.DB
	queue          chan proposal
	stop           chan struct{}
	wg             sync.WaitGroup
}

func newAsyncWriter(n *Node) (*asyncWriter, error) {
	db, err := bbolt.Open(filepath.Join(n.config.StateDir, "content-journal.db"), 0600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	if err = db.Update(func(tx *bbolt.Tx) error { _, err := tx.CreateBucketIfNotExists(pendingBucket); return err }); err != nil {
		db.Close()
		return nil, err
	}
	w := &asyncWriter{node: n, db: db, queue: make(chan proposal, maxPendingContent), stop: make(chan struct{})}
	w.wg.Add(2)
	go w.replicate()
	go w.heartbeat()
	return w, nil
}
func (w *asyncWriter) close() { close(w.stop); w.wg.Wait(); _ = w.db.Close() }

func (w *asyncWriter) heartbeat() {
	defer w.wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastTransfer time.Time
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			w.mu.Lock()
			failed := w.failed
			w.mu.Unlock()
			if failed && w.node.raft.State() == raft.Leader && time.Since(lastTransfer) > time.Second {
				lastTransfer = time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = w.node.wait(ctx, func() raft.Future { return w.node.raft.LeadershipTransfer() })
				cancel()
				continue
			}
			term := w.node.raft.CurrentTerm()
			started := time.Now()
			if w.node.raft.State() != raft.Leader {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), authorityWindow)
			err := w.node.wait(ctx, w.node.raft.VerifyLeader)
			cancel()
			if err == nil && time.Since(started) < authorityWindow {
				w.mu.Lock()
				if w.node.raft.State() == raft.Leader && w.node.raft.CurrentTerm() == term {
					w.lease = started.Add(authorityWindow)
					w.leaseTerm = term
				}
				w.mu.Unlock()
			}
		}
	}
}

func (w *asyncWriter) authoritative() bool {
	return w.node.raft.State() == raft.Leader && w.node.raft.CurrentTerm() == w.leaseTerm && authorityTimeValid(w.lease, time.Now())
}

// Both clocks must remain inside the proof window. This rejects wall-clock
// jumps and machine suspend where CLOCK_MONOTONIC stops but wall time advances.
// Fresh quorum verification is needed to establish a new window afterwards.
func authorityTimeValid(deadline, now time.Time) bool {
	start := deadline.Add(-authorityWindow)
	monotonicElapsed := now.Sub(start)
	wallElapsed := now.Round(0).Sub(start.Round(0))
	return monotonicElapsed >= 0 && monotonicElapsed < authorityWindow && wallElapsed >= 0 && wallElapsed < authorityWindow
}

// Called with mu held. Establish a committed baseline exactly once per term;
// subsequent reads use a short, periodically quorum-verified authority window.
func (w *asyncWriter) ensure(ctx context.Context) error {
	if w.node.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	term := w.node.raft.CurrentTerm()
	if w.term == term && !w.failed {
		if !w.authoritative() {
			return ErrNotLeader
		}
		return nil
	}
	// Every old future must settle before rebuilding the authorization view.
	if w.outstanding != 0 {
		return ErrNotLeader
	}
	if term == 0 || term > 0xffffffff {
		return ErrCapacity
	}
	if err := w.node.wait(ctx, func() raft.Future { return w.node.raft.Barrier(operationTimeout) }); err != nil {
		return err
	}
	// Anchor the new authority deadline before a fresh verification round,
	// not before potentially slow baseline replay, and never at response time.
	started := time.Now()
	if err := w.node.wait(ctx, w.node.raft.VerifyLeader); err != nil {
		return err
	}
	if time.Since(started) >= authorityWindow || w.node.raft.State() != raft.Leader || w.node.raft.CurrentTerm() != term {
		return ErrNotLeader
	}
	w.lease = started.Add(authorityWindow)
	w.leaseTerm = term
	view := newState(w.node.config.ClusterID)
	w.node.state.mu.RLock()
	for key, value := range w.node.state.records {
		view.records[key] = value
	}
	view.bytes = w.node.state.bytes
	view.index = w.node.state.index
	view.versionCeiling = w.node.state.versionCeiling
	view.historyDigest = w.node.state.historyDigest
	w.node.state.mu.RUnlock()
	var pending int
	if err := w.db.Update(func(tx *bbolt.Tx) error {
		pending = tx.Bucket(pendingBucket).Stats().KeyN
		if err := tx.DeleteBucket(pendingBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucket(pendingBucket)
		return err
	}); err != nil {
		return err
	}
	if pending > 0 {
		slog.Warn("unconfirmed local content history discarded after leadership change; recent uploads may be lost", "entries", pending)
	}
	w.view = view
	if w.term != term {
		w.sequence = 0
	}
	w.term = term
	w.failed = false
	if !w.authoritative() {
		return ErrNotLeader
	}
	return nil
}

func (w *asyncWriter) read(ctx context.Context, keys []string) (map[string]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := w.ensure(ctx); err != nil {
		return nil, err
	}
	return w.view.read(keys), nil
}

// A hint cannot downgrade security fields, unknown collections, or deletions.
func localContentAllowed(t Transaction, view *state) bool {
	if t.Durability != kv.Local {
		return false
	}
	found := false
	for _, write := range t.Writes {
		collection, _, _ := strings.Cut(write.Key, "/")
		if write.Delete && collection != "digests" {
			return false
		}
		switch collection {
		case "projects":
			var next project.Project
			if metadata.Decode(write.Value, &next) != nil || write.Key != "projects/"+next.ID {
				return false
			}
			var old *project.Project
			if r := view.records[write.Key]; r.Version != 0 {
				old = new(project.Project)
				if metadata.Decode(r.Value, old) != nil {
					return false
				}
			}
			if !project.ContentOnlyChange(old, next) {
				return false
			}
			found = true
		case "operations", "digests", "slugs", "quotas", "owner_projects":
		default:
			return false
		}
	}
	return found
}

func journalKey(revision uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, revision)
	return key
}

func (w *asyncWriter) commit(ctx context.Context, t Transaction) error {
	if err := validateTransaction(t); err != nil {
		return err
	}
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	w.mu.Lock()
	if err := ctx.Err(); err != nil {
		w.mu.Unlock()
		return err
	}
	if err := w.ensure(ctx); err != nil {
		w.mu.Unlock()
		return err
	}
	for _, check := range t.Checks {
		if w.view.records[check.Key].Version != check.Version {
			w.mu.Unlock()
			return ErrConflict
		}
	}
	local := localContentAllowed(t, w.view)
	if !local {
		// A singleton bootstrap is coordination-only. User/security state must
		// never be acknowledged as replicated before another voter exists.
		configuration := w.node.raft.GetConfiguration()
		if err := configuration.Error(); err != nil {
			w.mu.Unlock()
			return err
		}
		voters := 0
		for _, s := range configuration.Configuration().Servers {
			if s.Suffrage == raft.Voter {
				voters++
			}
		}
		internal := true
		for _, v := range t.Writes {
			if !strings.HasPrefix(v.Key, "cluster_") {
				internal = false
			}
		}
		if voters < 2 && !internal {
			w.mu.Unlock()
			return ErrNotLeader
		}
	}
	if w.sequence >= 0xffffffff {
		w.mu.Unlock()
		return ErrCapacity
	}
	w.sequence++
	t.Revision = w.term<<32 | w.sequence
	data, err := json.Marshal(t)
	if err != nil || len(data) > maxCommand {
		w.mu.Unlock()
		return ErrCapacity
	}
	// Preflight on a fork prevents a capacity error after returning local success.
	preview := newState(w.view.clusterID)
	preview.bytes = w.view.bytes
	preview.historyDigest = w.view.historyDigest
	preview.versionCeiling = w.view.versionCeiling
	for k, v := range w.view.records {
		preview.records[k] = v
	}
	if result := preview.Apply(&raft.Log{Index: t.Revision, Term: w.term, Data: data}); result != nil {
		w.mu.Unlock()
		return result.(error)
	}
	err = w.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(pendingBucket)
		if bucket.Stats().KeyN >= maxPendingContent {
			return ErrCapacity
		}
		return bucket.Put(journalKey(t.Revision), data)
	})
	if err != nil {
		w.mu.Unlock()
		return err
	}
	if !w.authoritative() {
		w.failed = true
		w.mu.Unlock()
		return ErrNotLeader
	}
	p := proposal{command: t, term: w.term, done: make(chan error, 1)}
	select {
	case w.queue <- p:
		w.outstanding++
	default:
		w.failed = true
		w.mu.Unlock()
		return ErrCapacity
	}
	if local {
		w.view = preview
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()
	select {
	case err := <-p.done:
		w.mu.Lock()
		defer w.mu.Unlock()
		if err != nil {
			return err
		}
		if !w.authoritative() || w.term != p.term || w.failed {
			if w.term == p.term {
				w.failed = true
			}
			return ErrNotLeader
		}
		w.view = preview
		return nil
	case <-ctx.Done():
		// The write may commit later. Do not serve an older policy while its
		// outcome is unknown; recovery requires a fresh committed baseline.
		w.mu.Lock()
		w.failed = true
		w.mu.Unlock()
		return ctx.Err()
	case <-w.stop:
		return ErrNotLeader
	}
}

func (w *asyncWriter) replicate() {
	defer w.wg.Done()
	for {
		select {
		case <-w.stop:
			return
		case p := <-w.queue:
			w.mu.Lock()
			valid := w.term == p.term && !w.failed && w.authoritative()
			w.mu.Unlock()
			var err error
			if !valid {
				err = ErrNotLeader
			} else {
				// The bounded worker owns the real future until it settles; a
				// request deadline cannot leave a late security write untracked.
				data, marshalErr := json.Marshal(p.command)
				if marshalErr != nil {
					err = marshalErr
				} else {
					future := w.node.raft.Apply(data, operationTimeout)
					err = future.Error()
					if err == nil && future.Response() != nil {
						err = future.Response().(error)
					}
				}
			}
			w.mu.Lock()
			w.outstanding--
			if err == nil {
				err = w.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(pendingBucket).Delete(journalKey(p.command.Revision)) })
			}
			if err != nil && w.term == p.term {
				w.failed = true
				slog.Warn("content replication interrupted; primary is fenced until recovery")
			}
			w.mu.Unlock()
			p.done <- err
		}
	}
}

func (w *asyncWriter) scan(ctx context.Context, prefix, cursor string, limit int) (kv.Page, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensure(ctx); err != nil {
		return kv.Page{}, err
	}
	keys := []string{}
	for key := range w.view.records {
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
		r := w.view.records[key]
		r.Value = bytes.Clone(r.Value)
		page.Records[key] = r
	}
	return page, nil
}

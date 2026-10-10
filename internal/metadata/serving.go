package metadata

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"
)

// Serving only: management and security mutations never use this boundary.
// Missing confirmations are bounded PER process/Store, including duplicate
// waiters. Existing local records (positive or negative) bypass this entirely.
type servingState struct {
	mu                                sync.Mutex
	slots                             chan struct{}
	callers                           chan struct{}
	flights                           map[string]*confirmation
	negative                          map[string]*list.Element
	lru                               *list.List
	attempts, denied, errors, retries atomic.Uint64
}
type confirmation struct {
	done    chan struct{}
	records map[string]kv.Record
	err     error
}
type negativeEntry struct {
	key   string
	until time.Time
}

func newServingState() *servingState {
	return &servingState{slots: make(chan struct{}, 64), callers: make(chan struct{}, 256), flights: map[string]*confirmation{}, negative: map[string]*list.Element{}, lru: list.New()}
}

type frozenBackend struct{ records map[string]kv.Record }

func (b frozenBackend) Read(_ context.Context, keys []string) (map[string]kv.Record, error) {
	out := map[string]kv.Record{}
	for _, k := range keys {
		r, ok := b.records[k]
		if !ok {
			return nil, kv.ErrConflict
		}
		out[k] = r
	}
	return out, nil
}
func (b frozenBackend) Check(context.Context, []kv.Check) error      { return nil }
func (b frozenBackend) Commit(context.Context, kv.Transaction) error { return kv.ErrInvalid }
func (b frozenBackend) CoherentSnapshot() bool                       { return true }

// RunServing evaluates the complete logical lookup in coherent local snapshots.
// Only ABSENT records cause a full leader lookup; denials, expired/pending state,
// and coordinator failures never silently promote to leader traffic. validate
// preserves final post-IO snapshot checking for previews. negative is used only
// for public slug absence, never private identities/authorization decisions.
func (s *Store) RunServing(ctx context.Context, key string, negative, validate bool, fn func(*Tx) error) error {
	factory, ok := s.Backend.(interface{ ServingBackend() Backend })
	if !ok {
		if validate {
			return s.Run(ctx, fn)
		}
		return s.RunSnapshot(ctx, fn)
	}
	s.servingOnce.Do(func() { s.serving = newServingState() })
	state := s.serving
	local := &Store{Backend: factory.ServingBackend()}
	missing := false
	var callbackErr, dependencyErr error
	run := local.RunSnapshot
	if validate {
		run = local.Run
	}
	err := run(ctx, func(tx *Tx) error {
		missing = false
		e := fn(tx)
		callbackErr = e
		dependencyErr = tx.dependencyError
		for _, r := range tx.reads {
			if r.Version == 0 {
				missing = true
			}
		}
		return e
	})
	if errors.Is(err, kv.ErrConflict) {
		state.retries.Add(1)
	}
	if !missing {
		if err != nil && !errors.Is(err, ErrNotFound) {
			state.errors.Add(1)
		}
		return err
	}
	// A failed dependent read can coexist with an earlier absent record. Never
	// turn that coordinator failure into confirmation traffic.
	if dependencyErr != nil || err != nil && callbackErr == nil {
		return err
	}
	select {
	case state.callers <- struct{}{}:
		defer func() { <-state.callers }()
	default:
		state.denied.Add(1)
		return ErrNotFound
	}
	state.mu.Lock()
	if negative {
		if e := state.negative[key]; e != nil {
			v := e.Value.(negativeEntry)
			if time.Now().Before(v.until) {
				state.lru.MoveToFront(e)
				state.mu.Unlock()
				return ErrNotFound
			}
			state.lru.Remove(e)
			delete(state.negative, key)
		}
	}
	flight := state.flights[key]
	first := flight == nil
	if flight == nil {
		select {
		case state.slots <- struct{}{}:
		default:
			state.denied.Add(1)
			state.mu.Unlock()
			return ErrNotFound
		}
		flight = &confirmation{done: make(chan struct{})}
		state.flights[key] = flight
		state.attempts.Add(1)
		state.mu.Unlock()
		func() {
			bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			var records map[string]kv.Record
			leaderRun := s.RunSnapshot
			if validate {
				leaderRun = s.Run
			}
			e := leaderRun(bounded, func(tx *Tx) error {
				result := fn(tx)
				callbackErr = result
				dependencyErr = tx.dependencyError
				records = tx.reads
				return result
			})
			absent := false
			for _, record := range records {
				if record.Version == 0 {
					absent = true
				}
			}
			if absent && dependencyErr == nil && callbackErr != nil {
				e = ErrNotFound
			}
			// All successful dependent reads have the same stamped identity; legacy
			// backend final checks remain intact. Never negative-cache transport failures.
			state.mu.Lock()
			// A domain denial belongs to this caller; peers must evaluate their own
			// callback against the shared records, never inherit that decision.
			if dependencyErr == nil && callbackErr != nil && !errors.Is(e, ErrNotFound) {
				e = nil
			}
			flight.records, flight.err = records, e
			if negative && errors.Is(e, ErrNotFound) {
				until := time.Now().Add(time.Second)
				if old := state.negative[key]; old != nil {
					state.lru.Remove(old)
				}
				state.negative[key] = state.lru.PushFront(negativeEntry{key, until})
				for state.lru.Len() > 1024 {
					old := state.lru.Back()
					delete(state.negative, old.Value.(negativeEntry).key)
					state.lru.Remove(old)
				}
			}
			delete(state.flights, key)
			close(flight.done)
			state.mu.Unlock()
			<-state.slots
		}()
	}
	if !first {
		state.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-flight.done:
	}
	if flight.err != nil {
		return flight.err
	}
	// Each caller reruns its own callback. Shared work shares records, never a
	// successful authorization result or a descriptor with another user's offsets.
	return (&Store{Backend: frozenBackend{flight.records}}).RunSnapshot(ctx, fn)
}

func (s *Store) ServingMetrics() map[string]uint64 {
	s.servingOnce.Do(func() { s.serving = newServingState() })
	v := s.serving
	return map[string]uint64{"confirmationAttempts": v.attempts.Load(), "confirmationCapDenials": v.denied.Load(), "localCoordinatorErrors": v.errors.Load(), "snapshotRetries": v.retries.Load()}
}

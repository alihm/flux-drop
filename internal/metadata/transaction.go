// Package metadata provides optimistic transactions over quorum-backed records.
// It is internal to trusted app code, never a browser-facing document API.
package metadata

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"
)

var ErrNotFound = errors.New("metadata record not found")

type Backend interface {
	Read(context.Context, []string) (map[string]kv.Record, error)
	Check(context.Context, []kv.Check) error
	Commit(context.Context, kv.Transaction) error
}

type Store struct{ Backend Backend }
type Tx struct {
	ctx        context.Context
	backend    Backend
	reads      map[string]kv.Record
	writes     map[string]kv.Write
	durability kv.Durability
}

// Encoding is independent of public JSON tags: project ownership/password fields
// intentionally hidden from browser JSON must survive durable serialization.
// Exported Go field names are storage schema; renames require a migration.
type envelope struct {
	Schema int    `json:"schema"`
	Data   []byte `json:"data"`
}

func Encode(value any) ([]byte, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(value); err != nil {
		return nil, err
	}
	data, err := json.Marshal(envelope{1, b.Bytes()})
	if len(data) > 64<<10 {
		return nil, kv.ErrCapacity
	}
	return data, err
}

func Decode(data []byte, value any) error {
	var e envelope
	if len(data) > 64<<10 || json.Unmarshal(data, &e) != nil || e.Schema != 1 {
		return kv.ErrInvalid
	}
	return gob.NewDecoder(bytes.NewReader(e.Data)).Decode(value)
}

func (s *Store) Run(ctx context.Context, fn func(*Tx) error) error {
	return s.run(ctx, kv.Replicated, fn)
}

// RunContent permits local-durable acknowledgement only for reviewed content
// transactions. It is never selected using browser input. Protected/unknown
// collections and RequireReplication promote the WHOLE transaction to replicated.
// Typed repositories must additionally inspect policy changes inside projects.
func (s *Store) RunContent(ctx context.Context, fn func(*Tx) error) error {
	return s.run(ctx, kv.Local, fn)
}

// RequireReplication is sticky for this attempt, including if a later Set
// overwrites the protected write. There is deliberately no downgrade method.
func (t *Tx) RequireReplication() { t.durability = kv.Replicated }

func (s *Store) run(ctx context.Context, durability kv.Durability, fn func(*Tx) error) error {
	if s == nil || s.Backend == nil {
		return kv.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for attempt := 0; attempt < 8; attempt++ {
		tx := &Tx{ctx: ctx, backend: s.Backend, reads: make(map[string]kv.Record), writes: make(map[string]kv.Write), durability: durability}
		err := fn(tx)
		if err == nil {
			err = tx.finish()
		}
		// Never replay an indeterminate transport/leadership error. Only a known
		// rejected CAS is safe to retry automatically.
		if !errors.Is(err, kv.ErrConflict) {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return kv.ErrConflict
}

func (t *Tx) record(key string) (kv.Record, error) {
	if r, ok := t.reads[key]; ok {
		return r, nil
	}
	if len(t.reads) >= 128 {
		return kv.Record{}, kv.ErrCapacity
	}
	r, err := t.backend.Read(t.ctx, []string{key})
	if err != nil {
		return kv.Record{}, err
	}
	t.reads[key] = r[key]
	return r[key], nil
}

func (t *Tx) Get(key string, into any) error {
	if w, ok := t.writes[key]; ok {
		if w.Delete {
			return ErrNotFound
		}
		if into == nil {
			return nil
		}
		return Decode(w.Value, into)
	}
	r, err := t.record(key)
	if err != nil {
		return err
	}
	if r.Version == 0 {
		return ErrNotFound
	}
	if into == nil {
		return nil
	}
	return Decode(r.Value, into)
}

func (t *Tx) Set(key string, value any) error {
	t.classifyWrite(key, false)
	if _, err := t.record(key); err != nil {
		return err
	}
	data, err := Encode(value)
	if err != nil {
		return err
	}
	t.writes[key] = kv.Write{Key: key, Value: data}
	return nil
}
func (t *Tx) Create(key string, value any) error {
	err := t.Get(key, nil)
	if err == nil {
		return kv.ErrConflict
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	return t.Set(key, value)
}
func (t *Tx) Delete(key string) error {
	t.classifyWrite(key, true)
	if _, err := t.record(key); err != nil {
		return err
	}
	t.writes[key] = kv.Write{Key: key, Delete: true}
	return nil
}

func (t *Tx) classifyWrite(key string, deleted bool) {
	collection, _, _ := strings.Cut(key, "/")
	// Removing an obsolete digest during activation is the only content-path
	// deletion. All other deletion types, security records, and future unknown
	// collections fail safe to replicated acknowledgement.
	if deleted && collection != "digests" {
		t.RequireReplication()
		return
	}
	switch collection {
	case "projects", "operations", "digests", "slugs", "quotas", "owner_projects":
	default:
		t.RequireReplication()
	}
}
func (t *Tx) finish() error {
	if len(t.reads) == 0 {
		return kv.ErrInvalid
	}
	keys := make([]string, 0, len(t.reads))
	for key := range t.reads {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	command := kv.Transaction{Schema: 1}
	if t.durability == kv.Local {
		command.Durability = kv.Local
	}
	for _, key := range keys {
		command.Checks = append(command.Checks, kv.Check{Key: key, Version: t.reads[key].Version})
		if w, ok := t.writes[key]; ok {
			command.Writes = append(command.Writes, w)
		}
	}
	if len(command.Writes) == 0 {
		return t.backend.Check(t.ctx, command.Checks)
	}
	return t.backend.Commit(t.ctx, command)
}

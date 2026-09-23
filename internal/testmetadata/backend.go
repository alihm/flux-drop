// Package testmetadata is a deterministic CAS test double, never a runtime store.
package testmetadata

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/runonflux/flux-drop/internal/kv"
)

type Backend struct {
	mu      sync.Mutex
	records map[string]kv.Record
	index   uint64
	Failure error
}

func (b *Backend) Scan(ctx context.Context, prefix, cursor string, limit int) (kv.Page, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.Failure != nil {
		return kv.Page{}, b.Failure
	}
	keys := []string{}
	for key := range b.records {
		if strings.HasPrefix(key, prefix) && key > cursor {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	page := kv.Page{Records: make(map[string]kv.Record)}
	if len(keys) > limit {
		keys = keys[:limit]
		page.Next = keys[len(keys)-1]
	}
	for _, key := range keys {
		r := b.records[key]
		r.Value = bytes.Clone(r.Value)
		page.Records[key] = r
	}
	return page, ctx.Err()
}

func (b *Backend) Read(ctx context.Context, keys []string) (map[string]kv.Record, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.Failure != nil {
		return nil, b.Failure
	}
	out := make(map[string]kv.Record)
	for _, k := range keys {
		if r, ok := b.records[k]; ok {
			r.Value = bytes.Clone(r.Value)
			out[k] = r
		}
	}
	return out, ctx.Err()
}
func (b *Backend) Check(ctx context.Context, checks []kv.Check) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.Failure != nil {
		return b.Failure
	}
	for _, c := range checks {
		if b.records[c.Key].Version != c.Version {
			return kv.ErrConflict
		}
	}
	return ctx.Err()
}
func (b *Backend) Commit(ctx context.Context, t kv.Transaction) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.Failure != nil {
		return b.Failure
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, c := range t.Checks {
		if b.records[c.Key].Version != c.Version {
			return kv.ErrConflict
		}
	}
	if b.records == nil {
		b.records = make(map[string]kv.Record)
	}
	b.index++
	for _, w := range t.Writes {
		if w.Delete {
			delete(b.records, w.Key)
		} else {
			b.records[w.Key] = kv.Record{Version: b.index, Value: bytes.Clone(w.Value)}
		}
	}
	return nil
}

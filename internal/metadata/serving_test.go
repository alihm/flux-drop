package metadata

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

type localTestBackend struct {
	Backend
	local       Backend
	leaderReads atomic.Uint64
	block       <-chan struct{}
}

func (b *localTestBackend) ServingBackend() Backend { return b.local }
func (b *localTestBackend) Read(ctx context.Context, keys []string) (map[string]kv.Record, error) {
	b.leaderReads.Add(1)
	if b.block != nil {
		select {
		case <-b.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.Backend.Read(ctx, keys)
}
func TestServingMissingOnlyConfirmationAndDenial(t *testing.T) {
	ctx := context.Background()
	leader := &testmetadata.Backend{}
	local := &testmetadata.Backend{}
	put := func(b Backend, value string) {
		t.Helper()
		if e := (&Store{Backend: b}).Run(ctx, func(tx *Tx) error { return tx.Set("tests/site", value) }); e != nil {
			t.Fatal(e)
		}
	}
	put(leader, "new")
	put(local, "old")
	b := &localTestBackend{Backend: leader, local: local}
	s := &Store{Backend: b}
	var value string
	read := func(tx *Tx) error { return tx.Get("tests/site", &value) }
	if e := s.RunServing(ctx, "slug:test", true, false, read); e != nil || value != "old" || b.leaderReads.Load() != 0 {
		t.Fatal(value, e, b.leaderReads.Load())
	}
	denied := errors.New("locally denied")
	if e := s.RunServing(ctx, "slug:test", true, false, func(tx *Tx) error {
		if e := read(tx); e != nil {
			return e
		}
		return denied
	}); !errors.Is(e, denied) || b.leaderReads.Load() != 0 {
		t.Fatal("denial went to leader", e)
	}
	b.local = &testmetadata.Backend{}
	if e := s.RunServing(ctx, "slug:test", true, false, read); e != nil || value != "new" || b.leaderReads.Load() != 1 {
		t.Fatal(value, e, b.leaderReads.Load())
	}
	b.local = &testmetadata.Backend{Failure: errors.New("coordinator down")}
	if e := s.RunServing(ctx, "slug:test", true, false, read); e == nil || b.leaderReads.Load() != 1 {
		t.Fatal("outage fell back", e)
	}
}
func TestServingConfirmationSharingCapAndNegativeTTL(t *testing.T) {
	ctx := context.Background()
	ready := make(chan struct{})
	b := &localTestBackend{Backend: &testmetadata.Backend{}, local: &testmetadata.Backend{}, block: ready}
	s := &Store{Backend: b}
	read := func(tx *Tx) error { return tx.Get("tests/absent", nil) }
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := s.RunServing(ctx, "slug:missing", true, false, read); !errors.Is(e, ErrNotFound) {
				t.Error(e)
			}
		}()
	}
	deadline := time.Now().Add(time.Second)
	for b.leaderReads.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(ready)
	wg.Wait()
	if b.leaderReads.Load() != 1 {
		t.Fatal("not coalesced", b.leaderReads.Load())
	}
	if e := s.RunServing(ctx, "slug:missing", true, false, read); !errors.Is(e, ErrNotFound) || b.leaderReads.Load() != 1 {
		t.Fatal("negative miss", e)
	}
	s.serving.mu.Lock()
	e := s.serving.negative["slug:missing"]
	v := e.Value.(negativeEntry)
	v.until = time.Now().Add(-time.Second)
	e.Value = v
	s.serving.mu.Unlock()
	_ = s.RunServing(ctx, "slug:missing", true, false, read)
	if b.leaderReads.Load() != 2 {
		t.Fatal("negative never expires")
	}
	for i := 0; i < cap(s.serving.slots); i++ {
		s.serving.slots <- struct{}{}
	}
	if e := s.RunServing(ctx, "slug:other", true, false, read); !errors.Is(e, ErrNotFound) || s.ServingMetrics()["confirmationCapDenials"] != 1 {
		t.Fatal("cap behavior", e)
	}
	for len(s.serving.slots) > 0 {
		<-s.serving.slots
	}
}

func TestMissingPreviewConfirmationValidatesEachCallersIO(t *testing.T) {
	ctx := context.Background()
	leader := &testmetadata.Backend{}
	base := &Store{Backend: leader}
	if e := base.Run(ctx, func(tx *Tx) error { return tx.Set("tests/preview", "public") }); e != nil {
		t.Fatal(e)
	}
	b := &localTestBackend{Backend: leader, local: &testmetadata.Backend{}}
	s := &Store{Backend: b}
	calls := 0
	denied := errors.New("private")
	err := s.RunServing(ctx, "preview:race", false, true, func(tx *Tx) error {
		var value string
		if e := tx.Get("tests/preview", &value); e != nil {
			return e
		}
		calls++
		if value != "public" {
			return denied
		}
		// First successful callback is the checked leader confirmation. Second is
		// this caller's replay/IO; a concurrent policy change must invalidate it too.
		if calls == 2 {
			return base.Run(ctx, func(other *Tx) error { return other.Set("tests/preview", "private") })
		}
		return nil
	})
	if err == nil {
		t.Fatal("post-confirmation IO bypassed final validation")
	}
}

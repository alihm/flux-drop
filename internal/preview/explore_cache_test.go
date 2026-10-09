package preview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

type countedReads struct {
	*testmetadata.Backend
	reads atomic.Int64
}

func (b *countedReads) Read(ctx context.Context, keys []string) (map[string]kv.Record, error) {
	b.reads.Add(1)
	time.Sleep(time.Millisecond)
	return b.Backend.Read(ctx, keys)
}
func TestExploreCacheCoalescesAndInvalidates(t *testing.T) {
	b := &countedReads{Backend: &testmetadata.Backend{}}
	store := &metadata.Store{Backend: b}
	s, _ := New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) { return nil, nil })
	p := previewProject("a")
	putProject(t, store, p)
	if err := s.remember(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	b.reads.Store(0)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := s.CachedExplore(context.Background())
			if err != nil || len(rows) != 1 {
				t.Error(rows, err)
			}
			if len(rows) > 0 {
				rows[0].Slug = "mutated"
			}
		}()
	}
	wg.Wait()
	if b.reads.Load() != 2 {
		t.Fatal("burst rebuilt multiple times", b.reads.Load())
	}
	rows, _ := s.CachedExplore(context.Background())
	if rows[0].Slug == "mutated" {
		t.Fatal("caller changed cache")
	}
	p.Private = true
	putProject(t, store, p)
	s.Notify(p)
	rows, err := s.CachedExplore(context.Background())
	if err != nil || len(rows) != 0 {
		t.Fatal("privacy invalidation failed", rows, err)
	}
	s.exploreMu.Lock()
	s.exploreUntil = time.Now().Add(-time.Second)
	s.exploreMu.Unlock()
	before := b.reads.Load()
	if _, err = s.CachedExplore(context.Background()); err != nil || b.reads.Load() != before+2 {
		t.Fatal("TTL did not rebuild", err)
	}
}

type invalidateDuringBuild struct {
	*testmetadata.Backend
	once             atomic.Bool
	entered, release chan struct{}
}

func (b *invalidateDuringBuild) Check(ctx context.Context, checks []kv.Check) error {
	err := b.Backend.Check(ctx, checks)
	if b.once.CompareAndSwap(true, false) {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}
func TestExploreInvalidationDiscardsInflightOldList(t *testing.T) {
	b := &invalidateDuringBuild{Backend: &testmetadata.Backend{}, entered: make(chan struct{}), release: make(chan struct{})}
	store := &metadata.Store{Backend: b}
	s, _ := New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) { return nil, nil })
	p := previewProject("a")
	putProject(t, store, p)
	if err := s.remember(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	b.once.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results := make(chan int, 2)
	read := func() {
		rows, err := s.CachedExplore(ctx)
		if err != nil {
			t.Error(err)
		}
		results <- len(rows)
	}
	go read()
	select {
	case <-b.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	p.Private = true
	putProject(t, store, p)
	s.Notify(p)
	go read()
	close(b.release)
	for i := 0; i < 2; i++ {
		if count := <-results; count != 0 {
			t.Fatal("in-flight gallery bypassed local invalidation", count)
		}
	}
}

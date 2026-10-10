package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var testNow = time.Date(2026, 10, 10, 13, 0, 0, 0, time.UTC)
var testID = strings.Repeat("a", 32)

func testArchive(t *testing.T) *Archive {
	t.Helper()
	a, e := NewArchive(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	a.now = func() time.Time { return testNow }
	if e = a.Sweep(context.Background()); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { a.Close() })
	return a
}
func snapshot(count, sequence uint64) Snapshot {
	var hours [24]uint64
	hours[12] = count
	return Snapshot{Node: "primary-one", Producer: strings.Repeat("b", 32), Day: "2026-10-10", Shard: 10, Sequence: sequence, UpdatedAt: testNow, Projects: map[string][24]uint64{testID: hours}}
}
func query(t *testing.T, a *Archive) Result {
	t.Helper()
	result, e := a.Query(context.Background(), Query{ProjectID: testID, From: Day(testNow), To: Day(testNow).Add(24 * time.Hour), Interval: "hour"})
	if e != nil {
		t.Fatal(e)
	}
	return result
}
func TestCumulativeSnapshotsRetryCompactionAndReplicaMerge(t *testing.T) {
	a := testArchive(t)
	ctx := context.Background()
	for _, s := range []Snapshot{snapshot(3, 1), snapshot(7, 2), snapshot(9, 3), snapshot(9, 3), snapshot(3, 1)} {
		if e := a.Accept(ctx, s); e != nil {
			t.Fatal(e)
		}
	}
	if got := query(t, a).PageViews; got != 9 {
		t.Fatal("retry double counted", got)
	}
	entries, e := a.entries(directory("2026-10-10", 10))
	if e != nil || len(entries) != 2 {
		t.Fatal("generations not compacted", len(entries), e)
	}
	if e = a.Accept(ctx, snapshot(100, 3)); !errors.Is(e, ErrInvalid) {
		t.Fatal("same sequence changed", e)
	}
	other := snapshot(5, 1)
	other.Node = "primary-two"
	other.Producer = strings.Repeat("c", 32)
	if e = a.Accept(ctx, other); e != nil {
		t.Fatal(e)
	}
	if got := query(t, a).PageViews; got != 14 {
		t.Fatal("different producer not added", got)
	}
	// An old leader's delayed lower snapshot may reappear after replication. Query
	// selects the highest immutable sequence; there is no shared total to overwrite.
	old := snapshot(3, 1)
	if e = atomicFile(a.root, filepath.Join(directory(old.Day, old.Shard), filename(old)), mustJSON(t, old), true); e != nil {
		t.Fatal(e)
	}
	a.generation.Add(1)
	if got := query(t, a).PageViews; got != 14 {
		t.Fatal("replicated old generation added", got)
	}
	result := query(t, a)
	result.Buckets[12].PageViews = 999
	if query(t, a).Buckets[12].PageViews != 14 {
		t.Fatal("caller mutated cached result")
	}
}
func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}

type archiveSource struct {
	archive *Archive
	loseAck bool
	mu      sync.Mutex
}

func (s *archiveSource) SubmitPageViews(ctx context.Context, snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.archive.Accept(ctx, snapshot); e != nil {
		return e
	}
	if s.loseAck {
		s.loseAck = false
		return ErrUnavailable
	}
	return nil
}
func (s *archiveSource) PageViews(ctx context.Context, q Query) (Result, error) {
	return s.archive.Query(ctx, q)
}
func TestCollectorLostAckOutboxRecoveryAndFreshProducer(t *testing.T) {
	a := testArchive(t)
	source := &archiveSource{archive: a, loseAck: true}
	outbox := t.TempDir()
	c, e := NewCollector("primary-one", outbox, source)
	if e != nil {
		t.Fatal(e)
	}
	c.now = func() time.Time { return testNow }
	for i := 0; i < 50; i++ {
		c.Record(testID, testNow.Add(-time.Hour))
	}
	c.Flush(context.Background())
	if got := query(t, a).PageViews; got != 50 {
		t.Fatal(got)
	}
	files, _ := os.ReadDir(outbox)
	if len(files) != 1 {
		t.Fatal("lost ACK discarded outbox", files)
	}
	c.Close()
	c, e = NewCollector("primary-one", outbox, source)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.now = func() time.Time { return testNow }
	c.Flush(context.Background()) // replay previous boot's identical snapshot
	for i := 0; i < 7; i++ {
		c.Record(testID, testNow.Add(-time.Hour))
	}
	c.Flush(context.Background())
	if got := query(t, a).PageViews; got != 57 {
		t.Fatal("restart/retry changed total", got)
	}
	files, _ = os.ReadDir(outbox)
	if len(files) != 0 {
		t.Fatal("ACKed outbox retained", files)
	}
	c.Record(testID, testNow.Add(-time.Hour))
	c.Flush(context.Background())
	if got := query(t, a).PageViews; got != 58 {
		t.Fatal("cumulative snapshot was added", got)
	}
}
func TestCollectorConcurrentFlushAndBoundedCounters(t *testing.T) {
	a := testArchive(t)
	c, e := NewCollector("primary-one", t.TempDir(), &archiveSource{archive: a})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.now = func() time.Time { return testNow }
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 2000; n++ {
				c.Record(testID, testNow.Add(-time.Hour))
			}
		}()
	}
	c.Flush(context.Background())
	wg.Wait()
	c.Flush(context.Background())
	if got := query(t, a).PageViews; got != 32000 {
		t.Fatal("concurrent snapshot lost counters", got)
	}
	for i := 0; i < ProjectsPerShard+10; i++ {
		id := fmt.Sprintf("a%031x", i)
		c.Record(id, testNow)
	}
	if got := c.Metrics()["droppedViews"]; got != 11 {
		t.Fatal("counter cap not enforced", got)
	}
}
func TestRetentionUTCAndValidation(t *testing.T) {
	a := testArchive(t)
	ctx := context.Background()
	old := snapshot(2, 1)
	old.Day = Cutoff(testNow).Add(-24 * time.Hour).Format("2006-01-02")
	if e := atomicFile(a.root, filepath.Join(directory(old.Day, old.Shard), filename(old)), mustJSON(t, old), true); e != nil {
		t.Fatal(e)
	}
	if e := a.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e := a.root.Stat(filepath.Join(directory(old.Day, old.Shard), filename(old))); !os.IsNotExist(e) {
		t.Fatal("expired snapshot retained", e)
	}
	if e := a.Accept(ctx, old); e != nil {
		t.Fatal("expired retry not ACKed", e)
	}
	invalid := snapshot(2, 1)
	invalid.Producer = "../../outside"
	if e := a.Accept(ctx, invalid); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	q := Query{ProjectID: testID, From: Cutoff(testNow).Add(-time.Hour), To: testNow, Interval: "hour"}
	if _, e := a.Query(ctx, q); !errors.Is(e, ErrInvalid) {
		t.Fatal("old range accepted", e)
	}
	q.From = Day(testNow).Add(time.Minute)
	if _, e := a.Query(ctx, q); !errors.Is(e, ErrInvalid) {
		t.Fatal("unaligned range accepted", e)
	}
	q.From = Day(testNow)
	q.Interval = "month"
	if _, e := a.Query(ctx, q); !errors.Is(e, ErrInvalid) {
		t.Fatal("bad interval accepted", e)
	}
	// An offset timestamp is counted in its UTC hour/day.
	c, e := NewCollector("primary-one", t.TempDir(), &archiveSource{archive: a})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.now = func() time.Time { return testNow }
	c.Record(testID, time.Date(2026, 10, 10, 0, 15, 0, 0, time.FixedZone("east", 2*3600)))
	c.Flush(ctx)
	q = Query{ProjectID: testID, From: Day(testNow).Add(-24 * time.Hour), To: Day(testNow), Interval: "hour"}
	result, e := a.Query(ctx, q)
	if e != nil || result.Buckets[22].PageViews != 1 {
		t.Fatal("not UTC", result, e)
	}
}
func TestArchiveCapacityAndUnavailableDoNotCorruptHistory(t *testing.T) {
	a := testArchive(t)
	a.bytes = MaxArchiveBytes
	if e := a.Accept(context.Background(), snapshot(1, 1)); !errors.Is(e, ErrBusy) {
		t.Fatal("disk cap", e)
	}
	if got := query(t, a).PageViews; got != 0 {
		t.Fatal(got)
	}
	a.bytes = 0
	if e := a.Accept(context.Background(), snapshot(1, 1)); e != nil {
		t.Fatal(e)
	}
	a.ready.Store(false)
	if e := a.Accept(context.Background(), snapshot(2, 2)); !errors.Is(e, ErrBusy) {
		t.Fatal("unready accepted", e)
	}
	if got := query(t, a).PageViews; got != 1 {
		t.Fatal("admission failure changed history", got)
	}
}
func BenchmarkRecordPageView(b *testing.B) {
	source := &archiveSource{}
	c, e := NewCollector("primary-one", b.TempDir(), source)
	if e != nil {
		b.Fatal(e)
	}
	defer c.Close()
	c.Record(testID, testNow)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Record(testID, testNow)
	}
}
func BenchmarkRecordPageViewParallel(b *testing.B) {
	source := &archiveSource{}
	c, e := NewCollector("primary-one", b.TempDir(), source)
	if e != nil {
		b.Fatal(e)
	}
	defer c.Close()
	c.Record(testID, testNow)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Record(testID, testNow)
		}
	})
}

type blockedSource struct {
	entered chan struct{}
	once    sync.Once
}

func (s *blockedSource) SubmitPageViews(ctx context.Context, _ Snapshot) error {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return ctx.Err()
}
func (s *blockedSource) PageViews(context.Context, Query) (Result, error) {
	return Result{}, ErrUnavailable
}
func TestBlockedWorkerDoesNotBlockServingCounters(t *testing.T) {
	source := &blockedSource{entered: make(chan struct{})}
	c, e := NewCollector("primary-one", t.TempDir(), source)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.now = func() time.Time { return testNow }
	c.Record(testID, testNow)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	flushed := make(chan struct{})
	go func() { defer close(flushed); c.Flush(ctx) }()
	<-source.entered
	counted := make(chan struct{})
	go func() {
		defer close(counted)
		for n := 0; n < 1000; n++ {
			c.Record(testID, testNow)
		}
	}()
	select {
	case <-counted:
	case <-time.After(time.Second):
		t.Fatal("serving waited for analytics network IO")
	}
	cancel()
	<-flushed
}

type failingSource struct{ attempted []int }

func (s *failingSource) SubmitPageViews(_ context.Context, snapshot Snapshot) error {
	s.attempted = append(s.attempted, snapshot.Shard)
	return ErrBusy
}
func (s *failingSource) PageViews(context.Context, Query) (Result, error) {
	return Result{}, ErrUnavailable
}
func TestPendingShardFairnessAndDayRollover(t *testing.T) {
	source := &failingSource{}
	c, e := NewCollector("primary-one", t.TempDir(), source)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.now = func() time.Time { return testNow }
	for attempt := 0; attempt < 3; attempt++ {
		for _, prefix := range []string{"0", "1", "2"} {
			c.Record(strings.Repeat(prefix, 32), testNow)
		}
		c.Flush(context.Background())
	}
	if fmt.Sprint(source.attempted) != "[0 1 2]" {
		t.Fatal("pending shard starved", source.attempted)
	}
	// Two persisted days stay bounded, and a new day can evict an old captured
	// counter without dropping new views or needing disk/network IO in Record.
	c.Record(testID, testNow)
	c.Record(testID, testNow.Add(-24*time.Hour))
	c.Flush(context.Background())
	c.Record(testID, testNow.Add(24*time.Hour))
	if c.Metrics()["droppedViews"] != 0 {
		t.Fatal("captured old day blocked rollover")
	}
}

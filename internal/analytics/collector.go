package analytics

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	jitter "math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type hours struct{ values [24]atomic.Uint64 }
type counterDay struct {
	projects  map[string]*hours
	revision  atomic.Uint64
	persisted uint64
}
type counterShard struct {
	sync.RWMutex
	days map[int64]*counterDay
}
type Collector struct {
	node, producer                   string
	root                             *os.Root
	source                           Source
	shards                           [Shards]counterShard
	flushMu                          sync.Mutex // one worker, including shutdown; never used by Record
	now                              func() time.Time
	cursor                           string
	pending                          atomic.Uint64
	dropped, errors, persisted, sent atomic.Uint64
}

func NewCollector(node, outbox string, source Source) (*Collector, error) {
	if !nodePattern.MatchString(node) || source == nil {
		return nil, ErrInvalid
	}
	if e := os.MkdirAll(outbox, 0700); e != nil {
		return nil, e
	}
	info, e := os.Lstat(outbox)
	if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrInvalid
	}
	root, e := os.OpenRoot(outbox)
	if e != nil {
		return nil, e
	}
	raw := make([]byte, 16)
	if _, e = rand.Read(raw); e != nil {
		root.Close()
		return nil, e
	}
	// Only one HTTP process owns this private outbox; leftover atomic-write
	// temporaries cannot be live after its previous boot has ended.
	dir, e := root.Open(".")
	if e != nil {
		root.Close()
		return nil, e
	}
	entries, e := dir.ReadDir(1025)
	dir.Close()
	if e != nil && e != io.EOF {
		root.Close()
		return nil, e
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".pending-") && entry.Type().IsRegular() {
			_ = root.Remove(entry.Name())
		}
	}
	c := &Collector{node: node, producer: hex.EncodeToString(raw), root: root, source: source, now: time.Now}
	for i := range c.shards {
		c.shards[i].days = map[int64]*counterDay{}
	}
	return c, nil
}
func (c *Collector) Close() error { return c.root.Close() }

// Record performs bounded memory work only. Readers share an RLock and existing
// counters use atomics; there is no disk, network, goroutine or queue per view.
func (c *Collector) Record(id string, at time.Time) {
	if !idPattern.MatchString(id) {
		return
	}
	at = at.UTC()
	day := at.Unix() / 86400
	s := &c.shards[shard(id)]
	s.RLock()
	d := s.days[day]
	var h *hours
	if d != nil {
		h = d.projects[id]
	}
	s.RUnlock()
	if h == nil {
		s.Lock()
		d = s.days[day]
		if d == nil {
			for oldDay, old := range s.days {
				if oldDay < day-1 && old.persisted == old.revision.Load() {
					delete(s.days, oldDay)
				}
			}
			if len(s.days) >= 2 {
				s.Unlock()
				c.dropped.Add(1)
				return
			}
			d = &counterDay{projects: map[string]*hours{}}
			s.days[day] = d
		}
		h = d.projects[id]
		if h == nil {
			if len(d.projects) >= ProjectsPerShard {
				s.Unlock()
				c.dropped.Add(1)
				return
			}
			h = &hours{}
			d.projects[id] = h
		}
		s.Unlock()
	}
	h.values[at.Hour()].Add(1)
	d.revision.Add(1)
}
func (c *Collector) PageViews(ctx context.Context, q Query) (Result, error) {
	return c.source.PageViews(ctx, q)
}
func (c *Collector) Metrics() map[string]uint64 {
	return map[string]uint64{"droppedViews": c.dropped.Load(), "workerErrors": c.errors.Load(), "persistedSnapshots": c.persisted.Load(), "sentSnapshots": c.sent.Load(), "pendingSnapshots": c.pending.Load()}
}

// One replaceable outbox file per producer/day/shard bounds backlog independently
// of request rate. Only the background worker writes it; replacement is atomic.
func (c *Collector) persist(snapshot Snapshot) error {
	data, e := json.Marshal(snapshot)
	if e != nil || len(data) > MaxSnapshotBytes {
		return ErrBusy
	}
	name := snapshot.Producer + "-" + snapshot.Day + "-" + string("0123456789abcdef"[snapshot.Shard]) + ".json"
	entries, e := c.root.Open(".")
	if e != nil {
		return e
	}
	list, e := entries.ReadDir(257)
	entries.Close()
	if e != nil && e != io.EOF {
		return e
	}
	var bytes int64
	files := 0
	for _, entry := range list {
		if entry.Type().IsRegular() {
			info, e := entry.Info()
			if e != nil {
				return e
			}
			if entry.Name() != name {
				bytes += info.Size()
				files++
			}
		}
	}
	if files >= 256 || bytes+int64(len(data)) > 32<<20 {
		return ErrBusy
	}
	if e = atomicFile(c.root, name, data, false); e == nil {
		c.persisted.Add(1)
	}
	return e
}
func (c *Collector) Flush(ctx context.Context) {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	now := c.now().UTC()
	for index := range c.shards {
		if ctx.Err() != nil {
			return
		}
		s := &c.shards[index]
		s.Lock()
		// Current-day counters remain cumulative after ACK. Old days can be dropped
		// once safely captured in the private outbox, or explicitly lost after 48h.
		for day, d := range s.days {
			date := time.Unix(day*86400, 0).UTC()
			if date.Before(Day(now).AddDate(0, 0, -1)) {
				if d.revision.Load() != d.persisted {
					c.dropped.Add(d.revision.Load() - d.persisted)
				}
				delete(s.days, day)
			}
		}
		days := make([]int64, 0, len(s.days))
		for day := range s.days {
			days = append(days, day)
		}
		s.Unlock()
		for _, day := range days {
			s.RLock()
			d := s.days[day]
			// A first view in a newer day can evict an already captured old day
			// between collecting the keys and reacquiring this lock.
			if d == nil {
				s.RUnlock()
				continue
			}
			revision := d.revision.Load()
			if revision == d.persisted {
				s.RUnlock()
				continue
			}
			// Capture revision BEFORE counters. Any concurrent later increment forces
			// another snapshot even if this copy happened to include its value already.
			snapshot := Snapshot{Node: c.node, Producer: c.producer, Day: time.Unix(day*86400, 0).UTC().Format("2006-01-02"), Shard: index, Sequence: revision, UpdatedAt: now, Projects: make(map[string][24]uint64, len(d.projects))}
			for id, h := range d.projects {
				var values [24]uint64
				for i := range values {
					values[i] = h.values[i].Load()
				}
				snapshot.Projects[id] = values
			}
			s.RUnlock()
			if e := c.persist(snapshot); e != nil {
				c.errors.Add(1)
				continue
			}
			s.Lock()
			d.persisted = revision
			s.Unlock()
		}
	}
	// Bound network work per tick and use a finite context. Failures leave durable
	// outbox snapshots in place, including snapshots from previous process boots.
	dir, e := c.root.Open(".")
	if e != nil {
		c.errors.Add(1)
		return
	}
	entries, e := dir.ReadDir(257)
	dir.Close()
	if e != nil && e != io.EOF {
		c.errors.Add(1)
		return
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	c.pending.Store(uint64(len(entries)))
	// Round-robin across snapshot names so a continuously dirty early shard cannot
	// starve later shards while IO/network admission is constrained.
	split := sort.Search(len(entries), func(i int) bool { return entries[i].Name() > c.cursor })
	entries = append(entries[split:], entries[:split]...)
	sent := 0
	for _, entry := range entries {
		if ctx.Err() != nil || sent >= 32 {
			return
		}
		if !entry.Type().IsRegular() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, e := readLimited(c.root, entry.Name(), MaxSnapshotBytes)
		var snapshot Snapshot
		if e != nil || decode(data, &snapshot) != nil || snapshot.Validate(now) != nil {
			c.errors.Add(1)
			continue
		}
		day, _ := time.Parse("2006-01-02", snapshot.Day)
		if day.Before(Cutoff(now)) {
			_ = c.root.Remove(entry.Name())
			continue
		}
		sent++
		c.cursor = entry.Name()
		if e = c.source.SubmitPageViews(ctx, snapshot); e != nil {
			c.errors.Add(1)
			return
		}
		if e = c.root.Remove(entry.Name()); e != nil && !os.IsNotExist(e) {
			c.errors.Add(1)
		} else {
			c.sent.Add(1)
			c.pending.Add(^uint64(0))
		}
	}
}
func (c *Collector) Run(ctx context.Context) {
	// Recover pending snapshots promptly, then collect every ten seconds. A failed
	// tick gets no tight retry loop, and leadership routing is handled by Source.
	flush := func(parent context.Context) {
		bounded, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		c.Flush(bounded)
	}
	flush(ctx)
	ticker := time.NewTimer(10*time.Second + time.Duration(jitter.IntN(1000))*time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			bounded, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			c.Flush(bounded)
			cancel()
			return
		case <-ticker.C:
			started := time.Now()
			flush(ctx)
			interval := 10*time.Second - time.Since(started) + time.Duration(jitter.IntN(1000))*time.Millisecond
			if interval < time.Second {
				interval = time.Second
			}
			ticker.Reset(interval)
		}
	}
}

package analytics

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/unix"
	"golang.org/x/time/rate"
)

func decode(raw []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrInvalid
	}
	return nil
}
func readLimited(root *os.Root, name string, limit int64) ([]byte, error) {
	f, e := root.Open(name)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() || info.Size() > limit {
		return nil, ErrInvalid
	}
	data, e := io.ReadAll(io.LimitReader(f, limit+1))
	if int64(len(data)) > limit {
		return nil, ErrInvalid
	}
	return data, e
}
func allocated(size int64) int64 { return ((size+4095)/4096 + 1) * 4096 }
func diskReady(root *os.Root, size int64) error {
	var info unix.Statfs_t
	if unix.Statfs(root.Name(), &info) != nil || info.Bsize <= 0 || info.Bavail*uint64(info.Bsize) < uint64(1<<30+allocated(size)) || info.Files != 0 && info.Ffree < 1024 {
		return ErrBusy
	}
	return nil
}
func atomicFile(root *os.Root, name string, data []byte, exclusive bool) error {
	if e := diskReady(root, int64(len(data))); e != nil {
		return e
	}
	parent := path.Dir(name)
	if e := root.MkdirAll(parent, 0700); e != nil {
		return e
	}
	nonce := make([]byte, 16)
	if _, e := rand.Read(nonce); e != nil {
		return e
	}
	temp := path.Join(parent, ".pending-"+hex.EncodeToString(nonce))
	f, e := root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer root.Remove(temp)
	if _, e = f.Write(data); e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if exclusive {
		e = root.Link(temp, name)
	} else {
		e = root.Rename(temp, name)
	}
	if e != nil {
		return e
	}
	directory, e := root.Open(parent)
	if e != nil {
		return e
	}
	defer directory.Close()
	return directory.Sync()
}

type cachedResult struct {
	key        string
	value      Result
	until      time.Time
	generation uint64
}
type Archive struct {
	tracked                    map[string]int64
	readBudget                 *rate.Limiter
	writeBudget                *rate.Limiter
	root                       *os.Root
	writeMu                    sync.Mutex // bounded background IO only; never serving or owner auth
	bytes                      int64
	files                      int
	ready                      atomic.Bool
	generation                 atomic.Uint64
	queries                    chan struct{}
	cacheMu                    sync.Mutex
	cache                      map[string]*list.Element
	lru                        *list.List
	flights                    singleflight.Group
	now                        func() time.Time
	accepted, rejected, errors atomic.Uint64
}

func NewArchive(directory string) (*Archive, error) {
	if e := os.MkdirAll(directory, 0700); e != nil {
		return nil, e
	}
	info, e := os.Lstat(directory)
	if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrInvalid
	}
	root, e := os.OpenRoot(directory)
	if e != nil {
		return nil, e
	}
	return &Archive{tracked: map[string]int64{}, readBudget: rate.NewLimiter(4<<20, MaxSnapshotBytes), writeBudget: rate.NewLimiter(1<<20, MaxSnapshotBytes), root: root, queries: make(chan struct{}, 2), cache: map[string]*list.Element{}, lru: list.New(), now: time.Now}, nil
}
func (a *Archive) Close() error              { return a.root.Close() }
func directory(day string, shard int) string { return fmt.Sprintf("%s/%x", day, shard) }
func filename(s Snapshot) string {
	return fmt.Sprintf("%s.%s.%020d.json", s.Node, s.Producer, s.Sequence)
}
func parseName(name string) (string, uint64, bool) {
	parts := strings.Split(name, ".")
	if len(parts) != 4 || !nodePattern.MatchString(parts[0]) || !idPattern.MatchString(parts[1]) || len(parts[2]) != 20 || parts[3] != "json" {
		return "", 0, false
	}
	seq, e := strconv.ParseUint(parts[2], 10, 64)
	return parts[0] + "." + parts[1], seq, e == nil && seq > 0
}
func (a *Archive) entries(dir string) ([]os.DirEntry, error) {
	f, e := a.root.Open(dir)
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	defer f.Close()
	entries, e := f.ReadDir(MaxDirectoryFiles + 1)
	if e != nil && e != io.EOF {
		return nil, e
	}
	if len(entries) > MaxDirectoryFiles {
		return nil, ErrBusy
	}
	return entries, nil
}

// Accept stores immutable producer snapshots. It deliberately does not mutate
// shared leader-owned totals: even an isolated old leader cannot overwrite or
// double-add another leader's result. The latest sequence wins when files merge.
func (a *Archive) Accept(ctx context.Context, s Snapshot) error {
	if e := s.Validate(a.now()); e != nil {
		return e
	}
	day, _ := time.Parse("2006-01-02", s.Day)
	if day.Before(Cutoff(a.now())) {
		return nil
	} // expired retries are ACKed and discarded
	raw, e := json.Marshal(s)
	if e != nil || len(raw) > MaxSnapshotBytes {
		return ErrInvalid
	}
	if !a.ready.Load() || !a.writeMu.TryLock() {
		a.rejected.Add(1)
		return ErrBusy
	}
	defer a.writeMu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if e := a.writeBudget.WaitN(ctx, len(raw)); e != nil {
		return ErrBusy
	}
	dir := directory(s.Day, s.Shard)
	name := path.Join(dir, filename(s))
	source := s.Node + "." + s.Producer
	entries, e := a.entries(dir)
	if e != nil {
		return e
	}
	var older []struct {
		name     string
		sequence uint64
	}
	for _, entry := range entries {
		producer, sequence, ok := parseName(entry.Name())
		if !ok || producer != source {
			continue
		}
		if sequence > s.Sequence {
			return nil
		}
		if sequence == s.Sequence {
			old, e := readLimited(a.root, name, MaxSnapshotBytes)
			if e != nil {
				return e
			}
			if !bytes.Equal(old, raw) {
				return ErrInvalid
			}
			return nil
		}
		older = append(older, struct {
			name     string
			sequence uint64
		}{entry.Name(), sequence})
	}
	if len(entries) >= MaxDirectoryFiles || a.files >= MaxArchiveFiles || a.bytes+allocated(int64(len(raw))) > MaxArchiveBytes {
		a.rejected.Add(1)
		return ErrBusy
	}
	if e = atomicFile(a.root, name, raw, true); e != nil {
		if os.IsExist(e) {
			old, e := readLimited(a.root, name, MaxSnapshotBytes)
			if e == nil && bytes.Equal(old, raw) {
				return nil
			}
		}
		a.errors.Add(1)
		return e
	}
	a.files++
	a.bytes += allocated(int64(len(raw)))
	a.tracked[name] = allocated(int64(len(raw)))
	a.accepted.Add(1)
	a.generation.Add(1)
	// Keep two immutable generations. Deleting an old generation never subtracts
	// views: new snapshots are cumulative. Replication can temporarily undercount
	// if deletion arrives first, which is part of approximate/eventual semantics.
	sort.Slice(older, func(i, j int) bool { return older[i].sequence > older[j].sequence })
	for _, old := range older[min(1, len(older)):] {
		file := path.Join(dir, old.name)
		_, e := a.root.Lstat(file)
		if e == nil && a.root.Remove(file) == nil {
			if charged, ok := a.tracked[file]; ok {
				a.files--
				a.bytes -= charged
				delete(a.tracked, file)
			}
		}
	}
	return nil
}
func (a *Archive) Metrics() map[string]uint64 {
	return map[string]uint64{"acceptedSnapshots": a.accepted.Load(), "rejectedSnapshots": a.rejected.Load(), "archiveErrors": a.errors.Load()}
}

// Sweep accounts for replicated files and removes expired data incrementally.
// Local admission is capped at 1 GiB/65536 files. External volume replication can
// temporarily exceed that tracked budget; statfs headroom is checked per write.
// No retention scan runs in the site-serving path.
func (a *Archive) Sweep(ctx context.Context) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	var used int64
	tracked := map[string]int64{}
	count, removed, visited := 0, 0, 0
	cutoff := Cutoff(a.now()).Format("2006-01-02")
	err := fs.WalkDir(a.root.FS(), ".", func(name string, d fs.DirEntry, e error) error {
		if e != nil {
			if os.IsNotExist(e) {
				return nil
			}
			return e
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		visited++
		if visited%128 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
		if d.IsDir() {
			return nil
		}
		parts := strings.Split(name, "/")
		_, validDate := time.Parse("2006-01-02", parts[0])
		expired := validDate == nil && parts[0] < cutoff
		info, e := d.Info()
		if e != nil {
			if os.IsNotExist(e) {
				return nil
			}
			return e
		}
		// Orphaned temporary files are from interrupted atomic writes. Leave recent
		// files alone because another primary may be replicating an active write.
		orphan := strings.HasPrefix(d.Name(), ".pending-") && info.ModTime().Before(a.now().Add(-time.Hour))
		if (expired || orphan) && removed < 4096 {
			if e = a.root.Remove(name); e == nil {
				removed++
				return nil
			}
		}
		used += allocated(info.Size())
		tracked[name] = allocated(info.Size())
		count++
		if count > MaxArchiveFiles*2 {
			return ErrBusy
		}
		return nil
	})
	if err != nil {
		a.ready.Store(false)
		a.errors.Add(1)
		return err
	}
	a.bytes, a.files = used, count
	a.tracked = tracked
	a.ready.Store(true)
	if removed > 0 {
		a.generation.Add(1)
	}
	// Remove empty old directory trees without recursively deleting newer files.
	days, e := a.entries(".")
	if e == nil {
		for _, day := range days {
			if !day.IsDir() || day.Name() >= cutoff {
				continue
			}
			if _, e := time.Parse("2006-01-02", day.Name()); e != nil {
				continue
			}
			children, e := a.entries(day.Name())
			if e != nil {
				continue
			}
			for _, child := range children {
				if child.IsDir() {
					_ = a.root.Remove(path.Join(day.Name(), child.Name()))
				}
			}
			_ = a.root.Remove(day.Name())
		}
	}
	return nil
}
func (a *Archive) Run(ctx context.Context) {
	for {
		err := a.Sweep(ctx)
		interval := time.Hour
		if err != nil {
			interval = time.Minute
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func cloneResult(r Result) Result {
	r.Buckets = append([]Bucket(nil), r.Buckets...)
	if r.UpdatedAt != nil {
		copy := *r.UpdatedAt
		r.UpdatedAt = &copy
	}
	return r
}
func (a *Archive) Query(ctx context.Context, q Query) (Result, error) {
	if e := q.Validate(a.now()); e != nil {
		return Result{}, e
	}
	key := fmt.Sprintf("%s/%d/%d/%s", q.ProjectID, q.From.Unix(), q.To.Unix(), q.Interval)
	lookup := func() (Result, bool) {
		a.cacheMu.Lock()
		defer a.cacheMu.Unlock()
		if entry := a.cache[key]; entry != nil {
			v := entry.Value.(cachedResult)
			if a.now().Before(v.until) && v.generation == a.generation.Load() {
				a.lru.MoveToFront(entry)
				return cloneResult(v.value), true
			}
		}
		return Result{}, false
	}
	if r, ok := lookup(); ok {
		return r, nil
	}
	// Admission precedes singleflight so duplicate callers cannot create an
	// unbounded wait queue. Owner checks happen outside this cache on every call.
	select {
	case a.queries <- struct{}{}:
		defer func() { <-a.queries }()
	default:
		return Result{}, ErrBusy
	}
	value, e, _ := a.flights.Do(key, func() (any, error) {
		if r, ok := lookup(); ok {
			return r, nil
		}
		generation := a.generation.Load()
		result, e := a.query(ctx, q)
		if e != nil {
			return Result{}, e
		}
		a.cacheMu.Lock()
		defer a.cacheMu.Unlock()
		if old := a.cache[key]; old != nil {
			a.lru.Remove(old)
		}
		a.cache[key] = a.lru.PushFront(cachedResult{key: key, value: cloneResult(result), until: a.now().Add(5 * time.Second), generation: generation})
		for a.lru.Len() > 32 {
			last := a.lru.Back()
			delete(a.cache, last.Value.(cachedResult).key)
			a.lru.Remove(last)
		}
		return result, nil
	})
	if e != nil {
		return Result{}, e
	}
	return cloneResult(value.(Result)), nil
}
func (a *Archive) query(ctx context.Context, q Query) (Result, error) {
	result := Result{ProjectID: q.ProjectID, Timezone: "UTC", Approximate: true, Interval: q.Interval, From: q.From.UTC(), To: q.To.UTC(), Buckets: []Bucket{}}
	step := time.Hour
	if q.Interval == "day" {
		step = 24 * time.Hour
	}
	indexes := map[int64]int{}
	for at := q.From; at.Before(q.To); at = at.Add(step) {
		indexes[at.Unix()] = len(result.Buckets)
		result.Buckets = append(result.Buckets, Bucket{Start: at.UTC()})
	}
	readBytes, readFiles := 0, 0
	for day := Day(q.From); day.Before(q.To); day = day.AddDate(0, 0, 1) {
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
		dir := directory(day.Format("2006-01-02"), shard(q.ProjectID))
		entries, e := a.entries(dir)
		if e != nil {
			return Result{}, e
		}
		type selected struct {
			name     string
			sequence uint64
		}
		latest := map[string]selected{}
		for _, entry := range entries {
			producer, seq, ok := parseName(entry.Name())
			if !ok || !entry.Type().IsRegular() {
				continue
			}
			if old := latest[producer]; seq > old.sequence {
				latest[producer] = selected{entry.Name(), seq}
			}
		}
		if len(latest) > 128 {
			return Result{}, ErrBusy
		}
		for _, file := range latest {
			if ctx.Err() != nil {
				return Result{}, ctx.Err()
			}
			raw, e := readLimited(a.root, path.Join(dir, file.name), MaxSnapshotBytes)
			if os.IsNotExist(e) {
				return Result{}, ErrUnavailable
			}
			if e != nil {
				return Result{}, e
			}
			readBytes += len(raw)
			readFiles++
			if readBytes > 64<<20 || readFiles > 4096 {
				return Result{}, ErrBusy
			}
			if e := a.readBudget.WaitN(ctx, len(raw)); e != nil {
				return Result{}, ErrBusy
			}
			var snapshot Snapshot
			if decode(raw, &snapshot) != nil || snapshot.Validate(a.now()) != nil || filename(snapshot) != file.name || snapshot.Day != day.Format("2006-01-02") || snapshot.Shard != shard(q.ProjectID) {
				return Result{}, ErrUnavailable
			}
			values, exists := snapshot.Projects[q.ProjectID]
			if !exists {
				continue
			}
			if result.UpdatedAt == nil || snapshot.UpdatedAt.After(*result.UpdatedAt) {
				updated := snapshot.UpdatedAt
				result.UpdatedAt = &updated
			}
			for hour, value := range values {
				at := day.Add(time.Duration(hour) * time.Hour)
				if at.Before(q.From) || !at.Before(q.To) {
					continue
				}
				bucket := at
				if q.Interval == "day" {
					bucket = day
				}
				index := indexes[bucket.Unix()]
				result.Buckets[index].PageViews = add(result.Buckets[index].PageViews, value)
				result.PageViews = add(result.PageViews, value)
			}
		}
	}
	return result, nil
}

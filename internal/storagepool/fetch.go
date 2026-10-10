package storagepool

import (
	"container/list"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/netip"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/unix"
)

var errFetchAdmission = errors.New("storage fetch admission unavailable")

type fetchMetrics struct{ QueueFull, WaitTimeout, DiskDenied, Hits, Misses, Shared, Retries atomic.Uint64 }
type fileFlight struct {
	done    chan struct{}
	refs    int
	path    string
	entry   content.File
	err     error
	release func()
}
type parsedManifest struct {
	files map[string]content.File
	bytes int64
}
type manifestEntry struct {
	key   string
	value parsedManifest
}
type fetchState struct {
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	closed         bool
	wg             sync.WaitGroup
	callers        chan struct{}
	flights        map[string]*fileFlight
	manifests      map[string]*list.Element
	lru            *list.List
	manifestBytes  int64
	manifestFlight singleflight.Group
	spoolBytes     int64
	spoolFiles     uint64
	wait           time.Duration
	budget         time.Duration
	metrics        fetchMetrics
}

func newFetchState(c Config) *fetchState {
	ctx, cancel := context.WithCancel(context.Background())
	return &fetchState{ctx: ctx, cancel: cancel, callers: make(chan struct{}, c.FetchConcurrency+c.FetchQueue), flights: map[string]*fileFlight{}, manifests: map[string]*list.Element{}, lru: list.New(), wait: 20 * time.Second, budget: 2 * time.Minute}
}
func (p *Pool) acquireFetch(ctx context.Context) error {
	timer := time.NewTimer(p.fetch.wait)
	defer timer.Stop()
	// Only distinct downloads consume slots. Duplicate waiters are separately
	// bounded by callers, so sharing cannot bypass the global admission bound.
	select {
	case p.downloads <- struct{}{}:
		return nil
	default:
	}
	select {
	case p.downloads <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		p.fetch.metrics.WaitTimeout.Add(1)
		return errFetchAdmission
	}
}
func (p *Pool) reserveSpool(size int64) (func(), error) {
	s := p.fetch
	s.mu.Lock()
	defer s.mu.Unlock()
	var fs unix.Statfs_t
	if size < 0 || unix.Statfs(p.cache.root, &fs) != nil || fs.Bsize <= 0 {
		s.metrics.DiskDenied.Add(1)
		return nil, errFull
	}
	// Reservations include a block-rounded file and a directory entry block.
	// statfs already includes installed cache/upload bytes; subtract ALL outstanding
	// reservations conservatively, including spools that have begun writing.
	bytes := ((size+fs.Bsize-1)/fs.Bsize + 1) * fs.Bsize
	if bytes > p.config.FetchBytes-s.spoolBytes || uint64(Headroom+s.spoolBytes+bytes) > fs.Bavail*uint64(fs.Bsize) || fs.Files != 0 && fs.Ffree < 1024+s.spoolFiles+1 {
		s.metrics.DiskDenied.Add(1)
		return nil, errFull
	}
	s.spoolBytes += bytes
	s.spoolFiles++
	var once sync.Once
	return func() { once.Do(func() { s.mu.Lock(); s.spoolBytes -= bytes; s.spoolFiles--; s.mu.Unlock() }) }, nil
}
func storagePrefix(pr project.Project) (string, error) {
	if pr.StorageGeneration != "" {
		if !digestRE.MatchString(pr.StorageGeneration) {
			return "", project.ErrStorage
		}
		return apiPrefix + "generations/" + pr.ID + "/" + pr.StorageGeneration + "/" + pr.ActiveDigest, nil
	}
	return apiPrefix + "versions/" + pr.ID + "/" + pr.ActiveDigest, nil
}
func (p *Pool) manifest(ctx context.Context, a *appRuntime, pr project.Project, prefix string) (parsedManifest, error) {
	key := cacheKey(pr, "")
	s := p.fetch
	lookup := func() (parsedManifest, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if e := s.manifests[key]; e != nil {
			s.lru.MoveToFront(e)
			return e.Value.(manifestEntry).value, true
		}
		return parsedManifest{}, false
	}
	if m, ok := lookup(); ok {
		return m, nil
	}
	// All callers are bounded by fetch admission and workers have a shutdown-bound
	// context. Manifest work is shared independently from the file flight.
	result, err, _ := s.manifestFlight.Do(key, func() (any, error) {
		if m, ok := lookup(); ok {
			return m, nil
		}
		for _, node := range a.readNodes() {
			addr, e := netip.ParseAddrPort(node.Address)
			if e != nil {
				continue
			}
			attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
			res, e := a.request(attempt, addr, "GET", prefix+"/manifest", nil, "")
			if e != nil {
				cancel()
				continue
			}
			data, e := io.ReadAll(io.LimitReader(res.Body, (8<<20)+1))
			status := res.StatusCode
			res.Body.Close()
			cancel()
			if e != nil || status != 200 || len(data) > 8<<20 {
				continue
			}
			m, e := content.ParseManifest(data, pr.ActiveDigest)
			if e != nil {
				continue
			}
			parsed := parsedManifest{files: make(map[string]content.File, len(m.Files)), bytes: int64(len(data)) + int64(len(m.Files))*256}
			for _, f := range m.Files {
				parsed.files[f.Path] = f
				parsed.bytes += int64(len(f.Path) + len(f.SHA256))
			}
			s.mu.Lock()
			for s.lru.Len() > 0 && (s.manifestBytes+parsed.bytes > 64<<20 || s.lru.Len() >= 256) {
				old := s.lru.Back()
				v := old.Value.(manifestEntry)
				delete(s.manifests, v.key)
				s.manifestBytes -= v.value.bytes
				s.lru.Remove(old)
			}
			// Oversize verified manifests are shared with current callers, never retained.
			if parsed.bytes <= 64<<20 {
				s.manifests[key] = s.lru.PushFront(manifestEntry{key, parsed})
				s.manifestBytes += parsed.bytes
			}
			s.mu.Unlock()
			return parsed, nil
		}
		return nil, project.ErrStorage
	})
	if err != nil {
		return parsedManifest{}, err
	}
	return result.(parsedManifest), nil
}
func (a *appRuntime) readNodes() []NodeHealth {
	nodes := a.nodes()
	rand.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
	a.mu.RLock()
	sort.SliceStable(nodes, func(i, j int) bool {
		left, _ := netip.ParseAddrPort(nodes[i].Address)
		right, _ := netip.ParseAddrPort(nodes[j].Address)
		return a.loads[left] < a.loads[right]
	})
	a.mu.RUnlock()
	return nodes
}

// fetchFile returns a descriptor with its OWN offset. A flight owns the spool
// until every waiter/reader releases its reference, including canceled callers.
// The cache pin is registered before done closes. Slots cover verification only,
// never downstream client streaming. Caller cancellation never cancels peers.
func (p *Pool) fetchFile(ctx context.Context, pr project.Project, name string) (*os.File, content.File, func(), error) {
	s := p.fetch
	select {
	case s.callers <- struct{}{}:
	default:
		s.metrics.QueueFull.Add(1)
		return nil, content.File{}, nil, errFetchAdmission
	}
	key := cacheKey(pr, name)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.callers
		return nil, content.File{}, nil, errFetchAdmission
	}
	flight := s.flights[key]
	first := flight == nil
	if flight == nil {
		flight = &fileFlight{done: make(chan struct{})}
		s.flights[key] = flight
		s.wg.Add(1)
	} else {
		s.metrics.Shared.Add(1)
	}
	flight.refs++
	s.mu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			var cleanup func()
			s.mu.Lock()
			flight.refs--
			if flight.refs == 0 {
				select {
				case <-flight.done:
					delete(s.flights, key)
					cleanup = flight.release
				default:
				}
			}
			s.mu.Unlock()
			if cleanup != nil {
				cleanup()
			}
			<-s.callers
		})
	}
	if first {
		go func() {
			defer s.wg.Done()
			bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.budget)
			stop := context.AfterFunc(s.ctx, cancel)
			defer stop()
			defer cancel()
			var file *os.File
			var entry content.File
			var cleanup func()
			var err error
			if err = p.acquireFetch(bounded); err == nil {
				func() {
					defer func() { <-p.downloads }()
					if f, e, rel, ok := p.cache.get(key); ok {
						file, entry, cleanup = f, e, rel
						return
					}
					a := p.app(pr.StorageApp)
					if a == nil {
						err = project.ErrStorage
						return
					}
					prefix, e := storagePrefix(pr)
					if e != nil {
						err = e
						return
					}
					m, e := p.manifest(bounded, a, pr, prefix)
					if e != nil {
						err = e
						return
					}
					var ok bool
					entry, ok = m.files[name]
					if !ok {
						err = project.ErrNotFound
						return
					}
					reserved, e := p.reserveSpool(entry.Size)
					if e != nil {
						err = e
						return
					}
					for _, node := range a.readNodes() {
						addr, e := netip.ParseAddrPort(node.Address)
						if e != nil {
							continue
						}
						attempt, cancel := context.WithTimeout(bounded, 45*time.Second)
						file, e = p.download(attempt, a, addr, prefix+"/files/"+name, entry)
						cancel()
						if e == nil {
							cacheRelease := p.cache.put(key, file, entry)
							cleanup = func() { cacheRelease(); reserved() }
							return
						}
						s.metrics.Retries.Add(1)
					}
					reserved()
					err = project.ErrStorage
				}()
			}
			s.mu.Lock()
			flight.err = err
			flight.entry = entry
			flight.release = cleanup
			if file != nil {
				flight.path = file.Name()
			}
			close(flight.done)
			abandoned := flight.refs == 0
			if abandoned {
				delete(s.flights, key)
			}
			s.mu.Unlock()
			if abandoned && cleanup != nil {
				cleanup()
			}
		}()
	}
	select {
	case <-ctx.Done():
		release()
		return nil, content.File{}, nil, ctx.Err()
	case <-s.ctx.Done():
		release()
		return nil, content.File{}, nil, errFetchAdmission
	case <-flight.done:
	}
	if flight.err != nil {
		release()
		return nil, content.File{}, nil, flight.err
	}
	f, err := os.Open(flight.path)
	if err != nil {
		release()
		return nil, content.File{}, nil, err
	}
	return f, flight.entry, func() { f.Close(); release() }, nil
}

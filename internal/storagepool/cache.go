package storagepool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
	"golang.org/x/sys/unix"
)

type cacheEntry struct {
	path string
	file content.File
	last time.Time
	pins int
}
type fileCache struct {
	mu          sync.Mutex
	root        string
	limit, used int64
	entries     map[string]*cacheEntry
}

func newFileCache(root string, limit int64) (*fileCache, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || limit < 0 {
		return nil, errors.New("invalid local cache")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("cache must be a real private directory")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	// Reconstructing an untrusted cache after a crash is unnecessary. Only remove
	// our own flat file namespace; a foreign entry fails startup for inspection.
	for _, e := range entries {
		if e.IsDir() || e.Type()&os.ModeSymlink != 0 || len(e.Name()) < 7 || e.Name()[:7] != "cached-" {
			return nil, errors.New("unexpected entry in private cache")
		}
		if err := os.Remove(filepath.Join(root, e.Name())); err != nil {
			return nil, err
		}
	}
	return &fileCache{root: root, limit: limit, entries: map[string]*cacheEntry{}}, nil
}
func (c *fileCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.entries {
		if e.pins == 0 {
			_ = os.Remove(e.path)
		}
	}
}
func cacheKey(p project.Project, name string) string {
	h := sha256.Sum256([]byte(p.StorageApp + "\x00" + p.ID + "\x00" + p.ActiveDigest + "\x00" + name))
	return hex.EncodeToString(h[:])
}
func (c *fileCache) get(key string) (*os.File, content.File, func(), bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		return nil, content.File{}, nil, false
	}
	f, err := os.Open(e.path)
	if err != nil {
		c.used -= e.file.Size
		delete(c.entries, key)
		return nil, content.File{}, nil, false
	}
	e.pins++
	e.last = time.Now()
	return f, e.file, func() { f.Close(); c.mu.Lock(); e.pins--; c.mu.Unlock() }, true
}
func (c *fileCache) invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e != nil && e.pins == 0 {
		if os.Remove(e.path) == nil || !fileExists(e.path) {
			c.used -= e.file.Size
			delete(c.entries, key)
		}
	}
}
func fileExists(path string) bool { _, err := os.Lstat(path); return !os.IsNotExist(err) }

// put keeps the already opened descriptor pinned through response delivery.
// Failed/oversize cache admission leaves a temporary verified proxy response.
func (c *fileCache) put(key string, f *os.File, entry content.File) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	temporary := func() { f.Close(); _ = os.Remove(f.Name()) }
	if entry.Size > c.limit || c.limit == 0 || c.entries[key] != nil {
		return temporary
	}
	for c.used+entry.Size > c.limit || len(c.entries) >= 4096 {
		var oldest string
		for k, e := range c.entries {
			if e.pins == 0 && (oldest == "" || e.last.Before(c.entries[oldest].last)) {
				oldest = k
			}
		}
		if oldest == "" {
			return temporary
		}
		e := c.entries[oldest]
		if err := os.Remove(e.path); err != nil {
			return temporary
		}
		c.used -= e.file.Size
		delete(c.entries, oldest)
	}
	e := &cacheEntry{path: f.Name(), file: entry, last: time.Now(), pins: 1}
	c.entries[key] = e
	c.used += entry.Size
	return func() { f.Close(); c.mu.Lock(); e.pins--; c.mu.Unlock() }
}
func verifyFile(f *os.File, entry content.File) error {
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, entry.Size+1))
	if err != nil {
		return err
	}
	if n != entry.Size || hex.EncodeToString(h.Sum(nil)) != entry.SHA256 {
		return content.ErrInvalid
	}
	_, err = f.Seek(0, 0)
	return err
}
func (p *Pool) RemotePrivateContent() bool { return true }
func (p *Pool) ServeProject(w http.ResponseWriter, r *http.Request, pr project.Project, name string) {
	p.serve(w, r, pr, name, nil)
}
func (p *Pool) ServeAuthorizedProject(w http.ResponseWriter, r *http.Request, pr project.Project, name string, authorize func(context.Context) error) {
	p.serve(w, r, pr, name, authorize)
}
func (p *Pool) serve(w http.ResponseWriter, r *http.Request, pr project.Project, name string, authorize func(context.Context) error) {
	check := func() bool {
		if authorize == nil {
			return true
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := authorize(ctx); err != nil {
			w.Header().Del("Access-Control-Allow-Origin")
			w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
			if errors.Is(err, project.ErrNotFound) {
				http.NotFound(w, r)
			} else {
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(503)
			}
			return false
		}
		return true
	}
	if !idRE.MatchString(pr.ID) || !digestRE.MatchString(pr.ActiveDigest) || content.ValidatePath(name) != nil {
		http.NotFound(w, r)
		return
	}
	if !pr.Private {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
	}
	key := cacheKey(pr, name)
	if f, entry, release, ok := p.cache.get(key); ok {
		if verifyFile(f, entry) == nil {
			defer release()
			if !check() {
				return
			}
			w.Header().Set("ETag", "\""+entry.SHA256+"\"")
			http.ServeContent(w, r, name, time.Time{}, f)
			return
		}
		release()
		p.cache.invalidate(key)
	}
	select {
	case p.downloads <- struct{}{}:
		defer func() { <-p.downloads }()
	case <-r.Context().Done():
		w.WriteHeader(503)
		return
	default:
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(503)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	a := p.app(pr.StorageApp)
	if a == nil {
		w.WriteHeader(503)
		return
	}
	for _, node := range a.nodes() {
		addr, err := netip.ParseAddrPort(node.Address)
		if err != nil {
			continue
		}
		prefix := apiPrefix + "versions/" + pr.ID + "/" + pr.ActiveDigest
		res, err := a.request(ctx, addr, "GET", prefix+"/manifest", nil, "")
		if err != nil {
			continue
		}
		data, err := io.ReadAll(io.LimitReader(res.Body, 8<<20+1))
		status := res.StatusCode
		res.Body.Close()
		if err != nil || status != 200 {
			continue
		}
		manifest, err := content.ParseManifest(data, pr.ActiveDigest)
		if err != nil {
			continue
		}
		var entry content.File
		found := false
		for _, item := range manifest.Files {
			if item.Path == name {
				entry = item
				found = true
				break
			}
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		f, err := p.download(ctx, a, addr, prefix+"/files/"+name, entry)
		if err != nil {
			continue
		}
		release := p.cache.put(key, f, entry)
		defer release()
		if !check() {
			return
		}
		w.Header().Set("ETag", "\""+entry.SHA256+"\"")
		http.ServeContent(w, r, name, time.Time{}, f)
		return
	}
	w.Header().Set("Retry-After", "5")
	w.WriteHeader(503)
}
func (p *Pool) download(ctx context.Context, a *appRuntime, addr netip.AddrPort, path string, entry content.File) (*os.File, error) {
	var fs unix.Statfs_t
	if unix.Statfs(p.cache.root, &fs) != nil || fs.Bsize <= 0 || fs.Bavail < uint64((Headroom+4*(200<<20))/fs.Bsize) || fs.Files != 0 && fs.Ffree < 1024 {
		return nil, errFull
	}
	res, err := a.request(ctx, addr, "GET", path, nil, "")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 || res.Header.Get("Content-Encoding") != "" {
		return nil, project.ErrStorage
	}
	f, err := os.CreateTemp(p.cache.root, "cached-")
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			f.Close()
			os.Remove(f.Name())
		}
	}()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(res.Body, entry.Size+1))
	if err != nil {
		return nil, err
	}
	if n != entry.Size || hex.EncodeToString(h.Sum(nil)) != entry.SHA256 {
		return nil, content.ErrInvalid
	}
	if _, err = f.Seek(0, 0); err != nil {
		return nil, err
	}
	success = true
	return f, nil
}

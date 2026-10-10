package storagepool

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
)

type cacheEntry struct {
	key, path    string
	file         content.File
	pins         int
	allocated    int64
	stat         os.FileInfo
	idle         *list.Element
	handoffUntil time.Time
}
type fileCache struct {
	paths       map[string]*cacheEntry
	mu          sync.Mutex
	root        string
	limit, used int64
	allocated   int64
	maxEntries  int
	entries     map[string]*cacheEntry
	idle        *list.List
	retired     map[string]*cacheEntry
	closed      bool
	now         func() time.Time
}

func newFileCache(root string, limit int64) (*fileCache, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || limit < 0 {
		return nil, errors.New("invalid local cache")
	}
	if e := os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	info, e := os.Lstat(root)
	if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("cache must be a real private directory")
	}
	entries, e := os.ReadDir(root)
	if e != nil {
		return nil, e
	}
	// Restart occurs after supervisor terminates nginx; no old redirect can open
	// these names. Only remove our own flat namespace, never foreign directories.
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasPrefix(entry.Name(), "cached-") {
			return nil, errors.New("unexpected entry in private cache")
		}
		if e := os.Remove(filepath.Join(root, entry.Name())); e != nil {
			return nil, e
		}
	}
	return &fileCache{root: root, limit: limit, maxEntries: 4096, entries: map[string]*cacheEntry{}, idle: list.New(), retired: map[string]*cacheEntry{}, now: time.Now, paths: map[string]*cacheEntry{}}, nil
}
func cacheKey(p project.Project, name string) string {
	h := sha256.Sum256([]byte(p.StorageApp + "\x00" + p.ID + "\x00" + p.ActiveDigest + "\x00" + name))
	return hex.EncodeToString(h[:])
}
func fileExists(path string) bool { _, e := os.Lstat(path); return !os.IsNotExist(e) }
func (c *fileCache) remove(e *cacheEntry) bool {
	if e.pins != 0 || c.now().Before(e.handoffUntil) {
		return false
	}
	if err := os.Remove(e.path); err != nil && !os.IsNotExist(err) {
		return false
	}
	c.used -= e.file.Size
	c.allocated -= e.allocated
	delete(c.retired, e.path)
	delete(c.paths, e.path)
	if c.entries[e.key] == e {
		delete(c.entries, e.key)
	}
	if e.idle != nil {
		c.idle.Remove(e.idle)
		e.idle = nil
	}
	return true
}
func (c *fileCache) sweep() {
	for _, e := range c.retired {
		c.remove(e)
	}
}
func (c *fileCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for _, e := range c.entries {
		c.remove(e)
	}
	c.sweep()
}
func (c *fileCache) release(e *cacheEntry, f *os.File) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			f.Close()
			c.mu.Lock()
			defer c.mu.Unlock()
			e.pins--
			if e.pins == 0 {
				if c.closed {
					c.remove(e)
				} else if c.entries[e.key] == e {
					e.idle = c.idle.PushFront(e)
				}
			}
		})
	}
}
func (c *fileCache) get(key string) (*os.File, content.File, func(), bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil || c.closed {
		return nil, content.File{}, nil, false
	}
	f, err := os.Open(e.path)
	if err != nil {
		c.remove(e)
		return nil, content.File{}, nil, false
	}
	info, err := f.Stat()
	link, linkErr := os.Lstat(e.path)
	if err != nil || linkErr != nil || !info.Mode().IsRegular() || !os.SameFile(info, link) || e.stat == nil || !content.SameFileSnapshot(e.stat, info) || info.Size() != e.file.Size {
		f.Close()
		c.remove(e)
		return nil, content.File{}, nil, false
	}
	if e.idle != nil {
		c.idle.Remove(e.idle)
		e.idle = nil
	}
	e.pins++
	return f, e.file, c.release(e, f), true
}
func (c *fileCache) invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.remove(e)
	}
}
func (c *fileCache) put(key string, f *os.File, entry content.File) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	var once sync.Once
	temporary := func() { once.Do(func() { f.Close(); _ = os.Remove(f.Name()) }) }
	if c.closed || entry.Size > c.limit || c.limit == 0 || c.entries[key] != nil {
		return temporary
	}
	info, err := f.Stat()
	if err != nil {
		return temporary
	}
	block := int64(4096)
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Blksize > 0 {
		block = stat.Blksize
	}
	allocated := ((entry.Size+block-1)/block + 1) * block
	if allocated > c.limit {
		return temporary
	}
	c.sweep()
	for c.allocated+allocated > c.limit || len(c.entries)+len(c.retired) >= c.maxEntries {
		oldest := c.idle.Back()
		if oldest == nil {
			return temporary
		}
		e := oldest.Value.(*cacheEntry)
		if !c.remove(e) {
			// Grace-retired bytes remain charged against the SAME cache budget. Failed
			// unlink also stays charged; if no room remains, use bounded Go spooling.
			c.idle.Remove(oldest)
			e.idle = nil
			delete(c.entries, e.key)
			c.retired[e.path] = e
		}
	}
	e := &cacheEntry{key: key, path: f.Name(), file: entry, pins: 1, stat: info, allocated: allocated}
	c.entries[key] = e
	c.paths[e.path] = e
	c.used += entry.Size
	c.allocated += allocated
	return c.release(e, f)
}
func (c *fileCache) owns(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paths[path] != nil
}

// handoff pins the NAME for nginx-open grace, independently of Go descriptors.
// Nginx must open within 60 seconds after the last response header; no timer can
// prove completion in an arbitrarily suspended nginx. Admission stops rather than
// violating that grace when retired bytes/entries exhaust the configured budget.
func (c *fileCache) handoff(key string, f *os.File) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if c.closed || e == nil || e.path != f.Name() {
		return "", false
	}
	e.handoffUntil = c.now().Add(60 * time.Second)
	return filepath.Base(e.path), true
}

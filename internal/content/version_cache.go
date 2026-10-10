package content

import (
	"container/list"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/singleflight"
)

// ServingVersions caches integrity only. Callers MUST resolve current ownership,
// privacy, slug and deletion independently on EVERY request, including validators.
// The absolute directory includes project ID; digest identifies immutable bytes.
var ServingVersions = NewVersionCache(256)

type verifiedVersion struct {
	key      string
	manifest Manifest
	stats    map[string]fs.FileInfo
}
type VersionCache struct {
	mu           sync.Mutex
	limit, files int
	entries      map[string]*list.Element
	lru          *list.List
	flight       singleflight.Group
	slots        chan struct{}
}

func NewVersionCache(limit int) *VersionCache { return NewVersionCacheWithConcurrency(limit, 8) }
func NewVersionCacheWithConcurrency(limit, concurrency int) *VersionCache {
	if concurrency < 1 {
		concurrency = 1
	}
	if limit < 1 {
		limit = 1
	}
	return &VersionCache{limit: limit, entries: map[string]*list.Element{}, lru: list.New(), slots: make(chan struct{}, concurrency)}
}
func sameStat(a, b fs.FileInfo) bool {
	if a == nil || b == nil {
		return false
	}
	if left, ok := a.Sys().(*syscall.Stat_t); ok {
		right, ok := b.Sys().(*syscall.Stat_t)
		if !ok || left.Ctim != right.Ctim {
			return false
		}
	}
	return a != nil && b != nil && os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime() == b.ModTime() && a.Mode() == b.Mode()
}
func versionStats(ctx context.Context, directory string) (map[string]fs.FileInfo, error) {
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrInvalid
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	stats := map[string]fs.FileInfo{}
	err = fs.WalkDir(root.FS(), ".", func(name string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		i, err := e.Info()
		if err != nil {
			return err
		}
		if i.Mode()&os.ModeSymlink != 0 {
			return ErrInvalid
		}
		stats[name] = i
		return nil
	})
	return stats, err
}
func (v *verifiedVersion) intact(directory, file string) bool {
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return false
	}
	defer root.Close()
	names := []string{".", "manifest.json"}
	if file == "" {
		names = names[:0]
		for name := range v.stats {
			names = append(names, name)
		}
	} else {
		for name := "public/" + file; name != "."; name = path.Dir(name) {
			names = append(names, name)
		}
	}
	for _, name := range names {
		info, err := root.Lstat(name)
		if err != nil || !sameStat(v.stats[name], info) {
			return false
		}
	}
	return true
}
func (c *VersionCache) Verify(directory, digest, file string) (Manifest, error) {
	return c.VerifyContext(context.Background(), directory, digest, file)
}

func (c *VersionCache) VerifyContext(ctx context.Context, directory, digest, file string) (Manifest, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if !versionDigestRE.MatchString(digest) || (file != "" && ValidatePath(file) != nil) {
		return Manifest{}, ErrInvalid
	}
	key := directory + "\x00" + digest
	c.mu.Lock()
	e := c.entries[key]
	var cached *verifiedVersion
	if e != nil {
		cached = e.Value.(*verifiedVersion)
		c.lru.MoveToFront(e)
	}
	c.mu.Unlock()
	if cached != nil && cached.intact(directory, file) {
		return cloneManifest(cached.manifest), nil
	}
	result := c.flight.DoChan(key, func() (any, error) {
		// Another flight may have completed between the first lookup and join.
		c.mu.Lock()
		current := c.entries[key]
		var v *verifiedVersion
		if current != nil {
			v = current.Value.(*verifiedVersion)
		}
		c.mu.Unlock()
		if v != nil && v.intact(directory, file) {
			return v, nil
		}
		work, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		select {
		case c.slots <- struct{}{}:
			defer func() { <-c.slots }()
		case <-work.Done():
			return nil, work.Err()
		}
		// A changed stat invalidates the entire version, including the parsed manifest.
		before, err := versionStats(work, directory)
		if err != nil {
			return nil, err
		}
		m, err := VerifyVersionContext(work, directory, digest)
		if err != nil {
			return nil, err
		}
		after, err := versionStats(work, directory)
		if err != nil {
			return nil, err
		}
		if len(before) != len(after) {
			return nil, ErrInvalid
		}
		for name, i := range before {
			if !sameStat(i, after[name]) {
				return nil, fmt.Errorf("%w: version changed during verification", ErrInvalid)
			}
		}
		v = &verifiedVersion{key: key, manifest: m, stats: after}
		c.mu.Lock()
		defer c.mu.Unlock()
		if old := c.entries[key]; old != nil {
			c.files -= len(old.Value.(*verifiedVersion).stats)
			c.lru.Remove(old)
			delete(c.entries, key)
		}
		c.entries[key] = c.lru.PushFront(v)
		c.files += len(after)
		// Bound both number of versions and aggregate stat/manifest entries.
		for c.lru.Len() > c.limit || c.files > 32768 {
			old := c.lru.Back()
			v := old.Value.(*verifiedVersion)
			delete(c.entries, v.key)
			c.files -= len(v.stats)
			c.lru.Remove(old)
		}
		return v, nil
	})
	select {
	case result := <-result:
		if result.Err != nil {
			return Manifest{}, result.Err
		}
		return cloneManifest(result.Val.(*verifiedVersion).manifest), nil
	case <-ctx.Done():
		return Manifest{}, ctx.Err()
	}

}

func cloneManifest(m Manifest) Manifest { m.Files = append([]File(nil), m.Files...); return m }

// SameFileSnapshot also includes Linux nanosecond ctime, detecting rewrites that
// deliberately restore size and mtime. This is integrity, never authorization.
func SameFileSnapshot(a, b fs.FileInfo) bool { return sameStat(a, b) }

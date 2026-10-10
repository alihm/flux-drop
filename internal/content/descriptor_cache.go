package content

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"syscall"

	"golang.org/x/sync/singleflight"
)

// DescriptorHashes caches integrity only, never authorization. The identity
// includes nanosecond ctime as well as mtime: rewriting bytes then restoring mtime
// cannot reuse a cached hash. Published content must remain immutable during send.
var DescriptorHashes = &DescriptorCache{entries: map[string]*list.Element{}, lru: list.New(), slots: make(chan struct{}, 8)}

type DescriptorCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	lru     *list.List
	flight  singleflight.Group
	slots   chan struct{}
}

func descriptorIdentity(f *os.File, digest, hash string, size int64) (string, error) {
	info, e := f.Stat()
	if e != nil {
		return "", e
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Size() != size {
		return "", ErrInvalid
	}
	return fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d/%d", digest, hash, st.Dev, st.Ino, size, st.Mtim.Sec, st.Mtim.Nsec, st.Ctim.Sec, st.Ctim.Nsec), nil
}
func (c *DescriptorCache) Verify(ctx context.Context, f *os.File, digest string, entry File) error {
	key, e := descriptorIdentity(f, digest, entry.SHA256, entry.Size)
	if e != nil {
		return e
	}
	hit := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if v := c.entries[key]; v != nil {
			c.lru.MoveToFront(v)
			return true
		}
		return false
	}
	if hit() {
		return nil
	}
	_, e, _ = c.flight.Do(key, func() (any, error) {
		if hit() {
			return nil, nil
		}
		select {
		case c.slots <- struct{}{}:
			defer func() { <-c.slots }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		h := sha256.New()
		reader := io.NewSectionReader(f, 0, entry.Size+1)
		buffer := make([]byte, 64<<10)
		var n int64
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			count, err := reader.Read(buffer)
			if count > 0 {
				h.Write(buffer[:count])
				n += int64(count)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
		}
		after, err := descriptorIdentity(f, digest, entry.SHA256, entry.Size)
		if err != nil || after != key || n != entry.Size || hex.EncodeToString(h.Sum(nil)) != entry.SHA256 {
			return nil, ErrInvalid
		}
		c.mu.Lock()
		c.entries[key] = c.lru.PushFront(key)
		for c.lru.Len() > 4096 {
			old := c.lru.Back()
			delete(c.entries, old.Value.(string))
			c.lru.Remove(old)
		}
		c.mu.Unlock()
		return nil, nil
	})
	if e != nil {
		return e
	}
	after, e := descriptorIdentity(f, digest, entry.SHA256, entry.Size)
	if e != nil || after != key {
		return ErrInvalid
	}
	return nil
}

// ConfigureServing is called once before starting HTTP workers in either role.
// Standalone users retain safe defaults without requiring environment variables.
func ConfigureServing(get func(string) string) error {
	slots := 8
	if raw := get("DROP_VERSION_VERIFY_CONCURRENCY"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 128 {
			return fmt.Errorf("invalid DROP_VERSION_VERIFY_CONCURRENCY")
		}
		slots = n
	}
	ServingVersions = NewVersionCacheWithConcurrency(256, slots)
	DescriptorHashes = &DescriptorCache{entries: map[string]*list.Element{}, lru: list.New(), slots: make(chan struct{}, slots)}
	return nil
}

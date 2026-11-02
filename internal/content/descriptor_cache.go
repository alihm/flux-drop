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
	"time"

	"golang.org/x/sync/singleflight"
)

// DescriptorHashes caches integrity only, never authorization. The identity
// includes ctime as well as mtime. Linux can give rapid writes identical coarse
// timestamps, despite nanosecond stat fields. Recently changed files therefore
// bypass both memoization and coalescing. Published content must remain immutable
// during send.
var DescriptorHashes = &DescriptorCache{entries: map[string]*list.Element{}, lru: list.New(), slots: make(chan struct{}, 8)}

const descriptorTimestampWindow = time.Second

type DescriptorCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	lru     *list.List
	flight  singleflight.Group
	slots   chan struct{}
}

func descriptorIdentity(f *os.File, digest, hash string, size int64) (string, bool, error) {
	info, e := f.Stat()
	if e != nil {
		return "", false, e
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Size() != size {
		return "", false, ErrInvalid
	}
	// A one-second settling window covers the local Linux filesystem/kernel
	// clock tick. Future timestamps also fail closed to a real hash. Restoring
	// mtime cannot make ctime older, so a fresh mutation cannot hit an entry
	// that was first cached after its timestamp had settled.
	cacheable := time.Since(time.Unix(st.Ctim.Sec, st.Ctim.Nsec)) >= descriptorTimestampWindow
	return fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d/%d", digest, hash, st.Dev, st.Ino, size, st.Mtim.Sec, st.Mtim.Nsec, st.Ctim.Sec, st.Ctim.Nsec), cacheable, nil
}
func (c *DescriptorCache) Verify(ctx context.Context, f *os.File, digest string, entry File) error {
	key, cacheable, e := descriptorIdentity(f, digest, entry.SHA256, entry.Size)
	if e != nil {
		return e
	}
	hit := func() bool {
		if !cacheable {
			return false
		}
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
	verify := func() (any, error) {
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
		after, _, err := descriptorIdentity(f, digest, entry.SHA256, entry.Size)
		if err != nil || after != key || n != entry.Size || hex.EncodeToString(h.Sum(nil)) != entry.SHA256 {
			return nil, ErrInvalid
		}
		if cacheable {
			c.mu.Lock()
			c.entries[key] = c.lru.PushFront(key)
			for c.lru.Len() > 4096 {
				old := c.lru.Back()
				delete(c.entries, old.Value.(string))
				c.lru.Remove(old)
			}
			c.mu.Unlock()
		}
		return nil, nil
	}
	if cacheable {
		_, e, _ = c.flight.Do(key, verify)
	} else {
		// Another request's result is not evidence about this fresh descriptor:
		// an intervening write may still share its timestamp and identity key.
		_, e = verify()
	}
	if e != nil {
		return e
	}
	after, _, e := descriptorIdentity(f, digest, entry.SHA256, entry.Size)
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

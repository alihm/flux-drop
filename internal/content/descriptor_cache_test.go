package content

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDescriptorHashCacheRejectsSameSizeMtimeRestoredMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "asset.js")
	if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("original"))
	entry := File{Path: "asset.js", Size: 8, SHA256: hex.EncodeToString(sum[:])}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	cache := testDescriptorCache()
	if e := cache.Verify(context.Background(), f, strings.Repeat("a", 64), entry); e != nil {
		t.Fatal(e)
	}
	if e := cache.Verify(context.Background(), f, strings.Repeat("a", 64), entry); e != nil {
		t.Fatal("second fresh verification", e)
	}
	if cache.lru.Len() != 0 {
		t.Fatal("fresh timestamp was memoized before it could distinguish mutations")
	}
	if err := os.WriteFile(path, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if e := cache.Verify(context.Background(), f, strings.Repeat("a", 64), entry); e == nil {
		t.Fatal("mtime restoration bypassed verification")
	}
}
func TestVersionConcurrencyConstructor(t *testing.T) {
	if cap(NewVersionCache(1).slots) != 8 || cap(NewVersionCacheWithConcurrency(2, 3).slots) != 3 {
		t.Fatal("verification concurrency ignored")
	}
}

func testDescriptorCache() *DescriptorCache {
	return &DescriptorCache{entries: map[string]*list.Element{}, lru: list.New(), slots: make(chan struct{}, 1)}
}

func TestDescriptorHashCacheUsesStableIdentityAndRejectsMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "asset.js")
	if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("original"))
	entry := File{Path: "asset.js", Size: 8, SHA256: hex.EncodeToString(sum[:])}
	digest := strings.Repeat("b", 64)
	deadline := time.Now().Add(3 * descriptorTimestampWindow)
	for {
		_, cacheable, err := descriptorIdentity(f, digest, entry.SHA256, entry.Size)
		if err != nil {
			t.Fatal(err)
		}
		if cacheable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("filesystem timestamp did not settle")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cache := testDescriptorCache()
	if err := cache.Verify(context.Background(), f, digest, entry); err != nil {
		t.Fatal(err)
	}
	if cache.lru.Len() != 1 {
		t.Fatal("stable verified descriptor was not cached")
	}
	// Occupy the hashing slot: a true cache hit must need no slot or file read.
	cache.slots <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = cache.Verify(ctx, f, digest, entry)
	cancel()
	<-cache.slots
	if err != nil {
		t.Fatal("stable cache hit tried to hash again", err)
	}
	if err := os.WriteFile(path, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := cache.Verify(context.Background(), f, digest, entry); err == nil {
		t.Fatal("stable cache accepted an in-place mutation with restored mtime")
	}
}

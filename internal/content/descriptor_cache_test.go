package content

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDescriptorHashCacheRejectsSameSizeMtimeRestoredMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "asset.js")
	os.WriteFile(path, []byte("original"), 0600)
	sum := sha256.Sum256([]byte("original"))
	entry := File{Path: "asset.js", Size: 8, SHA256: hex.EncodeToString(sum[:])}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	info, _ := f.Stat()
	if e := DescriptorHashes.Verify(context.Background(), f, strings.Repeat("a", 64), entry); e != nil {
		t.Fatal(e)
	}
	if e := DescriptorHashes.Verify(context.Background(), f, strings.Repeat("a", 64), entry); e != nil {
		t.Fatal("cache hit", e)
	}
	os.WriteFile(path, []byte("tampered"), 0600)
	os.Chtimes(path, info.ModTime(), info.ModTime())
	if e := DescriptorHashes.Verify(context.Background(), f, strings.Repeat("a", 64), entry); e == nil {
		t.Fatal("mtime restoration bypassed verification")
	}
}
func TestVersionConcurrencyConstructor(t *testing.T) {
	if cap(NewVersionCache(1).slots) != 8 || cap(NewVersionCacheWithConcurrency(2, 3).slots) != 3 {
		t.Fatal("verification concurrency ignored")
	}
}

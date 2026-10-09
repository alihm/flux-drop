package content

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestVerifiedVersionCacheInvalidatesChangedBytes(t *testing.T) {
	for _, change := range []string{"size", "mtime", "symlink", "manifest", "missing"} {
		t.Run(change, func(t *testing.T) {
			staged, err := StageHTML(t.TempDir(), strings.NewReader("<h1>safe</h1>"), DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			cache := NewVersionCache(2)
			if _, err = cache.Verify(staged.Directory, staged.Digest, "index.html"); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(staged.Directory, "public", "index.html")
			switch change {
			case "size":
				err = os.WriteFile(file, []byte("broken"), 0600)
			case "mtime":
				err = os.WriteFile(file, []byte("<h1>evil</h1>"), 0600)
				future := time.Now().Add(time.Second)
				if err == nil {
					err = os.Chtimes(file, future, future)
				}
			case "symlink":
				err = os.Remove(file)
				if err == nil {
					err = os.Symlink("../manifest.json", file)
				}
			case "manifest":
				err = os.WriteFile(filepath.Join(staged.Directory, "manifest.json"), []byte("{}"), 0600)
			case "missing":
				err = os.Remove(file)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = cache.Verify(staged.Directory, staged.Digest, "index.html"); err == nil {
				t.Fatal("cached corrupt version served")
			}
		})
	}
}
func TestVerifiedVersionCacheBoundedAndConcurrent(t *testing.T) {
	cache := NewVersionCache(2)
	for _, html := range []string{"one", "two", "three"} {
		s, err := StageHTML(t.TempDir(), strings.NewReader(html), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := cache.Verify(s.Directory, s.Digest, "index.html"); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
	}
	if cache.lru.Len() != 2 || len(cache.entries) != 2 {
		t.Fatal("unbounded cache")
	}
}

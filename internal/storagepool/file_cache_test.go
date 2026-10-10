package storagepool

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
)

func TestCacheHandoffGraceAndRetirementBudget(t *testing.T) {
	c, e := newFileCache(filepath.Join(t.TempDir(), "cache"), 8192)
	if e != nil {
		t.Fatal(e)
	}
	defer c.close()
	now := time.Now()
	c.now = func() time.Time { return now }
	put := func(key string) (*os.File, func()) {
		t.Helper()
		f, e := os.CreateTemp(c.root, "cached-")
		if e != nil {
			t.Fatal(e)
		}
		f.WriteString("12345678")
		return f, c.put(key, f, content.File{Size: 8})
	}
	f, release := put("first")
	path := f.Name()
	if _, ok := c.handoff("first", f); !ok {
		t.Fatal("retained file not handed off")
	}
	release()
	_, second := put("second")
	second()
	if c.used != 8 || len(c.retired) != 1 || !fileExists(path) {
		t.Fatal("grace/budget violated", c.used, len(c.retired))
	}
	now = now.Add(59 * time.Second)
	_, third := put("third")
	third()
	if !fileExists(path) {
		t.Fatal("deleted before nginx grace")
	}
	now = now.Add(2 * time.Second)
	_, fourth := put("fourth")
	fourth()
	if fileExists(path) || len(c.retired) != 0 || c.used != 8 {
		t.Fatal("retired cleanup failed")
	}
}

package httpserver

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
)

// StorageReady verifies writable durable staging and upload admission headroom.
// It creates and removes only its own temporary probe, never project content.
func StorageReady(root string, limits content.Limits) error {
	release, err := newDiskAdmission(root).acquire(limits)
	if err != nil {
		return err
	}
	defer release()
	dir := filepath.Join(root, "staging")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errStorageCapacity
	}
	f, err := os.CreateTemp(dir, ".readiness-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.WriteString("ready"); err != nil {
		return err
	}
	return f.Sync()
}

// Cache dependency checks briefly; concurrent health probes share one result.
func CachedReadiness(check func(context.Context) error) func(context.Context) error {
	var mu sync.Mutex
	var until time.Time
	var last error
	return func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().Before(until) {
			return last
		}
		bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		last = check(bounded)
		until = time.Now().Add(5 * time.Second)
		return last
	}
}

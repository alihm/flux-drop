package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/runonflux/flux-drop/internal/content"
)

func TestCachedReadiness(t *testing.T) {
	var calls atomic.Int32
	check := CachedReadiness(func(context.Context) error { calls.Add(1); return errors.New("not ready") })
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := check(context.Background()); err == nil {
				t.Error("failure lost")
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("uncached probes", calls.Load())
	}
}
func TestReadinessProbeDoesNotLeaveFiles(t *testing.T) {
	root := t.TempDir()
	if err := StorageReady(root, content.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "staging"))
	if err != nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
}
func TestStagingGate(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	h := StagingAccess(next, "tester", "a-long-private-test-password")
	for _, path := range []string{"/", "/api/projects", "/unlock/site-abcdef"} {
		r := httptest.NewRequest("GET", path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatal(path, w.Code)
		}
		r.SetBasicAuth("tester", "a-long-private-test-password")
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 204 {
			t.Fatal(w.Code)
		}
	}
	for _, path := range []string{"/healthz", "/readyz", "/site-abcdef/script.js"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 204 {
			t.Fatal(path, w.Code)
		}
	}
}

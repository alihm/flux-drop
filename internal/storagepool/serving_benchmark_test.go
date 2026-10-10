package storagepool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
)

// Identical driver/data on baseline and implementation. Real HTTP primary and
// pinned TLS secondary, 10ms injected secondary latency; no nginx/metadata claims.
func TestPrimaryServingMeasurements(t *testing.T) {
	if os.Getenv("DROP_TEST_PRIMARY_LOAD") != "1" {
		t.Skip("opt-in local load fixture")
	}
	var manifests, downloads atomic.Uint64
	_, _, p := fixture(t, t.TempDir(), func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" && strings.Contains(r.URL.Path, "/versions/") {
				time.Sleep(10 * time.Millisecond)
				if strings.HasSuffix(r.URL.Path, "/manifest") {
					manifests.Add(1)
				} else {
					downloads.Add(1)
				}
			}
			next.ServeHTTP(w, r)
		})
	})
	var sources []content.Source
	payload := strings.Repeat("x", 4096)
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("asset%d.js", i)
		if i == 0 {
			name = "index.html"
		}
		sources = append(sources, content.Source{Name: name, Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(payload)), nil }})
	}
	staged, e := content.StageFolder(t.TempDir(), sources, content.DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	defer staged.Discard()
	pr := prepared(staged)
	pr.Project.WatermarkDisabled = true
	pr.Operation.Bytes = 50 * 4096
	if e := p.Install(context.Background(), pr, staged); e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.ServeProject(w, r, pr.Project, strings.TrimPrefix(r.URL.Path, "/fixture/"))
	}))
	defer server.Close()
	paths := []string{}
	for _, f := range staged.Manifest.Files {
		paths = append(paths, "/fixture/"+f.Path)
	}
	raw, _ := json.Marshal(paths)
	pathFile := filepath.Join(t.TempDir(), "paths.json")
	os.WriteFile(pathFile, raw, 0600)
	driver := filepath.Join("..", "..", "scripts", "serving-load.mjs")
	for _, scenario := range []struct{ name, count, concurrency, slow string }{{"cold", "200", "200", "0"}, {"warm", "400", "50", "0"}, {"slow", "200", "50", "10"}, {"overload", "2000", "256", "0"}} {
		beforeM, beforeD := manifests.Load(), downloads.Load()
		cmd := exec.Command("node", driver, server.URL, "/fixture/", scenario.count, scenario.concurrency)
		cmd.Env = append(os.Environ(), "DROP_LOAD_PATHS="+pathFile, "DROP_LOAD_TIMEOUT_MS=150000", "DROP_LOAD_SLOW_MS="+scenario.slow)
		out, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatal(e, string(out))
		}
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		t.Logf("scenario=%s secondaryLatency=10ms fileCount=50 payloadBytes=204800 gomaxprocs=%d heap=%d manifestRequests=%d fileRequests=%d\n%s", scenario.name, runtime.GOMAXPROCS(0), memory.HeapAlloc, manifests.Load()-beforeM, downloads.Load()-beforeD, out)
	}
}

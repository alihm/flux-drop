package storagepool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/analytics"
	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/httpserver"
	"github.com/runonflux/flux-drop/internal/project"
)

func TestPrimaryCacheNginx(t *testing.T) {
	if os.Getenv("DROP_TEST_NGINX") != "1" {
		t.Skip("run delivery Docker fixture")
	}
	_, _, p := fixture(t, t.TempDir(), nil)
	p.cache.close()
	var err error
	p.cache, err = newFileCache("/var/lib/drop-cluster/storage-cache", 256<<20)
	if err != nil {
		t.Fatal(err)
	}
	if runtime, ok := any(p).(interface{ EnableNginxCache() }); ok {
		runtime.EnableNginxCache()
	}
	payload := "<!doctype html><h1>cached HTML</h1>" + strings.Repeat("<!-- content -->", 200)
	sources := []content.Source{{Name: "index.html", Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(payload)), nil }}, {Name: "app.js", Open: func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("console.log('cached asset');")), nil
	}}}
	for i := 2; i < 50; i++ {
		sources = append(sources, content.Source{Name: fmt.Sprintf("asset%d.js", i), Open: func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(strings.Repeat("console.log('asset');", 200))), nil
		}})
	}

	staged, err := content.StageFolder(t.TempDir(), sources, content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Discard()
	pr := prepared(staged)
	pr.Operation.Bytes = 0
	for _, f := range staged.Manifest.Files {
		pr.Operation.Bytes += f.Size
	}
	pr.Project.WatermarkDisabled = true
	if err = p.Install(context.Background(), pr, staged); err != nil {
		t.Fatal(err)
	}
	repo := &deliveryRepository{value: pr.Project}
	var views *analytics.Collector
	var archive *analytics.Archive
	if os.Getenv("DROP_TEST_ANALYTICS") == "1" {
		archive, err = analytics.NewArchive(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer archive.Close()
		if err = archive.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		views, err = analytics.NewCollector("primary-test", t.TempDir(), nginxAnalytics{archive})
		if err != nil {
			t.Fatal(err)
		}
		defer views.Close()
	}
	var observer httpserver.ProjectAnalytics
	if views != nil {
		observer = views
	}
	handler := httpserver.ProjectDeliveryWithAnalytics(repo, t.TempDir(), p, func(r *http.Request, _ project.Project) bool { return r.Header.Get("Test-Grant") == "valid" }, observer)
	listener, err := net.Listen("tcp", "127.0.0.1:8081")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	go server.Serve(listener)
	defer server.Close()
	nginx := exec.Command("nginx", "-g", "daemon off;")
	nginx.Stderr = os.Stderr
	if err = nginx.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { nginx.Process.Signal(os.Interrupt); nginx.Wait() }()
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	for deadline := time.Now().Add(5 * time.Second); ; {
		conn, e := net.DialTimeout("tcp", "127.0.0.1:8080", time.Millisecond*100)
		if e == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(e)
		}
		time.Sleep(20 * time.Millisecond)
	}
	call := func(method, path string, headers map[string]string) (*http.Response, string) {
		t.Helper()
		req, _ := http.NewRequest(method, "http://127.0.0.1:8080"+path, nil)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		res, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		body, e := io.ReadAll(res.Body)
		res.Body.Close()
		if e != nil {
			t.Fatal(e)
		}
		return res, string(body)
	}
	path := "/" + pr.Project.Slug + "/"

	if os.Getenv("DROP_TEST_PRIMARY_LOAD") == "1" {
		paths := []string{}
		for _, f := range staged.Manifest.Files {
			paths = append(paths, path+f.Path)
		}
		raw, _ := json.Marshal(paths)
		pathsFile := filepath.Join(t.TempDir(), "paths.json")
		os.WriteFile(pathsFile, raw, 0600)
		for _, scenario := range []struct{ name, count, concurrency, slow string }{{"cold", "200", "200", "0"}, {"warm", "400", "50", "0"}, {"slow", "200", "50", "10"}, {"overload", "2000", "256", "0"}} {
			driver := os.Getenv("DROP_LOAD_DRIVER")
			if driver == "" {
				driver = filepath.Join("..", "..", "scripts", "serving-load.mjs")
			}
			cmd := exec.Command("node", driver, "http://127.0.0.1:8080", path, scenario.count, scenario.concurrency)
			cmd.Env = append(os.Environ(), "DROP_LOAD_PATHS="+pathsFile, "DROP_LOAD_TIMEOUT_MS=150000", "DROP_LOAD_SLOW_MS="+scenario.slow)
			out, e := cmd.CombinedOutput()
			if e != nil {
				t.Fatal(e, string(out))
			}
			t.Logf("nginx scenario=%s files=50\n%s", scenario.name, out)
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			fds, _ := os.ReadDir("/proc/self/fd")
			var cacheBytes int64
			var cacheFiles int
			filepath.WalkDir("/var/lib/drop-cluster/storage-cache", func(_ string, entry os.DirEntry, err error) error {
				if err == nil && !entry.IsDir() {
					if info, err := entry.Info(); err == nil {
						cacheBytes += info.Size()
						cacheFiles++
					}
				}
				return nil
			})
			t.Logf("resources scenario=%s heapBytes=%d descriptors=%d cacheFiles=%d cacheLogicalBytes=%d", scenario.name, memory.HeapAlloc, len(fds), cacheFiles, cacheBytes)
			if metrics, ok := any(p).(interface{ ServingMetrics() map[string]any }); ok {
				raw, _ := json.Marshal(metrics.ServingMetrics())
				t.Logf("serving metrics scenario=%s %s", scenario.name, raw)
			}
			for _, name := range []string{"cpu.stat", "memory.current", "memory.peak"} {
				if value, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", name)); err == nil {
					t.Logf("cgroup scenario=%s %s: %s", scenario.name, name, strings.TrimSpace(string(value)))
				}
			}
		}
	}
	res, body := call("GET", path, nil)
	if res.StatusCode != 200 || body != payload || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/html") || res.Header.Get("ETag") == "" {
		t.Fatal(res.StatusCode, res.Header, body)
	}
	etag := res.Header.Get("ETag")
	res, body = call("HEAD", path, nil)
	if res.StatusCode != 200 || body != "" {
		t.Fatal("HEAD", res.StatusCode, body)
	}
	res, body = call("GET", path, map[string]string{"Range": "bytes=0-9"})
	if res.StatusCode != 206 || body != payload[:10] {
		t.Fatal("range", res.StatusCode, body)
	}
	res, _ = call("GET", path, map[string]string{"If-None-Match": etag})
	if res.StatusCode != 304 {
		t.Fatal("304", res.StatusCode)
	}
	res, body = call("GET", path+"app.js", nil)
	if res.StatusCode != 200 || !strings.Contains(res.Header.Get("Content-Type"), "javascript") || !strings.Contains(body, "console.log") {
		t.Fatal("MIME", res.StatusCode, res.Header, body)
	}
	for _, path := range []string{"/_drop_internal/cache-public/cached-fake", "/_drop_internal/cache-private/cached-fake", "/_drop_internal/cache-public/../identity.json"} {
		res, _ = call("GET", path, nil)
		if res.StatusCode == 200 {
			t.Fatal("internal path exposed", path)
		}
	}
	repo.mu.Lock()
	repo.value.WatermarkDisabled = false
	repo.mu.Unlock()
	res, body = call("GET", path, nil)
	if res.StatusCode != 200 || !strings.Contains(body, "data-drop-watermark") {
		t.Fatal("branding", res.StatusCode)
	}
	repo.mu.Lock()
	repo.value.Private = true
	repo.value.PolicyRevision++
	repo.value.WatermarkDisabled = true
	repo.mu.Unlock()
	res, _ = call("GET", path, nil)
	if res.StatusCode == 200 {
		t.Fatal("private denial bypassed")
	}
	res, body = call("GET", path, map[string]string{"Test-Grant": "valid"})
	if res.StatusCode != 200 || body != payload || res.Header.Get("Cache-Control") != "no-store" || res.Header.Get("Access-Control-Allow-Origin") != "" || res.Header.Get("Cross-Origin-Resource-Policy") != "same-origin" {
		t.Fatal("private headers", res.StatusCode, res.Header)
	}

	if views != nil && os.Getenv("DROP_TEST_PRIMARY_LOAD") != "1" {
		views.Flush(context.Background())
		now := time.Now().UTC()
		result, e := archive.Query(context.Background(), analytics.Query{ProjectID: pr.Project.ID, From: analytics.Day(now), To: analytics.Day(now).Add(24 * time.Hour), Interval: "hour"})
		if e != nil || result.PageViews != 4 {
			t.Fatal("nginx analytics did not match HTML/304/private views", result, e)
		}
	}
}

type nginxAnalytics struct{ archive *analytics.Archive }

func (s nginxAnalytics) SubmitPageViews(ctx context.Context, snapshot analytics.Snapshot) error {
	return s.archive.Accept(ctx, snapshot)
}
func (s nginxAnalytics) PageViews(ctx context.Context, query analytics.Query) (analytics.Result, error) {
	return s.archive.Query(ctx, query)
}

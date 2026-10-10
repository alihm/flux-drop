package storagepool

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

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
	p.EnableNginxCache()
	payload := "<!doctype html><h1>cached HTML</h1>" + strings.Repeat("<!-- content -->", 200)
	sources := []content.Source{{Name: "index.html", Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(payload)), nil }}, {Name: "app.js", Open: func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("console.log('cached asset');")), nil
	}}}
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
	handler := httpserver.ProjectDeliveryWithAccess(repo, t.TempDir(), p, func(r *http.Request, _ project.Project) bool { return r.Header.Get("Test-Grant") == "valid" })
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
}

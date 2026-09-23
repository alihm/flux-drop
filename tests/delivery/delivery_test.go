package delivery

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/httpserver"
	"github.com/runonflux/flux-drop/internal/project"
)

type resolver struct {
	sync.Mutex
	p   project.Project
	err error
}

func (r *resolver) Resolve(context.Context, string) (project.Project, error) {
	r.Lock()
	defer r.Unlock()
	return r.p, r.err
}

func TestProductionNginxDelivery(t *testing.T) {
	if os.Getenv("DROP_TEST_NGINX") != "1" {
		t.Skip("run with tests/delivery/compose.yaml")
	}
	const body = "<!doctype html><h1>Flux delivery</h1>"
	s, err := content.StageHTML("/data/staging", strings.NewReader(body), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	p := project.Project{ID: strings.Repeat("a", 32), Slug: "test-" + s.Digest[:6], ActiveDigest: s.Digest, Status: "active"}
	if err := s.Install("/data", p.ID, p.Slug); err != nil {
		t.Fatal(err)
	}
	repo := &resolver{p: p}
	listener, err := net.Listen("tcp", "127.0.0.1:8081")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: httpserver.ProjectDeliveryWithAccess(repo, "/data", nil, func(r *http.Request, _ project.Project) bool { return r.Header.Get("X-Test-Private-Grant") == "yes" }), ReadHeaderTimeout: time.Second}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	nginx := exec.Command("nginx", "-g", "daemon off;")
	nginx.Stdout = os.Stdout
	nginx.Stderr = os.Stderr
	if err := nginx.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nginx.Process.Signal(os.Interrupt); _ = nginx.Wait() })
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for deadline := time.Now().Add(10 * time.Second); ; {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:8080", 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("nginx did not start")
		}
		time.Sleep(50 * time.Millisecond)
	}
	request := func(method, path string, headers map[string]string) (*http.Response, string) {
		t.Helper()
		req, err := http.NewRequest(method, "http://127.0.0.1:8080"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return res, string(b)
	}
	path := "/" + p.Slug + "/"
	res, b := request("GET", path, nil)
	if res.StatusCode != 200 || b != body {
		t.Fatalf("delivery: %d %q", res.StatusCode, b)
	}
	assertHeaders := func(res *http.Response) {
		t.Helper()
		csp := res.Header.Get("Content-Security-Policy")
		if !strings.Contains(csp, "sandbox allow-scripts;") || strings.Contains(csp, "allow-same-origin") {
			t.Fatal("unsafe CSP", res.Header)
		}
		if res.Header.Get("Access-Control-Allow-Origin") != "*" || res.Header.Get("Cache-Control") != "no-store" || res.Header.Get("X-Content-Type-Options") != "nosniff" || res.Header.Get("X-Accel-Redirect") != "" {
			t.Fatal("unsafe headers", res.Header)
		}
		if len(res.Header.Values("Access-Control-Allow-Origin")) != 1 {
			t.Fatal("duplicate CORS headers", res.Header)
		}
	}
	assertHeaders(res)
	etag := res.Header.Get("ETag")
	if etag == "" {
		t.Fatal("missing etag")
	}
	for _, tc := range []struct {
		method  string
		headers map[string]string
		status  int
		body    string
	}{
		{"HEAD", nil, 200, ""},
		{"GET", map[string]string{"Range": "bytes=0-3"}, 206, body[:4]},
		{"GET", map[string]string{"If-None-Match": etag}, 304, ""},
	} {
		res, b := request(tc.method, path, tc.headers)
		if res.StatusCode != tc.status || b != tc.body {
			t.Fatalf("conditional delivery: %d %q", res.StatusCode, b)
		}
		assertHeaders(res)
	}
	for _, bad := range []string{"/_drop_internal/files/" + p.ID + "/versions/" + p.ActiveDigest + "/public/index.html", path + "manifest.json", path + "%2e%2e/manifest.json", path + "%69ndex.html", path + "hash"} {
		res, _ := request("GET", bad, map[string]string{"X-Drop-Peer": "spoofed"})
		if res.StatusCode != 404 {
			t.Fatalf("exposed %s: %d", bad, res.StatusCode)
		}
	}
	for _, state := range []string{"private", "expired", "deleted", "reserved", "offline"} {
		repo.Lock()
		repo.p = p
		repo.err = nil
		switch state {
		case "private":
			repo.p.Private = true
		case "expired":
			past := time.Now().Add(-time.Second)
			repo.p.ExpiresAt = &past
		case "offline":
			repo.err = errors.New("metadata unavailable")
		default:
			repo.p.Status = state
		}
		repo.Unlock()
		for _, method := range []string{"GET", "HEAD"} {
			res, b := request(method, path, map[string]string{"If-None-Match": etag, "Range": "bytes=0-3"})
			want := 404
			if state == "offline" {
				want = 503
			}
			if res.StatusCode != want || strings.Contains(b, "Flux delivery") {
				t.Fatalf("%s %s: %d", state, method, res.StatusCode)
			}
		}
	}
	repo.Lock()
	repo.p = p
	repo.err = nil
	repo.p.Private = true
	repo.Unlock()
	for _, method := range []string{"GET", "HEAD"} {
		res, b := request(method, path, map[string]string{"X-Test-Private-Grant": "yes"})
		if res.StatusCode != 200 || res.Header.Get("Access-Control-Allow-Origin") != "" || res.Header.Get("Cross-Origin-Resource-Policy") != "same-origin" || !strings.Contains(res.Header.Get("Content-Security-Policy"), "sandbox allow-scripts;") {
			t.Fatal("private headers", res.StatusCode, res.Header)
		}
		if method == "GET" && b != body {
			t.Fatal("private bytes", b)
		}
	}
	repo.Lock()
	repo.p = p
	repo.Unlock()
	if err := os.WriteFile(filepath.Join("/data/projects", p.ID, "versions", p.ActiveDigest, "public/index.html"), []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	res, _ = request("GET", path, nil)
	if res.StatusCode != 503 {
		t.Fatal("corruption served", res.StatusCode)
	}
}

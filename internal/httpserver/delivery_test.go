package httpserver

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
)

type deliveryRepository struct {
	p     project.Project
	err   error
	calls int
}

func (d *deliveryRepository) Resolve(context.Context, string) (project.Project, error) {
	d.calls++
	return d.p, d.err
}

func TestProjectDelivery(t *testing.T) {
	root := t.TempDir()
	staged, err := content.StageHTML(root, strings.NewReader("<h1>hello</h1>"), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	p := project.Project{ID: strings.Repeat("a", 32), Slug: "hello-" + staged.Digest[:6], ActiveDigest: staged.Digest, Status: "active"}
	if err := staged.Install(root, p.ID, p.Slug); err != nil {
		t.Fatal(err)
	}
	repo := &deliveryRepository{p: p}
	h := ProjectDelivery(repo, root)
	request := func(method, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, path, nil)
		r.Header.Set("Range", "bytes=0-2")
		r.Header.Set("If-None-Match", `"old"`)
		h.ServeHTTP(w, r)
		return w
	}
	for _, method := range []string{"GET", "HEAD"} {
		w := request(method, "/"+p.Slug+"/")
		if w.Code != 200 || !strings.HasSuffix(w.Header().Get("X-Accel-Redirect"), "/public/index.html") || w.Header().Get("Access-Control-Allow-Origin") != "*" {
			t.Fatalf("delivery: %d %v", w.Code, w.Header())
		}
		if strings.Contains(w.Header().Get("Content-Security-Policy"), "allow-same-origin") {
			t.Fatal("unsafe sandbox")
		}
	}
	for _, path := range []string{"/" + p.Slug + "/../manifest.json", "/" + p.Slug + "/%69ndex.html", "/" + p.Slug + "/manifest.json", "/_drop_internal/files/anything"} {
		if w := request("GET", path); w.Code != 404 || w.Header().Get("X-Accel-Redirect") != "" {
			t.Fatalf("unsafe path %s: %d", path, w.Code)
		}
	}
	if w := request("GET", "/"+p.Slug); w.Code != 307 {
		t.Fatal(w.Code)
	}
	if w := request("POST", "/"+p.Slug+"/"); w.Code != 405 {
		t.Fatal(w.Code)
	}
	for _, status := range []string{"reserved", "deleted"} {
		repo.p.Status = status
		if w := request("GET", "/"+p.Slug+"/"); w.Code != 404 {
			t.Fatal(w.Code)
		}
	}
	repo.p = p
	repo.p.Private = true
	if w := request("HEAD", "/"+p.Slug+"/"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	repo.p = p
	past := time.Now().Add(-time.Second)
	repo.p.ExpiresAt = &past
	if w := request("GET", "/"+p.Slug+"/"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	repo.p = p
	repo.err = errors.New("offline")
	if w := request("GET", "/"+p.Slug+"/"); w.Code != 503 {
		t.Fatal(w.Code)
	}
	repo.err = nil
	if err := os.WriteFile(filepath.Join(root, "projects", p.ID, "versions", p.ActiveDigest, "public", "index.html"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if w := request("GET", "/"+p.Slug+"/"); w.Code != 503 || w.Header().Get("X-Accel-Redirect") != "" {
		t.Fatal(w.Code)
	}
}

func TestRenamedProjectRedirect(t *testing.T) {
	repo := &deliveryRepository{p: project.Project{ID: strings.Repeat("a", 32), Slug: "new-abcdef", ActiveDigest: strings.Repeat("b", 64), Status: "active"}}
	h := ProjectDelivery(repo, t.TempDir())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/old-abcdef/nested/page.html?x=1", nil))
	if w.Code != 307 || w.Header().Get("Location") != "/new-abcdef/nested/page.html?x=1" {
		t.Fatal(w.Code, w.Header())
	}
	repo.p.Private = true
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/old-abcdef/", nil))
	if w.Code != 404 || w.Header().Get("Location") != "" {
		t.Fatal("private alias leaked", w.Code, w.Header())
	}
}

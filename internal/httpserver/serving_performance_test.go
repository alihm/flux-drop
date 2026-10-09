package httpserver

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/preview"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

type servingReads struct {
	*testmetadata.Backend
	reads  atomic.Int64
	change func()
}

func (b *servingReads) Read(ctx context.Context, keys []string) (map[string]kv.Record, error) {
	b.reads.Add(1)
	return b.Backend.Read(ctx, keys)
}
func (b *servingReads) Check(ctx context.Context, checks []kv.Check) error {
	if fn := b.change; fn != nil {
		b.change = nil
		fn()
	}
	return b.Backend.Check(ctx, checks)
}
func TestPublicPreviewValidatorsAndPolicyRace(t *testing.T) {
	ctx := context.Background()
	b := &servingReads{Backend: &testmetadata.Backend{}}
	store := &metadata.Store{Backend: b}
	s, err := preview.New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	p := project.Project{ID: strings.Repeat("a", 32), Slug: "public", ActiveDigest: strings.Repeat("b", 64), Status: "active", PolicyRevision: 1}
	put := func() {
		t.Helper()
		if err := store.Run(ctx, func(tx *metadata.Tx) error { return tx.Set("projects/"+p.ID, p) }); err != nil {
			t.Fatal(err)
		}
	}
	put()
	var jpg bytes.Buffer
	_ = jpeg.Encode(&jpg, image.NewRGBA(image.Rect(0, 0, 320, 180)), nil)
	if err := os.WriteFile(s.ImagePath(p), jpg.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	h, err := NewWithDependencies(Config{PublicOrigin: "https://drop.test", Limits: content.DefaultLimits()}, Dependencies{Previews: s})
	if err != nil {
		t.Fatal(err)
	}
	get := func(tag string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/api/projects/"+p.ID+"/thumbnail?v="+p.ActiveDigest, nil)
		r.Header.Set("If-None-Match", tag)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	b.reads.Store(0)
	w := get("")
	tag := w.Header().Get("ETag")
	if w.Code != 200 || tag != `"`+p.ActiveDigest+`"` || b.reads.Load() != 1 {
		t.Fatal("public preview needs one lookup", w.Code, w.Header(), b.reads.Load())
	}
	for _, key := range []string{"Access-Control-Allow-Origin", "Access-Control-Expose-Headers"} {
		if w.Header().Get(key) != "*" {
			t.Fatal("cross-origin gallery broken", key)
		}
	}
	if w.Header().Get("Cross-Origin-Resource-Policy") != "cross-origin" || !strings.Contains(w.Header().Get("Server-Timing"), "metadata;dur=") {
		t.Fatal(w.Header())
	}
	if w = get("W/" + tag); w.Code != 304 || w.Body.Len() != 0 || w.Header().Get("X-Drop-Preview") != "ready" {
		t.Fatal("conditional preview", w.Code, w.Header())
	}
	b.change = func() { p.Private = true; p.PolicyRevision++; put() }
	if w = get(tag); w.Code != 404 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("policy changed during IO leaked bytes/304", w.Code, w.Header())
	}
}
func TestSiteValidatorsNeverCacheAuthorization(t *testing.T) {
	root := t.TempDir()
	staged, err := content.StageHTML(root, strings.NewReader("<!doctype html><h1>Hello</h1>"), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	p := project.Project{ID: strings.Repeat("d", 32), Slug: "site", ActiveDigest: staged.Digest, Status: "active"}
	if err = staged.Install(root, p.ID, p.Slug); err != nil {
		t.Fatal(err)
	}
	repo := &deliveryRepository{p: p}
	h := ProjectDelivery(repo, root)
	get := func(tag string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/site/", nil)
		r.Header.Set("If-None-Match", tag)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	first := get("")
	tag := first.Header().Get("ETag")
	if tag == "" || first.Header().Get("Cache-Control") != "no-cache" {
		t.Fatal(first.Header())
	}
	if w := get(tag); w.Code != 304 || w.Body.Len() != 0 {
		t.Fatal("watermarked conditional", w.Code, w.Header())
	}
	repo.p.WatermarkDisabled = true
	if w := get(tag); w.Code != 200 || w.Header().Get("ETag") == tag {
		t.Fatal("watermark did not change validator", w.Code, w.Header())
	}
	repo.p.Private = true
	if w := get(tag); w.Code != 404 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("cached public authorization", w.Code, w.Header())
	}
	repo.p.Private = false
	repo.p.Status = "deleted"
	if w := get(tag); w.Code != 404 {
		t.Fatal("deleted content served", w.Code)
	}
	repo.p.Status = "active"
	repo.p.Slug = "renamed"
	if w := get(tag); w.Code != 307 {
		t.Fatal("old slug served", w.Code)
	}
}

// Opt-in, isolated in-memory HTTP load fixture. This measures handler/queue
// overhead, not Flux network latency or Nginx sendfile. Use Raft measurements
// separately for quorum costs; tests/delivery checks the production Nginx path.
func TestServingLoadMeasurements(t *testing.T) {
	if os.Getenv("DROP_TEST_SERVING_LOAD") != "1" {
		t.Skip("opt-in load measurement")
	}
	b := &testmetadata.Backend{}
	store := &metadata.Store{Backend: b}
	s, _ := preview.New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) { return nil, nil })
	ids := []string{}
	var jpg bytes.Buffer
	_ = jpeg.Encode(&jpg, image.NewRGBA(image.Rect(0, 0, 320, 180)), nil)
	for i := 0; i < 20; i++ {
		p := project.Project{ID: fmt.Sprintf("%032x", i+1), Slug: fmt.Sprintf("load-%d", i), ActiveDigest: strings.Repeat("b", 64), Status: "active", Owner: project.Owner{Kind: "firebase", ID: "load"}, UpdatedAt: time.Now()}
		ids = append(ids, p.ID)
		if err := store.Run(context.Background(), func(tx *metadata.Tx) error { return tx.Set("projects/"+p.ID, p) }); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(s.ImagePath(p), jpg.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Run(context.Background(), func(tx *metadata.Tx) error { return tx.Set("preview_recent/public", preview.Recent{IDs: ids}) }); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	html := "<!doctype html><h1>Load site</h1>"
	sources := []content.Source{}
	for i := 0; i < 30; i++ {
		name := fmt.Sprintf("asset%d.js", i)
		html += fmt.Sprintf(`<script src="/load-site/%s"></script>`, name)
		payload := strings.Repeat("/* asset */", 6000)
		sources = append(sources, content.Source{Name: name, Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(payload)), nil }})
	}
	sources = append(sources, content.Source{Name: "index.html", Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(html)), nil }})
	staged, err := content.StageFolder(root, sources, content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	site := project.Project{ID: strings.Repeat("f", 32), Slug: "load-site", ActiveDigest: staged.Digest, Status: "active"}
	if err = staged.Install(root, site.ID, site.Slug); err != nil {
		t.Fatal(err)
	}
	if err = store.Run(context.Background(), func(tx *metadata.Tx) error {
		if e := tx.Set("slugs/"+site.Slug, struct{ ProjectID string }{site.ID}); e != nil {
			return e
		}
		return tx.Set("projects/"+site.ID, site)
	}); err != nil {
		t.Fatal(err)
	}
	sessions := &session.Service{Store: &session.RaftStore{Store: store}, Verifier: identityVerifier{}}
	h, err := NewWithDependencies(Config{PublicOrigin: "https://drop.test", Limits: content.DefaultLimits()}, Dependencies{Previews: s, Sessions: sessions, Projects: &project.Publisher{Repository: &project.RaftRepository{Store: store}, DataRoot: root}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	out, err := exec.Command("node", "../../scripts/serving-load.mjs", server.URL, "/load-site/", "400", "50").CombinedOutput()
	t.Log(string(out))
	if err != nil {
		t.Fatal(err)
	}
}
func TestTimingAndFavicon(t *testing.T) {
	h, _ := New(Config{PublicOrigin: "https://drop.test", Limits: content.DefaultLimits()})
	for _, path := range []string{"/api/config", "/favicon.ico", "/readyz"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Header().Get("Server-Timing") == "" {
			t.Fatal("missing timing", path)
		}
		if w.Code == 503 && w.Header().Get("Retry-After") == "" {
			t.Fatal("503 without retry")
		}
		if path == "/favicon.ico" && (w.Code != 204 || w.Header().Get("Cross-Origin-Resource-Policy") != "cross-origin") {
			t.Fatal(w.Code, w.Header())
		}
	}
}

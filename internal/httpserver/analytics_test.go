package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/runonflux/flux-drop/internal/analytics"
	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
)

type testPageViews struct {
	views, queries atomic.Uint64
	err            error
}

func (a *testPageViews) Record(string, time.Time) { a.views.Add(1) }
func (a *testPageViews) PageViews(_ context.Context, q analytics.Query) (analytics.Result, error) {
	a.queries.Add(1)
	if a.err != nil {
		return analytics.Result{}, a.err
	}
	return analytics.Result{ProjectID: q.ProjectID, Timezone: "UTC", Approximate: true, Interval: q.Interval, From: q.From, To: q.To, PageViews: 42, Buckets: []analytics.Bucket{{Start: q.From, PageViews: 42}}}, nil
}
func TestPageViewCountingDelivery(t *testing.T) {
	root := t.TempDir()
	staged, e := content.StageHTML(root, strings.NewReader("<h1>analytics</h1>"), content.DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	p := project.Project{ID: strings.Repeat("a", 32), Slug: "views", ActiveDigest: staged.Digest, Status: "active", WatermarkDisabled: true}
	if e = staged.Install(root, p.ID, p.Slug); e != nil {
		t.Fatal(e)
	}
	a := &testPageViews{}
	repo := &deliveryRepository{p: p}
	handler := ProjectDeliveryWithAnalytics(repo, root, nil, nil, a)
	request := func(method, path string, headers map[string]string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	w := request("GET", "/views/", nil)
	if w.Code != 200 || a.views.Load() != 1 {
		t.Fatal(w.Code, a.views.Load())
	}
	w = request("GET", "/views/", map[string]string{"If-None-Match": w.Header().Get("ETag")})
	if w.Code != 304 || a.views.Load() != 2 {
		t.Fatal(w.Code, a.views.Load())
	}
	for _, tc := range []struct {
		method, path string
		headers      map[string]string
	}{
		{"HEAD", "/views/", nil}, {"GET", "/views", nil}, {"GET", "/views/missing.html", nil}, {"GET", "/views/app.js", nil},
		{"GET", "/views/", map[string]string{"Sec-Fetch-Dest": "empty"}}, {"GET", "/views/", map[string]string{"Purpose": "prefetch"}},
		{"GET", "/views/", map[string]string{"Sec-Purpose": "prefetch;prerender"}}, {"GET", "/views/", map[string]string{"Range": "bytes=0-9"}},
	} {
		request(tc.method, tc.path, tc.headers)
	}
	if a.views.Load() != 2 {
		t.Fatal("non-view counted", a.views.Load())
	}
	repo.p.Private = true
	request("GET", "/views/", nil)
	repo.p.Private = false
	repo.err = errors.New("offline")
	request("GET", "/views/", nil)
	if a.views.Load() != 2 {
		t.Fatal("denial/failure counted", a.views.Load())
	}
	repo.err = nil
	repo.p.WatermarkDisabled = false
	w = request("GET", "/views/", map[string]string{"Sec-Fetch-Dest": "document"})
	if w.Code != 200 || !strings.Contains(w.Body.String(), "data-drop-watermark") || a.views.Load() != 3 {
		t.Fatal("branded view", w.Code, a.views.Load())
	}
	repo.p.Private = true
	handler = ProjectDeliveryWithAnalytics(repo, root, nil, func(*http.Request, project.Project) bool { return true }, a)
	w = request("GET", "/views/", nil)
	if w.Code != 200 || a.views.Load() != 4 {
		t.Fatal("unlocked view", w.Code, a.views.Load())
	}
}
func TestPageViewAPIAuthenticatesOwnerAndKeepsBearerIsolation(t *testing.T) {
	a := &testPageViews{}
	h := firebaseProjectHarness(t, true, a)
	alice := h.projectToken("alice", "google.com", nil)
	bob := h.projectToken("bob", "google.com", nil)
	p := h.publishProject(alice, "analytics", "<h1>Alice</h1>", nil).Project
	path := "/api/agent/projects/" + p.ID + "/analytics"
	w := h.projectRequest("GET", path, "", "", alice, 0, nil)
	projectStatus(t, w, 200, "")
	result := oauthDecode[analytics.Result](t, w)
	if result.ProjectID != p.ID || result.PageViews != 42 || !result.Approximate || result.Timezone != "UTC" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(result, w.Header())
	}
	for _, tc := range []struct {
		token  string
		status int
	}{{bob, 404}, {"drop_unsupported-key", 403}, {h.projectToken("alice", "password", nil), 403}, {"", 401}, {"broken.jwt.token", 401}} {
		w = h.projectRequest("GET", path, "", "", tc.token, 0, nil)
		projectStatus(t, w, tc.status, "")
	}
	if a.queries.Load() != 1 {
		t.Fatal("unauthorized request reached analytics", a.queries.Load())
	}
	for _, suffix := range []string{"?interval=month", "?from=bad", "?from=2020-01-01T00:00:00Z", "?interval=day&interval=hour", "?unknown=1"} {
		w = h.projectRequest("GET", path+suffix, "", "", alice, 0, nil)
		projectStatus(t, w, 400, "invalid_request")
	}
	// Browser session still requires a recent login; subsequent analytics GET uses
	// the existing owner/session path, with no CSRF requirement for a read.
	cookie, csrf := bootstrap(t, h.handler)
	recent := h.projectToken("alice", "google.com", func(c jwt.MapClaims) { c["auth_time"] = h.clock.Load() })
	login := call(h.handler, "POST", "/api/auth/google", h.auth.Origin, cookie, csrf, `{"idToken":"`+recent+`"}`)
	projectStatus(t, login, 200, "")
	browser := call(h.handler, "GET", "/api/projects/"+p.ID+"/analytics", "", login.Result().Cookies()[0], "", "")
	projectStatus(t, browser, 200, "")
	a.err = errors.New("analytics worker offline")
	w = h.projectRequest("GET", path, "", "", alice, 0, nil)
	projectStatus(t, w, 503, "analytics_unavailable")
	if w.Header().Get("Retry-After") == "" {
		t.Fatal("no retry header")
	}
	site := httptest.NewRecorder()
	h.handler.ServeHTTP(site, httptest.NewRequest("GET", "/"+p.Slug+"/", nil))
	if site.Code != 200 || a.views.Load() != 1 {
		t.Fatal("analytics outage affected delivery", site.Code)
	}
}

type benchmarkAnalyticsSource struct{}

func (benchmarkAnalyticsSource) SubmitPageViews(context.Context, analytics.Snapshot) error {
	return nil
}
func (benchmarkAnalyticsSource) PageViews(context.Context, analytics.Query) (analytics.Result, error) {
	return analytics.Result{}, nil
}
func BenchmarkHTMLDeliveryAnalytics(b *testing.B) {
	root := b.TempDir()
	staged, e := content.StageHTML(root, strings.NewReader("<h1>analytics benchmark</h1>"), content.DefaultLimits())
	if e != nil {
		b.Fatal(e)
	}
	p := project.Project{ID: strings.Repeat("a", 32), Slug: "views", ActiveDigest: staged.Digest, Status: "active", WatermarkDisabled: true}
	if e = staged.Install(root, p.ID, p.Slug); e != nil {
		b.Fatal(e)
	}
	collector, e := analytics.NewCollector("primary-bench", b.TempDir(), benchmarkAnalyticsSource{})
	if e != nil {
		b.Fatal(e)
	}
	defer collector.Close()
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		var observer ProjectAnalytics
		if enabled {
			name = "enabled"
			observer = collector
		}
		b.Run(name, func(b *testing.B) {
			handler := ProjectDeliveryWithAnalytics(&deliveryRepository{p: p}, root, nil, nil, observer)
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/views/", nil))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, httptest.NewRequest("GET", "/views/", nil))
				if w.Code != 200 {
					b.Fatal(w.Code)
				}
			}
		})
	}
}

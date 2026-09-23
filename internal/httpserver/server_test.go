package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/runonflux/flux-drop/internal/content"
)

func TestConfigRejectsUntrustedOrigins(t *testing.T) {
	for _, origin := range []string{"", "http://example.com", "https://example.com/", "https://user:pass@example.com", "https://example.com?x=1", "https://example.com#x", "https://example.com?"} {
		if (Config{origin, content.DefaultLimits()}).Validate() == nil {
			t.Fatalf("accepted %q", origin)
		}
	}
	if (Config{"https://drop.example.com", content.DefaultLimits()}).Validate() != nil {
		t.Fatal("valid origin rejected")
	}
}

func TestFoundationFailsClosed(t *testing.T) {
	h, err := New(Config{"https://drop.example.com", content.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []struct {
		path   string
		status int
	}{
		{"/healthz", 200}, {"/readyz", 503}, {"/api/config", 200},
		{"/api/projects", 503}, {"/hello-123456/index.html", 503}, {"/_drop_internal/secret", 503},
	} {
		r := httptest.NewRecorder()
		req := httptest.NewRequest("GET", route.path, nil)
		req.Header.Set("Origin", "null")
		h.ServeHTTP(r, req)
		if r.Code != route.status {
			t.Fatalf("%s: %d", route.path, r.Code)
		}
		if r.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatal("CORS must not be enabled")
		}
		if r.Header().Get("Cross-Origin-Resource-Policy") != "same-origin" {
			t.Fatal("management resources must reject cross-origin embedding")
		}
		if r.Header().Get("Cache-Control") != "no-store" || r.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatal("missing security headers")
		}
		if route.path == "/api/config" && !strings.Contains(r.Body.String(), `"publishingEnabled":false`) {
			t.Fatal("misleading configuration")
		}
	}
}

func TestBrowserMutationBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, method, origin, site string
		want                       int
	}{
		{"valid", "POST", "https://drop.example.com", "same-origin", 204},
		{"opaque sandbox", "POST", "null", "cross-site", 403},
		{"missing", "POST", "", "", 403},
		{"external", "POST", "https://evil.example", "", 403},
		{"similar hostname", "POST", "https://drop.example.com.evil.example", "", 403},
		{"cross site", "POST", "https://drop.example.com", "cross-site", 403},
		{"unsafe GET", "GET", "https://drop.example.com", "same-origin", 405},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := RequireBrowserMutation("https://drop.example.com", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
			r := httptest.NewRequest(tc.method, "/api/projects", nil)
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if tc.site != "" {
				r.Header.Set("Sec-Fetch-Site", tc.site)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("got %d want %d", w.Code, tc.want)
			}
		})
	}
}

func TestRejectMultipleOriginHeaders(t *testing.T) {
	h := RequireBrowserMutation("https://drop.example.com", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Fatal("reached mutation") }))
	r := httptest.NewRequest("POST", "/api/projects", nil)
	r.Header.Add("Origin", "https://drop.example.com")
	r.Header.Add("Origin", "null")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("ambiguous origin accepted")
	}
}

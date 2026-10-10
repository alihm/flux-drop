package httpserver

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/session"
)

func TestPublicLandingPage(t *testing.T) {
	// Test the real server route: retained HomePage helpers must not accidentally
	// restore session/upload controls on the public home page.
	h, err := NewWithDependencies(Config{PublicOrigin: "https://drop.example", Limits: content.DefaultLimits()}, Dependencies{Sessions: &session.Service{Store: &sessionStore{records: map[string]session.Record{}}, Verifier: identityVerifier{}}, FirebaseWeb: &FirebaseWebConfig{ProjectID: "fluxcore-prod", AuthDomain: "fluxcore-prod.firebaseapp.com", APIKey: strings.Repeat("a", 30), AppID: "1:123:web:abcdef"}})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	body := w.Body.String()
	if w.Code != 200 || !strings.Contains(body, `id="start-publishing"`) || !strings.Contains(body, `href="https://runonflux.com/apps/drop"`) {
		t.Fatal("missing public workspace CTA", w.Code)
	}
	for _, control := range []string{`id="sign-in"`, `id="publish-panel"`, `id="dropzone"`, `id="files"`, `id="folder"`, `id="projects"`, `id="manage-dialog"`, `id="sign-in-dialog"`, "DropAuth.init", "/api/session"} {
		if strings.Contains(body, control) {
			t.Fatalf("landing page contains workspace control/code: %s", control)
		}
	}
	if w.Header().Get("Set-Cookie") != "" || strings.Contains(w.Header().Get("Content-Security-Policy"), "googleapis") {
		t.Fatal("landing page initializes authentication")
	}
	for _, asset := range []struct{ name, directive string }{{"ui/landing.js", "script-src"}, {"ui/home.css", "style-src"}} {
		raw, _ := homeUI.ReadFile(asset.name)
		sum := sha256.Sum256(raw)
		if !strings.Contains(w.Header().Get("Content-Security-Policy"), asset.directive+" 'sha256-"+base64.StdEncoding.EncodeToString(sum[:])+"'") {
			t.Fatal("missing CSP asset hash", asset.name)
		}
	}
	for _, method := range []string{"HEAD", "POST"} {
		response := httptest.NewRecorder()
		LandingPage().ServeHTTP(response, httptest.NewRequest(method, "/", nil))
		if method == "HEAD" && (response.Code != 200 || response.Body.Len() != 0) || method == "POST" && response.Code != 405 {
			t.Fatal(method, response.Code)
		}
	}
}

func TestHomePage(t *testing.T) {
	h := HomePage()
	for _, method := range []string{"GET", "HEAD", "POST"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/", nil))
		if method == "POST" {
			if w.Code != 405 {
				t.Fatal(w.Code)
			}
			continue
		}
		if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Security-Policy"), "script-src 'sha256-") || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(w.Code, w.Header())
		}
		if method == "HEAD" && w.Body.Len() != 0 {
			t.Fatal("HEAD body")
		}
		if method == "GET" && (!strings.Contains(w.Body.String(), `id="publish"`) || !strings.Contains(w.Body.String(), `id="claim-result"`) || !strings.Contains(w.Body.String(), "manage-dialog")) {
			t.Fatal("missing publishing UI or release limitation")
		}
	}
}

func TestAgentGuide(t *testing.T) {
	h := AgentGuide()
	for _, method := range []string{"GET", "HEAD"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/agents", nil))
		if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Header().Get("Content-Security-Policy"), "style-src 'sha256-") {
			t.Fatal(method, w.Code, w.Header())
		}
		if method == "GET" && (!strings.Contains(w.Body.String(), "api/agent/projects") || !strings.Contains(w.Body.String(), "claimURL") || !strings.Contains(w.Body.String(), "drop-mcp") || !strings.Contains(w.Body.String(), "publish_folder")) {
			t.Fatal("missing agent instructions")
		}
		if method == "HEAD" && w.Body.Len() != 0 {
			t.Fatal("HEAD body")
		}
	}
}

package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/session"
)

// Test-only repository; transaction behavior is covered by the Firestore tests.
type sessionStore struct {
	records     map[string]session.Record
	unavailable bool
}

func (s *sessionStore) Get(_ context.Context, key string) (session.Record, error) {
	if s.unavailable {
		return session.Record{}, errors.New("database offline")
	}
	r, ok := s.records[key]
	if !ok {
		return r, session.ErrUnauthorized
	}
	return r, nil
}
func (s *sessionStore) Create(_ context.Context, key string, r session.Record, _ time.Time) error {
	if s.unavailable {
		return errors.New("database offline")
	}
	s.records[key] = r
	return nil
}
func (s *sessionStore) Rotate(_ context.Context, old, key, csrf string, r session.Record, now time.Time) error {
	if s.unavailable {
		return errors.New("database offline")
	}
	prev := s.records[old]
	if !prev.Active(now) || prev.CSRF != csrf {
		return session.ErrUnauthorized
	}
	prev.Revoked = true
	s.records[old] = prev
	s.records[key] = r
	return nil
}

type identityVerifier struct{}

func (identityVerifier) VerifyGoogle(_ context.Context, token string) (session.Identity, error) {
	if token != "verified-google-token" {
		return session.Identity{}, session.ErrUnauthorized
	}
	return session.Identity{UID: "verified-uid", AuthTime: time.Now().UTC()}, nil
}
func (identityVerifier) CheckAccount(context.Context, session.Identity) error { return nil }

func authServer(t *testing.T) (http.Handler, *sessionStore) {
	t.Helper()
	store := &sessionStore{records: map[string]session.Record{}}
	h, err := NewWithDependencies(Config{"https://drop.example.com", content.DefaultLimits()}, Dependencies{Sessions: &session.Service{Store: store, Verifier: identityVerifier{}}})
	if err != nil {
		t.Fatal(err)
	}
	return h, store
}

func call(h http.Handler, method, path, origin string, cookie *http.Cookie, csrf, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	if cookie != nil {
		r.AddCookie(cookie)
	}
	if csrf != "" {
		r.Header.Set("X-CSRF-Token", csrf)
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func bootstrap(t *testing.T, h http.Handler) (*http.Cookie, string) {
	t.Helper()
	w := call(h, "POST", "/api/session", "https://drop.example.com", nil, "", "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatal("missing cookie")
	}
	var value struct {
		CSRF string `json:"csrfToken"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	return cookies[0], value.CSRF
}

func TestSessionHTTPBootstrapAndCookieSecurity(t *testing.T) {
	h, store := authServer(t)
	for _, origin := range []string{"", "null", "https://other.example"} {
		if w := call(h, "POST", "/api/session", origin, nil, "", ""); w.Code != 403 {
			t.Fatalf("bootstrap accepted origin %q", origin)
		}
	}
	if len(store.records) != 0 {
		t.Fatal("untrusted bootstrap wrote a session")
	}
	cookie, csrf := bootstrap(t, h)
	if cookie.Name != "__Host-drop-session" || !cookie.HttpOnly || !cookie.Secure || cookie.Domain != "" || cookie.Path != "/" || cookie.SameSite != http.SameSiteLaxMode || cookie.MaxAge < 300*24*3600 {
		t.Fatalf("insecure cookie: %+v", cookie)
	}
	if len(csrf) != 43 {
		t.Fatal("invalid CSRF")
	}
	w := call(h, "POST", "/api/session", "https://drop.example.com", cookie, "", "")
	if w.Code != 200 || len(store.records) != 1 || len(w.Result().Cookies()) != 0 {
		t.Fatal("bootstrap failed to reuse session")
	}
	if strings.Contains(w.Body.String(), cookie.Value) || strings.Contains(w.Body.String(), "anonymousOwner") {
		t.Fatal("internal credentials exposed")
	}
	get := call(h, "GET", "/api/session", "", cookie, "", "")
	if get.Code != 200 || get.Header().Get("Cache-Control") != "no-store" || get.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("session response can leak")
	}
}

func TestSessionHTTPLoginLogoutAndStaleRequests(t *testing.T) {
	h, _ := authServer(t)
	old, csrf := bootstrap(t, h)
	login := call(h, "POST", "/api/auth/google", "https://drop.example.com", old, csrf, `{"idToken":"verified-google-token"}`)
	if login.Code != 200 || !strings.Contains(login.Body.String(), `"authenticated":true`) {
		t.Fatal(login.Body.String())
	}
	next := login.Result().Cookies()[0]
	if next.Value == old.Value {
		t.Fatal("cookie not rotated")
	}
	var view struct {
		CSRF string `json:"csrfToken"`
	}
	_ = json.Unmarshal(login.Body.Bytes(), &view)
	if view.CSRF == csrf {
		t.Fatal("csrf not rotated")
	}
	stale := call(h, "POST", "/api/session", "https://drop.example.com", old, "", "")
	if stale.Code != 200 || len(stale.Result().Cookies()) != 1 || stale.Result().Cookies()[0].Value == old.Value || !strings.Contains(stale.Body.String(), `"authenticated":false`) {
		t.Fatal("stale session was not replaced with a new anonymous session")
	}
	logout := call(h, "POST", "/api/auth/logout", "https://drop.example.com", next, view.CSRF, "")
	if logout.Code != 200 || !strings.Contains(logout.Body.String(), `"authenticated":false`) {
		t.Fatal(logout.Body.String())
	}
	if call(h, "GET", "/api/session", "", next, "", "").Code != 401 {
		t.Fatal("logged-out cookie survived")
	}
}

func TestSessionBootstrapDoesNotMaskStorageFailure(t *testing.T) {
	h, store := authServer(t)
	cookie, _ := bootstrap(t, h)
	store.unavailable = true
	w := call(h, "POST", "/api/session", "https://drop.example.com", cookie, "", "")
	if w.Code == 200 || len(w.Result().Cookies()) != 0 {
		t.Fatal("storage failure replaced an existing session", w.Code)
	}
}

func TestSessionBootstrapRecoversMalformedCookie(t *testing.T) {
	h, _ := authServer(t)
	w := call(h, "POST", "/api/session", "https://drop.example.com", &http.Cookie{Name: sessionCookie, Value: "invalid"}, "", "")
	if w.Code != 200 || len(w.Result().Cookies()) != 1 {
		t.Fatal("malformed cookie did not recover", w.Code)
	}
}

func TestSessionHTTPRejectsForgedRequestsAndPayloads(t *testing.T) {
	h, _ := authServer(t)
	cookie, csrf := bootstrap(t, h)
	for _, tc := range []struct {
		origin, csrf, body string
		status             int
	}{
		{"null", csrf, `{"idToken":"verified-google-token"}`, 403},
		{"https://drop.example.com", "", `{"idToken":"verified-google-token"}`, 403},
		{"https://drop.example.com", strings.Repeat("a", 43), `{"idToken":"verified-google-token"}`, 403},
		{"https://drop.example.com", csrf, `{"idToken":"forged"}`, 401},
		{"https://drop.example.com", csrf, `{"idToken":"verified-google-token","uid":"admin"}`, 400},
		{"https://drop.example.com", csrf, `{"idToken":"verified-google-token"}{}`, 400},
		{"https://drop.example.com", csrf, `{"idToken":"` + strings.Repeat("x", 17000) + `"}`, 400},
	} {
		w := call(h, "POST", "/api/auth/google", tc.origin, cookie, tc.csrf, tc.body)
		if w.Code != tc.status || len(w.Result().Cookies()) != 0 {
			t.Fatalf("got %d want %d: %s", w.Code, tc.status, w.Body.String())
		}
	}
	r := httptest.NewRequest("GET", "/api/session", nil)
	r.AddCookie(cookie)
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("duplicate session cookies accepted")
	}
}

func TestSessionHTTPDatabaseOutageFailsClosed(t *testing.T) {
	h, store := authServer(t)
	cookie, _ := bootstrap(t, h)
	store.unavailable = true
	w := call(h, "GET", "/api/session", "", cookie, "", "")
	if w.Code != 503 || strings.Contains(w.Body.String(), "database offline") || len(w.Result().Cookies()) != 0 {
		t.Fatal("outage response is unsafe")
	}
}

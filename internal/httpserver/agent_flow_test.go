package httpserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func agentTestServer(t *testing.T) http.Handler {
	t.Helper()
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	h, err := NewWithDependencies(Config{"https://drop.example.com", content.DefaultLimits()}, Dependencies{
		Sessions:    &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}, Verifier: identityVerifier{}},
		Projects:    &project.Publisher{Repository: &project.RaftRepository{Store: store}, DataRoot: t.TempDir()},
		StagingRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func uploadForTest(t *testing.T, h http.Handler, path, bearer, origin string, cookie *http.Cookie, csrf, key string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", path, strings.NewReader("<!doctype html><title>Test</title>"))
	r.Header.Set("Content-Type", "text/html")
	r.Header.Set("Idempotency-Key", key)
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	if cookie != nil {
		r.AddCookie(cookie)
	}
	if csrf != "" {
		r.Header.Set("X-CSRF-Token", csrf)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAgentHTTPKeyPublishAndRevoke(t *testing.T) {
	h := agentTestServer(t)
	cookie, csrf := bootstrap(t, h)
	login := call(h, "POST", "/api/auth/google", "https://drop.example.com", cookie, csrf, `{"idToken":"verified-google-token"}`)
	if login.Code != 200 {
		t.Fatal(login.Body.String())
	}
	cookie = login.Result().Cookies()[0]
	var logged struct {
		CSRF string `json:"csrfToken"`
	}
	if err := json.Unmarshal(login.Body.Bytes(), &logged); err != nil {
		t.Fatal(err)
	}
	csrf = logged.CSRF
	if w := call(h, "POST", "/api/agent-keys", "https://evil.example", cookie, csrf, `{"label":"CI"}`); w.Code != 403 {
		t.Fatal("cross-origin issuance", w.Code)
	}
	created := call(h, "POST", "/api/agent-keys", "https://drop.example.com", cookie, csrf, `{"label":"CI"}`)
	if created.Code != 201 {
		t.Fatal(created.Code, created.Body.String())
	}
	var credential struct {
		Key      string `json:"key"`
		Metadata struct {
			ID string `json:"id"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &credential); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(credential.Key, "drop_") {
		t.Fatal("missing key")
	}
	listed := call(h, "GET", "/api/agent-keys", "", cookie, "", "")
	if listed.Code != 200 || strings.Contains(listed.Body.String(), credential.Key) {
		t.Fatal("key leaked in list", listed.Code, listed.Body.String())
	}
	if w := uploadForTest(t, h, "/api/agent/projects?name=bad", credential.Key, "https://evil.example", nil, "", "origin-key"); w.Code != 401 {
		t.Fatal("browser origin accepted", w.Code)
	}
	w := uploadForTest(t, h, "/api/agent/projects?name=from-agent", credential.Key, "", nil, "", "agent-key-1")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var result struct {
		Project project.Project `json:"project"`
		Path    string          `json:"path"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Project.Owner.Kind != "firebase" || result.Project.ExpiresAt != nil || result.Path == "" {
		t.Fatal(result)
	}
	if w := call(h, "POST", "/api/projects/"+result.Project.ID+"/claim", "https://drop.example.com", nil, "", "{}"); w.Code != 401 {
		t.Fatal("key bypassed browser claim", w.Code)
	}
	if w := call(h, "DELETE", "/api/agent-keys/"+credential.Metadata.ID, "https://drop.example.com", cookie, csrf, ""); w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := uploadForTest(t, h, "/api/agent/projects?name=after-revoke", credential.Key, "", nil, "", "agent-key-2"); w.Code != 401 {
		t.Fatal("revoked key published", w.Code)
	}
}

func TestTransferHTTPAcrossSessions(t *testing.T) {
	h := agentTestServer(t)
	sourceCookie, sourceCSRF := bootstrap(t, h)
	w := uploadForTest(t, h, "/api/projects?name=handoff", "", "https://drop.example.com", sourceCookie, sourceCSRF, "handoff-1")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var result struct {
		Project project.Project `json:"project"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	path := "/api/projects/" + result.Project.ID + "/transfer"
	mint := func(cookie *http.Cookie, csrf string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, nil)
		r.Header.Set("Origin", "https://drop.example.com")
		r.Header.Set("X-CSRF-Token", csrf)
		r.Header.Set("If-Match", strconv.Quote(strconv.FormatInt(result.Project.Revision, 10)))
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	otherCookie, otherCSRF := bootstrap(t, h)
	if w := mint(otherCookie, otherCSRF); w.Code != 404 {
		t.Fatal("outsider minted link", w.Code)
	}
	link := mint(sourceCookie, sourceCSRF)
	if link.Code != 200 {
		t.Fatal(link.Code, link.Body.String())
	}
	var payload struct {
		ClaimURL string `json:"claimURL"`
	}
	if err := json.Unmarshal(link.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	secret := strings.TrimPrefix(payload.ClaimURL, "https://drop.example.com/#claim-token=")
	if secret == payload.ClaimURL || secret == "" {
		t.Fatal("invalid link")
	}
	if w := call(h, "POST", "/api/transfers/redeem", "https://drop.example.com", otherCookie, otherCSRF, `{"token":"`+secret+`"}`); w.Code != 403 {
		t.Fatal("anonymous recipient claimed", w.Code)
	}
	login := call(h, "POST", "/api/auth/google", "https://drop.example.com", otherCookie, otherCSRF, `{"idToken":"verified-google-token"}`)
	if login.Code != 200 {
		t.Fatal(login.Code, login.Body.String())
	}
	otherCookie = login.Result().Cookies()[0]
	var logged struct {
		CSRF string `json:"csrfToken"`
	}
	if err := json.Unmarshal(login.Body.Bytes(), &logged); err != nil {
		t.Fatal(err)
	}
	claimed := call(h, "POST", "/api/transfers/redeem", "https://drop.example.com", otherCookie, logged.CSRF, `{"token":"`+secret+`"}`)
	if claimed.Code != 200 {
		t.Fatal(claimed.Code, claimed.Body.String())
	}
	if w := call(h, "POST", "/api/transfers/redeem", "https://drop.example.com", otherCookie, logged.CSRF, `{"token":"`+secret+`"}`); w.Code != 404 {
		t.Fatal("link reused", w.Code)
	}
	if w := call(h, "GET", "/api/projects/"+result.Project.ID, "", sourceCookie, "", ""); w.Code != 404 {
		t.Fatal("source retained access", w.Code)
	}
	if w := call(h, "GET", "/api/projects/"+result.Project.ID, "", otherCookie, "", ""); w.Code != 200 {
		t.Fatal("recipient lacks access", w.Code)
	}
}

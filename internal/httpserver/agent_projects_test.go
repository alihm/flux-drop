package httpserver

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
)

// The session service still receives the original auth_time, while the bearer
// endpoints use the same signed token without creating a session.
type projectSessionVerifier struct {
	firebase *session.AgentFirebaseVerifier
}

func (v projectSessionVerifier) VerifyGoogle(ctx context.Context, raw string) (session.Identity, error) {
	identity, err := v.firebase.VerifyBearer(ctx, raw)
	if err != nil || identity.Provider != "google.com" {
		return session.Identity{}, session.ErrUnauthorized
	}
	claims := jwt.MapClaims{}
	if _, _, err := new(jwt.Parser).ParseUnverified(raw, claims); err != nil {
		return session.Identity{}, err
	}
	return session.Identity{UID: identity.UID, AuthTime: time.Unix(int64(claims["auth_time"].(float64)), 0), ExpiresAt: identity.ExpiresAt}, nil
}
func (v projectSessionVerifier) CheckAccount(context.Context, session.Identity) error { return nil }

func firebaseProjectHarness(t *testing.T, oauthEnabled bool, analytics ...ProjectAnalytics) *oauthHarness {
	t.Helper()
	h := newOAuthHarness(t)
	verifier := h.auth.verifier.(*session.AgentFirebaseVerifier)
	deps := Dependencies{
		Sessions: &session.Service{Store: &session.RaftStore{Store: h.auth.Repository.Store, CreationsPerMinute: 60}, Verifier: projectSessionVerifier{verifier}, Now: h.auth.now},
		Projects: &project.Publisher{Repository: h.auth.Repository, DataRoot: t.TempDir()}, StagingRoot: t.TempDir(),
		ProjectBearerVerifier: verifier,
	}
	if len(analytics) > 0 {
		deps.Analytics = analytics[0]
	}
	if oauthEnabled {
		deps.AgentAuth = h.auth
	}
	var err error
	h.handler, err = NewWithDependencies(Config{h.auth.Origin, content.DefaultLimits()}, deps)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *oauthHarness) projectToken(uid, provider string, edit func(jwt.MapClaims)) string {
	h.t.Helper()
	now := h.clock.Load()
	claims := jwt.MapClaims{"iss": "https://securetoken.google.com/fluxcore-prod", "aud": "fluxcore-prod", "sub": uid, "iat": now, "exp": now + 3600, "auth_time": now - 30*86400, "email": uid + "@example.com", "email_verified": true, "firebase": map[string]string{"sign_in_provider": provider}}
	if edit != nil {
		edit(claims)
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key"
	raw, err := token.SignedString(h.key)
	if err != nil {
		h.t.Fatal(err)
	}
	return raw
}

func (h *oauthHarness) projectRequest(method, path, body, media, token string, revision int64, headers http.Header) *httptest.ResponseRecorder {
	h.t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if media != "" {
		r.Header.Set("Content-Type", media)
	}
	if revision > 0 {
		r.Header.Set("If-Match", strconv.Quote(strconv.FormatInt(revision, 10)))
	}
	// None of these browser credentials/gates may influence bearer authority.
	r.Header.Set("Cookie", sessionCookie+"=invalid; "+sessionCookie+"=duplicate")
	r.Header.Add("Origin", "https://untrusted.example")
	r.Header.Add("Origin", "null")
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	r.Header.Add("X-CSRF-Token", "invalid")
	r.Header.Add("X-CSRF-Token", "duplicate")
	for key, values := range headers {
		r.Header[key] = values
	}
	w := httptest.NewRecorder()
	h.handler.ServeHTTP(w, r)
	for key := range w.Header() {
		if strings.HasPrefix(strings.ToLower(key), "access-control-") || strings.EqualFold(key, "Set-Cookie") {
			h.t.Fatal("server-to-server API sent browser credentials/CORS", key)
		}
	}
	return w
}

func projectStatus(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if w.Code != status || code != "" && oauthDecode[map[string]any](t, w)["error"] != code {
		t.Fatal(w.Code, w.Body.String(), "wanted", status, code)
	}
}

type bearerProjectResult struct {
	Project project.Project `json:"project"`
	Path    string          `json:"path"`
	Claim   string          `json:"claimPath"`
}

func (h *oauthHarness) publishProject(token, name, body string, headers http.Header) bearerProjectResult {
	h.t.Helper()
	if headers == nil {
		headers = http.Header{}
	}
	headers.Set("Idempotency-Key", "publish-"+name)
	w := h.projectRequest("POST", "/api/agent/projects?name="+name, body, "text/html", token, 0, headers)
	projectStatus(h.t, w, 200, "")
	result := oauthDecode[bearerProjectResult](h.t, w)
	if result.Project.ExpiresAt != nil || result.Project.Owner.Kind != "firebase" || result.Path != "/"+result.Project.Slug+"/" || result.Claim != "/?claim="+result.Project.ID || w.Header().Get("ETag") != strconv.Quote(strconv.FormatInt(result.Project.Revision, 10)) {
		h.t.Fatal("invalid account project response", result, w.Header())
	}
	return result
}

func TestFirebaseProjectBearerDoesNotRelaxBrowserLogin(t *testing.T) {
	h := firebaseProjectHarness(t, true)
	token := h.projectToken("alice", "google.com", nil)
	cookie, csrf := bootstrap(t, h.handler)
	login := call(h.handler, "POST", "/api/auth/google", h.auth.Origin, cookie, csrf, `{"idToken":"`+token+`"}`)
	projectStatus(t, login, 401, "authentication_required")
	result := h.publishProject(token, "old-auth", "<h1>Still signed in to Firebase</h1>", nil)
	var stored project.Project
	if err := h.auth.Repository.AgentTransaction(context.Background(), func(tx *metadata.Tx) error {
		return tx.Get("projects/"+result.Project.ID, &stored)
	}); err != nil || stored.Owner.ID != "alice" || stored.ExpiresAt != nil {
		t.Fatal("incorrect Firebase ownership", stored.Owner, err)
	}
	// A fresh browser login sees exactly the same account project.
	recent := h.projectToken("alice", "google.com", func(c jwt.MapClaims) { c["auth_time"] = h.clock.Load() })
	login = call(h.handler, "POST", "/api/auth/google", h.auth.Origin, cookie, csrf, `{"idToken":"`+recent+`"}`)
	projectStatus(t, login, 200, "")
	view := call(h.handler, "GET", "/api/projects/"+stored.ID, "", login.Result().Cookies()[0], "", "")
	projectStatus(t, view, 200, "")
	if oauthDecode[bearerProjectResult](t, view).Project.ID != stored.ID {
		t.Fatal("browser did not see bearer-owned project")
	}
}

func TestFirebaseProjectManagementAndOwnerIsolation(t *testing.T) {
	h := firebaseProjectHarness(t, true)
	alice, bob := h.projectToken("alice", "google.com", nil), h.projectToken("bob", "google.com", nil)
	p := h.publishProject(alice, "alice-site", "<h1>Alice</h1>", nil).Project
	other := h.publishProject(bob, "bob-site", "<h1>Bob</h1>", nil).Project
	path := "/api/agent/projects/" + p.ID
	for _, tc := range []struct{ method, suffix, body, media string }{
		{"GET", "", "", ""}, {"POST", "/versions", "<h1>Hijack</h1>", "text/html"}, {"PATCH", "", `{"name":"hijack"}`, "application/json"}, {"PUT", "/privacy", `{"private":false}`, "application/json"}, {"DELETE", "", "", ""},
	} {
		w := h.projectRequest(tc.method, path+tc.suffix, tc.body, tc.media, bob, p.Revision, http.Header{"Idempotency-Key": {"isolation-test"}})
		projectStatus(t, w, 404, "project_not_found")
	}
	for _, tc := range []struct{ token, id string }{{alice, p.ID}, {bob, other.ID}} {
		w := h.projectRequest("GET", "/api/agent/projects", "", "", tc.token, 0, nil)
		projectStatus(t, w, 200, "")
		list := oauthDecode[struct {
			Projects   []project.Project
			NextCursor string
		}](t, w)
		if len(list.Projects) != 1 || list.Projects[0].ID != tc.id || list.NextCursor != "" {
			t.Fatal("cross-user list leak", list)
		}
	}
	w := h.projectRequest("PATCH", path, `{"name":"renamed-site"}`, "application/json", alice, p.Revision, nil)
	projectStatus(t, w, 200, "")
	p = oauthDecode[bearerProjectResult](t, w).Project
	if p.Slug != "renamed-site" {
		t.Fatal("rename failed", p)
	}
	w = h.projectRequest("POST", path+"/versions", "<h1>Version two</h1>", "text/html", alice, p.Revision, http.Header{"Idempotency-Key": {"version-two"}})
	projectStatus(t, w, 200, "")
	next := oauthDecode[bearerProjectResult](t, w).Project
	if next.ID != p.ID || next.Revision != p.Revision+1 || next.ActiveDigest == p.ActiveDigest || next.Slug != p.Slug {
		t.Fatal("version update changed identity or failed", next)
	}
	p = next
	for _, tc := range []struct{ method, suffix, body, media string }{
		{"PATCH", "", `{"name":"stale"}`, "application/json"}, {"PUT", "/privacy", `{"private":false}`, "application/json"}, {"POST", "/versions", "<h1>Stale</h1>", "text/html"}, {"DELETE", "", "", ""},
	} {
		w = h.projectRequest(tc.method, path+tc.suffix, tc.body, tc.media, alice, p.Revision-1, http.Header{"Idempotency-Key": {"stale-version"}})
		projectStatus(t, w, 409, "project_conflict")
		w = h.projectRequest(tc.method, path+tc.suffix, tc.body, tc.media, alice, 0, http.Header{"Idempotency-Key": {"missing-revision"}})
		projectStatus(t, w, 428, "revision_required")
		w = h.projectRequest(tc.method, path+tc.suffix, tc.body, tc.media, alice, 0, http.Header{"Idempotency-Key": {"invalid-revision"}, "If-Match": {"unquoted"}})
		projectStatus(t, w, 400, "invalid_if_match")
	}
	w = h.projectRequest("PUT", path+"/privacy", `{"private":true,"password":"a sufficiently long password"}`, "application/json", alice, p.Revision, nil)
	projectStatus(t, w, 200, "")
	p = oauthDecode[bearerProjectResult](t, w).Project
	if !p.Private {
		t.Fatal("privacy not applied")
	}
	w = h.projectRequest("PUT", path+"/privacy", `{"private":false}`, "application/json", alice, p.Revision, nil)
	projectStatus(t, w, 200, "")
	p = oauthDecode[bearerProjectResult](t, w).Project
	if p.Private {
		t.Fatal("privacy not removed")
	}
	w = h.projectRequest("DELETE", path, "", "", alice, p.Revision, nil)
	projectStatus(t, w, 204, "")
	if w.Body.Len() != 0 {
		t.Fatal("delete returned body")
	}
	projectStatus(t, h.projectRequest("GET", path, "", "", alice, 0, nil), 404, "project_not_found")
}

func TestFirebaseProjectAuthenticationPolicy(t *testing.T) {
	h := firebaseProjectHarness(t, false) // Independent of the OAuth feature toggle.
	google := h.projectToken("alice", "google.com", nil)
	projectStatus(t, h.projectRequest("GET", "/api/agent/projects", "", "", google, 0, nil), 200, "")
	for _, provider := range []string{"password", "github.com", "anonymous"} {
		token := h.projectToken("alice", provider, nil)
		projectStatus(t, h.projectRequest("GET", "/api/agent/projects", "", "", token, 0, nil), 403, "account_required")
	}
	for _, edit := range []func(jwt.MapClaims){
		func(c jwt.MapClaims) { c["exp"] = h.clock.Load() },
		func(c jwt.MapClaims) { c["iat"] = h.clock.Load() + 1 },
		func(c jwt.MapClaims) { c["email_verified"] = false },
		func(c jwt.MapClaims) { c["aud"] = "another-project" },
		func(c jwt.MapClaims) { c["iss"] = "https://securetoken.google.com/another-project" },
		func(c jwt.MapClaims) {
			c["firebase"] = map[string]string{"sign_in_provider": "google.com", "tenant": "tenant"}
		},
	} {
		token := h.projectToken("alice", "google.com", edit)
		projectStatus(t, h.projectRequest("GET", "/api/agent/projects", "", "", token, 0, nil), 401, "authentication_required")
	}
	for _, token := range []string{"", "opaque-agent-access-token", google[:len(google)-12] + "xxxxxxxxxxxx"} {
		projectStatus(t, h.projectRequest("GET", "/api/agent/projects", "", "", token, 0, nil), 401, "authentication_required")
	}
	projectStatus(t, h.projectRequest("GET", "/api/agent/projects", "", "", google, 0, http.Header{"Authorization": {"Bearer " + google, "Bearer " + google}}), 401, "authentication_required")
	// A valid browser cookie still cannot replace the explicit bearer.
	cookie, csrf := bootstrap(t, h.handler)
	recent := h.projectToken("alice", "google.com", func(c jwt.MapClaims) { c["auth_time"] = h.clock.Load() })
	login := call(h.handler, "POST", "/api/auth/google", h.auth.Origin, cookie, csrf, `{"idToken":"`+recent+`"}`)
	projectStatus(t, login, 200, "")
	cookie = login.Result().Cookies()[0]
	projectStatus(t, h.projectRequest("GET", "/api/agent/projects", "", "", "", 0, http.Header{"Cookie": {cookie.String()}}), 401, "authentication_required")
}

func TestAgentKeysRemainPublishOnly(t *testing.T) {
	h := firebaseProjectHarness(t, true)
	cookie, csrf := bootstrap(t, h.handler)
	recent := h.projectToken("alice", "google.com", func(c jwt.MapClaims) { c["auth_time"] = h.clock.Load() })
	login := call(h.handler, "POST", "/api/auth/google", h.auth.Origin, cookie, csrf, `{"idToken":"`+recent+`"}`)
	projectStatus(t, login, 200, "")
	cookie = login.Result().Cookies()[0]
	csrf = oauthDecode[struct {
		CSRF string `json:"csrfToken"`
	}](t, login).CSRF
	issued := call(h.handler, "POST", "/api/agent-keys", h.auth.Origin, cookie, csrf, `{"label":"publish only"}`)
	projectStatus(t, issued, 201, "")
	key := oauthDecode[struct{ Key string }](t, issued).Key
	p := h.publishProject(key, "key-publish", "<h1>Agent key publish</h1>", nil).Project
	path := "/api/agent/projects/" + p.ID
	for _, tc := range []struct{ method, path, body, media string }{
		{"GET", "/api/agent/projects", "", ""}, {"GET", path, "", ""}, {"POST", path + "/versions", "<h1>Denied</h1>", "text/html"}, {"PATCH", path, `{"name":"denied"}`, "application/json"}, {"PUT", path + "/privacy", `{"private":false}`, "application/json"}, {"DELETE", path, "", ""},
	} {
		projectStatus(t, h.projectRequest(tc.method, tc.path, tc.body, tc.media, key, p.Revision, http.Header{"Idempotency-Key": {"denied"}}), 403, "account_required")
	}
}

func TestFirebasePrivatePublishAndExistingUploadValidation(t *testing.T) {
	h := firebaseProjectHarness(t, true)
	token := h.projectToken("alice", "google.com", nil)
	headers := http.Header{"X-Drop-Password": {base64.RawURLEncoding.EncodeToString([]byte("a sufficiently long password"))}}
	p := h.publishProject(token, "private-bearer", "<h1>Private from activation</h1>", headers).Project
	if !p.Private {
		t.Fatal("private publish became public")
	}
	if _, err := h.auth.Repository.Resolve(context.Background(), p.Slug); err != nil {
		t.Fatal(err)
	}
	w := h.request("GET", "/"+p.Slug+"/", "", "", "", "", nil)
	projectStatus(t, w, 404, "")
	headers = http.Header{"X-Drop-Password": {base64.RawURLEncoding.EncodeToString([]byte("short"))}, "Idempotency-Key": {"weak-password"}}
	projectStatus(t, h.projectRequest("POST", "/api/agent/projects?name=weak", "<h1>Weak</h1>", "text/html", token, 0, headers), 400, "invalid_password")
	// Multipart files and safe ZIPs go through the existing staging path.
	var multipartBody bytes.Buffer
	form := multipart.NewWriter(&multipartBody)
	part, err := form.CreateFormFile("files", "index.html")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write([]byte("<h1>Multipart bearer</h1>"))
	_ = form.Close()
	w = h.projectRequest("POST", "/api/agent/projects?name=multipart-bearer", multipartBody.String(), form.FormDataContentType(), token, 0, http.Header{"Idempotency-Key": {"multipart-bearer"}})
	projectStatus(t, w, 200, "")
	for _, unsafe := range []bool{false, true} {
		var body bytes.Buffer
		archive := zip.NewWriter(&body)
		name := "index.html"
		if unsafe {
			name = "../index.html"
		}
		part, err := archive.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = part.Write([]byte("<h1>ZIP bearer</h1>"))
		_ = archive.Close()
		w = h.projectRequest("POST", "/api/agent/projects?name=zip-bearer", body.String(), "application/zip", token, 0, http.Header{"Idempotency-Key": {fmt.Sprintf("zip-%t", unsafe)}})
		if unsafe {
			projectStatus(t, w, 400, "invalid_project")
		} else {
			projectStatus(t, w, 200, "")
		}
	}
}

func TestFirebaseProjectPagination(t *testing.T) {
	h := firebaseProjectHarness(t, true)
	token := h.projectToken("alice", "google.com", nil)
	ids := []string{}
	for i := 0; i < 51; i++ {
		p := h.publishProject(token, fmt.Sprintf("paged-%d", i), fmt.Sprintf("<h1>Page %d</h1>", i), nil).Project
		ids = append(ids, p.ID)
	}
	sort.Strings(ids)
	var seen []string
	cursor := ""
	for page := 0; page < 2; page++ {
		w := h.projectRequest("GET", "/api/agent/projects?cursor="+cursor, "", "", token, 0, nil)
		projectStatus(t, w, 200, "")
		list := oauthDecode[struct {
			Projects   []project.Project
			NextCursor string
		}](t, w)
		if page == 0 && len(list.Projects) != 50 || page == 1 && len(list.Projects) != 1 {
			t.Fatal("incorrect page size", page, len(list.Projects))
		}
		for _, p := range list.Projects {
			seen = append(seen, p.ID)
		}
		cursor = list.NextCursor
		if page == 0 && cursor == "" || page == 1 && cursor != "" {
			t.Fatal("incorrect nextCursor", page, cursor)
		}
	}
	if !slices.Equal(seen, ids) {
		t.Fatal("pagination lost or repeated projects")
	}
	projectStatus(t, h.projectRequest("GET", "/api/agent/projects?cursor=invalid", "", "", token, 0, nil), 400, "invalid_project")
}

func TestFirebaseProjectUploadRetryAndOptionalKey(t *testing.T) {
	h := firebaseProjectHarness(t, true)
	token := h.projectToken("alice", "google.com", nil)
	// A server-generated operation key allows simple clients to omit the header.
	w := h.projectRequest("POST", "/api/agent/projects?name=optional-key", "<h1>No key required</h1>", "text/html", token, 0, nil)
	projectStatus(t, w, 200, "")
	p := oauthDecode[bearerProjectResult](t, w).Project
	w = h.projectRequest("POST", "/api/agent/projects/"+p.ID+"/versions", "<h1>No key on version</h1>", "text/html", token, p.Revision, nil)
	projectStatus(t, w, 200, "")
	if updated := oauthDecode[bearerProjectResult](t, w).Project; updated.ID != p.ID || updated.Revision != p.Revision+1 {
		t.Fatal("version without a key failed", updated)
	}
	// Caller-supplied keys are scoped to the Firebase UID, so token renewal does
	// not publish a second project. Preserve the existing conflict on changed data.
	headers := http.Header{"Idempotency-Key": {"stable-upload-key"}}
	w = h.projectRequest("POST", "/api/agent/projects?name=retry-safe", "<h1>Safe retry</h1>", "text/html", token, 0, headers)
	projectStatus(t, w, 200, "")
	first := oauthDecode[bearerProjectResult](t, w)
	renewed := h.projectToken("alice", "google.com", func(c jwt.MapClaims) { c["auth_time"] = h.clock.Load() - 86400 })
	w = h.projectRequest("POST", "/api/agent/projects?name=retry-safe", "<h1>Safe retry</h1>", "text/html", renewed, 0, headers)
	projectStatus(t, w, 200, "")
	if retry := oauthDecode[bearerProjectResult](t, w); retry.Project.ID != first.Project.ID || retry.Project.Revision != first.Project.Revision {
		t.Fatal("retry published twice", first, retry)
	}
	w = h.projectRequest("POST", "/api/agent/projects?name=retry-safe", "<h1>Different content</h1>", "text/html", token, 0, headers)
	projectStatus(t, w, 409, "project_conflict")
	for _, keys := range [][]string{{""}, {"short"}, {"valid-key-one", "valid-key-two"}} {
		w = h.projectRequest("POST", "/api/agent/projects?name=invalid-key", "<h1>Invalid key</h1>", "text/html", token, 0, http.Header{"Idempotency-Key": keys})
		projectStatus(t, w, 400, "invalid_project")
	}
}

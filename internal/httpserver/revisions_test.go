package httpserver

import (
	"net/http"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v4"
	"github.com/runonflux/flux-drop/internal/project"
)

func TestRevisionHTTPBearerAndBrowserContracts(t *testing.T) {
	h := firebaseProjectHarness(t, true)
	alice := h.projectToken("alice", "google.com", nil)
	bob := h.projectToken("bob", "google.com", nil)
	p := h.publishProject(alice, "revision-api", "<h1>First</h1>", nil).Project
	base := "/api/agent/projects/" + p.ID + "/versions"
	w := h.projectRequest("GET", base, "", "", alice, 0, nil)
	projectStatus(t, w, 200, "")
	versions := oauthDecode[struct {
		Versions   []project.Revision
		NextCursor string
	}](t, w)
	if len(versions.Versions) != 1 || !versions.Versions[0].Active || versions.NextCursor != "" {
		t.Fatal(versions)
	}
	first := versions.Versions[0].ID
	w = h.projectRequest("POST", base, "<h1>Second</h1>", "text/html", alice, p.Revision, http.Header{"Idempotency-Key": {"revision-second"}})
	projectStatus(t, w, 200, "")
	p = oauthDecode[bearerProjectResult](t, w).Project
	activate := base + "/" + first + "/activate"
	remove := base + "/" + first
	for _, tc := range []struct{ method, path string }{{"GET", base}, {"POST", activate}, {"DELETE", remove}} {
		projectStatus(t, h.projectRequest(tc.method, tc.path, "", "", bob, p.Revision, nil), 404, "project_not_found")
		projectStatus(t, h.projectRequest(tc.method, tc.path, "", "", "drop_invalid", p.Revision, nil), 403, "account_required")
		projectStatus(t, h.projectRequest(tc.method, tc.path, "", "", h.projectToken("alice", "password", nil), p.Revision, nil), 403, "account_required")
	}
	projectStatus(t, h.projectRequest("POST", activate, "", "", alice, 0, nil), 428, "revision_required")
	projectStatus(t, h.projectRequest("POST", activate, "", "", alice, p.Revision-1, nil), 409, "project_conflict")
	w = h.projectRequest("POST", activate, "", "", alice, p.Revision, nil)
	projectStatus(t, w, 200, "")
	selected := oauthDecode[bearerProjectResult](t, w).Project
	if selected.ActiveDigest == p.ActiveDigest || selected.Revision != p.Revision+1 {
		t.Fatal(selected, p)
	}
	p = selected
	served := call(h.handler, "GET", "/"+p.Slug+"/", "", nil, "", "")
	if served.Code != 200 || !strings.Contains(served.Body.String(), "<h1>First</h1>") || strings.Contains(served.Body.String(), "<h1>Second</h1>") {
		t.Fatal("rollback did not serve original bytes", served.Code, served.Body.String())
	}
	projectStatus(t, h.projectRequest("DELETE", remove, "", "", alice, p.Revision, nil), 409, "project_conflict")
	// Browser route keeps its session and CSRF requirements.
	cookie, csrf := bootstrap(t, h.handler)
	recent := h.projectToken("alice", "google.com", func(c jwt.MapClaims) { c["auth_time"] = h.clock.Load() })
	login := call(h.handler, "POST", "/api/auth/google", h.auth.Origin, cookie, csrf, `{"idToken":"`+recent+`"}`)
	projectStatus(t, login, 200, "")
	cookie = login.Result().Cookies()[0]
	csrf = oauthDecode[struct {
		CSRF string `json:"csrfToken"`
	}](t, login).CSRF
	browserBase := "/api/projects/" + p.ID + "/versions"
	w = call(h.handler, "GET", browserBase, "", cookie, "", "")
	projectStatus(t, w, 200, "")
	versions = oauthDecode[struct {
		Versions   []project.Revision
		NextCursor string
	}](t, w)
	second := ""
	for _, v := range versions.Versions {
		if !v.Active {
			second = v.ID
		}
	}
	if second == "" {
		t.Fatal(versions)
	}
	validBrowser := http.Header{}
	validBrowser.Set("Cookie", cookie.String())
	validBrowser.Set("Origin", h.auth.Origin)
	validBrowser.Set("Sec-Fetch-Site", "same-origin")
	validBrowser.Set("X-CSRF-Token", csrf)
	w = h.projectRequest("POST", browserBase+"/"+first+"/activate", "", "", "", p.Revision, validBrowser)
	projectStatus(t, w, 200, "")
	if current := oauthDecode[bearerProjectResult](t, w).Project; current.Revision != p.Revision {
		t.Fatal("selecting active version was not a no-op", current)
	}
	for _, origin := range []string{h.auth.Origin, "https://evil.example"} {
		// Missing CSRF and a cross-origin request both remain rejected.
		projectStatus(t, call(h.handler, "DELETE", browserBase+"/"+second, origin, cookie, "", ""), 403, "")
	}
	// Bearer mutations ignore the bogus browser credentials set by this helper
	// and never emit cookies or CORS headers.
	w = h.projectRequest("DELETE", base+"/"+second, "", "", alice, p.Revision, nil)
	projectStatus(t, w, 200, "")
	p = oauthDecode[bearerProjectResult](t, w).Project
	if w.Header().Get("Set-Cookie") != "" || w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal(w.Header())
	}
	projectStatus(t, h.projectRequest("POST", base+"/"+second+"/activate", "", "", alice, p.Revision, nil), 404, "project_not_found")
	projectStatus(t, h.projectRequest("GET", base+"?cursor=invalid", "", "", alice, 0, nil), 400, "invalid_project")
	w = h.projectRequest("DELETE", "/api/agent/projects/"+p.ID, "", "", alice, p.Revision, nil)
	projectStatus(t, w, 204, "")
	projectStatus(t, h.projectRequest("GET", base, "", "", alice, 0, nil), 404, "project_not_found")
	replacement := h.publishProject(bob, "revision-api", "<h1>New owner</h1>", nil).Project
	if replacement.Slug != p.Slug {
		t.Fatal("deleted name still reserved", replacement.Slug, p.Slug)
	}
}

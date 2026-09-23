package httpserver

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
)

func TestUploadHTTPPreservesFolderPathsAndRejectsTraversal(t *testing.T) {
	for _, name := range []string{"folder/assets/main.js", "../main.js", `folder\main.js`} {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		first, _ := writer.CreateFormFile("files", "folder/index.html")
		_, _ = io.WriteString(first, "<h1>hi</h1>")
		second, _ := writer.CreateFormFile("files", name)
		_, _ = io.WriteString(second, "console.log(1)")
		_ = writer.Close()
		r := httptest.NewRequest("POST", "/api/projects", &body)
		r.Header.Set("Content-Type", writer.FormDataContentType())
		staged, err := stageRequest(httptest.NewRecorder(), r, t.TempDir(), content.DefaultLimits())
		if name != "folder/assets/main.js" {
			if err == nil {
				staged.Discard()
				t.Fatal("traversal was sanitized/accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		defer staged.Discard()
		if staged.Manifest.Files[0].Path != "assets/main.js" {
			t.Fatal("multipart filename lost folder path")
		}
	}
}

func TestUploadHTTPFormatsAndLimits(t *testing.T) {
	var packed bytes.Buffer
	zw := zip.NewWriter(&packed)
	f, _ := zw.Create("site/index.html")
	_, _ = f.Write([]byte("hello"))
	_ = zw.Close()
	for _, tc := range []struct {
		kind string
		body []byte
	}{{"text/html", []byte("hello")}, {"application/zip", packed.Bytes()}} {
		r := httptest.NewRequest("POST", "/api/projects", bytes.NewReader(tc.body))
		r.Header.Set("Content-Type", tc.kind)
		staged, err := stageRequest(httptest.NewRecorder(), r, t.TempDir(), content.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		defer staged.Discard()
		if len(staged.Manifest.Files) != 1 || staged.Manifest.Files[0].Path != "index.html" {
			t.Fatal("incorrect normalized upload")
		}
	}
	r := httptest.NewRequest("POST", "/api/projects", strings.NewReader(strings.Repeat("x", 100)))
	r.Header.Set("Content-Type", "text/html")
	_, err := stageRequest(httptest.NewRecorder(), r, t.TempDir(), content.Limits{UploadBytes: 10, ExpandedBytes: 200, Files: 10})
	w := httptest.NewRecorder()
	projectError(w, err)
	if w.Code != 413 {
		t.Fatalf("oversize returned %d: %v", w.Code, err)
	}
}

func TestFirestoreProjectHTTP(t *testing.T) {
	if os.Getenv("DROP_TEST_FIRESTORE") != "1" {
		t.Skip("requires localhost Firestore emulator")
	}
	host := os.Getenv("FIRESTORE_EMULATOR_HOST")
	if !strings.HasPrefix(host, "127.0.0.1:") && !strings.HasPrefix(host, "localhost:") {
		t.Fatal("refusing non-local emulator")
	}
	// A temp directory's random suffix isolates the emulator namespace.
	root := t.TempDir()
	pieces := strings.Split(root, "/")
	name := "demo-http-" + strings.ToLower(pieces[len(pieces)-2])
	if len(name) > 50 {
		name = name[:50]
	}
	client, err := firestore.NewClient(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	repo := &project.FirestoreRepository{Client: client}
	h, err := NewWithDependencies(Config{"https://drop.example.com", content.DefaultLimits()}, Dependencies{
		Sessions: &session.Service{Store: &session.FirestoreStore{Client: client, CreationsPerMinute: 60}, Verifier: identityVerifier{}},
		Projects: &project.Publisher{Repository: repo, DataRoot: root},
	})
	if err != nil {
		t.Fatal(err)
	}
	cookie, csrf := bootstrap(t, h)
	// Storage failure must reject an authenticated upload before consuming bytes
	// or creating a publication reservation.
	unavailable, err := NewWithDependencies(Config{"https://drop.example.com", content.DefaultLimits()}, Dependencies{
		Sessions: &session.Service{Store: &session.FirestoreStore{Client: client}, Verifier: identityVerifier{}},
		Projects: &project.Publisher{Repository: repo, DataRoot: root + "/missing-storage"},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := strings.NewReader("must not be consumed")
	rejected := httptest.NewRequest("POST", "/api/projects?name=unavailable", body)
	rejected.Header.Set("Origin", "https://drop.example.com")
	rejected.Header.Set("Content-Type", "text/html")
	rejected.Header.Set("X-CSRF-Token", csrf)
	rejected.Header.Set("Idempotency-Key", "storage-unavailable")
	rejected.AddCookie(cookie)
	response := httptest.NewRecorder()
	unavailable.ServeHTTP(response, rejected)
	if response.Code != 503 || response.Header().Get("Retry-After") != "30" || body.Len() != len("must not be consumed") {
		t.Fatalf("storage failure did not reject before body read: %d %s remaining=%d", response.Code, response.Body.String(), body.Len())
	}
	outsider, _ := bootstrap(t, h)
	request := func(method, path, key, match, body string, c *http.Cookie, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Origin", "https://drop.example.com")
		r.Header.Set("Content-Type", "text/html")
		r.AddCookie(c)
		r.Header.Set("X-CSRF-Token", token)
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		if match != "" {
			r.Header.Set("If-Match", match)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	decode := func(w *httptest.ResponseRecorder) project.Project {
		t.Helper()
		if w.Code != 200 {
			t.Fatalf("%d: %s", w.Code, w.Body.String())
		}
		var value struct {
			Project project.Project `json:"project"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		return value.Project
	}
	created := decode(request("POST", "/api/projects?name=example", "http-create", "", "first", cookie, csrf))
	if created.Revision != 1 {
		t.Fatal("not activated")
	}
	if w := request("GET", "/api/projects/"+created.ID, "", "", "", outsider, ""); w.Code != 404 {
		t.Fatal("foreign metadata access")
	}
	if w := request("POST", "/api/projects?name=example", "http-duplicate", "", "first", cookie, csrf); w.Code != 409 || !strings.Contains(w.Body.String(), "duplicate_content") {
		t.Fatal("duplicate API result")
	}
	updated := decode(request("POST", "/api/projects/"+created.ID+"/versions", "http-update", `"1"`, "second", cookie, csrf))
	if updated.Slug != created.Slug || updated.Revision != 2 {
		t.Fatal("update changed URL")
	}
	if w := request("POST", "/api/projects/"+created.ID+"/versions", "stale-update", `"1"`, "third", cookie, csrf); w.Code != 409 {
		t.Fatal("stale update succeeded")
	}
	login := call(h, "POST", "/api/auth/google", "https://drop.example.com", cookie, csrf, `{"idToken":"verified-google-token"}`)
	if login.Code != 200 {
		t.Fatal(login.Body.String())
	}
	cookie = login.Result().Cookies()[0]
	var logged struct {
		CSRF string `json:"csrfToken"`
	}
	_ = json.Unmarshal(login.Body.Bytes(), &logged)
	csrf = logged.CSRF
	claimed := decode(request("POST", "/api/projects/"+created.ID+"/claim", "", `"2"`, "", cookie, csrf))
	if claimed.ExpiresAt != nil || claimed.Owner.Kind != "firebase" {
		t.Fatal("claim API failed")
	}
	privacyRequest := func(match, secret string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("PUT", "/api/projects/"+created.ID+"/privacy", strings.NewReader(`{"private":true,"password":"`+secret+`"}`))
		r.Header.Set("Origin", "https://drop.example.com")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-CSRF-Token", csrf)
		r.Header.Set("If-Match", match)
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	decode(privacyRequest(`"3"`, "a long private password"))
	unlock := call(h, "POST", "/api/unlock", "https://drop.example.com", cookie, csrf, `{"slug":"`+created.Slug+`","password":"a long private password"}`)
	if unlock.Code != 200 || len(unlock.Result().Cookies()) != 1 {
		t.Fatal("unlock failed", unlock.Code, unlock.Body.String())
	}
	grant := unlock.Result().Cookies()[0]
	if grant.Name != grantCookie(created.ID) || !grant.Secure || !grant.HttpOnly || grant.Path != "/" || grant.Domain != "" || grant.SameSite != http.SameSiteStrictMode {
		t.Fatal("unsafe grant cookie", grant)
	}
	if strings.Contains(unlock.Body.String(), grant.Value) {
		t.Fatal("grant token exposed in JSON")
	}
	privateRequest := func(c *http.Cookie, mode, file string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/"+created.Slug+"/"+file, nil)
		r.AddCookie(c)
		r.AddCookie(grant)
		r.Header.Set("Sec-Fetch-Mode", mode)
		r.Header.Set("Sec-Fetch-Dest", "document")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := privateRequest(cookie, "navigate", ""); w.Code != 200 || !strings.HasPrefix(w.Header().Get("X-Accel-Redirect"), "/_drop_internal/private/") || w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("private delivery failed", w.Code, w.Header())
	}
	if w := privateRequest(outsider, "navigate", ""); w.Code != 303 || w.Header().Get("Location") != "/unlock/"+created.Slug {
		t.Fatal("grant crossed session", w.Code)
	}
	if w := privateRequest(cookie, "cors", ""); w.Code != 404 {
		t.Fatal("private fetch allowed", w.Code)
	}
	if w := privateRequest(cookie, "navigate", "assets/main.js"); w.Code != 404 {
		t.Fatal("private asset allowed", w.Code)
	}
	if w := call(h, "POST", "/api/unlock", "null", cookie, csrf, `{}`); w.Code != 403 {
		t.Fatal("opaque origin accepted")
	}
	decode(privacyRequest(`"4"`, "a changed private password"))
	if w := privateRequest(cookie, "navigate", ""); w.Code != 303 {
		t.Fatal("rotated password retained grant", w.Code)
	}
	if w := request("DELETE", "/api/projects/"+created.ID, "", `"5"`, "", cookie, csrf); w.Code != 204 {
		t.Fatal(w.Body.String())
	}
}

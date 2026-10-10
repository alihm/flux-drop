package storagepool

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/httpserver"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

type noGoogle struct{}

func (noGoogle) VerifyGoogle(context.Context, string) (session.Identity, error) {
	return session.Identity{}, session.ErrUnauthorized
}
func (noGoogle) CheckAccount(context.Context, session.Identity) error { return session.ErrUnauthorized }

// Exercise the actual browser handlers, metadata transactions, remote TLS upload,
// cache delivery, privacy/unlock, update and deletion together. Firebase and Flux
// are replaced only by explicitly private test dependencies.
func TestBrowserLifecycleUsesRemoteStorageAndCurrentAccess(t *testing.T) {
	secondary, _, pool := fixture(t, t.TempDir(), nil)
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}, Verifier: noGoogle{}}
	token, view, err := sessions.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	repo := &project.RaftRepository{Store: store, StorageOffers: pool.Offers}
	root := t.TempDir()
	handler, err := httpserver.NewWithDependencies(httpserver.Config{PublicOrigin: "https://drop.example.com", Limits: content.DefaultLimits()}, httpserver.Dependencies{Sessions: sessions, Projects: &project.Publisher{Repository: repo, DataRoot: root, Installer: pool}, StagingRoot: t.TempDir(), Fallback: pool, StorageStatus: pool.AdminHandlerWithOperations(store)})
	if err != nil {
		t.Fatal(err)
	}
	cookies := []*http.Cookie{{Name: "__Host-drop-session", Value: token}}
	call := func(method, path, body, kind, key string, revision int64, navigate bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Origin", "https://drop.example.com")
		req.Header.Set("X-CSRF-Token", view.Record.CSRF)
		if kind != "" {
			req.Header.Set("Content-Type", kind)
		}
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		if revision > 0 {
			req.Header.Set("If-Match", "\""+strconv.FormatInt(revision, 10)+"\"")
		}
		if navigate {
			req.Header.Set("Sec-Fetch-Mode", "navigate")
			req.Header.Set("Sec-Fetch-Dest", "document")
		}
		for _, cookie := range cookies {
			req.AddCookie(cookie)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	upload := call("POST", "/api/projects?name=remote", "<h1>initial</h1>", "text/html", "browser_upload_1", 0, false)
	if upload.Code != 200 {
		t.Fatal(upload.Code, upload.Body.String())
	}
	var response struct {
		Project project.Project `json:"project"`
	}
	if err := json.Unmarshal(upload.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	pr := response.Project
	authority, err := repo.Resolve(context.Background(), pr.Slug)
	if err != nil || authority.StorageApp != "storagea" {
		t.Fatal(authority, err)
	}
	if _, err := content.VerifyVersion(secondary.operationVersion(Operation{ProjectID: pr.ID, Digest: pr.ActiveDigest, Generation: authority.StorageGeneration}), pr.ActiveDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "projects", pr.ID)); !os.IsNotExist(err) {
		t.Fatal("primary retained full project files", err)
	}
	if rec := call("GET", "/"+pr.Slug+"/", "", "", "", 0, false); rec.Code != 200 || (!strings.HasPrefix(rec.Body.String(), "<h1>initial</h1>") || !strings.Contains(rec.Body.String(), `data-drop-watermark="runonflux"`)) {
		t.Fatal(rec.Code, rec.Body.String())
	}
	if pr.Slug != "remote" || pr.InitialSuffix != "" {
		t.Fatal("clean remote URL unavailable", pr.Slug)
	}
	for _, body := range []string{`{}`, `{"enabled":"false"}`, `{"enabled":false,"extra":true}`, `{"enabled":false} {}`} {
		if rec := call("PUT", "/api/projects/"+pr.ID+"/watermark", body, "application/json", "", pr.Revision, false); rec.Code != 400 {
			t.Fatal("malformed preference accepted", body, rec.Code)
		}
	}
	badCSRF := httptest.NewRequest("PUT", "/api/projects/"+pr.ID+"/watermark", strings.NewReader(`{"enabled":false}`))
	badCSRF.AddCookie(cookies[0])
	badCSRF.Header.Set("Origin", "https://drop.example.com")
	badCSRF.Header.Set("Content-Type", "application/json")
	badCSRF.Header.Set("If-Match", `"1"`)
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, badCSRF)
	if denied.Code < 400 {
		t.Fatal("preference mutation without CSRF", denied.Code)
	}
	disabled := call("PUT", "/api/projects/"+pr.ID+"/watermark", `{"enabled":false}`, "application/json", "", pr.Revision, false)
	if disabled.Code != 200 {
		t.Fatal(disabled.Code, disabled.Body.String())
	}
	json.Unmarshal(disabled.Body.Bytes(), &response)
	pr = response.Project
	if !pr.WatermarkDisabled {
		t.Fatal("opt-out absent from JSON")
	}
	if rec := call("GET", "/"+pr.Slug+"/", "", "", "", 0, false); rec.Code != 200 || rec.Body.String() != "<h1>initial</h1>" {
		t.Fatal("cache ignored preference", rec.Code, rec.Body.String())
	}
	restored := call("PUT", "/api/projects/"+pr.ID+"/watermark", `{"enabled":true}`, "application/json", "", pr.Revision, false)
	if restored.Code != 200 {
		t.Fatal(restored.Code, restored.Body.String())
	}
	json.Unmarshal(restored.Body.Bytes(), &response)
	pr = response.Project
	// HTTP retry returns the same project and does not allocate storage again.
	retry := call("POST", "/api/projects?name=remote", "<h1>initial</h1>", "text/html", "browser_upload_1", 0, false)
	if retry.Code != 200 {
		t.Fatal(retry.Code, retry.Body.String())
	}
	update := call("POST", "/api/projects/"+pr.ID+"/versions", "<h1>updated</h1>", "text/html", "browser_upload_2", pr.Revision, false)
	if update.Code != 200 {
		t.Fatal(update.Code, update.Body.String())
	}
	json.Unmarshal(update.Body.Bytes(), &response)
	pr = response.Project
	if rec := call("GET", "/"+pr.Slug+"/", "", "", "", 0, false); rec.Code != 200 || (!strings.HasPrefix(rec.Body.String(), "<h1>updated</h1>") || !strings.Contains(rec.Body.String(), `data-drop-watermark="runonflux"`)) {
		t.Fatal(rec.Code, rec.Body.String())
	}
	private := call("PUT", "/api/projects/"+pr.ID+"/privacy", `{"private":true,"password":"a very long test password"}`, "application/json", "", pr.Revision, false)
	if private.Code != 200 {
		t.Fatal(private.Code, private.Body.String())
	}
	json.Unmarshal(private.Body.Bytes(), &response)
	pr = response.Project
	if rec := call("GET", "/"+pr.Slug+"/", "", "", "", 0, false); rec.Code != 404 {
		t.Fatal("private cache leaked", rec.Code)
	}
	unlock := call("POST", "/api/unlock", `{"slug":"`+pr.Slug+`","password":"a very long test password"}`, "application/json", "", 0, false)
	if unlock.Code != 200 {
		t.Fatal(unlock.Code, unlock.Body.String())
	}
	cookies = append(cookies, unlock.Result().Cookies()...)
	if rec := call("GET", "/"+pr.Slug+"/", "", "", "", 0, true); rec.Code != 200 || (!strings.HasPrefix(rec.Body.String(), "<h1>updated</h1>") || !strings.Contains(rec.Body.String(), `data-drop-watermark="runonflux"`)) {
		t.Fatal("remote private navigation failed", rec.Code, rec.Body.String())
	}
	if rec := call("GET", "/api/storage/apps", "", "", "", 0, false); rec.Code != 404 {
		t.Fatal("browser session accessed admin data")
	}
	admin := httptest.NewRequest("GET", "/api/storage/apps", nil)
	admin.Header.Set("Authorization", "Bearer "+testKey)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, admin)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "allocatedBytes") {
		t.Fatal("primary accounting absent", rec.Code, rec.Body.String())
	}
	deleted := call("DELETE", "/api/projects/"+pr.ID, "", "", "", pr.Revision, false)
	if deleted.Code != 204 {
		t.Fatal(deleted.Code, deleted.Body.String())
	}
	if rec := call("GET", "/"+pr.Slug+"/", "", "", "", 0, true); rec.Code != 404 {
		t.Fatal("deleted cached bytes served", rec.Code)
	}
}

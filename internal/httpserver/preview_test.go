package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/preview"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func TestThumbnailsRecheckPrivacyOwnersVersionsAndDeletion(t *testing.T) {
	ctx := context.Background()
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}, Verifier: identityVerifier{}}
	token, view, err := sessions.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	actor, _ := project.ActorFrom(token, view)
	p := project.Project{ID: strings.Repeat("a", 32), Slug: "sample-abcdef", ActiveDigest: strings.Repeat("b", 64), Owner: actor.Owner(), Status: "active", CreatedAt: time.Now(), PolicyRevision: 1, Revision: 1}
	put := func() {
		t.Helper()
		if err := store.Run(ctx, func(tx *metadata.Tx) error { return tx.Set("projects/"+p.ID, p) }); err != nil {
			t.Fatal(err)
		}
	}
	put()
	previews, err := preview.New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	repo := &project.RaftRepository{Store: store}
	handler, err := NewWithDependencies(Config{PublicOrigin: "https://drop.test", Limits: content.DefaultLimits()}, Dependencies{Sessions: sessions, Projects: &project.Publisher{Repository: repo, DataRoot: t.TempDir()}, Previews: previews})
	if err != nil {
		t.Fatal(err)
	}
	request := func(version, cookie string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/api/projects/"+p.ID+"/thumbnail"+version, nil)
		if cookie != "" {
			r.AddCookie(&http.Cookie{Name: sessionCookie, Value: cookie})
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	w := request("", "")
	if w.Code != 200 || w.Header().Get("X-Drop-Preview") != "pending" {
		t.Fatal("missing replica thumbnail should be pending", w.Code)
	}
	var imageBytes bytes.Buffer
	if err := jpeg.Encode(&imageBytes, image.NewRGBA(image.Rect(0, 0, 320, 180)), nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(previews.ImagePath(p), imageBytes.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	w = request("?v="+p.ActiveDigest, "")
	if w.Code != 200 || w.Header().Get("X-Drop-Preview") != "ready" || !bytes.Equal(w.Body.Bytes(), imageBytes.Bytes()) {
		t.Fatal("public thumbnail failed", w.Code)
	}
	p.Private = true
	p.PolicyRevision++
	put()
	if w = request("", ""); w.Code != 404 {
		t.Fatal("private image leaked", w.Code)
	}
	if w = request("", token); w.Code != 200 {
		t.Fatal("owner lost private preview", w.Code)
	}
	stranger, _, _ := sessions.Create(ctx)
	if w = request("", stranger); w.Code != 404 {
		t.Fatal("another user saw private preview", w.Code)
	}
	p.Private = false
	p.ActiveDigest = strings.Repeat("c", 64)
	put()
	if w = request("?v="+strings.Repeat("b", 64), ""); w.Code != 404 {
		t.Fatal("old version leaked", w.Code)
	}
	p.Status = "deleted"
	put()
	if w = request("", token); w.Code != 404 {
		t.Fatal("deleted image leaked", w.Code)
	}
}
func TestExploreJSONNeverIncludesOwnerOrPrivateRecords(t *testing.T) {
	ctx := context.Background()
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	previews, _ := preview.New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) { return nil, nil })
	id := strings.Repeat("a", 32)
	p := project.Project{ID: id, Slug: "sample-abcdef", Owner: project.Owner{Kind: "firebase", ID: "secret-owner"}, ActiveDigest: strings.Repeat("b", 64), Status: "active", CreatedAt: time.Now()}
	if err := store.Run(ctx, func(tx *metadata.Tx) error {
		if err := tx.Set("projects/"+id, p); err != nil {
			return err
		}
		return tx.Set("preview_recent/public", preview.Recent{IDs: []string{id}})
	}); err != nil {
		t.Fatal(err)
	}
	handler, _ := NewWithDependencies(Config{PublicOrigin: "https://drop.test", Limits: content.DefaultLimits()}, Dependencies{Previews: previews})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/api/explore", nil))
	if w.Code != 200 || strings.Contains(w.Body.String(), "secret-owner") {
		t.Fatal("unsafe explore response", w.Body.String())
	}
	var data struct{ Projects []preview.Card }
	if err := json.Unmarshal(w.Body.Bytes(), &data); err != nil || len(data.Projects) != 1 {
		t.Fatal("gallery response", err)
	}
}

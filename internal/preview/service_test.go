package preview

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/jpeg"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func previewProject(id string) project.Project {
	return project.Project{ID: strings.Repeat(id, 32), Slug: "sample-abcdef", ActiveDigest: strings.Repeat("a", 64), Owner: project.Owner{Kind: "firebase", ID: "secret-user-id"}, CreatedAt: time.Now().Add(-time.Hour), UpdatedAt: time.Now(), Status: "active", PolicyRevision: 1, Revision: 1}
}
func thumbnailBytes(t *testing.T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	if err := jpeg.Encode(&buffer, image.NewRGBA(image.Rect(0, 0, 320, 180)), nil); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
func putProject(t *testing.T, store *metadata.Store, p project.Project) {
	t.Helper()
	if err := store.Run(context.Background(), func(tx *metadata.Tx) error { return tx.Set("projects/"+p.ID, p) }); err != nil {
		t.Fatal(err)
	}
}
func TestSharedLeaseAndReplicatedImageDoesNotRenderOnEveryPrimary(t *testing.T) {
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	p := previewProject("a")
	putProject(t, store, p)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	raw := thumbnailBytes(t)
	render := func(context.Context, project.Project) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return raw, nil
	}
	a, err := New(store, t.TempDir(), render)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(store, t.TempDir(), render)
	if err != nil {
		t.Fatal(err)
	}
	go func() { a.process(context.Background(), p.ID); close(done) }()
	<-started
	b.process(context.Background(), p.ID)
	if calls.Load() != 1 {
		t.Fatal("another primary rendered during lease")
	}
	close(release)
	<-done
	b.process(context.Background(), p.ID)
	if calls.Load() != 1 {
		t.Fatal("another primary rendered instead of awaiting replication")
	}
	if _, err := os.Stat(b.ImagePath(p)); !os.IsNotExist(err) {
		t.Fatal("second primary fabricated an image")
	}
	data, err := os.ReadFile(a.ImagePath(p))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b.ImagePath(p), data, 0600); err != nil {
		t.Fatal(err)
	}
	b.process(context.Background(), p.ID)
	if calls.Load() != 1 {
		t.Fatal("replicated image was rendered again")
	}
}
func TestExploreCurrentPrivacyDeletionExpiryAndRecentOrder(t *testing.T) {
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	service, _ := New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) { return nil, errors.New("unused") })
	a, b, c := previewProject("a"), previewProject("b"), previewProject("c")
	a.UpdatedAt = time.Now().Add(-time.Hour)
	b.UpdatedAt = time.Now()
	c.Private = true
	for _, p := range []project.Project{a, b, c} {
		putProject(t, store, p)
		if err := service.remember(context.Background(), p.ID); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := service.Explore(context.Background())
	if err != nil || len(rows) != 2 || rows[0].ID != b.ID {
		t.Fatal("incorrect public ordering", rows, err)
	}
	b.Private = true
	putProject(t, store, b)
	rows, err = service.Explore(context.Background())
	if err != nil || len(rows) != 1 || rows[0].ID != a.ID {
		t.Fatal("privacy change leaked into gallery", rows, err)
	}
	expiry := time.Now().Add(-time.Second)
	a.ExpiresAt = &expiry
	putProject(t, store, a)
	rows, err = service.Explore(context.Background())
	if err != nil || len(rows) != 0 {
		t.Fatal("expired site leaked", rows, err)
	}
	a.ExpiresAt = nil
	a.Status = "deleted"
	putProject(t, store, a)
	rows, err = service.Explore(context.Background())
	if err != nil || len(rows) != 0 {
		t.Fatal("deleted site leaked", rows, err)
	}
}
func TestUpdateDuringRenderCannotCommitStalePreview(t *testing.T) {
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	p := previewProject("a")
	putProject(t, store, p)
	raw := thumbnailBytes(t)
	service, _ := New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) {
		p.ActiveDigest = strings.Repeat("b", 64)
		p.UpdatedAt = time.Now()
		putProject(t, store, p)
		return raw, nil
	})
	service.process(context.Background(), p.ID)
	err := store.Run(context.Background(), func(tx *metadata.Tx) error { var state State; return tx.Get("previews/"+p.ID, &state) })
	if !errors.Is(err, metadata.ErrNotFound) {
		t.Fatal("stale worker committed readiness", err)
	}
	if _, err := os.Stat(service.ImagePath(p)); !os.IsNotExist(err) {
		t.Fatal("stale screenshot overwrote current version")
	}
}
func TestRecentIndexBoundAndBackfillDoesNotDisplaceNewerSites(t *testing.T) {
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	service, _ := New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) { return nil, nil })
	now := time.Now()
	for i := 0; i < 105; i++ {
		p := previewProject("a")
		p.ID = fmtID(i)
		p.UpdatedAt = now.Add(-time.Duration(i) * time.Minute)
		putProject(t, store, p)
		if err := service.remember(context.Background(), p.ID); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := service.Explore(context.Background())
	if err != nil || len(rows) != 24 || rows[0].ID != fmtID(0) || rows[23].ID != fmtID(23) {
		t.Fatal("backfill order", rows, err)
	}
}
func fmtID(i int) string {
	const digits = "0123456789abcdef"
	id := []byte(strings.Repeat("0", 32))
	id[30] = digits[i/16]
	id[31] = digits[i%16]
	return string(id)
}

func TestOnlyClaimedPublicProjectsAppearAndClaimReusesThumbnail(t *testing.T) {
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	p := previewProject("a")
	p.Owner = project.Owner{Kind: "anonymous", ID: "browser"}
	expires := time.Now().Add(7 * 24 * time.Hour)
	p.ExpiresAt = &expires
	putProject(t, store, p)
	raw := thumbnailBytes(t)
	calls := 0
	service, _ := New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) { calls++; return raw, nil })
	service.process(context.Background(), p.ID)
	rows, err := service.Explore(context.Background())
	if err != nil || len(rows) != 0 {
		t.Fatal("unclaimed project was listed", rows, err)
	}
	p.Owner = project.Owner{Kind: "firebase", ID: "account"}
	p.ExpiresAt = nil
	p.PolicyRevision++
	putProject(t, store, p)
	service.process(context.Background(), p.ID)
	rows, err = service.Explore(context.Background())
	if err != nil || len(rows) != 1 || !rows[0].Claimed || calls != 1 {
		t.Fatal("claim did not reuse preview", rows, err, calls)
	}
	p.Private = true
	p.PolicyRevision++
	putProject(t, store, p)
	service.process(context.Background(), p.ID)
	rows, err = service.Explore(context.Background())
	if err != nil || len(rows) != 0 || calls != 1 {
		t.Fatal("claimed private project listed or rerendered", rows, err, calls)
	}
	p.Private = false
	p.PolicyRevision++
	putProject(t, store, p)
	service.process(context.Background(), p.ID)
	rows, err = service.Explore(context.Background())
	if err != nil || len(rows) != 1 || calls != 1 {
		t.Fatal("make public did not restore gallery", rows, err, calls)
	}
}

func TestClaimUpdatesGalleryEvenWhenThumbnailFailed(t *testing.T) {
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	p := previewProject("a")
	p.Owner = project.Owner{Kind: "anonymous", ID: "browser"}
	expires := time.Now().Add(time.Hour)
	p.ExpiresAt = &expires
	putProject(t, store, p)
	calls := 0
	service, _ := New(store, t.TempDir(), func(context.Context, project.Project) ([]byte, error) {
		calls++
		return nil, errors.New("renderer unavailable")
	})
	service.process(context.Background(), p.ID)
	p.Owner = project.Owner{Kind: "firebase", ID: "account"}
	p.ExpiresAt = nil
	putProject(t, store, p)
	service.process(context.Background(), p.ID)
	rows, err := service.Explore(context.Background())
	if err != nil || len(rows) != 1 || calls != 1 {
		t.Fatal("claim waited for screenshot retry", rows, err, calls)
	}
}

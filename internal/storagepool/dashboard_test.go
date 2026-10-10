package storagepool

import (
	"context"
	"errors"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestRemovalRacesUploadAllocation(t *testing.T) {
	for i := 0; i < 20; i++ {
		ctx := context.Background()
		store := &metadata.Store{Backend: &testmetadata.Backend{}}
		p := &Pool{store: store, apps: []*appRuntime{{config: App{AppName: "storagea"}}}}
		sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
		token, view, err := sessions.Create(ctx)
		if err != nil {
			t.Fatal(err)
		}
		actor, err := project.ActorFrom(token, view)
		if err != nil {
			t.Fatal(err)
		}
		repo := &project.RaftRepository{Store: store, StorageOffers: func() []project.StorageOffer {
			return []project.StorageOffer{{App: "storagea", LimitBytes: 1 << 30, AvailableBytes: 1 << 30, BlockBytes: 4096}}
		}}
		var removeErr, uploadErr error
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			removeErr = store.Run(ctx, func(tx *metadata.Tx) error { return p.ChangeApp(tx, "storagea", "remove") })
		}()
		go func() {
			defer wg.Done()
			<-start
			_, uploadErr = repo.Reserve(ctx, actor, project.Reservation{Key: "racing_upload", Name: "racing", Digest: strings.Repeat("a", 64), Bytes: 1, Files: 1})
		}()
		close(start)
		wg.Wait()
		if removeErr == nil {
			if !errors.Is(uploadErr, project.ErrStorage) {
				t.Fatal("removed app received allocation", uploadErr)
			}
		} else if uploadErr == nil {
			if !errors.Is(removeErr, ErrAppInUse) {
				t.Fatal("populated app removed", removeErr)
			}
		} else {
			t.Fatal("unexpected race result", removeErr, uploadErr)
		}
	}
}

func TestRemovalRetainedAccountingAndRestore(t *testing.T) {
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	p := &Pool{store: store, apps: []*appRuntime{{config: App{AppName: "storagea"}}}}
	ctx := context.Background()
	change := func(action string) error {
		return store.Run(ctx, func(tx *metadata.Tx) error { return p.ChangeApp(tx, "storagea", action) })
	}
	if err := change("remove"); err != nil {
		t.Fatal(err)
	}
	// A primary running the prior image decodes only the older exported fields.
	// Its allocation check must still see a negative block size and fail closed.
	var legacy struct{ BlockBytes int64 }
	if err := store.Run(ctx, func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &legacy) }); err != nil || legacy.BlockBytes != -1 {
		t.Fatal("legacy placement remains enabled", legacy, err)
	}
	c, err := p.controls(ctx)
	if err != nil || !c["storagea"].Removed {
		t.Fatal(c, err)
	}
	if err := change("restore"); err != nil {
		t.Fatal(err)
	}
	legacy.BlockBytes = 0
	if err := store.Run(ctx, func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &legacy) }); err != nil || legacy.BlockBytes != 0 {
		t.Fatal("allocation block size not restored", legacy, err)
	}
	if err := store.Run(ctx, func(tx *metadata.Tx) error {
		return tx.Set("storage_allocations/storagea", project.StorageAllocation{Bytes: 1, Inodes: 1, BlockBytes: 4096})
	}); err != nil {
		t.Fatal(err)
	}
	if err := change("remove"); !errors.Is(err, ErrAppInUse) {
		t.Fatal("retained data disconnected", err)
	}
	if err := change("drain"); err != nil {
		t.Fatal(err)
	}
	c, err = p.controls(ctx)
	if err != nil || !c["storagea"].Drain || c["storagea"].Removed {
		t.Fatal(c, err)
	}
	if err := change("drain"); err != nil {
		t.Fatal(err)
	}
	if err := change("restore"); err != nil {
		t.Fatal(err)
	}
	if err := store.Run(ctx, func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &legacy) }); err != nil || legacy.BlockBytes != 4096 {
		t.Fatal("drain lost accounting block size", legacy, err)
	}
	if err := store.Run(ctx, func(tx *metadata.Tx) error { return p.ChangeApp(tx, "unknown", "remove") }); !errors.Is(err, ErrUnknownApp) {
		t.Fatal(err)
	}
}

func TestProtectedStatusRetainsLocalMetricsWhenMetadataUnavailable(t *testing.T) {
	_, _, p := fixture(t, t.TempDir(), nil)
	backend := &testmetadata.Backend{Failure: errors.New("leader unavailable")}
	store := &metadata.Store{Backend: backend}
	p.BindMetadata(store)
	r := httptest.NewRequest("GET", "/api/storage/apps", nil)
	r.Header.Set("Authorization", "Bearer "+testKey)
	rec := httptest.NewRecorder()
	p.AdminHandlerWithOperations(store).ServeHTTP(rec, r)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"partial":true`) || !strings.Contains(rec.Body.String(), `"serving":`) || !strings.Contains(rec.Body.String(), `"metadataAvailable":false`) {
		t.Fatal(rec.Code, rec.Body.String())
	}
	denied := httptest.NewRecorder()
	p.AdminHandlerWithOperations(store).ServeHTTP(denied, httptest.NewRequest("GET", "/api/storage/apps", nil))
	if denied.Code != 404 {
		t.Fatal("status metrics public", denied.Code)
	}
}

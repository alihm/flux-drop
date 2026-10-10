package storagepool

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

func testDevice(letter string) string {
	return strings.Repeat(strings.Repeat(letter, 7)+"-", 7) + strings.Repeat(letter, 7)
}

type fakeSyncthing struct {
	mu                    sync.Mutex
	root, device, version string
	members               []string
	paused, failIgnore    bool
	ignores               []string
	events                []string
}

func bindFakeSyncthing(t *testing.T, s *Secondary, device string, members []string) *fakeSyncthing {
	t.Helper()
	f := &fakeSyncthing{root: s.root, device: device, members: members, version: "v1.30.0"}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Header.Get("X-API-Key") != testKey {
			w.WriteHeader(403)
			return
		}
		switch r.URL.Path {
		case "/rest/system/status":
			jsonReply(w, 200, map[string]string{"myID": f.device})
		case "/rest/system/version":
			jsonReply(w, 200, map[string]string{"version": f.version})
		case "/rest/config":
			devices := []map[string]string{}
			for _, member := range f.members {
				devices = append(devices, map[string]string{"deviceID": member})
			}
			jsonReply(w, 200, map[string]any{"folders": []any{map[string]any{"id": "dropdata", "path": f.root, "type": "sendreceive", "paused": f.paused, "devices": devices}}})
		case "/rest/config/folders/dropdata":
			var v struct{ Paused bool }
			if json.NewDecoder(r.Body).Decode(&v) != nil {
				w.WriteHeader(400)
				return
			}
			f.paused = v.Paused
			if v.Paused {
				f.events = append(f.events, "paused")
			} else {
				f.events = append(f.events, "resumed")
			}
			w.WriteHeader(200)
		case "/rest/db/ignores":
			if r.URL.Query().Get("folder") != "dropdata" {
				w.WriteHeader(400)
				return
			}
			if r.Method == "POST" {
				if !f.paused {
					t.Error("ignore changed before pullers stopped")
				}
				if f.failIgnore {
					w.WriteHeader(500)
					return
				}
				var v struct{ Ignore []string }
				if json.NewDecoder(r.Body).Decode(&v) != nil {
					w.WriteHeader(400)
					return
				}
				f.ignores = v.Ignore
				if err := os.WriteFile(filepath.Join(f.root, ".stignore"), []byte(strings.Join(f.ignores, "\n")+"\n"), 0600); err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}
				f.events = append(f.events, "ignored")
			}
			jsonReply(w, 200, map[string]any{"ignore": f.ignores})
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(api.Close)
	s.config.SyncthingURL, s.config.SyncthingAPIKey, s.config.SyncthingFolder = api.URL, testKey, "dropdata"
	return f
}

func publishTwoGenerations(t *testing.T, pool *Pool) (*project.RaftRepository, project.Actor, project.Project, project.StorageVersion) {
	t.Helper()
	pool.config.Reclamation = true
	repo, actor := accountingRepository(t, pool)
	ctx := context.Background()
	publisher := &project.Publisher{Repository: repo, DataRoot: t.TempDir(), Installer: pool}
	first, err := publisher.Publish(ctx, actor, project.Reservation{Key: "gc_first_version"}, stagedHTML(t, "<h1>old</h1>"))
	if err != nil {
		t.Fatal(err)
	}
	if first.StorageGeneration == "" {
		t.Fatal("cleanup writer did not allocate generation")
	}
	second, err := publisher.Publish(ctx, actor, project.Reservation{Key: "gc_second_version", ProjectID: first.ID, ExpectedRevision: first.Revision}, stagedHTML(t, "<h1>current</h1>"))
	if err != nil {
		t.Fatal(err)
	}
	// These tests exercise cleanup of an explicitly removed historical revision.
	second, err = repo.RemoveRevision(ctx, actor, first.ID, first.StorageGeneration, second.Revision)
	if err != nil {
		t.Fatal(err)
	}
	id := project.StorageVersionID(first.ID, first.ActiveDigest, first.StorageGeneration)
	var old project.StorageVersion
	if err = pool.store.Run(ctx, func(tx *metadata.Tx) error {
		if err := tx.Get("storage_versions/"+id, &old); err != nil {
			return err
		}
		old.CreatedAt = time.Now().Add(-2 * time.Hour)
		return tx.Set("storage_versions/"+id, old)
	}); err != nil {
		t.Fatal(err)
	}
	return repo, actor, second, old
}

func TestRestoredGenerationServesOriginalBytesAndProjectDeletionReclaimsAll(t *testing.T) {
	s, _, pool := fixture(t, t.TempDir(), nil)
	bindFakeSyncthing(t, s, testDevice("A"), []string{testDevice("A")})
	pool.config.Reclamation = true
	repo, actor := accountingRepository(t, pool)
	ctx := context.Background()
	publisher := &project.Publisher{Repository: repo, DataRoot: t.TempDir(), Installer: pool}
	first, err := publisher.Publish(ctx, actor, project.Reservation{Key: "restore_first"}, stagedHTML(t, "<h1>First generation</h1>"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := publisher.Publish(ctx, actor, project.Reservation{Key: "restore_second", ProjectID: first.ID, ExpectedRevision: first.Revision}, stagedHTML(t, "<h1>Second generation</h1>"))
	if err != nil {
		t.Fatal(err)
	}
	active, err := repo.SelectRevision(ctx, actor, first.ID, first.StorageGeneration, second.Revision)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	pool.ServeProject(w, httptest.NewRequest("GET", "/"+active.Slug+"/", nil), active, "index.html")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "<h1>First generation</h1>") {
		t.Fatal(w.Code, w.Body.String())
	}
	if err = repo.Tombstone(ctx, actor, active.ID, active.Revision); err != nil {
		t.Fatal(err)
	}
	for _, p := range []project.Project{first, second} {
		id := project.StorageVersionID(p.ID, p.ActiveDigest, p.StorageGeneration)
		if err = pool.store.Run(ctx, func(tx *metadata.Tx) error {
			var v project.StorageVersion
			if err := tx.Get("storage_versions/"+id, &v); err != nil {
				return err
			}
			v.CreatedAt = time.Now().Add(-2 * time.Hour)
			return tx.Set("storage_versions/"+id, v)
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = pool.ReclaimObsolete(ctx, "storagea", ""); err != nil {
		t.Fatal(err)
	}
	if a := allocation(t, pool); a.Bytes != 0 || a.LiveBytes != 0 || a.Versions != 0 {
		t.Fatal("deleted revision storage not refunded", a)
	}
	if _, err = os.Stat(filepath.Join(s.root, "projects", first.ID)); !os.IsNotExist(err) {
		t.Fatal("deleted project files remain", err)
	}
}

func TestReclamationRequiresAllReplicationMembersAndFencesRetries(t *testing.T) {
	s, _, pool := fixture(t, t.TempDir(), nil)
	members := []string{testDevice("A"), testDevice("B")}
	f := bindFakeSyncthing(t, s, members[0], members)
	_, _, active, old := publishTwoGenerations(t, pool)
	before := allocation(t, pool)
	ctx := context.Background()
	if _, err := pool.ReclaimObsolete(ctx, "storagea", ""); err == nil {
		t.Fatal("offline device ignored")
	}
	if got := allocation(t, pool); got != before {
		t.Fatal("refunded offline member", got)
	}
	f.mu.Lock()
	f.members = members[:1]
	f.mu.Unlock()
	if _, err := pool.ReclaimObsolete(ctx, "storagea", ""); err != nil {
		t.Fatal(err)
	}
	a := allocation(t, pool)
	if a.Bytes != before.Bytes-old.Bytes || a.ContentBytes != int64(len("<h1>current</h1>")) || a.LiveBytes != a.ContentBytes || a.Versions != 1 {
		t.Fatal(a)
	}
	oldPath := s.operationVersion(Operation{ProjectID: old.ProjectID, Digest: old.Digest, Generation: old.Generation})
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatal("old generation still present", err)
	}
	if _, err := content.VerifyVersion(s.operationVersion(Operation{ProjectID: active.ID, Digest: active.ActiveDigest, Generation: active.StorageGeneration}), active.ActiveDigest); err != nil {
		t.Fatal("active generation deleted", err)
	}
	f.mu.Lock()
	if strings.Join(f.events, ",") != "paused,ignored,resumed" {
		t.Fatal(f.events)
	}
	f.mu.Unlock()
	if err := s.notRetired(Operation{ProjectID: old.ProjectID, Digest: old.Digest, Generation: old.Generation}); !errors.Is(err, errConflict) {
		t.Fatal("late install not fenced", err)
	}
	if _, err := pool.ReclaimObsolete(ctx, "storagea", ""); err != nil || allocation(t, pool) != a {
		t.Fatal("GC retry refunded twice", err)
	}
}

func TestReclamationInterruptedIgnoreFailsClosedAndRetries(t *testing.T) {
	s, _, pool := fixture(t, t.TempDir(), nil)
	f := bindFakeSyncthing(t, s, testDevice("A"), []string{testDevice("A")})
	_, _, _, old := publishTwoGenerations(t, pool)
	before := allocation(t, pool)
	f.mu.Lock()
	f.failIgnore = true
	f.mu.Unlock()
	if _, err := pool.ReclaimObsolete(context.Background(), "storagea", ""); err == nil {
		t.Fatal("failed ignore acknowledged")
	}
	f.mu.Lock()
	if !f.paused {
		t.Fatal("replication resumed without a fence")
	}
	f.failIgnore = false
	f.mu.Unlock()
	if got := allocation(t, pool); got != before {
		t.Fatal("failed cleanup refunded", got)
	}
	if err := s.notRetired(Operation{ProjectID: old.ProjectID, Digest: old.Digest, Generation: old.Generation}); err == nil {
		t.Fatal("late install was not fenced before API failure")
	}
	if _, err := os.Stat(s.operationVersion(Operation{ProjectID: old.ProjectID, Digest: old.Digest, Generation: old.Generation})); err != nil {
		t.Fatal("deleted before replication fence", err)
	}
	if _, err := pool.ReclaimObsolete(context.Background(), "storagea", ""); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.paused {
		t.Fatal("retry did not restore original running state")
	}
}

func TestReclamationMembershipEpochCannotShrinkAfterRetirement(t *testing.T) {
	s, _, pool := fixture(t, t.TempDir(), nil)
	members := []string{testDevice("A"), testDevice("B")}
	f := bindFakeSyncthing(t, s, members[0], members)
	repo, _, _, old := publishTwoGenerations(t, pool)
	snapshot, _, err := s.replication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.RetireStorageVersion(context.Background(), old.ID, snapshot.Epoch, members); err != nil {
		t.Fatal(err)
	}
	before := allocation(t, pool)
	f.mu.Lock()
	f.members = members[:1]
	f.mu.Unlock()
	if _, err = pool.ReclaimObsolete(context.Background(), "storagea", ""); err != nil {
		t.Fatal(err)
	}
	if allocation(t, pool) != before {
		t.Fatal("membership shrink refunded an offline replica")
	}
}

func TestReclamationWithoutAdapterDoesNothing(t *testing.T) {
	_, _, pool := fixture(t, t.TempDir(), nil)
	_, _, _, _ = publishTwoGenerations(t, pool)
	before := allocation(t, pool)
	if _, err := pool.ReclaimObsolete(context.Background(), "storagea", ""); err == nil {
		t.Fatal("cleanup without replication authority")
	}
	if allocation(t, pool) != before {
		t.Fatal("unverified cleanup reduced allocation")
	}
}

func TestUnsupportedSyncthingVersionCannotDeleteContent(t *testing.T) {
	s, _, pool := fixture(t, t.TempDir(), nil)
	f := bindFakeSyncthing(t, s, testDevice("A"), []string{testDevice("A")})
	_, _, _, old := publishTwoGenerations(t, pool)
	before := allocation(t, pool)
	f.mu.Lock()
	f.version = "v2.0.0"
	f.mu.Unlock()
	if _, err := pool.ReclaimObsolete(context.Background(), "storagea", ""); err == nil {
		t.Fatal("unaudited pause semantics accepted")
	}
	if allocation(t, pool) != before {
		t.Fatal("unsupported replication service refunded")
	}
	if err := s.notRetired(Operation{ProjectID: old.ProjectID, Digest: old.Digest, Generation: old.Generation}); err != nil {
		t.Fatal("unsupported service retired data", err)
	}
}

func TestCleanupWaitsForOldReadersAndAllowsCurrentGeneration(t *testing.T) {
	s, _, _ := fixture(t, t.TempDir(), nil)
	old := project.StorageVersion{ProjectID: strings.Repeat("a", 32), Digest: strings.Repeat("b", 64), Generation: strings.Repeat("c", 64)}
	readOld := s.lockVersionRead(old.ProjectID, old.Digest, old.Generation)
	acquired, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() { defer close(done); unlock := s.lockVersionRemoval(old); close(acquired); <-release; unlock() }()
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer func() { finish(); <-done }()
	select {
	case <-acquired:
		readOld()
		t.Fatal("cleanup passed an existing file stream")
	case <-time.After(25 * time.Millisecond):
	}
	readOld()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("cleanup never acquired released version")
	}
	current := make(chan struct{})
	go func() {
		unlock := s.lockVersionRead(old.ProjectID, strings.Repeat("d", 64), strings.Repeat("e", 64))
		unlock()
		close(current)
	}()
	select {
	case <-current:
	case <-time.After(time.Second):
		t.Fatal("old-version cleanup blocked the current generation")
	}
	finish()
}

func TestRemoveVersionRejectsSymlinksAndPreservesOtherGenerations(t *testing.T) {
	root := t.TempDir()
	projectID := strings.Repeat("a", 32)
	generation := strings.Repeat("b", 64)
	relative, _ := content.GenerationPath(projectID, generation)
	if err := os.MkdirAll(filepath.Join(root, filepath.Dir(relative)), 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "keep.html")
	if err := os.WriteFile(sentinel, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, relative)); err != nil {
		t.Fatal(err)
	}
	if err := removeVersion(root, relative); err == nil {
		t.Fatal("symlink target accepted")
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatal(err)
	}
}

func TestSecondaryRetirementSurvivesRestart(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	s, err := NewSecondary(secondaryConfig(), root, state)
	if err != nil {
		t.Fatal(err)
	}
	f := bindFakeSyncthing(t, s, testDevice("A"), []string{testDevice("A")})
	snapshot, _, err := s.replication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	v := project.StorageVersion{App: "storagea", ProjectID: strings.Repeat("a", 32), Digest: strings.Repeat("b", 64), Generation: strings.Repeat("c", 64), State: "retiring", MembershipEpoch: snapshot.Epoch, Members: snapshot.Members}
	v.ID = project.StorageVersionID(v.ProjectID, v.Digest, v.Generation)
	raw, _ := json.Marshal(v)
	req := httptest.NewRequest("POST", apiPrefix+"reclaim", strings.NewReader(string(raw)))
	rec := httptest.NewRecorder()
	s.reclaim(rec, req)
	if rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSecondary(secondaryConfig(), root, state)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err = reopened.notRetired(Operation{ProjectID: v.ProjectID, Digest: v.Digest, Generation: v.Generation}); !errors.Is(err, errConflict) {
		t.Fatal("retirement expired on restart", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.paused {
		t.Fatal("replication not resumed")
	}
}

// Ensure discovery alone can never silently drop a required device.
func TestUnknownReplicationDeviceDoesNotMapToAnAddress(t *testing.T) {
	s, _, pool := fixture(t, t.TempDir(), nil)
	bindFakeSyncthing(t, s, testDevice("A"), []string{testDevice("A"), testDevice("B")})
	_, _, _, _ = publishTwoGenerations(t, pool)
	pool.apps[0].discovery.(*testDiscovery).set([]netip.AddrPort{}, true)
	if _, err := pool.ReclaimObsolete(context.Background(), "storagea", ""); err == nil {
		t.Fatal("empty discovery proved reclamation")
	}
}

type heldUpload struct {
	io.Reader
	once             sync.Once
	started, release chan struct{}
}

func (h *heldUpload) Read(p []byte) (int, error) {
	h.once.Do(func() { close(h.started); <-h.release })
	return h.Reader.Read(p)
}

func TestReclamationFencesUploadAlreadyReadingItsBody(t *testing.T) {
	s, _, pool := fixture(t, t.TempDir(), nil)
	bindFakeSyncthing(t, s, testDevice("A"), []string{testDevice("A")})
	_, _, _, old := publishTwoGenerations(t, pool)
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	file, err := writer.Create("index.html")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte("<h1>old</h1>")); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader := &heldUpload{Reader: bytes.NewReader(archive.Bytes()), started: make(chan struct{}), release: make(chan struct{})}
	req := httptest.NewRequest("PUT", apiPrefix+"operations/"+old.Generation+"/content", reader)
	req.SetPathValue("operationId", old.Generation)
	req.Header.Set("Content-Type", "application/zip")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); s.upload(rec, req) }()
	var released sync.Once
	release := func() { released.Do(func() { close(reader.release) }) }
	defer func() { release(); <-done }()
	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	if _, err = pool.ReclaimObsolete(context.Background(), "storagea", ""); err != nil {
		t.Fatal(err)
	}
	release()
	<-done
	if rec.Code != 409 {
		t.Fatal("late installer accepted", rec.Code, rec.Body.String())
	}
	if _, err = os.Stat(s.operationVersion(Operation{ProjectID: old.ProjectID, Digest: old.Digest, Generation: old.Generation})); !os.IsNotExist(err) {
		t.Fatal("late installer restored retired bytes", err)
	}
}

func TestDeletedProjectReclamationRemovesSharedNamespaceAndFencesAllWriters(t *testing.T) {
	s, _, pool := fixture(t, t.TempDir(), nil)
	bindFakeSyncthing(t, s, testDevice("A"), []string{testDevice("A")})
	repo, actor, active, old := publishTwoGenerations(t, pool)
	if err := repo.Tombstone(context.Background(), actor, active.ID, active.Revision); err != nil {
		t.Fatal(err)
	}
	// Both generations are old enough for this maintenance pass.
	if err := pool.store.Run(context.Background(), func(tx *metadata.Tx) error {
		key := "storage_versions/" + project.StorageVersionID(active.ID, active.ActiveDigest, active.StorageGeneration)
		var v project.StorageVersion
		if err := tx.Get(key, &v); err != nil {
			return err
		}
		v.CreatedAt = time.Now().Add(-2 * time.Hour)
		return tx.Set(key, v)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.ReclaimObsolete(context.Background(), "storagea", ""); err != nil {
		t.Fatal(err)
	}
	if a := allocation(t, pool); a.Bytes != 0 || a.Inodes != 0 || a.ContentBytes != 0 || a.Versions != 0 {
		t.Fatal("deleted namespace still charged", a)
	}
	if _, err := os.Stat(filepath.Join(s.root, "projects", active.ID)); !os.IsNotExist(err) {
		t.Fatal("unaccounted project marker remained", err)
	}
	if err := s.notRetired(Operation{ProjectID: old.ProjectID, Digest: strings.Repeat("c", 64), Generation: strings.Repeat("d", 64)}); !errors.Is(err, errConflict) {
		t.Fatal("project-wide writer fence missing", err)
	}
}

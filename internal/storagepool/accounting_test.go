package storagepool

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func accountingRepository(t *testing.T, p *Pool) (*project.RaftRepository, project.Actor) {
	t.Helper()
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	p.BindMetadata(store)
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
	token, view, err := sessions.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	actor, err := project.ActorFrom(token, view)
	if err != nil {
		t.Fatal(err)
	}
	return &project.RaftRepository{Store: store, StorageOffers: p.Offers}, actor
}

func allocation(t *testing.T, p *Pool) project.StorageAllocation {
	t.Helper()
	var a project.StorageAllocation
	if err := p.store.Run(context.Background(), func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &a) }); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestAllocationUpgradeAuditsLegacyOperationsAndManifests(t *testing.T) {
	_, _, pool := fixture(t, t.TempDir(), nil)
	repo, actor := accountingRepository(t, pool)
	ctx := context.Background()
	var current project.Project
	var usage int64
	for i, html := range []string{"<h1>first</h1>", "<h1>second</h1>"} {
		staged := stagedHTML(t, html)
		r := project.Reservation{Key: strings.Repeat(string(rune('a'+i)), 16), Digest: staged.Digest, Bytes: int64(len(html)), Files: len(staged.Manifest.Files)}
		if i > 0 {
			r.ProjectID, r.ExpectedRevision = current.ID, current.Revision
		}
		prepared, err := repo.Reserve(ctx, actor, r)
		if err != nil {
			t.Fatal(err)
		}
		if err = pool.Install(ctx, prepared, staged); err != nil {
			t.Fatal(err)
		}
		current, err = repo.Activate(ctx, actor, prepared.Operation.ID)
		if err != nil {
			t.Fatal(err)
		}
		footprint, err := staged.Manifest.Footprint(pool.Offers()[0].BlockBytes)
		if err != nil {
			t.Fatal(err)
		}
		usage += footprint.AllocatedBytes
	}
	before := allocation(t, pool)
	if before.Bytes < 16<<20 {
		t.Fatal("test did not create legacy allowance", before)
	}
	if err := pool.ReconcileAllocation(ctx, "storagea"); err != nil {
		t.Fatal(err)
	}
	a := allocation(t, pool)
	if a.Schema != 2 || a.BlockBytes != -2 || a.Bytes != usage || a.LiveBytes != int64(len("<h1>second</h1>")) || a.ContentBytes != int64(len("<h1>first</h1>")+len("<h1>second</h1>")) || a.LegacyBytes != 0 || a.Versions != 2 {
		t.Fatal(a)
	}
	if err := pool.ReconcileAllocation(ctx, "storagea"); err != nil || allocation(t, pool) != a {
		t.Fatal("upgrade not idempotent", err)
	}
	// An old allocator remains fenced after the accounting upgrade.
	if _, err := repo.Reserve(ctx, actor, project.Reservation{Key: "legacy_new_upload", Digest: strings.Repeat("c", 64), Bytes: 1, Files: 1}); !errors.Is(err, project.ErrStorage) {
		t.Fatal("old schema writer bypassed fence", err)
	}
}

func TestAllocationUpgradeKeepsUnverifiedAndPendingBytes(t *testing.T) {
	_, _, pool := fixture(t, t.TempDir(), nil)
	repo, actor := accountingRepository(t, pool)
	ctx := context.Background()
	r := project.Reservation{Key: "incomplete_upload", Digest: strings.Repeat("a", 64), Bytes: 100, Files: 1}
	prepared, err := repo.Reserve(ctx, actor, r)
	if err != nil {
		t.Fatal(err)
	}
	before := allocation(t, pool)
	if err = pool.ReconcileAllocation(ctx, "storagea"); !errors.Is(err, project.ErrConflict) {
		t.Fatal("pending operation not held", err)
	}
	if err = repo.Abort(ctx, actor, prepared.Operation.ID); err != nil {
		t.Fatal(err)
	}
	if err = pool.ReconcileAllocation(ctx, "storagea"); err != nil {
		t.Fatal(err)
	}
	a := allocation(t, pool)
	if a.Schema != 2 || a.Bytes != before.Bytes || a.LegacyBytes != before.Bytes || a.LiveBytes != 0 {
		t.Fatal("missing content mistaken for deletion proof", a)
	}
}

func TestAllocationUpgradeRejectsIncompleteHistory(t *testing.T) {
	_, _, pool := fixture(t, t.TempDir(), nil)
	repo, actor := accountingRepository(t, pool)
	ctx := context.Background()
	prepared, err := repo.Reserve(ctx, actor, project.Reservation{Key: "history_mismatch", Digest: strings.Repeat("a", 64), Bytes: 100, Files: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.Abort(ctx, actor, prepared.Operation.ID); err != nil {
		t.Fatal(err)
	}
	before := allocation(t, pool)
	if err = pool.store.Run(ctx, func(tx *metadata.Tx) error { a := before; a.Bytes++; return tx.Set("storage_allocations/storagea", a) }); err != nil {
		t.Fatal(err)
	}
	if err = pool.ReconcileAllocation(ctx, "storagea"); !errors.Is(err, project.ErrStorage) {
		t.Fatal(err)
	}
	a := allocation(t, pool)
	if a.Bytes != before.Bytes+1 || a.Schema != 1 || a.BlockBytes != -2 {
		t.Fatal("unknown usage reduced", a)
	}
}

func TestMeasuredDrainRestorePreservesOldWriterFence(t *testing.T) {
	_, _, pool := fixture(t, t.TempDir(), nil)
	_, _ = accountingRepository(t, pool)
	ctx := context.Background()
	want := project.StorageAllocation{Schema: 2, BlockBytes: -2, AccountingBlockBytes: 4096, Bytes: 32768, ContentBytes: 100, LiveBytes: 100, Inodes: 8}
	if err := pool.store.Run(ctx, func(tx *metadata.Tx) error { return tx.Set("storage_allocations/storagea", want) }); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"drain", "drain", "restore"} {
		if err := pool.store.Run(ctx, func(tx *metadata.Tx) error { return pool.ChangeApp(tx, "storagea", action) }); err != nil {
			t.Fatal(err)
		}
	}
	if got := allocation(t, pool); got != want {
		t.Fatal("restore re-enabled old allocation schema", got, want)
	}
}

func TestMeasuredPublisherUsesManifestAccountingWithLegacySecondary(t *testing.T) {
	s, _, pool := fixture(t, t.TempDir(), nil)
	repo, actor := accountingRepository(t, pool)
	pool.apps[0].mu.Lock()
	for addr, n := range pool.apps[0].health {
		n.Capacity.Protocol = 0
		pool.apps[0].health[addr] = n
	}
	pool.apps[0].mu.Unlock()
	staged := stagedHTML(t, "<h1>measured</h1>")
	publisher := &project.Publisher{Repository: repo, DataRoot: t.TempDir(), Installer: pool}
	pr, err := publisher.Publish(context.Background(), actor, project.Reservation{Key: "manifest_upload"}, staged)
	if err != nil {
		t.Fatal(err)
	}
	if pr.StorageGeneration != "" {
		t.Fatal("generation sent to legacy secondary", pr)
	}
	if _, err = content.VerifyVersion(s.version(pr.ID, pr.ActiveDigest), pr.ActiveDigest); err != nil {
		t.Fatal(err)
	}
	a := allocation(t, pool)
	if a.Schema != 2 || a.Bytes > 1<<20 || a.LiveBytes != int64(len("<h1>measured</h1>")) {
		t.Fatal(a)
	}
}

func TestAllocationUpgradeWaitsForHealthBeforeAuditing(t *testing.T) {
	_, _, pool := fixture(t, t.TempDir(), nil)
	repo, actor := accountingRepository(t, pool)
	ctx := context.Background()
	prepared, err := repo.Reserve(ctx, actor, project.Reservation{Key: "health_pending", Digest: strings.Repeat("a", 64), Bytes: 100, Files: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.Abort(ctx, actor, prepared.Operation.ID); err != nil {
		t.Fatal(err)
	}
	before := allocation(t, pool)
	discovery := pool.apps[0].discovery.(*testDiscovery)
	peers, _ := discovery.Snapshot()
	discovery.set(nil, false)
	if err = pool.ReconcileAllocation(ctx, "storagea"); err == nil {
		t.Fatal("startup without health permanently classified all manifests as missing")
	}
	if allocation(t, pool) != before {
		t.Fatal("unreachable app was changed")
	}
	discovery.set(peers, true)
	if err = pool.ReconcileAllocation(ctx, "storagea"); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyAuditRetriesManifestAfterReplicationReturns(t *testing.T) {
	s, _, pool := fixture(t, t.TempDir(), nil)
	repo, actor := accountingRepository(t, pool)
	ctx := context.Background()
	staged := stagedHTML(t, "<h1>delayed replication</h1>")
	prepared, err := repo.Reserve(ctx, actor, project.Reservation{Key: "delayed_manifest", Digest: staged.Digest, Bytes: staged.Manifest.Files[0].Size, Files: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.Abort(ctx, actor, prepared.Operation.ID); err != nil {
		t.Fatal(err)
	}
	before := allocation(t, pool)
	if err = pool.ReconcileAllocation(ctx, "storagea"); err != nil {
		t.Fatal(err)
	}
	if a := allocation(t, pool); a.LegacyBytes != before.Bytes {
		t.Fatal(a)
	}
	if _, err = pool.ReconcileUnverified(ctx, "storagea", ""); err != nil {
		t.Fatal(err)
	}
	if allocation(t, pool).Bytes != before.Bytes {
		t.Fatal("missing manifest freed space")
	}
	// Simulate a previously offline Syncthing member finally supplying the tree.
	if err = staged.Install(s.root, prepared.Project.ID, prepared.Project.Slug); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.ReconcileUnverified(ctx, "storagea", ""); err != nil {
		t.Fatal(err)
	}
	usage, _ := staged.Manifest.Footprint(before.BlockBytes)
	a := allocation(t, pool)
	if a.Bytes != usage.AllocatedBytes || a.Inodes != usage.Inodes || a.LegacyBytes != 0 || a.LegacyInodes != 0 {
		t.Fatal("returned manifest not reconciled", a, usage)
	}
	if _, err = pool.ReconcileUnverified(ctx, "storagea", ""); err != nil || allocation(t, pool) != a {
		t.Fatal("retry reduced twice", err)
	}
}

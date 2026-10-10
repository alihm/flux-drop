package project

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/session"
)

func measuredReservation(key, digest string, bytes int64) Reservation {
	m := &content.Manifest{Schema: 1, Files: []content.File{{Path: "index.html", Size: bytes, SHA256: strings.Repeat(digest, 64)}}}
	return Reservation{Key: key, Digest: strings.Repeat(digest, 64), Bytes: bytes, Files: 1, StorageManifest: m}
}

func measuredFixture(t *testing.T) (*RaftRepository, Actor, func() StorageAllocation) {
	t.Helper()
	repo, actor, _ := storageRepository(t, 1<<30)
	repo.StorageOffers = func() []StorageOffer {
		return []StorageOffer{{App: "storagea", LimitBytes: 1 << 30, AvailableBytes: 1 << 30, BlockBytes: 4096, Generations: true}}
	}
	read := func() StorageAllocation {
		var a StorageAllocation
		if err := repo.Store.Run(context.Background(), func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &a) }); err != nil {
			t.Fatal(err)
		}
		return a
	}
	return repo, actor, read
}

func TestMeasuredAccountingLifecycleAndReplay(t *testing.T) {
	ctx := context.Background()
	repo, actor, read := measuredFixture(t)
	r := measuredReservation("measured_first", "a", 100)
	first, err := repo.Reserve(ctx, actor, r)
	if err != nil {
		t.Fatal(err)
	}
	usage, _ := r.StorageManifest.Footprint(4096)
	before := read()
	if before.Bytes != usage.AllocatedBytes || before.ContentBytes != 100 || before.LiveBytes != 0 || before.Versions != 1 || before.BlockBytes != -2 || first.Operation.StorageGeneration != first.Operation.ID {
		t.Fatal(before, first)
	}
	if _, err = repo.Reserve(ctx, actor, r); err != nil || read() != before {
		t.Fatal("retry charged twice", err)
	}
	p, err := repo.Activate(ctx, actor, first.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if p.StorageGeneration != first.Operation.ID || read().LiveBytes != 100 {
		t.Fatal(p, read())
	}
	next := measuredReservation("measured_second", "b", 200)
	next.ProjectID, next.ExpectedRevision = p.ID, p.Revision
	second, err := repo.Reserve(ctx, actor, next)
	if err != nil {
		t.Fatal(err)
	}
	p, err = repo.Activate(ctx, actor, second.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	a := read()
	if a.ContentBytes != 300 || a.LiveBytes != 200 || a.Versions != 2 {
		t.Fatal(a)
	}
	if err = repo.Tombstone(ctx, actor, p.ID, p.Revision); err != nil {
		t.Fatal(err)
	}
	a = read()
	if a.LiveBytes != 0 || a.ContentBytes != 300 || a.Bytes != 2*usage.AllocatedBytes {
		t.Fatal("delete prematurely refunded bytes", a)
	}
}

func TestReclamationAllMembersRefundExactlyOnce(t *testing.T) {
	ctx := context.Background()
	repo, actor, read := measuredFixture(t)
	one := measuredReservation("first_version", "a", 100)
	first, err := repo.Reserve(ctx, actor, one)
	if err != nil {
		t.Fatal(err)
	}
	p, err := repo.Activate(ctx, actor, first.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	epoch := strings.Repeat("e", 64)
	members := []string{"device-a", "device-b"}
	if _, err = repo.RetireStorageVersion(ctx, first.Operation.StorageVersionKey, epoch, members); !errors.Is(err, ErrConflict) {
		t.Fatal("active version retired", err)
	}
	two := measuredReservation("second_version", "b", 200)
	two.ProjectID, two.ExpectedRevision = p.ID, p.Revision
	second, err := repo.Reserve(ctx, actor, two)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.RetireStorageVersion(ctx, first.Operation.StorageVersionKey, epoch, members); !errors.Is(err, ErrConflict) {
		t.Fatal("pending publication ignored", err)
	}
	p, err = repo.Activate(ctx, actor, second.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	before := read()
	v, err := repo.RetireStorageVersion(ctx, first.Operation.StorageVersionKey, epoch, members)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.RetireStorageVersion(ctx, v.ID, strings.Repeat("f", 64), members[:1]); !errors.Is(err, ErrConflict) {
		t.Fatal("offline member was dropped", err)
	}
	if err = repo.AcknowledgeStorageReclamation(ctx, v.ID, epoch, "unknown"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err = repo.AcknowledgeStorageReclamation(ctx, v.ID, epoch, members[0]); err != nil {
		t.Fatal(err)
	}
	if read() != before {
		t.Fatal("refunded without every replica")
	}
	if _, err = repo.Reserve(ctx, actor, one); !errors.Is(err, ErrConflict) {
		t.Fatal("retired operation replay succeeded", err)
	}
	if _, err = repo.Activate(ctx, actor, first.Operation.ID); !errors.Is(err, ErrConflict) {
		t.Fatal("retired activation replay succeeded", err)
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := repo.AcknowledgeStorageReclamation(ctx, v.ID, epoch, members[1]); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	a := read()
	if a.Bytes != before.Bytes-v.Bytes || a.ContentBytes != 200 || a.LiveBytes != 200 || a.Versions != 1 {
		t.Fatal(a)
	}
	owned, err := repo.GetOwned(ctx, actor, p.ID)
	if err != nil || owned.ChargedBytes != versionCharge(200) {
		t.Fatal(owned, err)
	}
	// Identical bytes can be restored under a different physical generation.
	restore := measuredReservation("restore_version", "a", 100)
	restore.ProjectID, restore.ExpectedRevision = p.ID, p.Revision
	third, err := repo.Reserve(ctx, actor, restore)
	if err != nil || third.Operation.StorageGeneration == first.Operation.StorageGeneration {
		t.Fatal(third, err)
	}
	p, err = repo.Activate(ctx, actor, third.Operation.ID)
	if err != nil || p.StorageGeneration != third.Operation.StorageGeneration {
		t.Fatal(p, err)
	}
}

func TestLegacyManifestDeduplicatesPhysicalBudget(t *testing.T) {
	ctx := context.Background()
	repo, actor, read := measuredFixture(t)
	repo.StorageOffers = func() []StorageOffer {
		return []StorageOffer{{App: "storagea", LimitBytes: 1 << 30, AvailableBytes: 1 << 30, BlockBytes: 4096}}
	}
	r := measuredReservation("legacy_first", "a", 100)
	first, err := repo.Reserve(ctx, actor, r)
	if err != nil {
		t.Fatal(err)
	}
	p, err := repo.Activate(ctx, actor, first.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	a := read()
	r.Key, r.ProjectID, r.ExpectedRevision = "legacy_second", p.ID, p.Revision
	second, err := repo.Reserve(ctx, actor, r)
	if err != nil {
		t.Fatal(err)
	}
	p, err = repo.Activate(ctx, actor, second.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := read(); got.Bytes != a.Bytes || got.Versions != 1 || got.ContentBytes != 100 {
		t.Fatal("same directory charged twice", got)
	}
	if err = repo.Tombstone(ctx, actor, p.ID, p.Revision); err != nil {
		t.Fatal(err)
	}
	epoch := strings.Repeat("e", 64)
	v, err := repo.RetireStorageVersion(ctx, first.Operation.StorageVersionKey, epoch, []string{"member"})
	if err != nil {
		t.Fatal(err)
	}
	if v.QuotaBytes != 2*versionCharge(100) {
		t.Fatal("lost duplicate operation charges", v)
	}
	if err = repo.AcknowledgeStorageReclamation(ctx, v.ID, epoch, "member"); err != nil {
		t.Fatal(err)
	}
	if got := read(); got.Bytes != 0 || got.ContentBytes != 0 || got.Versions != 0 {
		t.Fatal(got)
	}
}

func TestMeasuredAccountingDrainAndLegacyFence(t *testing.T) {
	ctx := context.Background()
	repo, actor, _ := measuredFixture(t)
	if err := repo.Store.Run(ctx, func(tx *metadata.Tx) error {
		return tx.Set("storage_allocations/storagea", StorageAllocation{Bytes: 100, Inodes: 1, BlockBytes: 4096})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Reserve(ctx, actor, measuredReservation("needs_upgrade", "a", 1)); !errors.Is(err, ErrStorage) {
		t.Fatal("unaudited allocation silently reset", err)
	}
}

func TestReclamationRefundsCurrentOwnerAfterClaim(t *testing.T) {
	ctx := context.Background()
	repo, actor, _ := measuredFixture(t)
	first, err := repo.Reserve(ctx, actor, measuredReservation("before_claim", "a", 100))
	if err != nil {
		t.Fatal(err)
	}
	p, err := repo.Activate(ctx, actor, first.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	anonymous := p.Owner
	if err = repo.Store.Run(ctx, func(tx *metadata.Tx) error {
		var record session.Record
		if err := tx.Get("sessions/"+actor.SessionDigest, &record); err != nil {
			return err
		}
		record.UID, record.AuthUntil = "account-user", time.Now().Add(time.Hour)
		return tx.Set("sessions/"+actor.SessionDigest, record)
	}); err != nil {
		t.Fatal(err)
	}
	actor.UID = "account-user"
	p, err = repo.Claim(ctx, actor, p.ID, p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	next := measuredReservation("after_claim", "b", 200)
	next.ProjectID, next.ExpectedRevision = p.ID, p.Revision
	second, err := repo.Reserve(ctx, actor, next)
	if err != nil {
		t.Fatal(err)
	}
	p, err = repo.Activate(ctx, actor, second.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	epoch := strings.Repeat("e", 64)
	if _, err = repo.RetireStorageVersion(ctx, first.Operation.StorageVersionKey, epoch, []string{"member"}); err != nil {
		t.Fatal(err)
	}
	if err = repo.AcknowledgeStorageReclamation(ctx, first.Operation.StorageVersionKey, epoch, "member"); err != nil {
		t.Fatal(err)
	}
	var oldQuota, currentQuota quota
	if err = repo.Store.Run(ctx, func(tx *metadata.Tx) error {
		if err := tx.Get("quotas/"+ownerKey(anonymous), &oldQuota); err != nil {
			return err
		}
		return tx.Get("quotas/"+ownerKey(p.Owner), &currentQuota)
	}); err != nil {
		t.Fatal(err)
	}
	if oldQuota.ChargedBytes != 0 || currentQuota.ChargedBytes != versionCharge(200) {
		t.Fatal(oldQuota, currentQuota)
	}
}

func TestReclamationIncludesBlockSizeUplift(t *testing.T) {
	ctx := context.Background()
	repo, actor, read := measuredFixture(t)
	first, err := repo.Reserve(ctx, actor, measuredReservation("small_blocks", "a", 100))
	if err != nil {
		t.Fatal(err)
	}
	p, err := repo.Activate(ctx, actor, first.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	before := read()
	repo.StorageOffers = func() []StorageOffer {
		return []StorageOffer{{App: "storagea", LimitBytes: 1 << 30, AvailableBytes: 1 << 30, BlockBytes: 8192, Generations: true}}
	}
	r := measuredReservation("large_blocks", "b", 200)
	r.ProjectID, r.ExpectedRevision = p.ID, p.Revision
	second, err := repo.Reserve(ctx, actor, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Activate(ctx, actor, second.Operation.ID); err != nil {
		t.Fatal(err)
	}
	epoch := strings.Repeat("e", 64)
	v, err := repo.RetireStorageVersion(ctx, first.Operation.StorageVersionKey, epoch, []string{"member"})
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.AcknowledgeStorageReclamation(ctx, v.ID, epoch, "member"); err != nil {
		t.Fatal(err)
	}
	usage, _ := r.StorageManifest.Footprint(8192)
	if a := read(); a.Bytes != usage.AllocatedBytes || a.AccountingBlockBytes != 8192 || before.Bytes >= a.Bytes {
		t.Fatal(a, usage)
	}
}

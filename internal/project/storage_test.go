package project

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func storageRepository(t *testing.T, limit int64) (*RaftRepository, Actor, *testmetadata.Backend) {
	t.Helper()
	backend := &testmetadata.Backend{}
	store := &metadata.Store{Backend: backend}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
	token, view, err := sessions.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	actor, err := ActorFrom(token, view)
	if err != nil {
		t.Fatal(err)
	}
	repo := &RaftRepository{Store: store, StorageOffers: func() []StorageOffer {
		return []StorageOffer{{App: "storagea", LimitBytes: limit, AvailableBytes: 1 << 30, BlockBytes: 4096}}
	}}
	return repo, actor, backend
}

func TestRemovedOrDrainedAppRejectsStaleOffers(t *testing.T) {
	for _, control := range []StorageControl{{Removed: true}, {Drain: true}} {
		repo, actor, _ := storageRepository(t, 1<<30)
		ctx := context.Background()
		if err := repo.Store.Run(ctx, func(tx *metadata.Tx) error {
			return tx.Set("storage_allocations/storagea", StorageAllocation{Control: control})
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.Reserve(ctx, actor, Reservation{Key: "stale_offer", Name: "test", Digest: strings.Repeat("a", 64), Bytes: 1, Files: 1}); !errors.Is(err, ErrStorage) {
			t.Fatal("stale offer allocated removed app", err)
		}
	}
}
func TestStorageAllocationConcurrentBoundaryAndRetry(t *testing.T) {
	charge := int64(1 + (21+64)*4096 + 8<<20)
	repo, actor, _ := storageRepository(t, charge*2)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var successes []Prepared
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			request := Reservation{Key: "storage_key_" + string(rune('a'+i)), Digest: strings.Repeat(string(rune('a'+i%6)), 64), Name: "project-" + string(rune('a'+i)), Bytes: 1, Files: 1}
			prepared, err := repo.Reserve(context.Background(), actor, request)
			if err == nil {
				mu.Lock()
				successes = append(successes, prepared)
				mu.Unlock()
			} else if !errors.Is(err, ErrStorage) && !errors.Is(err, ErrConflict) {
				t.Errorf("unexpected reservation failure: %v", err)
			}
		}()
	}
	wg.Wait()
	if len(successes) != 2 {
		t.Fatal("capacity boundary violated", len(successes))
	}
	var allocated StorageAllocation
	if err := repo.Store.Run(context.Background(), func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &allocated) }); err != nil || allocated.Bytes != 2*charge {
		t.Fatal(allocated, err)
	}
	// A completed operation can be retried even with no healthy storage offer.
	chosen := successes[0]
	p, err := repo.Activate(context.Background(), actor, chosen.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	repo.StorageOffers = func() []StorageOffer { return nil }
	if p.StorageApp != "storagea" {
		t.Fatal("placement lost on activation")
	}
	if _, err := repo.Reserve(context.Background(), actor, Reservation{Key: "new_version", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: strings.Repeat("0", 64), Bytes: 1, Files: 1}); !errors.Is(err, ErrStorage) {
		t.Fatal("update moved away from unavailable assigned app", err)
	}
}
func TestStoragePlacementStickyAndAccountingIdempotent(t *testing.T) {
	repo, actor, _ := storageRepository(t, 1<<30)
	ctx := context.Background()
	request := Reservation{Key: "stable_request", Name: "stable", Digest: strings.Repeat("a", 64), Bytes: 100, Files: 1}
	first, err := repo.Reserve(ctx, actor, request)
	if err != nil {
		t.Fatal(err)
	}
	repo.StorageOffers = func() []StorageOffer {
		return []StorageOffer{{App: "storageb", LimitBytes: 1 << 30, AvailableBytes: 1 << 30, BlockBytes: 4096}}
	}
	second, err := repo.Reserve(ctx, actor, request)
	if err != nil || second.Project.StorageApp != first.Project.StorageApp || second.Operation.ID != first.Operation.ID {
		t.Fatal("retry changed placement", second, err)
	}
	var before StorageAllocation
	repo.Store.Run(ctx, func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &before) })
	p, err := repo.Activate(ctx, actor, first.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Reserve(ctx, actor, Reservation{Key: "sticky_update", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: strings.Repeat("b", 64), Bytes: 100, Files: 1}); !errors.Is(err, ErrStorage) {
		t.Fatal("update moved to other app", err)
	}
	repo.StorageOffers = func() []StorageOffer { return nil }
	again, err := repo.Reserve(ctx, actor, request)
	if err != nil || again.Project.ID != p.ID {
		t.Fatal("completed retry depended on health", err)
	}
	if err := repo.Tombstone(ctx, actor, p.ID, p.Revision); err != nil {
		t.Fatal(err)
	}
	var after StorageAllocation
	repo.Store.Run(ctx, func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &after) })
	if before != after {
		t.Fatal("deletion refunded unverified bytes", before, after)
	}
}
func TestStorageReservationUnknownOutcomeAndAbortRetainCharge(t *testing.T) {
	repo, actor, backend := storageRepository(t, 1<<30)
	request := Reservation{Key: "unknown_result", Name: "uncertain", Digest: strings.Repeat("a", 64), Bytes: 1, Files: 1}
	backend.Failure = errors.New("authority unavailable")
	if _, err := repo.Reserve(context.Background(), actor, request); err == nil {
		t.Fatal("reservation succeeded without authority")
	}
	backend.Failure = nil
	prepared, err := repo.Reserve(context.Background(), actor, request)
	if err != nil {
		t.Fatal(err)
	}
	var before StorageAllocation
	repo.Store.Run(context.Background(), func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &before) })
	if err := repo.Abort(context.Background(), actor, prepared.Operation.ID); err != nil {
		t.Fatal(err)
	}
	var after StorageAllocation
	repo.Store.Run(context.Background(), func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &after) })
	if before != after {
		t.Fatal("abort refunded potential orphan")
	}
}

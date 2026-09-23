package project

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
)

func TestByteQuotaArithmetic(t *testing.T) {
	if fitsBytes(math.MaxInt64, 1, math.MaxInt64) || fitsBytes(-1, 1, 10) || fitsBytes(0, 0, 10) || !fitsBytes(9, 1, 10) {
		t.Fatal("unsafe quota arithmetic")
	}
	if versionCharge(0) != 1<<20 || versionCharge(2<<20) != 2<<20 {
		t.Fatal("invalid charge")
	}
}

func loadQuota(t *testing.T, r *FirestoreRepository, o Owner) quota {
	t.Helper()
	doc, err := r.ref("quotas", ownerKey(o)).Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var q quota
	if err := doc.DataTo(&q); err != nil {
		t.Fatal(err)
	}
	return q
}

func TestFirestoreRetainedByteQuota(t *testing.T) {
	r := testRepo(t)
	r.AnonymousByteLimit = 2 << 20
	a := actor(t, r, "")
	ctx := context.Background()
	request := Reservation{Key: "initial-byte-charge", Digest: hash("first"), Bytes: 1}
	first, err := r.Reserve(ctx, a, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Reserve(ctx, a, request); err != nil {
		t.Fatal(err)
	}
	active, err := r.Activate(ctx, a, first.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	update, err := r.Reserve(ctx, a, Reservation{Key: "update-byte-charge", ProjectID: active.ID, ExpectedRevision: active.Revision, Digest: hash("second"), Bytes: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Abort(ctx, a, update.Operation.ID); err != nil {
		t.Fatal(err)
	}
	if err = r.Abort(ctx, a, update.Operation.ID); err != nil {
		t.Fatal(err)
	}
	if q := loadQuota(t, r, a.Owner()); q.ChargedBytes != 2<<20 || q.Count != 1 {
		t.Fatal(q)
	}
	if _, err = r.Reserve(ctx, a, Reservation{Key: "over-byte-limit", Digest: hash("third"), Bytes: 1}); !errors.Is(err, ErrQuota) {
		t.Fatal(err)
	}
	if err = r.Tombstone(ctx, a, active.ID, active.Revision); err != nil {
		t.Fatal(err)
	}
	if q := loadQuota(t, r, a.Owner()); q.ChargedBytes != 2<<20 || q.Count != 0 {
		t.Fatal("deletion refunded retained files", q)
	}
}

func TestFirestoreConcurrentByteQuota(t *testing.T) {
	r := testRepo(t)
	r.AnonymousByteLimit = 1 << 20
	a := actor(t, r, "")
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, key := range []string{"concurrent-byte-one", "concurrent-byte-two"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			_, err := r.Reserve(context.Background(), a, Reservation{Key: key, Digest: hash(key), Bytes: 1})
			results <- err
		}(key)
	}
	wg.Wait()
	close(results)
	success, denied := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrQuota) {
			denied++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || denied != 1 {
		t.Fatal(success, denied)
	}
	if q := loadQuota(t, r, a.Owner()); q.Count != 1 || q.ChargedBytes != 1<<20 {
		t.Fatal(q)
	}
}

func TestFirestoreClaimTransfersRetainedBytes(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "account")
	anonymous := a
	anonymous.UID = ""
	ctx := context.Background()
	first, err := r.Reserve(ctx, anonymous, Reservation{Key: "claim-byte-charge", Digest: hash("claim"), Bytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	active, err := r.Activate(ctx, anonymous, first.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	r.AccountByteLimit = 1 << 20
	if _, err = r.Claim(ctx, a, active.ID, active.Revision); !errors.Is(err, ErrQuota) {
		t.Fatal("claim bypassed bytes", err)
	}
	if q := loadQuota(t, r, anonymous.Owner()); q.ChargedBytes != 2<<20 || q.Count != 1 {
		t.Fatal(q)
	}
	r.AccountByteLimit = 2 << 20
	if _, err = r.Claim(ctx, a, active.ID, active.Revision); err != nil {
		t.Fatal(err)
	}
	if q := loadQuota(t, r, anonymous.Owner()); q.ChargedBytes != 0 || q.Count != 0 {
		t.Fatal(q)
	}
	if q := loadQuota(t, r, a.Owner()); q.ChargedBytes != 2<<20 || q.Count != 1 {
		t.Fatal(q)
	}
}

func TestFirestoreLegacyQuotaFailsClosed(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	ctx := context.Background()
	if _, err := r.ref("quotas", ownerKey(a.Owner())).Set(ctx, map[string]any{"count": 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reserve(ctx, a, Reservation{Key: "legacy-byte-charge", Digest: hash("legacy"), Bytes: 1}); !errors.Is(err, ErrConflict) {
		t.Fatal("legacy usage assumed zero", err)
	}
}

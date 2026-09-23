package project

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFirestoreExpiredReservationRecovery(t *testing.T) {
	r := testRepo(t)
	r.AnonymousLimit = 1
	a := actor(t, r, "")
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	r.Now = func() time.Time { return now }
	request := Reservation{Key: "interrupted-upload", Name: "recovered", Digest: hash("bytes"), Bytes: 1}
	prepared, err := r.Reserve(ctx, a, request)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := r.RecoverExpired(ctx, 10); err != nil || count != 0 {
		t.Fatal("live reservation was recovered")
	}
	now = now.Add(16 * time.Minute)
	if _, err := r.Activate(ctx, a, prepared.Operation.ID); !errors.Is(err, ErrConflict) {
		t.Fatal("expired reservation activated")
	}
	if count, err := r.RecoverExpired(ctx, 10); err != nil || count != 1 {
		t.Fatalf("recovery failed: %d %v", count, err)
	}
	request.Key = "new-upload-key"
	replacement, err := r.Reserve(ctx, a, request)
	if err != nil {
		t.Fatalf("recovery did not release slug/digest/quota: %v", err)
	}
	if _, err := r.Activate(ctx, a, replacement.Operation.ID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(16 * time.Minute)
	if count, err := r.RecoverExpired(ctx, 10); err != nil || count != 0 {
		t.Fatal("completed operation rolled back")
	}
}

package session

import (
	"context"
	"crypto/subtle"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FirestoreStore is retained for isolated legacy emulator regression tests.
// Firestore TTL is cleanup only: reads and rotations enforce expiry themselves.
type FirestoreStore struct {
	Client *firestore.Client
	// Global per-minute bootstrap budget across replicas; not a substitute for
	// ingress/IP abuse controls. Required to prevent unbounded anonymous records.
	CreationsPerMinute int64
}

func (s *FirestoreStore) Get(ctx context.Context, digest string) (Record, error) {
	doc, err := s.Client.Collection("drop_sessions").Doc(digest).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return Record{}, ErrUnauthorized
	}
	if err != nil {
		return Record{}, err
	}
	var record Record
	if err := doc.DataTo(&record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func (s *FirestoreStore) Create(ctx context.Context, digest string, record Record, now time.Time) error {
	if s.CreationsPerMinute <= 0 {
		return fmt.Errorf("session creation budget is not configured")
	}
	bucket := s.Client.Collection("drop_session_budgets").Doc(now.UTC().Format("200601021504"))
	return s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		doc, err := tx.Get(bucket)
		var count int64
		if err == nil {
			var value struct {
				Count int64 `firestore:"count"`
			}
			if err := doc.DataTo(&value); err != nil {
				return err
			}
			count = value.Count
		} else if status.Code(err) != codes.NotFound {
			return err
		}
		if count >= s.CreationsPerMinute {
			return ErrRateLimited
		}
		if err := tx.Set(bucket, map[string]any{"count": count + 1, "expiresAt": now.Add(2 * time.Minute)}); err != nil {
			return err
		}
		return tx.Create(s.Client.Collection("drop_sessions").Doc(digest), record)
	})
}

func (s *FirestoreStore) Rotate(ctx context.Context, oldDigest, newDigest, expectedCSRF string, record Record, now time.Time) error {
	oldRef := s.Client.Collection("drop_sessions").Doc(oldDigest)
	return s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		doc, err := tx.Get(oldRef)
		if status.Code(err) == codes.NotFound {
			return ErrUnauthorized
		}
		if err != nil {
			return err
		}
		var old Record
		if err := doc.DataTo(&old); err != nil {
			return err
		}
		if !old.Active(now) || subtle.ConstantTimeCompare([]byte(old.CSRF), []byte(expectedCSRF)) != 1 {
			return ErrUnauthorized
		}
		// Carry the budget across token rotation so login/logout cannot bypass it.
		record.RotationWindow, record.RotationCount = now.Truncate(time.Minute), int64(1)
		if old.RotationWindow.Equal(record.RotationWindow) {
			record.RotationCount += old.RotationCount
		}
		if record.RotationCount > 20 {
			return ErrRateLimited
		}
		if err := tx.Create(s.Client.Collection("drop_sessions").Doc(newDigest), record); err != nil {
			return err
		}
		return tx.Update(oldRef, []firestore.Update{{Path: "revoked", Value: true}})
	})
}

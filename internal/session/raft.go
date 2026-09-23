package session

import (
	"context"
	"crypto/subtle"
	"errors"
	"regexp"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
)

type RaftStore struct {
	Store              *metadata.Store
	CreationsPerMinute int64
}

var sessionKey = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (s *RaftStore) Get(ctx context.Context, digest string) (Record, error) {
	if !sessionKey.MatchString(digest) {
		return Record{}, ErrUnauthorized
	}
	var record Record
	err := s.Store.Run(ctx, func(tx *metadata.Tx) error { return tx.Get("sessions/"+digest, &record) })
	if errors.Is(err, metadata.ErrNotFound) {
		err = ErrUnauthorized
	}
	return record, err
}

func (s *RaftStore) Create(ctx context.Context, digest string, record Record, now time.Time) error {
	if !sessionKey.MatchString(digest) || !record.Active(now) || s.CreationsPerMinute < 1 || s.CreationsPerMinute > 10000 {
		return kv.ErrInvalid
	}
	return s.Store.Run(ctx, func(tx *metadata.Tx) error {
		// One rolling bucket avoids unbounded per-minute records without relying
		// on an external TTL service. A clock moving backward never resets it.
		var bucket struct {
			Window time.Time
			Count  int64
		}
		err := tx.Get("session_budgets/global", &bucket)
		if err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		window := now.UTC().Truncate(time.Minute)
		if window.After(bucket.Window) {
			bucket.Window = window
			bucket.Count = 0
		}
		if bucket.Count >= s.CreationsPerMinute {
			return ErrRateLimited
		}
		bucket.Count++
		if err := tx.Set("session_budgets/global", bucket); err != nil {
			return err
		}
		return tx.Create("sessions/"+digest, record)
	})
}

func (s *RaftStore) Rotate(ctx context.Context, oldDigest, newDigest, expectedCSRF string, record Record, now time.Time) error {
	if !sessionKey.MatchString(oldDigest) || !sessionKey.MatchString(newDigest) || oldDigest == newDigest || !record.Active(now) {
		return ErrUnauthorized
	}
	return s.Store.Run(ctx, func(tx *metadata.Tx) error {
		var old Record
		err := tx.Get("sessions/"+oldDigest, &old)
		if errors.Is(err, metadata.ErrNotFound) {
			return ErrUnauthorized
		}
		if err != nil {
			return err
		}
		if !old.Active(now) || subtle.ConstantTimeCompare([]byte(old.CSRF), []byte(expectedCSRF)) != 1 {
			return ErrUnauthorized
		}
		next := record
		next.RotationWindow, next.RotationCount = now.Truncate(time.Minute), 1
		if !next.RotationWindow.After(old.RotationWindow) {
			next.RotationWindow = old.RotationWindow
			next.RotationCount += old.RotationCount
		}
		if next.RotationCount > 20 {
			return ErrRateLimited
		}
		if err := tx.Create("sessions/"+newDigest, next); err != nil {
			return err
		}
		old.Revoked = true
		return tx.Set("sessions/"+oldDigest, old)
	})
}

var _ Store = (*RaftStore)(nil)

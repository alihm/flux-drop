package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func TestRaftSessionRotationRace(t *testing.T) {
	ctx := context.Background()
	s := &Service{Store: &RaftStore{Store: &metadata.Store{Backend: &testmetadata.Backend{}}, CreationsPerMinute: 60}}
	token, view, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := s.Logout(ctx, token, view.Record.CSRF)
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, ErrUnauthorized) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatal("rotation winners", winners.Load())
	}
	if _, err = s.Read(ctx, token); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked session accepted", err)
	}
}

func TestRaftSessionGlobalBudget(t *testing.T) {
	now := time.Now().UTC()
	s := &Service{Store: &RaftStore{Store: &metadata.Store{Backend: &testmetadata.Backend{}}, CreationsPerMinute: 1}, Now: func() time.Time { return now }}
	if _, _, err := s.Create(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Create(context.Background()); !errors.Is(err, ErrRateLimited) {
		t.Fatal(err)
	}
	now = now.Add(-time.Hour)
	if _, _, err := s.Create(context.Background()); !errors.Is(err, ErrRateLimited) {
		t.Fatal("clock rollback bypass", err)
	}
	now = now.Add(2 * time.Hour)
	if _, _, err := s.Create(context.Background()); err != nil {
		t.Fatal(err)
	}
}

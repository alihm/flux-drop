package session

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
)

func emulatorStore(t *testing.T, budget int64) *FirestoreStore {
	t.Helper()
	if os.Getenv("DROP_TEST_FIRESTORE") != "1" {
		t.Skip("set DROP_TEST_FIRESTORE=1 and FIRESTORE_EMULATOR_HOST to run integration tests")
	}
	host := os.Getenv("FIRESTORE_EMULATOR_HOST")
	if !strings.HasPrefix(host, "127.0.0.1:") && !strings.HasPrefix(host, "localhost:") {
		t.Fatal("integration tests require explicit localhost emulator")
	}
	name, err := random()
	if err != nil {
		t.Fatal(err)
	}
	project := "demo-drop-" + strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name[:20], "_", "a"), "-", "b"))
	client, err := firestore.NewClient(context.Background(), project)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &FirestoreStore{Client: client, CreationsPerMinute: budget}
}

func TestFirestoreRotationAndStoredCredentials(t *testing.T) {
	store := emulatorStore(t, 20)
	now := time.Now().UTC()
	s := &Service{Store: store, Verifier: &testVerifier{now: now}, Now: func() time.Time { return now }}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, anon, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := Digest(raw)
	doc, err := store.Client.Collection("drop_sessions").Doc(digest).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range doc.Data() {
		if value == raw {
			t.Fatal("raw cookie was stored")
		}
	}
	if _, err := store.Client.Collection("drop_sessions").Doc(raw).Get(ctx); err == nil {
		t.Fatal("raw cookie used as key")
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _, err := s.Login(ctx, raw, anon.Record.CSRF, "alice"); results <- err }()
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrUnauthorized) {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatalf("%d transactions won", winners)
	}
	if _, err := s.Read(ctx, raw); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked token accepted")
	}
}

func TestFirestoreBootstrapBudgetAndExpiry(t *testing.T) {
	store := emulatorStore(t, 1)
	now := time.Now().UTC()
	s := &Service{Store: store, Verifier: &testVerifier{now: now}, Now: func() time.Time { return now }}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, view, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Create(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("budget not enforced: %v", err)
	}
	s.Now = func() time.Time { return view.Record.ExpiresAt }
	if _, err := s.Read(ctx, raw); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("expiry depended on TTL deletion")
	}
	if _, _, err := s.Logout(ctx, raw, view.Record.CSRF); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("expired session rotated")
	}
}

func TestFirestoreConcurrentBootstrapQuota(t *testing.T) {
	store := emulatorStore(t, 1)
	now := time.Now().UTC()
	s := &Service{Store: store, Verifier: &testVerifier{now: now}, Now: func() time.Time { return now }}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, _, err := s.Create(ctx); results <- err }()
	}
	winners := 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrRateLimited) {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatalf("%d bootstrap transactions bypassed quota", winners)
	}
}

func TestFirestoreRotationBudgetSurvivesNewTokens(t *testing.T) {
	store := emulatorStore(t, 5)
	now := time.Now().UTC()
	s := &Service{Store: store, Verifier: &testVerifier{now: now}, Now: func() time.Time { return now }}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, view, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		raw, view, err = s.Logout(ctx, raw, view.Record.CSRF)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := s.Logout(ctx, raw, view.Record.CSRF); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("rotation bypassed budget: %v", err)
	}
}

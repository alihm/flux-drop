package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type memoryStore struct {
	sync.Mutex
	records map[string]Record
}

func (m *memoryStore) Get(_ context.Context, key string) (Record, error) {
	m.Lock()
	defer m.Unlock()
	r, ok := m.records[key]
	if !ok {
		return Record{}, ErrUnauthorized
	}
	return r, nil
}
func (m *memoryStore) Create(_ context.Context, key string, r Record, _ time.Time) error {
	m.Lock()
	defer m.Unlock()
	m.records[key] = r
	return nil
}
func (m *memoryStore) Rotate(_ context.Context, old, key, csrf string, r Record, now time.Time) error {
	m.Lock()
	defer m.Unlock()
	previous, ok := m.records[old]
	if !ok || !previous.Active(now) || previous.CSRF != csrf {
		return ErrUnauthorized
	}
	previous.Revoked = true
	m.records[old] = previous
	m.records[key] = r
	return nil
}

type testVerifier struct {
	now     time.Time
	revoked bool
	failed  bool
}

func (v *testVerifier) VerifyGoogle(_ context.Context, token string) (Identity, error) {
	switch token {
	case "alice", "bob":
		return Identity{UID: token, AuthTime: v.now}, nil
	case "old":
		return Identity{UID: "alice", AuthTime: v.now.Add(-6 * time.Minute)}, nil
	case "future":
		return Identity{UID: "alice", AuthTime: v.now.Add(2 * time.Minute)}, nil
	default:
		return Identity{}, ErrUnauthorized
	}
}
func (v *testVerifier) CheckAccount(_ context.Context, _ Identity) error {
	if v.failed {
		return errors.New("provider unavailable")
	}
	if v.revoked {
		return ErrUnauthorized
	}
	return nil
}

func testService() (*Service, *memoryStore, *testVerifier) {
	now := time.Now().UTC()
	m := &memoryStore{records: make(map[string]Record)}
	v := &testVerifier{now: now}
	return &Service{Store: m, Verifier: v, Now: func() time.Time { return now }}, m, v
}

func TestLifecycleAndIsolation(t *testing.T) {
	s, store, _ := testService()
	ctx := context.Background()
	raw, anonymous, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	other, otherView, err := s.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if raw == other || anonymous.Record.AnonymousOwner == otherView.Record.AnonymousOwner {
		t.Fatal("shared anonymous identity")
	}
	if _, exists := store.records[raw]; exists {
		t.Fatal("raw token persisted")
	}
	loggedIn, account, err := s.Login(ctx, raw, anonymous.Record.CSRF, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if loggedIn == raw || account.Record.CSRF == anonymous.Record.CSRF || !account.Authenticated || account.Record.AnonymousOwner != anonymous.Record.AnonymousOwner {
		t.Fatal("incorrect login rotation")
	}
	if _, err := s.Read(ctx, raw); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("old cookie remains valid")
	}
	if _, _, err := s.Login(ctx, loggedIn, account.Record.CSRF, "bob"); !errors.Is(err, ErrAccountSwitch) {
		t.Fatal("account switch inherited ownership")
	}
	loggedOut, next, err := s.Logout(ctx, loggedIn, account.Record.CSRF)
	if err != nil {
		t.Fatal(err)
	}
	if next.Authenticated || next.Record.UID != "" || next.Record.AnonymousOwner == account.Record.AnonymousOwner {
		t.Fatal("logout leaked identity")
	}
	if _, err := s.Read(ctx, loggedIn); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("account cookie survived logout")
	}
	if _, _, err := s.Login(ctx, loggedOut, next.Record.CSRF, "bob"); err != nil {
		t.Fatal(err)
	}
	if view, err := s.Read(ctx, other); err != nil || view.Record.AnonymousOwner != otherView.Record.AnonymousOwner {
		t.Fatal("unrelated session changed")
	}
}

func TestLoginRequiresCSRFAndRecentIdentity(t *testing.T) {
	s, _, _ := testService()
	ctx := context.Background()
	raw, v, _ := s.Create(ctx)
	if _, _, err := s.Login(ctx, raw, "wrong", "alice"); !errors.Is(err, ErrCSRF) {
		t.Fatal("bad CSRF accepted")
	}
	for _, token := range []string{"invalid", "old", "future"} {
		if _, _, err := s.Login(ctx, raw, v.Record.CSRF, token); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("accepted identity %s", token)
		}
	}
	if _, err := s.Read(ctx, raw); err != nil {
		t.Fatal("failed login revoked session")
	}
}

func TestAccountExpiryDoesNotRestoreAccountAuthority(t *testing.T) {
	s, _, verifier := testService()
	ctx := context.Background()
	raw, anon, _ := s.Create(ctx)
	login, _, err := s.Login(ctx, raw, anon.Record.CSRF, "alice")
	if err != nil {
		t.Fatal(err)
	}
	s.Now = func() time.Time { return verifier.now.Add(AccountLifetime) }
	v, err := s.Read(ctx, login)
	if err != nil || v.Authenticated || v.Record.UID != "alice" {
		t.Fatal("account expiry not enforced")
	}
	// Even after expiry, another account cannot inherit anonymous ownership.
	verifier.now = s.now()
	// Replace the closure before mutating the verifier's clock again.
	now := verifier.now
	s.Now = func() time.Time { return now }
	if _, _, err := s.Login(ctx, login, v.Record.CSRF, "bob"); !errors.Is(err, ErrAccountSwitch) {
		t.Fatal("expired account switch accepted")
	}
}

func TestRevocationOutageAndSessionExpiryFailClosed(t *testing.T) {
	s, _, verifier := testService()
	ctx := context.Background()
	raw, anon, _ := s.Create(ctx)
	login, account, _ := s.Login(ctx, raw, anon.Record.CSRF, "alice")
	verifier.revoked = true
	if _, err := s.Read(ctx, login); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked account accepted")
	}
	verifier.revoked = false
	verifier.failed = true
	if _, err := s.Read(ctx, login); err == nil {
		t.Fatal("provider outage granted authority")
	}
	// Logout remains possible without a working Google provider.
	if _, _, err := s.Logout(ctx, login, account.Record.CSRF); err != nil {
		t.Fatal(err)
	}
	s.Now = func() time.Time { return anon.Record.ExpiresAt }
	if _, err := s.Read(ctx, raw); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("expired cookie accepted")
	}
}

func TestConcurrentRotationHasOneWinner(t *testing.T) {
	s, _, _ := testService()
	ctx := context.Background()
	raw, anon, _ := s.Create(ctx)
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, _, err := s.Login(ctx, raw, anon.Record.CSRF, "alice"); results <- err }()
	}
	winners := 0
	for i := 0; i < 2; i++ {
		if <-results == nil {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("%d rotations won", winners)
	}
}

func TestDigestAndEnvironmentGuards(t *testing.T) {
	for _, token := range []string{"", "../sessions", "null", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB"} {
		if _, err := Digest(token); err == nil {
			t.Fatalf("accepted malformed token %q", token)
		}
	}
	for _, tc := range []struct {
		env, project, db, auth string
		valid                  bool
	}{
		{"production", "real", "", "", true},
		{"production", "demo-test", "localhost:1", "localhost:2", false},
		{"development", "real", "localhost:1", "localhost:2", false},
		{"development", "demo-test", "localhost:1", "", false},
		{"development", "demo-test", "localhost:1", "localhost:2", true},
	} {
		if (ValidateEnvironment(tc.env, tc.project, tc.db, tc.auth) == nil) != tc.valid {
			t.Fatalf("unexpected environment validation: %+v", tc)
		}
	}
}

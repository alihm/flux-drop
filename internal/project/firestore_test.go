package project

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/session"
)

func testID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func testRepo(t *testing.T) *FirestoreRepository {
	t.Helper()
	if os.Getenv("DROP_TEST_FIRESTORE") != "1" {
		t.Skip("requires localhost Firestore emulator")
	}
	host := os.Getenv("FIRESTORE_EMULATOR_HOST")
	if !strings.HasPrefix(host, "127.0.0.1:") && !strings.HasPrefix(host, "localhost:") {
		t.Fatal("refusing non-local emulator")
	}
	client, err := firestore.NewClient(context.Background(), "demo-project-"+testID())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &FirestoreRepository{Client: client}
}
func actor(t *testing.T, r *FirestoreRepository, uid string) Actor {
	t.Helper()
	now := time.Now().UTC()
	a := Actor{SessionDigest: hash(testID()), AnonymousID: testID(), UID: uid}
	record := session.Record{AnonymousOwner: a.AnonymousID, CSRF: testID(), UID: uid, AuthTime: now, AuthUntil: now.Add(time.Hour), CreatedAt: now, ExpiresAt: now.Add(365 * 24 * time.Hour)}
	if _, err := r.ref("sessions", a.SessionDigest).Create(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	return a
}
func publish(t *testing.T, p *Publisher, a Actor, key, text string) Project {
	t.Helper()
	staged, err := content.StageHTML(filepath.Join(p.DataRoot, "staging"), strings.NewReader(text), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := p.Publish(ctx, a, Reservation{Key: key, Name: "site"}, staged)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestFirestorePublishUpdateDedupAndDelete(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	other := actor(t, r, "")
	p := &Publisher{r, t.TempDir()}
	ctx := context.Background()
	first := publish(t, p, a, "first-publish", "first")
	if first.Revision != 1 || first.ExpiresAt == nil {
		t.Fatal("invalid anonymous lifecycle")
	}
	if _, err := content.VerifyVersion(filepath.Join(p.DataRoot, "projects", first.ID, "versions", first.ActiveDigest), first.ActiveDigest); err != nil {
		t.Fatal(err)
	}
	if replay := publish(t, p, a, "first-publish", "first"); replay.ID != first.ID || replay.Revision != 1 {
		t.Fatal("idempotent replay changed project")
	}
	_, err := r.Reserve(ctx, other, Reservation{Key: "duplicate-key", Name: "site", Digest: first.ActiveDigest, Bytes: 5})
	var duplicate *Duplicate
	if !errors.As(err, &duplicate) || duplicate.Slug != first.Slug {
		t.Fatalf("missing public duplicate link: %v", err)
	}
	staged, _ := content.StageHTML(filepath.Join(p.DataRoot, "staging"), strings.NewReader("second"), content.DefaultLimits())
	second, err := p.Publish(ctx, a, Reservation{Key: "update-key", ProjectID: first.ID, ExpectedRevision: 1}, staged)
	if err != nil {
		t.Fatal(err)
	}
	if second.Slug != first.Slug || second.InitialSuffix != first.InitialSuffix || second.ActiveDigest == first.ActiveDigest || second.Revision != 2 || !second.ExpiresAt.Equal(*first.ExpiresAt) {
		t.Fatal("update changed stable URL/expiry or missed revision")
	}
	// Old content can be uploaded again; its hash must not link to changed bytes.
	oldContent, _ := content.StageHTML(filepath.Join(p.DataRoot, "staging"), strings.NewReader("first"), content.DefaultLimits())
	third, err := p.Publish(ctx, other, Reservation{Key: "old-content", Name: "old-copy"}, oldContent)
	if err != nil {
		t.Fatal(err)
	}
	if third.ID == first.ID {
		t.Fatal("old digest points at updated project")
	}
	if _, err := r.GetOwned(ctx, other, first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign project disclosed")
	}
	if err := r.Tombstone(ctx, other, first.ID, 2); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign project deleted")
	}
	if err := r.Tombstone(ctx, a, first.ID, 1); !errors.Is(err, ErrConflict) {
		t.Fatal("stale revision deleted project")
	}
	if err := r.Tombstone(ctx, a, first.ID, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(ctx, first.Slug); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted project still resolves")
	}
}

func TestFirestoreClaimAndOwnershipQueries(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	other := actor(t, r, "other-user")
	p := &Publisher{r, t.TempDir()}
	ctx := context.Background()
	first := publish(t, p, a, "claim-project", "claim me")
	if _, err := r.Claim(ctx, other, first.ID, 1); !errors.Is(err, ErrNotFound) {
		t.Fatal("project URL granted ownership")
	}
	// Simulate the verified session's Google login while preserving anonymous ID.
	a.UID = "owner-user"
	if _, err := r.ref("sessions", a.SessionDigest).Update(ctx, []firestore.Update{{Path: "uid", Value: a.UID}}); err != nil {
		t.Fatal(err)
	}
	before, _, err := r.ListOwned(ctx, a, "", 20)
	if err != nil || len(before) != 1 {
		t.Fatalf("anonymous project lost on login: %v", err)
	}
	claimed, err := r.Claim(ctx, a, first.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ExpiresAt != nil || claimed.Owner.Kind != "firebase" || claimed.Owner.ID != a.UID {
		t.Fatal("claim failed")
	}
	restored := actor(t, r, a.UID)
	list, _, err := r.ListOwned(ctx, restored, "", 20)
	if err != nil || len(list) != 1 || list[0].ID != first.ID {
		t.Fatalf("UID restoration failed: %v", err)
	}
	auto := publish(t, p, restored, "auto-claimed", "account content")
	if auto.ExpiresAt != nil || auto.Owner.Kind != "firebase" {
		t.Fatal("account publication not claimed")
	}
	foreign, _, err := r.ListOwned(ctx, other, "", 20)
	if err != nil || len(foreign) != 0 {
		t.Fatal("listing leaked projects")
	}
}

func TestFirestoreConcurrentQuotaAndUpdates(t *testing.T) {
	r := testRepo(t)
	r.AnonymousLimit = 1
	a := actor(t, r, "")
	ctx := context.Background()
	type result struct {
		prepared Prepared
		err      error
	}
	results := make(chan result, 2)
	for _, key := range []string{"concurrent-one", "concurrent-two"} {
		go func(key string) {
			prepared, err := r.Reserve(ctx, a, Reservation{Key: key, Digest: hash(key), Bytes: 1})
			results <- result{prepared, err}
		}(key)
	}
	var winner Prepared
	successes := 0
	for i := 0; i < 2; i++ {
		value := <-results
		if value.err == nil {
			successes++
			winner = value.prepared
		} else if !errors.Is(value.err, ErrQuota) {
			t.Fatal(value.err)
		}
	}
	if successes != 1 {
		t.Fatal("quota race")
	}
	active, err := r.Activate(ctx, a, winner.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"update-race-one", "update-race-two"} {
		go func(key string) {
			prepared, err := r.Reserve(ctx, a, Reservation{Key: key, ProjectID: active.ID, ExpectedRevision: 1, Digest: hash(key), Bytes: 1})
			results <- result{prepared, err}
		}(key)
	}
	successes = 0
	for i := 0; i < 2; i++ {
		value := <-results
		if value.err == nil {
			successes++
			winner = value.prepared
		} else if !errors.Is(value.err, ErrConflict) {
			t.Fatal(value.err)
		}
	}
	if successes != 1 {
		t.Fatal("concurrent updates both reserved")
	}
	if err := r.Abort(ctx, a, winner.Operation.ID); err != nil {
		t.Fatal(err)
	}
	current, err := r.Resolve(ctx, active.Slug)
	if err != nil || current.ActiveDigest != active.ActiveDigest {
		t.Fatal("failed update damaged previous version")
	}
}

func TestFirestoreSessionRevocationAndPrivateDuplicate(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	other := actor(t, r, "")
	ctx := context.Background()
	prepared, err := r.Reserve(ctx, a, Reservation{Key: "pending-project", Digest: hash("private"), Bytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ref("sessions", a.SessionDigest).Update(ctx, []firestore.Update{{Path: "revoked", Value: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Activate(ctx, a, prepared.Operation.ID); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("revoked actor activated content")
	}
	if _, err := r.ref("sessions", a.SessionDigest).Update(ctx, []firestore.Update{{Path: "revoked", Value: false}}); err != nil {
		t.Fatal(err)
	}
	active, err := r.Activate(ctx, a, prepared.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ref("projects", active.ID).Update(ctx, []firestore.Update{{Path: "private", Value: true}}); err != nil {
		t.Fatal(err)
	}
	_, err = r.Reserve(ctx, other, Reservation{Key: "private-duplicate", Digest: active.ActiveDigest, Bytes: 1})
	var duplicate *Duplicate
	if !errors.Is(err, ErrConflict) || errors.As(err, &duplicate) {
		t.Fatal("private existence leaked")
	}
}

func TestFirestoreFailedInstallRecoveryAndExpiryBlocksClaim(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	r.AnonymousLimit = 1
	ctx := context.Background()
	root := t.TempDir()
	// A file where the project directory must be forces installation failure.
	if err := os.WriteFile(filepath.Join(root, "projects"), []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	staged, _ := content.StageHTML(filepath.Join(root, "staging"), strings.NewReader("data"), content.DefaultLimits())
	p := &Publisher{r, root}
	if _, err := p.Publish(ctx, a, Reservation{Key: "failed-install"}, staged); err == nil {
		t.Fatal("failed filesystem activated metadata")
	}
	list, _, err := r.ListOwned(ctx, a, "", 10)
	if err != nil || len(list) != 0 {
		t.Fatal("failed project visible")
	}
	now := time.Now().UTC().Add(16 * time.Minute)
	r.Now = func() time.Time { return now }
	if _, err := r.RecoverExpired(ctx, 10); err != nil {
		t.Fatal(err)
	}
	p.DataRoot = t.TempDir()
	active := publish(t, p, a, "after-failure", "data") // quota/digest released
	a.UID = "owner"
	if _, err := r.ref("sessions", a.SessionDigest).Update(ctx, []firestore.Update{{Path: "uid", Value: a.UID}, {Path: "authUntil", Value: active.ExpiresAt.Add(time.Hour)}}); err != nil {
		t.Fatal(err)
	}
	r.Now = func() time.Time { return *active.ExpiresAt }
	if _, err := r.Claim(ctx, a, active.ID, active.Revision); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired project claimed: %v", err)
	}
	if _, err := r.Resolve(ctx, active.Slug); !errors.Is(err, ErrNotFound) {
		t.Fatal("expiry relies on background deletion")
	}
}

type lostActivationResponse struct {
	Repository
	lose bool
}

func (r *lostActivationResponse) Activate(ctx context.Context, a Actor, id string) (Project, error) {
	p, err := r.Repository.Activate(ctx, a, id)
	if err == nil && r.lose {
		r.lose = false
		return Project{}, context.DeadlineExceeded
	}
	return p, err
}

func TestFirestorePublishRetryAfterLostActivationResponse(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	wrapped := &lostActivationResponse{Repository: r, lose: true}
	p := &Publisher{Repository: wrapped, DataRoot: t.TempDir()}
	ctx := context.Background()
	stage, _ := content.StageHTML(filepath.Join(p.DataRoot, "staging"), strings.NewReader("once"), content.DefaultLimits())
	if _, err := p.Publish(ctx, a, Reservation{Key: "idempotent-retry", Name: "site"}, stage); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("failure fixture did not run")
	}
	retry := publish(t, p, a, "idempotent-retry", "once")
	if retry.Revision != 1 {
		t.Fatal("retry created another version")
	}
	list, _, err := r.ListOwned(ctx, a, "", 50)
	if err != nil || len(list) != 1 {
		t.Fatal("retry created another project")
	}
	different, _ := content.StageHTML(filepath.Join(p.DataRoot, "staging"), strings.NewReader("different"), content.DefaultLimits())
	if _, err := p.Publish(ctx, a, Reservation{Key: "idempotent-retry", Name: "site"}, different); !errors.Is(err, ErrConflict) {
		t.Fatal("idempotency key reused with different content")
	}
}

func TestFirestoreShortHashCollisionIsNotDuplicate(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	ctx := context.Background()
	one := Reservation{Key: "short-hash-one", Name: "same", Digest: "abcdef" + strings.Repeat("1", 58), Bytes: 1}
	prepared, err := r.Reserve(ctx, a, one)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Activate(ctx, a, prepared.Operation.ID); err != nil {
		t.Fatal(err)
	}
	two := Reservation{Key: "short-hash-two", Name: "same", Digest: "abcdef" + strings.Repeat("2", 58), Bytes: 1}
	if _, err := r.Reserve(ctx, a, two); !errors.Is(err, ErrConflict) {
		t.Fatal("short hash collision incorrectly accepted or deduplicated")
	}
	two.Name = "different"
	if _, err := r.Reserve(ctx, a, two); err != nil {
		t.Fatal("full hashes were not distinguished")
	}
}

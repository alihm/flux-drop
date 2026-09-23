package project

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/runonflux/flux-drop/internal/password"
)

func TestFirestorePrivacyLifecycle(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	other := actor(t, r, "")
	pub := &Publisher{r, t.TempDir()}
	ctx := context.Background()
	p := publish(t, pub, a, "privacy-publish", "private site")
	s := &PrivacyService{Repository: r, DataRoot: pub.DataRoot, Hasher: password.NewHasher()}
	if _, err := s.Change(ctx, other, p.ID, p.Revision, true, "a long secret password"); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign mutation", err)
	}
	private, err := s.Change(ctx, a, p.ID, p.Revision, true, "a long secret password")
	if err != nil {
		t.Fatal(err)
	}
	if !private.Private || private.PolicyRevision != p.PolicyRevision+1 || private.PasswordDigest == "" || private.ActiveDigest != p.ActiveDigest || !private.ExpiresAt.Equal(*p.ExpiresAt) {
		t.Fatal("incorrect policy transition", private)
	}
	record, err := password.Load(pub.DataRoot, p.ID, private.PasswordRevision, private.PasswordDigest)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Hasher.Verify(ctx, "a long secret password", record.Hash); !ok || err != nil {
		t.Fatal(ok, err)
	}
	b, _ := json.Marshal(private)
	if strings.Contains(string(b), private.PasswordDigest) || strings.Contains(string(b), record.Hash.Key) {
		t.Fatal("password metadata exposed")
	}
	if _, err := s.Change(ctx, a, p.ID, p.Revision, false, ""); !errors.Is(err, ErrConflict) {
		t.Fatal("stale privacy update", err)
	}
	renamed, err := r.Rename(ctx, a, p.ID, "private-name", private.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := password.Load(pub.DataRoot, p.ID, renamed.PasswordRevision, renamed.PasswordDigest); err != nil {
		t.Fatal("rename broke password record", err)
	}
	rotated, err := s.Change(ctx, a, p.ID, renamed.Revision, true, "a different long password")
	if err != nil {
		t.Fatal(err)
	}
	if rotated.PasswordDigest == private.PasswordDigest || rotated.PolicyRevision <= private.PolicyRevision {
		t.Fatal("password rotation did not change binding")
	}
	// Both valid candidates race on one revision; exactly one can activate.
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := r.SetPrivacy(ctx, a, p.ID, rotated.Revision, "", 0); results <- err }()
	}
	wg.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal(success, conflict)
	}
	public, err := r.GetOwned(ctx, a, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if public.Private || public.PasswordDigest != "" || public.PasswordRevision != 0 || public.PolicyRevision != rotated.PolicyRevision+1 {
		t.Fatal("public transition retained password authority")
	}
}

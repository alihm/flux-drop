package metadata

import (
	"context"
	"errors"
	"testing"

	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func TestDurableEncodingIncludesHiddenFields(t *testing.T) {
	type value struct {
		Public string `json:"public"`
		Secret string `json:"-"`
	}
	original := value{"public", "owner-and-password-data"}
	raw, err := Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored value
	if err = Decode(raw, &restored); err != nil || restored != original {
		t.Fatal(restored, err)
	}
	if err = Decode([]byte(`{"schema":2,"data":""}`), &restored); err == nil {
		t.Fatal("unknown schema accepted")
	}
}

func TestPrefetchedAuthorizationStillChecksConcurrentRevocation(t *testing.T) {
	store := &Store{Backend: &testmetadata.Backend{}}
	ctx := context.Background()
	if err := store.Run(ctx, func(tx *Tx) error { return tx.Set("tests/authorization", "allowed") }); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	denied := errors.New("revoked")
	err := store.Run(ctx, func(tx *Tx) error {
		attempts++
		if err := tx.Prefetch([]string{"tests/authorization", "tests/missing", "tests/authorization"}); err != nil {
			return err
		}
		var value string
		if err := tx.Get("tests/authorization", &value); err != nil {
			return err
		}
		if value == "revoked" {
			return denied
		}
		if err := store.Run(ctx, func(other *Tx) error { return other.Set("tests/authorization", "revoked") }); err != nil {
			return err
		}
		// A repeated prefetch must not replace an already checked authorization.
		if err := tx.Prefetch([]string{"tests/authorization"}); err != nil {
			return err
		}
		if err := tx.Get("tests/authorization", &value); err != nil || value != "allowed" {
			t.Fatal("prefetch replaced snapshot", value, err)
		}
		if err := tx.Get("tests/missing", nil); !errors.Is(err, ErrNotFound) {
			t.Fatal("missing record", err)
		}
		return nil
	})
	if !errors.Is(err, denied) || attempts != 2 {
		t.Fatal("prefetch bypassed CAS", attempts, err)
	}
}

func TestValidateReadsRetriesSkewBeforeReportingInvariantFailure(t *testing.T) {
	ctx := context.Background()
	store := &Store{Backend: &testmetadata.Backend{}}
	if err := store.Run(ctx, func(tx *Tx) error { return tx.Set("tests/invariant", 1) }); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	if err := store.Run(ctx, func(tx *Tx) error {
		attempts++
		var value int
		if err := tx.Get("tests/invariant", &value); err != nil {
			return err
		}
		if attempts == 1 {
			if err := store.Run(ctx, func(other *Tx) error { return other.Set("tests/invariant", 2) }); err != nil {
				return err
			}
		}
		if err := tx.ValidateReads(); err != nil {
			return err
		}
		if value != 2 {
			t.Fatal("stale value passed validation", value)
		}
		return nil
	}); err != nil || attempts != 2 {
		t.Fatal(err, attempts)
	}
}

type failAfterCommit struct {
	*testmetadata.Backend
	calls int
}

func (b *failAfterCommit) Commit(ctx context.Context, t kv.Transaction) error {
	b.calls++
	if err := b.Backend.Commit(ctx, t); err != nil {
		return err
	}
	return context.DeadlineExceeded
}
func TestUnknownCommitOutcomeNotReplayed(t *testing.T) {
	b := &failAfterCommit{Backend: &testmetadata.Backend{}}
	s := &Store{Backend: b}
	err := s.Run(context.Background(), func(tx *Tx) error { return tx.Create("tests/one", "value") })
	if !errors.Is(err, context.DeadlineExceeded) || b.calls != 1 {
		t.Fatal("indeterminate commit replayed", b.calls, err)
	}
	r, _ := b.Read(context.Background(), []string{"tests/one"})
	if r["tests/one"].Version == 0 {
		t.Fatal("fixture did not commit")
	}
}

func TestReadOnlyRetriesChangedAuthorization(t *testing.T) {
	s := &Store{Backend: &testmetadata.Backend{}}
	ctx := context.Background()
	if err := s.Run(ctx, func(tx *Tx) error { return tx.Set("sessions/test", false) }); err != nil {
		t.Fatal(err)
	}
	calls := 0
	denied := errors.New("revoked")
	err := s.Run(ctx, func(tx *Tx) error {
		calls++
		var revoked bool
		if err := tx.Get("sessions/test", &revoked); err != nil {
			return err
		}
		if revoked {
			return denied
		}
		return s.Run(ctx, func(other *Tx) error { return other.Set("sessions/test", true) })
	})
	if !errors.Is(err, denied) || calls != 2 {
		t.Fatal("stale authorization read accepted", calls, err)
	}
}

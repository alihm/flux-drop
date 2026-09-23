package project

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func retirementFixture(t *testing.T) (*FirestoreRepository, Actor, Project, Prepared, Reservation) {
	t.Helper()
	r := testRepo(t)
	a := actor(t, r, "")
	ctx := context.Background()
	request := Reservation{Key: "retirement-original", Digest: hash("original"), Bytes: 1}
	first, err := r.Reserve(ctx, a, request)
	if err != nil {
		t.Fatal(err)
	}
	p, err := r.Activate(ctx, a, first.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	update, err := r.Reserve(ctx, a, Reservation{Key: "retirement-current", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: hash("current"), Bytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	p, err = r.Activate(ctx, a, update.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	return r, a, p, first, request
}

func TestRetirementValidation(t *testing.T) {
	r := &FirestoreRepository{}
	for _, ref := range []VersionRef{{"../outside", hash("x")}, {strings.Repeat("a", 32), "bad"}} {
		if _, err := r.RetireVersion(context.Background(), ref, hash("operation"), 1); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}

func TestFirestoreRetirementFencesPublication(t *testing.T) {
	r, a, p, first, original := retirementFixture(t)
	ctx := context.Background()
	ref := VersionRef{p.ID, first.Operation.Digest}
	if _, err := r.RetireVersion(ctx, ref, first.Operation.ID, p.Revision-1); !errors.Is(err, ErrConflict) {
		t.Fatal("stale revision accepted", err)
	}
	record, err := r.RetireVersion(ctx, ref, first.Operation.ID, p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := r.RetireVersion(ctx, ref, first.Operation.ID, p.Revision)
	if err != nil || record != retry {
		t.Fatal("retirement not idempotent", record, retry, err)
	}
	if _, err := r.Reserve(ctx, a, original); !errors.Is(err, ErrConflict) {
		t.Fatal("completed operation replay bypassed retirement", err)
	}
	if _, err := r.Activate(ctx, a, first.Operation.ID); !errors.Is(err, ErrConflict) {
		t.Fatal("activation replay bypassed retirement", err)
	}
	if _, err := r.Reserve(ctx, a, Reservation{Key: "restore-retired", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: ref.Digest, Bytes: 1}); !errors.Is(err, ErrConflict) {
		t.Fatal("restore bypassed retirement", err)
	}
	report, err := r.AuditRetention(ctx, []VersionRef{ref})
	if err != nil || report[0].Reason != "retirement_recorded" {
		t.Fatal(report, err)
	}
	if q := loadQuota(t, r, a.Owner()); q.ChargedBytes != 2<<20 || q.Count != 1 {
		t.Fatal("retirement changed charges", q)
	}
	current, err := r.GetOwned(ctx, a, p.ID)
	if err != nil || current.ActiveDigest != p.ActiveDigest || current.Revision != p.Revision {
		t.Fatal("retirement changed active project", current, err)
	}
	// The fence is scoped to this project, not a global ban on these bytes.
	if _, err := r.Reserve(ctx, a, Reservation{Key: "retired-other-project", Digest: ref.Digest, Bytes: 1}); err != nil {
		t.Fatal("cross-project fence", err)
	}
}

func TestFirestoreRetirementRejectsUnsafeTargets(t *testing.T) {
	r, a, p, first, _ := retirementFixture(t)
	ctx := context.Background()
	if _, err := r.RetireVersion(ctx, VersionRef{p.ID, p.ActiveDigest}, first.Operation.ID, p.Revision); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong operation provenance accepted", err)
	}
	currentOp := operationID(a, "retirement-current")
	if _, err := r.RetireVersion(ctx, VersionRef{p.ID, p.ActiveDigest}, currentOp, p.Revision); !errors.Is(err, ErrConflict) {
		t.Fatal("active version retired", err)
	}
	pending, err := r.Reserve(ctx, a, Reservation{Key: "retirement-pending", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: hash("pending"), Bytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.RetireVersion(ctx, VersionRef{p.ID, first.Operation.Digest}, first.Operation.ID, p.Revision); !errors.Is(err, ErrConflict) {
		t.Fatal("pending installer ignored", err)
	}
	if err := r.Abort(ctx, a, pending.Operation.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RetireVersion(ctx, VersionRef{p.ID, pending.Operation.Digest}, pending.Operation.ID, p.Revision); err != nil {
		t.Fatal("aborted provenance rejected", err)
	}
	// Even a malformed record must fail closed, rather than allowing publication.
	bad := VersionRef{p.ID, hash("malformed-record")}
	if _, err := r.ref("retirements", retirementID(bad)).Set(ctx, map[string]any{"schema": 999}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reserve(ctx, a, Reservation{Key: "malformed-retirement", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: bad.Digest, Bytes: 1}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}

func TestFirestoreRetirementRestoreRace(t *testing.T) {
	r, a, p, first, _ := retirementFixture(t)
	ctx := context.Background()
	ref := VersionRef{p.ID, first.Operation.Digest}
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, err := r.RetireVersion(ctx, ref, first.Operation.ID, p.Revision)
		results <- err
	}()
	go func() {
		defer wg.Done()
		<-start
		_, err := r.Reserve(ctx, a, Reservation{Key: "racing-restore", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: ref.Digest, Bytes: 1})
		results <- err
	}()
	close(start)
	wg.Wait()
	close(results)
	success, conflicts := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatal("retirement and restore both succeeded", success, conflicts)
	}
}

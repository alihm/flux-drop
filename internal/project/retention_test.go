package project

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRetentionDecisions(t *testing.T) {
	now := time.Now()
	ref := VersionRef{strings.Repeat("a", 32), hash("old")}
	base := Project{ID: ref.ProjectID, Status: "active", ActiveDigest: hash("current"), Revision: 2, ChargedBytes: 2 << 20}
	for _, tc := range []struct {
		name, reason, classification string
		change                       func(*Project)
	}{
		{"old", "superseded_version", "candidate", func(*Project) {}},
		{"active", "active_version", "hold", func(p *Project) { p.ActiveDigest = ref.Digest }},
		{"pending", "publication_pending", "hold", func(p *Project) { p.PendingOperation = hash("pending") }},
		{"reserved", "publication_reserved", "hold", func(p *Project) { p.Status = "reserved" }},
		{"deleted", "project_deleted", "candidate", func(p *Project) { p.Status = "deleted" }},
		{"expired", "expiry_not_finalized", "hold", func(p *Project) { p.ExpiresAt = &now }},
		{"legacy", "metadata_incomplete", "hold", func(p *Project) { p.ChargedBytes = 0 }},
		{"malformed", "metadata_incomplete", "hold", func(p *Project) { p.ActiveDigest = "bad" }},
		{"unknown", "metadata_incomplete", "hold", func(p *Project) { p.Status = "unknown" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			tc.change(&p)
			f := retentionFinding(ref, p, now)
			if f.Reason != tc.reason || f.Classification != tc.classification {
				t.Fatal(f)
			}
		})
	}
}

func TestRetentionInputValidation(t *testing.T) {
	r := &FirestoreRepository{}
	ref := VersionRef{strings.Repeat("a", 32), hash("x")}
	for _, refs := range [][]VersionRef{nil, make([]VersionRef, 101), {{ProjectID: "../escape", Digest: ref.Digest}}, {ref, ref}} {
		if report, err := r.AuditRetention(context.Background(), refs); !errors.Is(err, ErrInvalid) || report != nil {
			t.Fatal(report, err)
		}
	}
}

func TestFirestoreRetentionSnapshot(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	ctx := context.Background()
	first, err := r.Reserve(ctx, a, Reservation{Key: "retention-initial", Digest: hash("first"), Bytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	p, err := r.Activate(ctx, a, first.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	old := VersionRef{p.ID, p.ActiveDigest}
	next := VersionRef{p.ID, hash("next")}
	check := func(reasons ...string) {
		t.Helper()
		report, err := r.AuditRetention(ctx, []VersionRef{old, next, {strings.Repeat("f", 32), hash("missing")}})
		if err != nil {
			t.Fatal(err)
		}
		for i, want := range append(reasons, "metadata_missing") {
			if report[i].Reason != want {
				t.Fatal(report)
			}
		}
	}
	check("active_version", "superseded_version")
	update, err := r.Reserve(ctx, a, Reservation{Key: "retention-update", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: next.Digest, Bytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	check("publication_pending", "publication_pending")
	p, err = r.Activate(ctx, a, update.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	check("superseded_version", "active_version")
	// A candidate can become current again: a report is never a deletion lease.
	restore, err := r.Reserve(ctx, a, Reservation{Key: "retention-restore", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: old.Digest, Bytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	p, err = r.Activate(ctx, a, restore.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	check("active_version", "superseded_version")
	if err = r.Tombstone(ctx, a, p.ID, p.Revision); err != nil {
		t.Fatal(err)
	}
	check("project_deleted", "project_deleted")
	if q := loadQuota(t, r, a.Owner()); q.ChargedBytes != 3<<20 {
		t.Fatal("audit altered charges", q)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if report, err := r.AuditRetention(cancelled, []VersionRef{old}); err == nil || report != nil {
		t.Fatal("failed read yielded a report", report, err)
	}
}

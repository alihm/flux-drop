package project

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/runonflux/flux-drop/internal/metadata"
)

func revisionFixture(t *testing.T) (*RaftRepository, Actor, Project, Prepared, Prepared) {
	t.Helper()
	r, a, _ := measuredFixture(t)
	ctx := context.Background()
	one, err := r.Reserve(ctx, a, measuredReservation("history_first", "a", 100))
	if err != nil {
		t.Fatal(err)
	}
	p, err := r.Activate(ctx, a, one.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	next := measuredReservation("history_second", "b", 200)
	next.ProjectID, next.ExpectedRevision = p.ID, p.Revision
	two, err := r.Reserve(ctx, a, next)
	if err != nil {
		t.Fatal(err)
	}
	p, err = r.Activate(ctx, a, two.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	return r, a, p, one, two
}

func TestRevisionSelectionPreservesPolicyAndAccounting(t *testing.T) {
	r, a, p, one, two := revisionFixture(t)
	ctx := context.Background()
	p, err := r.SetPrivacy(ctx, a, p.ID, p.Revision, strings.Repeat("c", 64), p.PolicyRevision+1)
	if err != nil {
		t.Fatal(err)
	}
	p, err = r.SetWatermark(ctx, a, p.ID, p.Revision, false)
	if err != nil {
		t.Fatal(err)
	}
	p, err = r.Rename(ctx, a, p.ID, "renamed-history", p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	before := p
	versions, next, err := r.ListRevisions(ctx, a, p.ID, "")
	if err != nil || next != "" || len(versions) != 2 {
		t.Fatal(versions, next, err)
	}
	for _, v := range versions {
		if v.Active != (v.ID == two.Operation.ID) || v.CreatedAt.IsZero() {
			t.Fatal(v)
		}
	}
	if _, err = r.RetireStorageVersion(ctx, one.Operation.StorageVersionKey, hash("epoch"), []string{"a"}); !errors.Is(err, ErrConflict) {
		t.Fatal("retained history reclaimed", err)
	}
	p, err = r.SelectRevision(ctx, a, p.ID, one.Operation.ID, p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if p.ActiveDigest != one.Operation.Digest || p.StorageGeneration != one.Operation.StorageGeneration || p.ActiveBytes != 100 || p.Slug != before.Slug || !p.Private || p.PasswordDigest != before.PasswordDigest || p.PasswordRevision != before.PasswordRevision || p.WatermarkDisabled != before.WatermarkDisabled || p.Owner != before.Owner || p.ChargedBytes != before.ChargedBytes || p.Revision != before.Revision+1 || p.PolicyRevision != before.PolicyRevision+1 {
		t.Fatal(before, p)
	}
	if _, err = r.RemoveRevision(ctx, a, p.ID, one.Operation.ID, p.Revision); !errors.Is(err, ErrConflict) {
		t.Fatal("active removed", err)
	}
	p, err = r.RemoveRevision(ctx, a, p.ID, two.Operation.ID, p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.SelectRevision(ctx, a, p.ID, two.Operation.ID, p.Revision); !errors.Is(err, ErrNotFound) {
		t.Fatal("removed revision selected", err)
	}
	if _, err = r.Activate(ctx, a, two.Operation.ID); !errors.Is(err, ErrConflict) {
		t.Fatal("removed publication replayed", err)
	}
	versions, _, err = r.ListRevisions(ctx, a, p.ID, "")
	if err != nil || len(versions) != 1 || !versions[0].Active {
		t.Fatal(versions, err)
	}
	v, err := r.RetireStorageVersion(ctx, two.Operation.StorageVersionKey, hash("epoch"), []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.SelectRevision(ctx, a, p.ID, two.Operation.ID, p.Revision); !errors.Is(err, ErrNotFound) {
		t.Fatal("retiring revision selected", err)
	}
	if err = r.AcknowledgeStorageReclamation(ctx, v.ID, hash("epoch"), "a"); err != nil {
		t.Fatal(err)
	}
	still, err := r.GetOwned(ctx, a, p.ID)
	if err != nil || still.ChargedBytes != before.ChargedBytes {
		t.Fatal("early refund", still, err)
	}
	if err = r.AcknowledgeStorageReclamation(ctx, v.ID, hash("epoch"), "b"); err != nil {
		t.Fatal(err)
	}
	var alloc StorageAllocation
	if err = r.Store.Run(ctx, func(tx *metadata.Tx) error { return tx.Get("storage_allocations/storagea", &alloc) }); err != nil || alloc.LiveBytes != 100 || alloc.ContentBytes != 100 {
		t.Fatal(alloc, err)
	}
}

func TestRevisionCASAndPendingUpload(t *testing.T) {
	r, a, p, one, _ := revisionFixture(t)
	ctx := context.Background()
	if _, err := r.SelectRevision(ctx, a, p.ID, one.Operation.ID, p.Revision-1); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	request := measuredReservation("history_pending", "d", 300)
	request.ProjectID, request.ExpectedRevision = p.ID, p.Revision
	pending, err := r.Reserve(ctx, a, request)
	if err != nil {
		t.Fatal(err)
	}
	for _, remove := range []bool{false, true} {
		if _, err = r.changeRevision(ctx, a, p.ID, one.Operation.ID, p.Revision, remove); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	versions, _, err := r.ListRevisions(ctx, a, p.ID, "")
	if err != nil || len(versions) != 2 {
		t.Fatal(versions, err)
	}
	if err = r.Abort(ctx, a, pending.Operation.ID); err != nil {
		t.Fatal(err)
	}
	p, err = r.GetOwned(ctx, a, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, remove := range []bool{false, true} {
		wg.Add(1)
		go func(remove bool) {
			defer wg.Done()
			_, err := r.changeRevision(ctx, a, p.ID, one.Operation.ID, p.Revision, remove)
			errs <- err
		}(remove)
	}
	wg.Wait()
	close(errs)
	success := 0
	for err := range errs {
		if err == nil {
			success++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatal("both selection and removal succeeded", success)
	}
}

func TestRevisionCatalogBackfillAndPagination(t *testing.T) {
	repository, a, _ := namingRepository(t, false)
	r := repository.(*RaftRepository)
	ctx := context.Background()
	var p Project
	ids := []string{}
	for i := 0; i < 43; i++ {
		request := Reservation{Key: fmt.Sprintf("revision_%03d", i), Digest: hash(i), Bytes: 10, ProjectID: p.ID, ExpectedRevision: p.Revision}
		prepared, err := r.Reserve(ctx, a, request)
		if err != nil {
			t.Fatal(err)
		}
		p, err = r.Activate(ctx, a, prepared.Operation.ID)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, prepared.Operation.ID)
	}
	// Simulate pre-upgrade operation history with no publication index or date.
	if err := r.Store.Run(ctx, func(tx *metadata.Tx) error {
		var op Operation
		if err := tx.Get("operations/"+ids[0], &op); err != nil {
			return err
		}
		op.PublishedAt = op.PublishedAt.AddDate(-1, 0, 0)
		if err := tx.Set("operations/"+ids[0], op); err != nil {
			return err
		}
		return tx.Delete(revisionIndexPrefix(p.ID) + ids[0])
	}); err != nil {
		t.Fatal(err)
	}
	check := func() {
		seen := map[string]bool{}
		cursor := ""
		for {
			versions, next, err := r.ListRevisions(ctx, a, p.ID, cursor)
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range versions {
				if seen[v.ID] {
					t.Fatal("duplicate", v)
				}
				seen[v.ID] = true
			}
			if next == "" {
				break
			}
			if next == cursor {
				t.Fatal("stuck cursor")
			}
			cursor = next
		}
		if len(seen) != len(ids) {
			t.Fatal("missing history", len(seen), len(ids))
		}
	}
	check()
	if err := r.Maintain(ctx, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	check()
	if _, _, err := r.ListRevisions(ctx, a, p.ID, "bad"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestRaftRevisionOwnerIsolationAndNameReuse(t *testing.T) {
	testRevisionOwnerIsolationAndNameReuse(t, false)
}
func TestFirestoreRevisionOwnerIsolationAndNameReuse(t *testing.T) {
	testRevisionOwnerIsolationAndNameReuse(t, true)
}
func testRevisionOwnerIsolationAndNameReuse(t *testing.T, firestore bool) {
	repo, alice, bob := namingRepository(t, firestore)
	r := repo.(RevisionRepository)
	ctx := context.Background()
	prepared, err := repo.Reserve(ctx, alice, Reservation{Key: "history_owner", Name: "reusable-history", Digest: hash("one"), Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	p, err := repo.Activate(ctx, alice, prepared.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	original := p.Slug
	p, err = repo.Rename(ctx, alice, p.ID, "renamed-reusable-history", p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	next, err := repo.Reserve(ctx, alice, Reservation{Key: "history_owner2", ProjectID: p.ID, ExpectedRevision: p.Revision, Digest: hash("two"), Bytes: 20})
	if err != nil {
		t.Fatal(err)
	}
	p, err = repo.Activate(ctx, alice, next.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	versions, _, err := r.ListRevisions(ctx, alice, p.ID, "")
	if err != nil || len(versions) != 2 {
		t.Fatal(versions, err)
	}
	if _, _, err = r.ListRevisions(ctx, bob, p.ID, ""); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err = r.SelectRevision(ctx, bob, p.ID, prepared.Operation.ID, p.Revision); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err = r.RemoveRevision(ctx, bob, p.ID, prepared.Operation.ID, p.Revision); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	p, err = r.SelectRevision(ctx, alice, p.ID, prepared.Operation.ID, p.Revision)
	if err != nil || p.ActiveDigest != prepared.Operation.Digest {
		t.Fatal(p, err)
	}
	p, err = r.RemoveRevision(ctx, alice, p.ID, next.Operation.ID, p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.Tombstone(ctx, alice, p.ID, p.Revision); err != nil {
		t.Fatal(err)
	}
	if _, _, err = r.ListRevisions(ctx, alice, p.ID, ""); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	for i, slug := range []string{original, p.Slug} {
		if _, err = repo.Resolve(ctx, slug); !errors.Is(err, ErrNotFound) {
			t.Fatal("deleted alias served", slug, err)
		}
		fresh, err := repo.Reserve(ctx, bob, Reservation{Key: fmt.Sprintf("reuse_history_%d", i), Name: slug, Digest: hash([]string{"reuse", slug}), Bytes: 10})
		if err != nil || fresh.Project.Slug != slug {
			t.Fatal("name not freed", fresh, err)
		}
		if _, err = repo.Activate(ctx, bob, fresh.Operation.ID); err != nil {
			t.Fatal(err)
		}
	}
	// Repeating deletion must never remove another project's reused name.
	if err = repo.Tombstone(ctx, alice, p.ID, p.Revision); err != nil {
		t.Fatal(err)
	}
	resolved, err := repo.Resolve(ctx, original)
	if err != nil || resolved.ID == p.ID {
		t.Fatal(resolved, err)
	}
}

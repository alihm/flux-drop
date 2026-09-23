package project

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/runonflux/flux-drop/internal/content"
)

func TestFirestoreRenameAndUpdate(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	other := actor(t, r, "")
	pub := &Publisher{r, t.TempDir()}
	ctx := context.Background()
	original := publish(t, pub, a, "rename-publish", "original")
	renamed, err := r.Rename(ctx, a, original.ID, "new-name", original.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if renamed.Slug != "new-name-"+original.InitialSuffix || renamed.ActiveDigest != original.ActiveDigest || !renamed.ExpiresAt.Equal(*original.ExpiresAt) || renamed.InitialSlug != original.Slug {
		t.Fatal("rename changed content or lifecycle", renamed)
	}
	if alias, err := r.Resolve(ctx, original.Slug); err != nil || alias.Slug != renamed.Slug {
		t.Fatal("old alias lost", alias, err)
	}
	if _, err := r.Rename(ctx, other, original.ID, "stolen", renamed.Revision); !errors.Is(err, ErrNotFound) {
		t.Fatal("ownership bypass", err)
	}
	if _, err := r.Rename(ctx, a, original.ID, "stale", original.Revision); !errors.Is(err, ErrConflict) {
		t.Fatal("stale revision accepted", err)
	}
	staged, err := content.StageHTML(filepath.Join(pub.DataRoot, "staging"), strings.NewReader("updated after rename"), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	updated, err := pub.Publish(ctx, a, Reservation{Key: "rename-update", ProjectID: renamed.ID, ExpectedRevision: renamed.Revision}, staged)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Slug != renamed.Slug {
		t.Fatal("update lost name")
	}
	back, err := r.Rename(ctx, a, original.ID, "site", updated.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if back.Slug != original.Slug || back.AliasCount != 1 {
		t.Fatal("alias reuse failed", back)
	}
	if err := r.Tombstone(ctx, a, back.ID, back.Revision); err != nil {
		t.Fatal(err)
	}
	for _, slug := range []string{original.Slug, renamed.Slug} {
		if _, err := r.Resolve(ctx, slug); !errors.Is(err, ErrNotFound) {
			t.Fatal("deleted alias visible", err)
		}
	}
}

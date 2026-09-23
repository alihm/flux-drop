package project

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFirestoreExpiryReleasesQuotaOnce(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "")
	p := publish(t, &Publisher{r, t.TempDir()}, a, "expire-project", "expire me")
	ctx := context.Background()
	r.Now = func() time.Time { return p.ExpiresAt.Add(time.Second) }
	if _, err := r.Resolve(ctx, p.Slug); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired visible: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := r.ExpireProjects(ctx, 100); err != nil {
			t.Fatal(err)
		}
	}
	doc, err := r.ref("quotas", ownerKey(a.Owner())).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var q quota
	if err := doc.DataTo(&q); err != nil || q.Count != 0 || q.ChargedBytes != versionCharge(p.ActiveBytes) {
		t.Fatalf("quota: %+v %v", q, err)
	}
	doc, err = r.ref("projects", p.ID).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var deleted Project
	if err := doc.DataTo(&deleted); err != nil || deleted.Status != "deleted" || deleted.Revision != p.Revision+1 {
		t.Fatalf("tombstone: %+v %v", deleted, err)
	}
	if _, err := r.ref("slugs", p.Slug).Get(ctx); err != nil {
		t.Fatal("slug tombstone lost", err)
	}
	if _, err := r.ref("digests", p.ActiveDigest).Get(ctx); !missing(err) {
		t.Fatal("digest not released", err)
	}
}

func TestFirestoreExpiryRechecksClaimedProject(t *testing.T) {
	r := testRepo(t)
	a := actor(t, r, "google-owner")
	p := publish(t, &Publisher{r, t.TempDir()}, a, "claimed-project", "keep me")
	if err := r.expireProject(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(context.Background(), p.Slug); err != nil {
		t.Fatal(err)
	}
}

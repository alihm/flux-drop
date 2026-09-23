package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
)

func TestApplicationMetadataSurvivesLeaderLoss(t *testing.T) {
	g := newTestGroup(t)
	leader := g.leader(t, -1)
	ctx := context.Background()
	oldStore := &metadata.Store{Backend: g.nodes[leader]}
	oldSessions := &session.Service{Store: &session.RaftStore{Store: oldStore, CreationsPerMinute: 60}}
	token, view, err := oldSessions.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	actor, err := project.ActorFrom(token, view)
	if err != nil {
		t.Fatal(err)
	}
	oldRepo := &project.RaftRepository{Store: oldStore}
	prepared, err := oldRepo.Reserve(ctx, actor, project.Reservation{Key: "failover_publish", Name: "survivor", Digest: strings.Repeat("d", 64), Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	p, err := oldRepo.Activate(ctx, actor, prepared.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	g.isolate(leader)
	next := g.leader(t, leader)
	store := &metadata.Store{Backend: g.nodes[next]}
	repo := &project.RaftRepository{Store: store}
	p, err = repo.Rename(ctx, actor, p.ID, "new-leader", p.Revision)
	if err != nil {
		t.Fatal(err)
	}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
	if _, _, err = sessions.Logout(ctx, token, view.Record.CSRF); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.GetOwned(ctx, actor, p.ID); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("revocation missing after failover", err)
	}
	short, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	if _, err = oldRepo.GetOwned(short, actor, p.ID); err == nil {
		t.Fatal("isolated leader authorized a revoked session")
	}
	cancel()
	g.connect(leader)
	current := g.leader(t, -1)
	resolved, err := (&project.RaftRepository{Store: &metadata.Store{Backend: g.nodes[current]}}).Resolve(ctx, p.Slug)
	if err != nil || resolved.Slug != p.Slug || resolved.Owner.ID != actor.AnonymousID {
		t.Fatal("lost project metadata", resolved, err)
	}
	for i := range g.nodes {
		g.isolate(i)
	}
	short, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if _, err = repo.Resolve(short, p.Slug); err == nil || errors.Is(err, project.ErrNotFound) {
		t.Fatal("quorum loss must not serve stale data or claim absence", err)
	}
}

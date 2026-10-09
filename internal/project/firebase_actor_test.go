package project

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func TestFirebaseActorAuthorityAndExpiry(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	repo := &RaftRepository{Store: &metadata.Store{Backend: &testmetadata.Backend{}}, Now: func() time.Time { return now }}
	a, err := ActorFromFirebase(session.AgentIdentity{UID: "alice", Provider: "google.com", ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.ListOwned(context.Background(), a, "", 50); err != nil {
		t.Fatal("verified Firebase actor needed a browser session", err)
	}
	if a.Owner() != (Owner{"firebase", "alice"}) || a.Owns(Owner{"firebase", "bob"}) || a.Owns(Owner{"anonymous", "alice"}) {
		t.Fatal("incorrect Firebase account authority", a.Owner())
	}
	for _, invalid := range []Actor{
		{UID: "alice"}, // UID alone does not confer authority.
		{UID: "alice", firebaseBearerUntil: now},
		{firebaseBearerUntil: now.Add(time.Hour)},
		{UID: "alice", firebaseBearerUntil: now.Add(time.Hour), SessionDigest: strings.Repeat("a", 64)},
		{UID: "alice", firebaseBearerUntil: now.Add(time.Hour), AnonymousID: "anonymous"},
		{UID: "alice", firebaseBearerUntil: now.Add(time.Hour), AgentKeyDigest: strings.Repeat("a", 64)},
		{UID: "alice", firebaseBearerUntil: now.Add(time.Hour), UploadTicketDigest: strings.Repeat("a", 64)},
	} {
		if _, _, err := repo.ListOwned(context.Background(), invalid, "", 50); !errors.Is(err, session.ErrUnauthorized) {
			t.Fatal("invalid actor gained management authority", err)
		}
	}
	// Recheck at commit/activation time, not only at the HTTP authentication gate.
	now = now.Add(time.Hour)
	if _, _, err := repo.ListOwned(context.Background(), a, "", 50); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("expired bearer retained authority", err)
	}
	if err := repo.run(context.Background(), func(_ context.Context, tx *raftTx) error { return repo.authorizePublish(tx, a) }); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("expired bearer retained publish authority", err)
	}
	if _, err := ActorFromFirebase(session.AgentIdentity{UID: "alice", Provider: "password", ExpiresAt: now.Add(time.Hour)}); !errors.Is(err, ErrForbidden) {
		t.Fatal("non-Google actor accepted", err)
	}
}

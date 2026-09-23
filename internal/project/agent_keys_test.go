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

func TestAgentKeyPublishOnlyAndRevocation(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
	token, view, err := sessions.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := ActorFrom(token, view)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Run(ctx, func(tx *metadata.Tx) error {
		var record session.Record
		if err := tx.Get("sessions/"+owner.SessionDigest, &record); err != nil {
			return err
		}
		record.UID = "agent-test-user"
		record.AuthUntil = now.Add(time.Hour)
		return tx.Set("sessions/"+owner.SessionDigest, record)
	})
	if err != nil {
		t.Fatal(err)
	}
	owner.UID = "agent-test-user"
	repo := &RaftRepository{Store: store, Now: func() time.Time { return now }}
	secret, info, err := repo.IssueAgentKey(ctx, owner, "CI deploy")
	if err != nil || !strings.HasPrefix(secret, "drop_") || info.UID != owner.UID {
		t.Fatal(info, err)
	}
	keys, err := repo.ListAgentKeys(ctx, owner)
	if err != nil || len(keys) != 1 || keys[0].ID != info.ID {
		t.Fatal(keys, err)
	}
	actor, err := repo.AuthenticateAgentKey(ctx, secret)
	if err != nil || actor.AgentKeyDigest != info.ID {
		t.Fatal(actor, err)
	}
	prepared, err := repo.Reserve(ctx, actor, Reservation{Key: "agent-publish-1", Name: "agent-site", Digest: strings.Repeat("a", 64), Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	project, err := repo.Activate(ctx, actor, prepared.Operation.ID)
	if err != nil || project.Owner.ID != owner.UID || project.ExpiresAt != nil {
		t.Fatal(project, err)
	}
	if _, err := repo.Rename(ctx, actor, project.ID, "forbidden", project.Revision); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("agent key could rename", err)
	}
	if _, err := repo.Reserve(ctx, actor, Reservation{Key: "agent-update-1", ProjectID: project.ID, ExpectedRevision: project.Revision, Digest: strings.Repeat("b", 64), Bytes: 10}); !errors.Is(err, ErrForbidden) {
		t.Fatal("agent key could update", err)
	}
	if _, err := repo.GetOwned(ctx, actor, project.ID); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("agent key could read private management", err)
	}
	if err := repo.RevokeAgentKey(ctx, owner, info.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AuthenticateAgentKey(ctx, secret); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("revoked key accepted", err)
	}
	if _, err := repo.Reserve(ctx, actor, Reservation{Key: "agent-publish-2", Name: "later", Digest: strings.Repeat("c", 64), Bytes: 10}); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("revoked actor accepted", err)
	}
}

func TestAgentKeyExpiresAndIsNeverReturnedInList(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	sessions := &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: 60}}
	token, view, err := sessions.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	actor, err := ActorFrom(token, view)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Run(ctx, func(tx *metadata.Tx) error {
		var record session.Record
		if err := tx.Get("sessions/"+actor.SessionDigest, &record); err != nil {
			return err
		}
		record.UID = "expiry-user"
		record.AuthUntil = now.Add(180 * 24 * time.Hour)
		return tx.Set("sessions/"+actor.SessionDigest, record)
	})
	if err != nil {
		t.Fatal(err)
	}
	actor.UID = "expiry-user"
	repo := &RaftRepository{Store: store, Now: func() time.Time { return now }}
	secret, _, err := repo.IssueAgentKey(ctx, actor, "test")
	if err != nil {
		t.Fatal(err)
	}
	list, err := repo.ListAgentKeys(ctx, actor)
	if err != nil || len(list) != 1 || strings.Contains(list[0].Label, secret) {
		t.Fatal(list, err)
	}
	now = now.Add(agentKeyLifetime + time.Second)
	if _, err := repo.AuthenticateAgentKey(ctx, secret); !errors.Is(err, session.ErrUnauthorized) {
		t.Fatal("expired key accepted", err)
	}
	list, err = repo.ListAgentKeys(ctx, actor)
	if err != nil || len(list) != 0 {
		t.Fatal(list, err)
	}
}

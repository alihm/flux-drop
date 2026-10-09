package project

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

func TestAgentMaintenanceRetainsReplayAndDeletesExpiredPrivateState(t *testing.T) {
	now := time.Now().UTC()
	s := &RaftRepository{Store: &metadata.Store{Backend: &testmetadata.Backend{}}, Now: func() time.Time { return now }}
	ctx := context.Background()
	grant := AgentGrant{ID: "grant-id", UID: "user", ExpiresAt: now.Add(90 * 24 * time.Hour), IdleUntil: now.Add(30 * 24 * time.Hour), FirebaseRefresh: []byte("ciphertext")}
	records := map[string]any{
		"agent_grants/grant-id":                          grant,
		"agent_grant_owners/" + agentOwnerDigest("user"): []string{"grant-id"},
		"agent_credentials/used-code":                    AgentCredential{GrantID: grant.ID, Used: true, ExpiresAt: now.Add(-time.Minute), RetainUntil: grant.ExpiresAt.Add(24 * time.Hour)},
		"agent_rate/old-bucket":                          AgentRate{Count: 20, ExpiresAt: now.Add(-time.Minute)},
		"agent_pending/expired":                          AgentPending{ExpiresAt: now.Add(-2 * time.Hour)},
		"agent_clients/expired":                          AgentClient{ExpiresAt: now.Add(-25 * time.Hour)},
		"agent_upload_tickets/expired":                   AgentUploadTicket{ExpiresAt: now.Add(-2 * time.Hour)},
	}
	if err := s.AgentTransaction(ctx, func(tx *metadata.Tx) error {
		for key, value := range records {
			if err := tx.Set(key, value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.MaintainAgents(ctx, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if err := s.AgentTransaction(ctx, func(tx *metadata.Tx) error {
		for _, key := range []string{"agent_rate/old-bucket", "agent_pending/expired", "agent_clients/expired", "agent_upload_tickets/expired"} {
			if err := tx.Get(key, nil); !errors.Is(err, metadata.ErrNotFound) {
				t.Error("expired record retained", key, err)
			}
		}
		return tx.Get("agent_credentials/used-code", nil)
	}); err != nil {
		t.Fatal("live grant replay record removed", err)
	}
	now = now.Add(31 * 24 * time.Hour)
	if err := s.MaintainAgents(ctx, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if err := s.AgentTransaction(ctx, func(tx *metadata.Tx) error {
		var g AgentGrant
		if err := tx.Get("agent_grants/grant-id", &g); err != nil {
			return err
		}
		if !g.Revoked || len(g.FirebaseRefresh) > 0 {
			t.Error("expired idle grant retained a secret")
		}
		return tx.Get("agent_credentials/used-code", nil)
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(61 * 24 * time.Hour)
	if err := s.MaintainAgents(ctx, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if err := s.AgentTransaction(ctx, func(tx *metadata.Tx) error {
		for _, key := range []string{"agent_grants/grant-id", "agent_credentials/used-code", "agent_grant_owners/" + agentOwnerDigest("user")} {
			if err := tx.Get(key, nil); !errors.Is(err, metadata.ErrNotFound) {
				t.Error("expired private state retained", key, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func agentOwnerDigest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

package project

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
)

// MaintainAgents rechecks bounded scan candidates transactionally. Replay
// tombstones outlive the grant's absolute lifetime, so cleanup cannot silently
// turn token reuse into a harmless unknown-token lookup for a live connection.
func (s *RaftRepository) MaintainAgents(ctx context.Context, cursors map[string]string) error {
	scanner, ok := s.Store.Backend.(metadataScanner)
	if !ok {
		return ErrInvalid
	}
	for _, prefix := range []string{"agent_rate/", "agent_pending/", "agent_clients/", "agent_credentials/", "agent_upload_tickets/", "agent_grants/"} {
		page, err := scanner.Scan(ctx, prefix, cursors[prefix], 100)
		if err != nil {
			return err
		}
		for key, record := range page.Records {
			expired, err := agentRecordExpired(prefix, record.Value, s.now())
			if err != nil {
				return err
			}
			if !expired {
				continue
			}
			err = s.AgentTransaction(ctx, func(tx *metadata.Tx) error {
				switch prefix {
				case "agent_rate/":
					var v AgentRate
					if err := tx.Get(key, &v); err != nil {
						return ignoreAgentMissing(err)
					}
					if s.now().Before(v.ExpiresAt) {
						return nil
					}
				case "agent_pending/":
					var v AgentPending
					if err := tx.Get(key, &v); err != nil {
						return ignoreAgentMissing(err)
					}
					if s.now().Before(v.ExpiresAt.Add(time.Hour)) {
						return nil
					}
				case "agent_clients/":
					var v AgentClient
					if err := tx.Get(key, &v); err != nil {
						return ignoreAgentMissing(err)
					}
					if s.now().Before(v.ExpiresAt.Add(24 * time.Hour)) {
						return nil
					}
				case "agent_credentials/":
					var v AgentCredential
					if err := tx.Get(key, &v); err != nil {
						return ignoreAgentMissing(err)
					}
					if v.RetainUntil.IsZero() || s.now().Before(v.RetainUntil) {
						return nil
					}
				case "agent_upload_tickets/":
					var v AgentUploadTicket
					if err := tx.Get(key, &v); err != nil {
						return ignoreAgentMissing(err)
					}
					if s.now().Before(v.ExpiresAt.Add(time.Hour)) {
						return nil
					}
				case "agent_grants/":
					var v AgentGrant
					if err := tx.Get(key, &v); err != nil {
						return ignoreAgentMissing(err)
					}
					if s.now().Before(v.ExpiresAt.Add(24 * time.Hour)) {
						if len(v.FirebaseRefresh) > 0 && (v.Revoked || !s.now().Before(v.IdleUntil) || !s.now().Before(v.ExpiresAt)) {
							v.Revoked = true
							v.FirebaseRefresh = nil
							return tx.Set(key, v)
						}
						return nil
					}
					sum := sha256.Sum256([]byte(v.UID))
					indexKey := "agent_grant_owners/" + hex.EncodeToString(sum[:])
					var ids []string
					if err := tx.Get(indexKey, &ids); err != nil && !errors.Is(err, metadata.ErrNotFound) {
						return err
					}
					kept := []string{}
					for _, id := range ids {
						if id != v.ID {
							kept = append(kept, id)
						}
					}
					if len(kept) == 0 {
						if err := tx.Delete(indexKey); err != nil {
							return err
						}
					} else {
						if err := tx.Set(indexKey, kept); err != nil {
							return err
						}
					}
				}
				return tx.Delete(key)
			})
			if err != nil {
				return err
			}
		}
		cursors[prefix] = page.Next
	}
	return nil
}
func ignoreAgentMissing(err error) error {
	if errors.Is(err, metadata.ErrNotFound) {
		return nil
	}
	return err
}
func agentRecordExpired(prefix string, data []byte, now time.Time) (bool, error) {
	switch prefix {
	case "agent_rate/":
		var v AgentRate
		err := metadata.Decode(data, &v)
		return !now.Before(v.ExpiresAt), err
	case "agent_pending/":
		var v AgentPending
		err := metadata.Decode(data, &v)
		return !now.Before(v.ExpiresAt.Add(time.Hour)), err
	case "agent_clients/":
		var v AgentClient
		err := metadata.Decode(data, &v)
		return !now.Before(v.ExpiresAt.Add(24 * time.Hour)), err
	case "agent_credentials/":
		var v AgentCredential
		err := metadata.Decode(data, &v)
		return !v.RetainUntil.IsZero() && !now.Before(v.RetainUntil), err
	case "agent_upload_tickets/":
		var v AgentUploadTicket
		err := metadata.Decode(data, &v)
		return !now.Before(v.ExpiresAt.Add(time.Hour)), err
	case "agent_grants/":
		var v AgentGrant
		err := metadata.Decode(data, &v)
		return !now.Before(v.ExpiresAt.Add(24*time.Hour)) || len(v.FirebaseRefresh) > 0 && (v.Revoked || !now.Before(v.IdleUntil) || !now.Before(v.ExpiresAt)), err
	}
	return false, nil
}

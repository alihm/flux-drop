package project

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/runonflux/flux-drop/internal/session"
)

const agentKeyLifetime = 90 * 24 * time.Hour

// AgentKey is safe to return as metadata; the bearer secret is never stored.
type AgentKey struct {
	ID        string    `json:"id"`
	UID       string    `json:"-"`
	Label     string    `json:"label"`
	CreatedAt time.Time `json:"createdAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type agentKeyIndex struct{ IDs []string }

func agentKeyDigest(secret string) (string, bool) {
	if !strings.HasPrefix(secret, "drop_") || len(secret) != 48 {
		return "", false
	}
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(secret, "drop_"))
	if err != nil || len(b) != 32 || "drop_"+base64.RawURLEncoding.EncodeToString(b) != secret {
		return "", false
	}
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:]), true
}

func (s *RaftRepository) IssueAgentKey(ctx context.Context, a Actor, label string) (string, AgentKey, error) {
	label = strings.TrimSpace(label)
	if len(label) < 1 || len(label) > 64 || !utf8.ValidString(label) || strings.IndexFunc(label, unicode.IsControl) >= 0 {
		return "", AgentKey{}, ErrInvalid
	}
	if a.UID == "" {
		return "", AgentKey{}, ErrForbidden
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", AgentKey{}, err
	}
	secret := "drop_" + base64.RawURLEncoding.EncodeToString(b)
	id, _ := agentKeyDigest(secret)
	key := AgentKey{ID: id, UID: a.UID, Label: label, CreatedAt: s.now(), ExpiresAt: s.now().Add(agentKeyLifetime)}
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		indexKey := s.raftRef("account_agent_keys", ownerKey(a.Owner()))
		index, err := raftRead[agentKeyIndex](tx, indexKey)
		if err != nil && !raftMissing(err) {
			return err
		}
		active := make([]string, 0, len(index.IDs)+1)
		for _, prior := range index.IDs {
			stored, err := raftRead[AgentKey](tx, s.raftRef("agent_keys", prior))
			if err != nil && !raftMissing(err) {
				return err
			}
			if err == nil && s.now().Before(stored.ExpiresAt) {
				active = append(active, prior)
			} else if err == nil {
				if err := tx.Delete(s.raftRef("agent_keys", prior)); err != nil {
					return err
				}
			}
		}
		if len(active) >= 5 {
			return ErrQuota
		}
		active = append(active, id)
		if err := tx.Create(s.raftRef("agent_keys", id), key); err != nil {
			return err
		}
		return tx.Set(indexKey, agentKeyIndex{active})
	})
	if err != nil {
		return "", AgentKey{}, err
	}
	return secret, key, nil
}

func (s *RaftRepository) ListAgentKeys(ctx context.Context, a Actor) ([]AgentKey, error) {
	if a.UID == "" {
		return nil, ErrForbidden
	}
	var keys []AgentKey
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		index, err := raftRead[agentKeyIndex](tx, s.raftRef("account_agent_keys", ownerKey(a.Owner())))
		if raftMissing(err) {
			keys = []AgentKey{}
			return nil
		}
		if err != nil {
			return err
		}
		keys = make([]AgentKey, 0, len(index.IDs))
		for _, id := range index.IDs {
			key, err := raftRead[AgentKey](tx, s.raftRef("agent_keys", id))
			if err != nil && !raftMissing(err) {
				return err
			}
			if err == nil && key.UID == a.UID && s.now().Before(key.ExpiresAt) {
				keys = append(keys, key)
			}
		}
		return nil
	})
	return keys, err
}

func (s *RaftRepository) RevokeAgentKey(ctx context.Context, a Actor, id string) error {
	if !digestRE.MatchString(id) {
		return ErrInvalid
	}
	if a.UID == "" {
		return ErrForbidden
	}
	return s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		key, err := raftRead[AgentKey](tx, s.raftRef("agent_keys", id))
		if raftMissing(err) || (err == nil && key.UID != a.UID) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		indexKey := s.raftRef("account_agent_keys", ownerKey(a.Owner()))
		index, err := raftRead[agentKeyIndex](tx, indexKey)
		if err != nil {
			return err
		}
		kept := make([]string, 0, len(index.IDs))
		for _, prior := range index.IDs {
			if prior != id {
				kept = append(kept, prior)
			}
		}
		if err := tx.Delete(s.raftRef("agent_keys", id)); err != nil {
			return err
		}
		return tx.Set(indexKey, agentKeyIndex{kept})
	})
}

func (s *RaftRepository) AuthenticateAgentKey(ctx context.Context, secret string) (Actor, error) {
	id, ok := agentKeyDigest(secret)
	if !ok {
		return Actor{}, session.ErrUnauthorized
	}
	var actor Actor
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		key, err := raftRead[AgentKey](tx, s.raftRef("agent_keys", id))
		if raftMissing(err) {
			return session.ErrUnauthorized
		}
		if err != nil {
			return err
		}
		if key.UID == "" || !s.now().Before(key.ExpiresAt) {
			return session.ErrUnauthorized
		}
		actor = Actor{UID: key.UID, AgentKeyDigest: id}
		return nil
	})
	return actor, err
}

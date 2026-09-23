package project

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"time"
)

const transferLifetime = 24 * time.Hour

type transferRecord struct {
	ProjectID string
	Owner     Owner
	Digest    string
	ExpiresAt time.Time
}

func (s *RaftRepository) clearTransfer(tx *raftTx, id string) error {
	key := s.raftRef("transfers", id)
	if _, err := tx.Get(key); raftMissing(err) {
		return nil
	} else if err != nil {
		return err
	}
	return tx.Delete(key)
}

func transferParts(token string) (string, string, bool) {
	id, secret, ok := strings.Cut(token, ".")
	if !ok || !idRE.MatchString(id) || len(secret) != 43 {
		return "", "", false
	}
	bytes, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil || len(bytes) != 32 || base64.RawURLEncoding.EncodeToString(bytes) != secret {
		return "", "", false
	}
	hash := sha256.Sum256([]byte(token))
	return id, hex.EncodeToString(hash[:]), true
}

// CreateTransfer replaces any previous link for this project. Only the owner
// receives the secret; replicated metadata stores its digest, never the token.
func (s *RaftRepository) CreateTransfer(ctx context.Context, a Actor, id string, revision int64) (string, time.Time, error) {
	if !idRE.MatchString(id) || revision < 1 {
		return "", time.Time{}, ErrInvalid
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", time.Time{}, err
	}
	token := id + "." + base64.RawURLEncoding.EncodeToString(secret)
	_, digest, _ := transferParts(token)
	expires := s.now().Add(transferLifetime)
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) || !p.Live(s.now()) || p.Owner.Kind != "anonymous" {
			return ErrNotFound
		}
		if p.Revision != revision || p.PendingOperation != "" {
			return ErrConflict
		}
		return tx.Set(s.raftRef("transfers", id), transferRecord{id, p.Owner, digest, expires})
	})
	if err != nil {
		return "", time.Time{}, err
	}
	return token, expires, nil
}

func (s *RaftRepository) RevokeTransfer(ctx context.Context, a Actor, id string, revision int64) error {
	if !idRE.MatchString(id) || revision < 1 {
		return ErrInvalid
	}
	return s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) || p.Owner.Kind != "anonymous" || !p.Live(s.now()) {
			return ErrNotFound
		}
		if p.Revision != revision {
			return ErrConflict
		}
		if _, err := tx.Get(s.raftRef("transfers", id)); raftMissing(err) {
			return nil
		} else if err != nil {
			return err
		}
		return tx.Delete(s.raftRef("transfers", id))
	})
}

func (s *RaftRepository) RedeemTransfer(ctx context.Context, a Actor, token string) (Project, error) {
	id, digest, ok := transferParts(token)
	if !ok {
		return Project{}, ErrNotFound
	}
	if a.UID == "" {
		return Project{}, ErrForbidden
	}
	var result Project
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		record, err := raftRead[transferRecord](tx, s.raftRef("transfers", id))
		if raftMissing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if record.ProjectID != id || subtle.ConstantTimeCompare([]byte(record.Digest), []byte(digest)) != 1 || !s.now().Before(record.ExpiresAt) {
			return ErrNotFound
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !p.Live(s.now()) || p.Owner != record.Owner || p.Owner.Kind != "anonymous" || p.PendingOperation != "" {
			return ErrNotFound
		}
		if err := s.moveToAccount(tx, &p, a.UID); err != nil {
			return err
		}
		if err := tx.Delete(s.raftRef("transfers", id)); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

func (s *RaftRepository) moveToAccount(tx *raftTx, p *Project, uid string) error {
	oldQuota, err := s.readQuota(tx, p.Owner)
	if err != nil {
		return err
	}
	newOwner := Owner{"firebase", uid}
	newQuota, err := s.readQuota(tx, newOwner)
	if err != nil && !raftMissing(err) {
		return err
	}
	if newQuota.Count >= s.limit(newOwner) {
		return ErrQuota
	}
	if oldQuota.Count < 1 || p.ChargedBytes <= 0 || p.ChargedBytes > oldQuota.ChargedBytes {
		return ErrConflict
	}
	if !fitsBytes(newQuota.ChargedBytes, p.ChargedBytes, s.byteLimit(newOwner)) {
		return ErrQuota
	}
	oldQuota.Count--
	oldQuota.ChargedBytes -= p.ChargedBytes
	newQuota.Count++
	newQuota.ChargedBytes += p.ChargedBytes
	if err := tx.Set(s.raftRef("quotas", ownerKey(p.Owner)), oldQuota); err != nil {
		return err
	}
	if err := tx.Set(s.raftRef("quotas", ownerKey(newOwner)), newQuota); err != nil {
		return err
	}
	p.Owner, p.ExpiresAt = newOwner, nil
	p.OwnerKey = ownerKey(newOwner)
	p.Revision++
	return tx.Set(s.raftRef("projects", p.ID), *p)
}

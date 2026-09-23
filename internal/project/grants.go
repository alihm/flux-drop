package project

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strconv"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/runonflux/flux-drop/internal/password"
	"github.com/runonflux/flux-drop/internal/session"
)

var ErrUnlockDenied = errors.New("project unlock denied")
var ErrUnlockLimited = errors.New("project unlock rate limit exceeded")

type Grant struct {
	ProjectID      string    `firestore:"projectID"`
	SessionDigest  string    `firestore:"sessionDigest"`
	PolicyRevision int64     `firestore:"policyRevision"`
	ExpiresAt      time.Time `firestore:"expiresAt"`
}
type unlockBudget struct {
	Count     int       `firestore:"count"`
	ExpiresAt time.Time `firestore:"expiresAt"`
}
type UnlockRepository interface {
	BeginUnlock(context.Context, Actor, string) (Project, error)
	CompleteUnlock(context.Context, Actor, Project, string) (Grant, error)
}
type UnlockService struct {
	Repository UnlockRepository
	DataRoot   string
	Hasher     *password.Hasher
}

// Unlock never grants ownership. Only an opaque token is returned; its hash is
// persisted, bound to this browser session, project and exact access policy.
func (s *UnlockService) Unlock(ctx context.Context, a Actor, slug, secret string) (string, Grant, error) {
	p, err := s.Repository.BeginUnlock(ctx, a, slug)
	if err != nil {
		return "", Grant{}, err
	}
	record, err := password.Load(s.DataRoot, p.ID, p.PasswordRevision, p.PasswordDigest)
	if err != nil {
		return "", Grant{}, errors.Join(ErrStorage, err)
	}
	ok, err := s.Hasher.Verify(ctx, secret, record.Hash)
	if errors.Is(err, password.ErrInvalid) || (err == nil && !ok) {
		return "", Grant{}, ErrUnlockDenied
	}
	if err != nil {
		return "", Grant{}, err
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", Grant{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(random[:])
	digest, _ := session.Digest(token)
	grant, err := s.Repository.CompleteUnlock(ctx, a, p, digest)
	if err != nil {
		return "", Grant{}, err
	}
	return token, grant, nil
}

// BeginUnlock charges shared budgets before any hashing or filesystem reads.
// Unknown/public projects consume global/session budgets too. Budgets are fixed
// one-minute windows; TTL is cleanup only, never part of enforcement.
func (s *FirestoreRepository) BeginUnlock(ctx context.Context, a Actor, slug string) (Project, error) {
	if !slugRE.MatchString(slug) {
		return Project{}, ErrUnlockDenied
	}
	var result Project
	eligible := false
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		eligible = false
		result = Project{}
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		now := s.now()
		window := now.Truncate(time.Minute)
		bucket := strconv.FormatInt(window.Unix(), 10)
		keys := []string{hash([]string{"global", bucket}), hash([]string{"session", a.SessionDigest, bucket})}
		limits := []int{60, 10}
		idx, err := read[slugRecord](tx, s.ref("slugs", slug))
		if err != nil && !missing(err) {
			return err
		}
		if err == nil {
			p, err := read[Project](tx, s.ref("projects", idx.ProjectID))
			if err != nil && !missing(err) {
				return err
			}
			if err == nil && p.Live(now) && p.Private && digestRE.MatchString(p.PasswordDigest) && p.PasswordRevision > 0 {
				result = p
				eligible = true
				keys = append(keys, hash([]string{"project", p.ID, bucket}))
				limits = append(limits, 10)
			}
		}
		budgets := make([]unlockBudget, len(keys))
		for i, key := range keys {
			b, err := read[unlockBudget](tx, s.ref("unlock_budgets", key))
			if err != nil && !missing(err) {
				return err
			}
			if b.Count >= limits[i] {
				return ErrUnlockLimited
			}
			budgets[i] = unlockBudget{Count: b.Count + 1, ExpiresAt: window.Add(2 * time.Minute)}
		}
		for i, key := range keys {
			if err := tx.Set(s.ref("unlock_budgets", key), budgets[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Project{}, err
	}
	if !eligible {
		return Project{}, ErrUnlockDenied
	}
	return result, nil
}

// CompleteUnlock is internal and is called only after successful verification.
// A policy change, expiry, deletion or session rotation during hashing wins.
func (s *FirestoreRepository) CompleteUnlock(ctx context.Context, a Actor, verified Project, digest string) (Grant, error) {
	if !idRE.MatchString(verified.ID) || !digestRE.MatchString(digest) {
		return Grant{}, ErrInvalid
	}
	var result Grant
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := read[Project](tx, s.ref("projects", verified.ID))
		if missing(err) {
			return ErrUnlockDenied
		}
		if err != nil {
			return err
		}
		if !p.Live(s.now()) || !p.Private || p.PolicyRevision != verified.PolicyRevision || p.PasswordDigest != verified.PasswordDigest || p.PasswordRevision != verified.PasswordRevision {
			return ErrUnlockDenied
		}
		result = Grant{ProjectID: p.ID, SessionDigest: a.SessionDigest, PolicyRevision: p.PolicyRevision, ExpiresAt: s.now().Add(time.Hour)}
		if p.ExpiresAt != nil && p.ExpiresAt.Before(result.ExpiresAt) {
			result.ExpiresAt = *p.ExpiresAt
		}
		return tx.Create(s.ref("grants", digest), result)
	})
	return result, err
}

func (s *FirestoreRepository) ValidateGrant(ctx context.Context, a Actor, id, token string) error {
	digest, err := session.Digest(token)
	if err != nil || !idRE.MatchString(id) {
		return ErrUnlockDenied
	}
	return s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		g, err := read[Grant](tx, s.ref("grants", digest))
		if missing(err) {
			return ErrUnlockDenied
		}
		if err != nil {
			return err
		}
		if g.ProjectID != id || g.SessionDigest != a.SessionDigest || !s.now().Before(g.ExpiresAt) {
			return ErrUnlockDenied
		}
		p, err := read[Project](tx, s.ref("projects", id))
		if missing(err) {
			return ErrUnlockDenied
		}
		if err != nil {
			return err
		}
		if !p.Live(s.now()) || !p.Private || p.PolicyRevision != g.PolicyRevision {
			return ErrUnlockDenied
		}
		return nil
	}, firestore.ReadOnly)
}

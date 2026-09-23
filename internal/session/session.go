// Package session manages opaque browser credentials. Raw cookie tokens are
// returned once to the browser and never persisted; the store uses SHA-256 keys.
package session

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"
)

var (
	ErrUnauthorized  = errors.New("invalid or expired session")
	ErrCSRF          = errors.New("invalid csrf token")
	ErrAccountSwitch = errors.New("logout required before switching accounts")
	ErrRateLimited   = errors.New("session creation rate limit exceeded")
)

const AnonymousLifetime = 365 * 24 * time.Hour
const AccountLifetime = 12 * time.Hour

type Record struct {
	AnonymousOwner string    `firestore:"anonymousOwner"`
	CSRF           string    `firestore:"csrf"`
	UID            string    `firestore:"uid"`
	AuthTime       time.Time `firestore:"authTime"`
	AuthUntil      time.Time `firestore:"authUntil"`
	CreatedAt      time.Time `firestore:"createdAt"`
	ExpiresAt      time.Time `firestore:"expiresAt"`
	Revoked        bool      `firestore:"revoked"`
	RotationWindow time.Time `firestore:"rotationWindow"`
	RotationCount  int64     `firestore:"rotationCount"`
}

func (r Record) Active(now time.Time) bool {
	return !r.Revoked && now.Before(r.ExpiresAt) && r.AnonymousOwner != "" && r.CSRF != ""
}

type Identity struct {
	UID      string
	AuthTime time.Time
	// ExpiresAt bounds sessions created by credential-free token verification.
	// Zero is allowed only for legacy verifiers with live account checks.
	ExpiresAt time.Time
}

// Verifier must validate issuer, audience, expiry, signature, provider and
// the provider's account/session policy. Public-key-only verification cannot
// perform live Firebase revocation checks and must supply a bounded ExpiresAt.
// Identity fields must never come from an unverified client payload.
type Verifier interface {
	VerifyGoogle(context.Context, string) (Identity, error)
	CheckAccount(context.Context, Identity) error
}

type Store interface {
	Get(context.Context, string) (Record, error)
	Create(ctx context.Context, digest string, record Record, now time.Time) error
	// Rotate atomically revokes old and creates new; compare the old CSRF and
	// active state inside the transaction so concurrent rotations cannot both win.
	Rotate(ctx context.Context, oldDigest, newDigest, expectedCSRF string, record Record, now time.Time) error
}

type Service struct {
	Store    Store
	Verifier Verifier
	Now      func() time.Time
}

type View struct {
	Record        Record
	Authenticated bool
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func random() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// Digest rejects malformed/non-canonical tokens before any database access.
func Digest(token string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(b) != 32 || base64.RawURLEncoding.EncodeToString(b) != token {
		return "", ErrUnauthorized
	}
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:]), nil
}

func fresh(now time.Time) (string, Record, error) {
	token, err := random()
	if err != nil {
		return "", Record{}, err
	}
	owner, err := random()
	if err != nil {
		return "", Record{}, err
	}
	csrf, err := random()
	if err != nil {
		return "", Record{}, err
	}
	return token, Record{AnonymousOwner: owner, CSRF: csrf, CreatedAt: now, ExpiresAt: now.Add(AnonymousLifetime)}, nil
}

func (s *Service) Create(ctx context.Context) (string, View, error) {
	now := s.now()
	token, record, err := fresh(now)
	if err != nil {
		return "", View{}, err
	}
	digest, _ := Digest(token)
	if err := s.Store.Create(ctx, digest, record, now); err != nil {
		return "", View{}, err
	}
	return token, View{Record: record}, nil
}

func (s *Service) load(ctx context.Context, token string) (Record, error) {
	digest, err := Digest(token)
	if err != nil {
		return Record{}, err
	}
	r, err := s.Store.Get(ctx, digest)
	if err != nil {
		return Record{}, err
	}
	if !r.Active(s.now()) {
		return Record{}, ErrUnauthorized
	}
	return r, nil
}

func (s *Service) Read(ctx context.Context, token string) (View, error) {
	r, err := s.load(ctx, token)
	if err != nil {
		return View{}, err
	}
	v := View{Record: r}
	if r.UID != "" && s.now().Before(r.AuthUntil) {
		if err := s.Verifier.CheckAccount(ctx, Identity{UID: r.UID, AuthTime: r.AuthTime, ExpiresAt: r.AuthUntil}); err != nil {
			return View{}, err
		}
		v.Authenticated = true
	}
	return v, nil
}

func CheckCSRF(record Record, token string) error {
	if token == "" || subtle.ConstantTimeCompare([]byte(record.CSRF), []byte(token)) != 1 {
		return ErrCSRF
	}
	return nil
}

func (s *Service) Login(ctx context.Context, token, csrf, idToken string) (string, View, error) {
	old, err := s.load(ctx, token)
	if err != nil {
		return "", View{}, err
	}
	if err := CheckCSRF(old, csrf); err != nil {
		return "", View{}, err
	}
	identity, err := s.Verifier.VerifyGoogle(ctx, idToken)
	if err != nil {
		return "", View{}, err
	}
	now := s.now()
	if identity.UID == "" || identity.AuthTime.After(now.Add(time.Minute)) || now.Sub(identity.AuthTime) > 5*time.Minute {
		return "", View{}, ErrUnauthorized
	}
	if !identity.ExpiresAt.IsZero() && !now.Before(identity.ExpiresAt) {
		return "", View{}, ErrUnauthorized
	}
	if old.UID != "" && old.UID != identity.UID {
		return "", View{}, ErrAccountSwitch
	}
	newToken, next, err := fresh(now)
	if err != nil {
		return "", View{}, err
	}
	next.AnonymousOwner = old.AnonymousOwner // preserve the ability to claim owned anonymous projects
	next.UID = identity.UID
	next.AuthTime = identity.AuthTime
	next.AuthUntil = now.Add(AccountLifetime)
	if !identity.ExpiresAt.IsZero() && identity.ExpiresAt.Before(next.AuthUntil) {
		next.AuthUntil = identity.ExpiresAt
	}
	oldDigest, _ := Digest(token)
	newDigest, _ := Digest(newToken)
	if err := s.Store.Rotate(ctx, oldDigest, newDigest, old.CSRF, next, now); err != nil {
		return "", View{}, err
	}
	return newToken, View{Record: next, Authenticated: true}, nil
}

func (s *Service) Logout(ctx context.Context, token, csrf string) (string, View, error) {
	old, err := s.load(ctx, token)
	if err != nil {
		return "", View{}, err
	}
	if err := CheckCSRF(old, csrf); err != nil {
		return "", View{}, err
	}
	now := s.now()
	newToken, next, err := fresh(now)
	if err != nil {
		return "", View{}, err
	}
	// New anonymous ownership prevents the next account on a shared browser from
	// inheriting the previous account's unclaimed projects.
	oldDigest, _ := Digest(token)
	newDigest, _ := Digest(newToken)
	if err := s.Store.Rotate(ctx, oldDigest, newDigest, old.CSRF, next, now); err != nil {
		return "", View{}, err
	}
	return newToken, View{Record: next}, nil
}

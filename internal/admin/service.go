// Package admin provides wallet-only administration with quorum-backed state.
package admin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
)

var ErrUnauthorized = errors.New("admin authentication required")
var ErrLimited = errors.New("admin login rate limited")
var ErrAppInUse = errors.New("app contains retained allocations")
var ErrUnknownApp = errors.New("unknown app")
var tokenPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type Service struct {
	Store           *metadata.Store
	Origin, Address string
	now             func() time.Time
}
type Challenge struct {
	ID        string    `json:"id"`
	Message   string    `json:"message"`
	Expires   time.Time `json:"expiresAt"`
	PollToken string    `json:"pollToken"`
}
type challengeRecord struct {
	ID, Message, PollDigest, BrowserDigest string
	Expires                                time.Time
	Approved, Used                         bool
}
type sessionRecord struct {
	Digest, Address, CSRF string
	Expires               time.Time
	Revoked               bool
}
type budget struct {
	Window               time.Time
	Count                int
	Challenges, Sessions uint64
}
type Session struct {
	CSRF    string    `json:"csrfToken"`
	Expires time.Time `json:"expiresAt"`
}

func New(store *metadata.Store, origin, address string) (*Service, error) {
	if address == "" {
		address = DefaultAddress
	}
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || addressPayload(address) == nil || store == nil || store.Backend == nil {
		return nil, errors.New("invalid primary admin configuration")
	}
	return &Service{Store: store, Origin: origin, Address: address, now: time.Now}, nil
}
func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func digest(value string) string { h := sha256.Sum256([]byte(value)); return hex.EncodeToString(h[:]) }
func equal(a, b string) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
func slot(token, prefix string, limit int) (string, error) {
	if !tokenPattern.MatchString(token) {
		return "", ErrUnauthorized
	}
	i, err := strconv.ParseUint(token[:4], 16, 16)
	if err != nil || i >= uint64(limit) {
		return "", ErrUnauthorized
	}
	return prefix + token[:4], nil
}
func loadBudget(tx *metadata.Tx) (budget, error) {
	var b budget
	err := tx.Get("admin_budget/global", &b)
	if errors.Is(err, metadata.ErrNotFound) {
		err = nil
	}
	return b, err
}

// Fixed slots bound durable challenge/session storage across all primary nodes.
// Nonces remain unpredictable; overwritten sessions fail closed by digest.
func (s *Service) Issue(ctx context.Context) (Challenge, string, error) {
	nonce, err := randomToken()
	if err != nil {
		return Challenge{}, "", err
	}
	poll, err := randomToken()
	if err != nil {
		return Challenge{}, "", err
	}
	browser, err := randomToken()
	if err != nil {
		return Challenge{}, "", err
	}
	now := s.now().UTC()
	var result Challenge
	err = s.Store.Run(ctx, func(tx *metadata.Tx) error {
		b, err := loadBudget(tx)
		if err != nil {
			return err
		}
		window := now.Truncate(time.Minute)
		if window.After(b.Window) {
			b.Window, b.Count = window, 0
		}
		if b.Count >= 20 {
			return ErrLimited
		}
		b.Count++
		id := fmt.Sprintf("%04x", b.Challenges%256) + nonce[4:]
		b.Challenges++
		expires := now.Add(5 * time.Minute)
		message := fmt.Sprintf("Flux Drop Admin login\nOrigin: %s\nNonce: %s\nExpires: %s", s.Origin, id, expires.Format(time.RFC3339))
		if len(message) > 512 {
			return errors.New("admin origin is too long")
		}
		result = Challenge{ID: id, Message: message, Expires: expires, PollToken: poll}
		if err := tx.Set("admin_budget/global", b); err != nil {
			return err
		}
		return tx.Set("admin_challenges/"+id[:4], challengeRecord{ID: id, Message: message, PollDigest: digest(poll), BrowserDigest: digest(browser), Expires: expires})
	})
	return result, browser, err
}
func (s *Service) challenge(tx *metadata.Tx, id string) (challengeRecord, error) {
	key, err := slot(id, "admin_challenges/", 256)
	if err != nil {
		return challengeRecord{}, err
	}
	var c challengeRecord
	err = tx.Get(key, &c)
	if errors.Is(err, metadata.ErrNotFound) {
		return c, ErrUnauthorized
	}
	if err != nil {
		return c, err
	}
	if c.Used || c.ID != id || !s.now().Before(c.Expires) {
		return c, ErrUnauthorized
	}
	return c, nil
}
func (s *Service) Approve(ctx context.Context, message, signature string) error {
	// Signature verification occurs locally; no Flux credentials/API keys enter
	// the browser or another identity service.
	if !VerifyWallet(message, s.Address, signature) {
		return ErrUnauthorized
	}
	var id string
	for _, line := range strings.Split(message, "\n") {
		if len(line) == 71 && line[:7] == "Nonce: " {
			id = line[7:]
		}
	}
	return s.Store.Run(ctx, func(tx *metadata.Tx) error {
		c, err := s.challenge(tx, id)
		if err != nil {
			return err
		}
		if c.Message != message {
			return ErrUnauthorized
		}
		c.Approved = true
		return tx.Set("admin_challenges/"+id[:4], c)
	})
}
func (s *Service) Redeem(ctx context.Context, id, poll, browser, signature string) (string, Session, bool, error) {
	if !tokenPattern.MatchString(poll) || !tokenPattern.MatchString(browser) {
		return "", Session{}, false, ErrUnauthorized
	}
	raw, err := randomToken()
	if err != nil {
		return "", Session{}, false, err
	}
	csrfRaw, err := randomToken()
	if err != nil {
		return "", Session{}, false, err
	}
	csrfBytes, _ := hex.DecodeString(csrfRaw)
	csrf := base64.RawURLEncoding.EncodeToString(csrfBytes)
	var token string
	var result Session
	ready := false
	err = s.Store.Run(ctx, func(tx *metadata.Tx) error {
		c, err := s.challenge(tx, id)
		if err != nil {
			return err
		}
		if !equal(c.PollDigest, digest(poll)) || !equal(c.BrowserDigest, digest(browser)) {
			return ErrUnauthorized
		}
		if signature != "" {
			if !VerifyWallet(c.Message, s.Address, signature) {
				return ErrUnauthorized
			}
			c.Approved = true
		}
		if !c.Approved {
			ready = false
			return nil
		}
		b, err := loadBudget(tx)
		if err != nil {
			return err
		}
		token = fmt.Sprintf("%04x", b.Sessions%2048) + raw[4:]
		b.Sessions++
		result = Session{CSRF: csrf, Expires: s.now().UTC().Add(8 * time.Hour)}
		c.Used = true
		ready = true
		if err := tx.Set("admin_challenges/"+id[:4], c); err != nil {
			return err
		}
		if err := tx.Set("admin_budget/global", b); err != nil {
			return err
		}
		return tx.Set("admin_sessions/"+token[:4], sessionRecord{Digest: digest(token), Address: s.Address, CSRF: csrf, Expires: result.Expires})
	})
	return token, result, ready, err
}
func (s *Service) authorize(tx *metadata.Tx, token, csrf string) (Session, error) {
	key, err := slot(token, "admin_sessions/", 2048)
	if err != nil {
		return Session{}, err
	}
	var r sessionRecord
	err = tx.Get(key, &r)
	if errors.Is(err, metadata.ErrNotFound) {
		return Session{}, ErrUnauthorized
	}
	if err != nil {
		return Session{}, err
	}
	if r.Revoked || r.Address != s.Address || !s.now().Before(r.Expires) || !equal(r.Digest, digest(token)) || (csrf != "" && !equal(r.CSRF, csrf)) {
		return Session{}, ErrUnauthorized
	}
	return Session{CSRF: r.CSRF, Expires: r.Expires}, nil
}
func (s *Service) Read(ctx context.Context, token string) (Session, error) {
	var result Session
	err := s.Store.Run(ctx, func(tx *metadata.Tx) error { var err error; result, err = s.authorize(tx, token, ""); return err })
	return result, err
}
func (s *Service) RunAuthorized(ctx context.Context, token, csrf string, fn func(*metadata.Tx) error) error {
	if len(csrf) != 43 {
		return ErrUnauthorized
	}
	return s.Store.Run(ctx, func(tx *metadata.Tx) error {
		if _, err := s.authorize(tx, token, csrf); err != nil {
			return err
		}
		return fn(tx)
	})
}
func (s *Service) Logout(ctx context.Context, token, csrf string) error {
	return s.RunAuthorized(ctx, token, csrf, func(tx *metadata.Tx) error {
		var r sessionRecord
		key := "admin_sessions/" + token[:4]
		if err := tx.Get(key, &r); err != nil {
			return err
		}
		r.Revoked = true
		return tx.Set(key, r)
	})
}

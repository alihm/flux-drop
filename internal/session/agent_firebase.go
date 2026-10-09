package session

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

const SecureTokenJWKS = "https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com"

type AgentIdentity struct {
	UID, Email, Provider string
	ExpiresAt            time.Time
}

// AgentFirebaseVerifier verifies signed Firebase identities. Verify restricts
// consent to Google/password accounts; VerifyBearer leaves provider policy to
// the caller. Neither changes Drop's Google-only browser session verifier.
// JWKSURL/Client are injectable for hermetic tests, not browser/env input.
type AgentFirebaseVerifier struct {
	ProjectID      string
	JWKSURL        string
	Client         *http.Client
	Now            func() time.Time
	mu             sync.Mutex
	keys           map[string]*rsa.PublicKey
	until, fetched time.Time
}
type agentClaims struct {
	jwt.RegisteredClaims
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	AuthTime      int64  `json:"auth_time"`
	Firebase      struct {
		Provider string `json:"sign_in_provider"`
		Tenant   string `json:"tenant"`
	} `json:"firebase"`
}

func (v *AgentFirebaseVerifier) Verify(ctx context.Context, raw string) (AgentIdentity, error) {
	identity, err := v.verifyIDToken(ctx, raw, time.Minute)
	if err == nil && identity.Provider != "google.com" && identity.Provider != "password" {
		return AgentIdentity{}, ErrUnauthorized
	}
	return identity, err
}

// VerifyBearer verifies a live Firebase identity without requiring a recent
// auth_time. Callers must enforce their provider policy separately. Unlike the
// browser/consent verifier, this server-to-server API permits no future iat.
func (v *AgentFirebaseVerifier) VerifyBearer(ctx context.Context, raw string) (AgentIdentity, error) {
	return v.verifyIDToken(ctx, raw, 0)
}

func (v *AgentFirebaseVerifier) verifyIDToken(ctx context.Context, raw string, issuedAtSkew time.Duration) (AgentIdentity, error) {
	if len(raw) == 0 || len(raw) > 16<<10 {
		return AgentIdentity{}, ErrUnauthorized
	}
	now := time.Now()
	if v.Now != nil {
		now = v.Now()
	}
	claims := &agentClaims{}
	// Do not use jwt.TimeFunc (a process-global and unsafe to change concurrently).
	parser := jwt.Parser{ValidMethods: []string{"RS256"}, SkipClaimsValidation: true}
	parsed, err := parser.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		kid, ok := t.Header["kid"].(string)
		if !ok || kid == "" || len(kid) > 256 {
			return nil, ErrUnauthorized
		}
		return v.key(ctx, kid, now)
	})
	if err != nil || !parsed.Valid || claims.Issuer != "https://securetoken.google.com/"+v.ProjectID || len(claims.Audience) != 1 || claims.Audience[0] != v.ProjectID || claims.Subject == "" || len(claims.Subject) > 128 || claims.ExpiresAt == nil || !now.Before(claims.ExpiresAt.Time) || claims.ExpiresAt.Time.After(now.Add(time.Hour+time.Minute)) || claims.IssuedAt == nil || claims.IssuedAt.Time.After(now.Add(issuedAtSkew)) || (claims.NotBefore != nil && claims.NotBefore.Time.After(now)) || claims.AuthTime <= 0 || claims.AuthTime > now.Add(time.Minute).Unix() || !claims.EmailVerified || claims.Email == "" || claims.Firebase.Tenant != "" {
		return AgentIdentity{}, ErrUnauthorized
	}
	return AgentIdentity{UID: claims.Subject, Email: claims.Email, Provider: claims.Firebase.Provider, ExpiresAt: claims.ExpiresAt.Time}, nil
}
func (v *AgentFirebaseVerifier) key(ctx context.Context, kid string, now time.Time) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if now.Before(v.until) {
		if k := v.keys[kid]; k != nil {
			return k, nil
		}
		if now.Sub(v.fetched) < time.Minute {
			return nil, ErrUnauthorized
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	endpoint := v.JWKSURL
	if endpoint == "" {
		endpoint = SecureTokenJWKS
	}
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	client := v.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New("Firebase key service unavailable")
	}
	var document struct {
		Keys []struct{ Kty, Alg, Use, Kid, N, E string }
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 128<<10+1))
	if err != nil || len(data) > 128<<10 || json.Unmarshal(data, &document) != nil {
		return nil, errors.New("invalid Firebase keys")
	}
	keys := map[string]*rsa.PublicKey{}
	for _, j := range document.Keys {
		if j.Kty != "RSA" || j.Alg != "RS256" || j.Use != "sig" || j.Kid == "" {
			continue
		}
		n, e1 := base64.RawURLEncoding.DecodeString(j.N)
		e, e2 := base64.RawURLEncoding.DecodeString(j.E)
		if e1 != nil || e2 != nil || len(n) < 256 || len(e) == 0 || len(e) > 4 {
			continue
		}
		exponent := int64(0)
		for _, b := range e {
			exponent = exponent*256 + int64(b)
		}
		if exponent < 3 || exponent > 1<<31-1 || exponent%2 == 0 {
			continue
		}
		keys[j.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(exponent)}
	}
	if len(keys) == 0 {
		return nil, errors.New("empty Firebase keys")
	}
	v.keys, v.until, v.fetched = keys, now.Add(time.Hour), now
	if k := keys[kid]; k != nil {
		return k, nil
	}
	return nil, ErrUnauthorized
}

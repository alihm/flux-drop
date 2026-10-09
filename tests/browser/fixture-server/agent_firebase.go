package main

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	dropsession "github.com/runonflux/flux-drop/internal/session"
)

// Only compiled into the isolated browser fixture image. The production verifier
// still checks real RSA signatures, claims and refresh-token UID consistency;
// this transport supplies hermetic JWKS/securetoken responses instead of Google.
type agentFirebaseFixtureTransport struct {
	key      *rsa.PrivateKey
	fallback http.RoundTripper
}

func registerAgentFirebaseFixtures(mux *http.ServeMux) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	f := &agentFirebaseFixtureTransport{key: key, fallback: http.DefaultTransport}
	http.DefaultTransport = f
	mux.HandleFunc("GET /_agent-fixture/identity", func(w http.ResponseWriter, r *http.Request) {
		uid := randomToken()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]string{"uid": uid, "email": uid + "@example.test", "idToken": f.token(uid), "refreshToken": "browser-refresh-" + uid})
	})
}

func (f *agentFirebaseFixtureTransport) token(uid string) string {
	now := time.Now().Unix()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": "https://securetoken.google.com/fluxcore-prod", "aud": "fluxcore-prod", "sub": uid, "iat": now, "exp": now + 3600, "auth_time": now, "email": uid + "@example.test", "email_verified": true, "firebase": map[string]string{"sign_in_provider": "google.com"}})
	token.Header["kid"] = "browser-fixture"
	raw, err := token.SignedString(f.key)
	if err != nil {
		panic(err)
	}
	return raw
}

func (f *agentFirebaseFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var body any
	status := http.StatusOK
	switch {
	case r.URL.String() == dropsession.SecureTokenJWKS:
		body = map[string]any{"keys": []any{map[string]string{"kty": "RSA", "alg": "RS256", "use": "sig", "kid": "browser-fixture", "n": base64.RawURLEncoding.EncodeToString(f.key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(f.key.E)).Bytes())}}}
	case r.URL.Host == "securetoken.googleapis.com" && r.URL.Path == "/v1/token":
		if err := r.ParseForm(); err != nil {
			return nil, err
		}
		refresh := r.PostForm.Get("refresh_token")
		uid, ok := strings.CutPrefix(refresh, "browser-refresh-")
		if r.Method != "POST" || r.PostForm.Get("grant_type") != "refresh_token" || !ok || uid == "" {
			status, body = http.StatusBadRequest, map[string]string{"error": "invalid_grant"}
		} else {
			body = map[string]string{"id_token": f.token(uid), "refresh_token": refresh, "user_id": uid, "expires_in": "3600"}
		}
	default:
		return f.fallback.RoundTrip(r)
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(data))), Request: r}, nil
}

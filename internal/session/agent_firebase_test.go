package session

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

func TestAgentFirebaseJWKSVerification(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "key-1", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
	}))
	defer jwks.Close()
	now := time.Now().UTC().Truncate(time.Second)
	v := &AgentFirebaseVerifier{ProjectID: "fluxcore-prod", JWKSURL: jwks.URL, Now: func() time.Time { return now }}
	sign := func(edit func(jwt.MapClaims)) string {
		t.Helper()
		c := jwt.MapClaims{"iss": "https://securetoken.google.com/fluxcore-prod", "aud": "fluxcore-prod", "sub": "user", "iat": now.Unix(), "auth_time": now.Unix(), "exp": now.Add(time.Hour).Unix(), "email": "user@example.com", "email_verified": true, "firebase": map[string]string{"sign_in_provider": "google.com"}}
		if edit != nil {
			edit(c)
		}
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
		token.Header["kid"] = "key-1"
		raw, err := token.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	raw := sign(nil)
	for i := 0; i < 2; i++ {
		identity, err := v.Verify(context.Background(), raw)
		if err != nil || identity.UID != "user" || identity.Email != "user@example.com" || identity.Provider != "google.com" {
			t.Fatal(identity, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("JWKS not cached")
	}
	for _, edit := range []func(jwt.MapClaims){
		func(c jwt.MapClaims) { c["aud"] = "wrong-project" }, func(c jwt.MapClaims) { c["iss"] = "https://evil.example" }, func(c jwt.MapClaims) { c["sub"] = "" }, func(c jwt.MapClaims) { c["exp"] = now.Unix() }, func(c jwt.MapClaims) { c["iat"] = now.Add(2 * time.Minute).Unix() }, func(c jwt.MapClaims) { c["email_verified"] = false }, func(c jwt.MapClaims) { c["firebase"] = map[string]string{"sign_in_provider": "github.com"} }, func(c jwt.MapClaims) {
			c["firebase"] = map[string]string{"sign_in_provider": "google.com", "tenant": "other"}
		}, func(c jwt.MapClaims) { delete(c, "exp") }, func(c jwt.MapClaims) { c["aud"] = []string{"fluxcore-prod", "other"} },
	} {
		if _, err := v.Verify(context.Background(), sign(edit)); err == nil {
			t.Fatal("invalid claims accepted")
		}
	}
	password := sign(func(c jwt.MapClaims) { c["firebase"] = map[string]string{"sign_in_provider": "password"} })
	if _, err := v.Verify(context.Background(), password); err != nil {
		t.Fatal("password policy rejected", err)
	}
	if _, err := v.Verify(context.Background(), raw[:len(raw)-10]+"xxxxxxxxxx"); err == nil {
		t.Fatal("bad signature accepted")
	}
	unsigned := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{"sub": "user"})
	none, _ := unsigned.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if _, err := v.Verify(context.Background(), none); err == nil {
		t.Fatal("unsigned token accepted")
	}
}

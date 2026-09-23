package session

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"firebase.google.com/go/v4/auth"
)

func TestPublicVerifierNeedsNoCredentialsAndRejectsUnsigned(t *testing.T) {
	t.Setenv("FIREBASE_AUTH_EMULATOR_HOST", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/not-a-service-account.json")
	v, err := NewPublicFirebaseVerifier(context.Background(), "demo-drop-auth")
	if err != nil {
		t.Fatal("unexpected credential dependency", err)
	}
	encode := func(raw string) string { return base64.RawURLEncoding.EncodeToString([]byte(raw)) }
	now := time.Now().Unix()
	claims := fmt.Sprintf(`{"aud":"demo-drop-auth","iss":"https://securetoken.google.com/demo-drop-auth","sub":"attacker","iat":%d,"exp":%d,"auth_time":%d,"email_verified":true,"firebase":{"sign_in_provider":"google.com"}}`, now, now+3600, now)
	raw := encode(`{"alg":"none","typ":"JWT"}`) + "." + encode(claims) + "."
	for _, token := range []string{"", "invalid", raw} {
		if identity, err := v.VerifyGoogle(context.Background(), token); !errors.Is(err, ErrUnauthorized) || identity.UID != "" {
			t.Fatal(identity, err)
		}
	}
	t.Setenv("FIREBASE_AUTH_EMULATOR_HOST", "127.0.0.1:9099")
	if _, err := NewPublicFirebaseVerifier(context.Background(), "demo-drop-auth"); err == nil {
		t.Fatal("unsigned emulator mode allowed")
	}
}

func TestPublicVerifierClaimsAndSessionExpiry(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	valid := func() *auth.Token {
		return &auth.Token{UID: "alice", AuthTime: now.Unix(), IssuedAt: now.Unix(), Expires: now.Add(time.Hour).Unix(), Firebase: auth.FirebaseInfo{SignInProvider: "google.com"}, Claims: map[string]any{"email_verified": true}}
	}
	token := valid()
	v := &PublicFirebaseVerifier{verify: func(context.Context, string) (*auth.Token, error) { return token, nil }, now: func() time.Time { return now }}
	identity, err := v.VerifyGoogle(context.Background(), "verified-by-sdk")
	if err != nil || !identity.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatal(identity, err)
	}
	for _, change := range []func(*auth.Token){
		func(t *auth.Token) { t.Firebase.SignInProvider = "password" }, func(t *auth.Token) { t.Claims["email_verified"] = false },
		func(t *auth.Token) { t.Firebase.Tenant = "unexpected" }, func(t *auth.Token) { t.Expires = now.Unix() }, func(t *auth.Token) { t.Expires = now.Add(12 * time.Hour).Unix() },
		func(t *auth.Token) { t.UID = "" }, func(t *auth.Token) { t.AuthTime = 0 }, func(t *auth.Token) { t.AuthTime = now.Add(2 * time.Minute).Unix() },
	} {
		token = valid()
		change(token)
		if _, err := v.VerifyGoogle(context.Background(), "verified-by-sdk"); !errors.Is(err, ErrUnauthorized) {
			t.Fatal("bad claims accepted", err)
		}
	}
	token = valid()
	s, _, _ := testService()
	s.Now = func() time.Time { return now }
	s.Verifier = v
	old, view, err := s.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cookie, view, err := s.Login(context.Background(), old, view.Record.CSRF, "verified-by-sdk")
	if err != nil {
		t.Fatal(err)
	}
	if !view.Record.AuthUntil.Equal(now.Add(time.Hour)) {
		t.Fatal("session outlives ID token", view.Record.AuthUntil)
	}
	if read, err := s.Read(context.Background(), cookie); err != nil || !read.Authenticated {
		t.Fatal(read, err)
	}
	now = now.Add(time.Hour)
	if read, err := s.Read(context.Background(), cookie); err != nil || read.Authenticated {
		t.Fatal("expired identity still authenticated", read, err)
	}
	if err := v.CheckAccount(context.Background(), identity); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("expired account check accepted", err)
	}
	if err := v.CheckAccount(context.Background(), Identity{UID: "alice"}); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("unbounded session accepted", err)
	}
}

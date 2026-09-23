package session

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	firebase "firebase.google.com/go/v4"
	"google.golang.org/api/option"
)

func TestFirebaseVerifierRejectsUnsignedAndMalformedTokens(t *testing.T) {
	// The Auth emulator intentionally accepts unsigned tokens. This test must
	// exercise the real verifier, not that emulator behavior.
	t.Setenv("FIREBASE_AUTH_EMULATOR_HOST", "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: "demo-verifier"}, option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	client, err := app.Auth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v := &FirebaseVerifier{Client: client}
	encode := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	now := time.Now().Unix()
	claims := fmt.Sprintf(`{"aud":"demo-verifier","iss":"https://securetoken.google.com/demo-verifier","sub":"attacker","iat":%d,"exp":%d,"auth_time":%d,"firebase":{"sign_in_provider":"google.com"}}`, now, now+3600, now)
	unsigned := encode(`{"alg":"none","typ":"JWT"}`) + "." + encode(claims) + "."
	for _, raw := range []string{"", "not-a-token", unsigned} {
		identity, err := v.VerifyGoogle(ctx, raw)
		if !errors.Is(err, ErrUnauthorized) || identity.UID != "" {
			t.Fatal("unverified token produced an identity")
		}
	}
}

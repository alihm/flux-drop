package session

import (
	"context"
	"errors"
	"os"
	"regexp"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/auth"
	"google.golang.org/api/option"
)

// PublicFirebaseVerifier uses the SDK's public Google certificate verification,
// explicitly without service credentials. It never reads/writes Firestore,
// fetches user records, creates Firebase cookies or checks admin revocation state.
// App session expiry must not exceed the verified token expiry.
type PublicFirebaseVerifier struct {
	verify func(context.Context, string) (*auth.Token, error)
	now    func() time.Time
}

func NewPublicFirebaseVerifier(ctx context.Context, projectID string) (*PublicFirebaseVerifier, error) {
	if !regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`).MatchString(projectID) {
		return nil, errors.New("invalid Firebase project ID")
	}
	// The SDK's emulator mode accepts unsigned tokens. This verifier has NO
	// emulator escape hatch, even if development environment variables are set.
	if os.Getenv("FIREBASE_AUTH_EMULATOR_HOST") != "" {
		return nil, errors.New("public-key Firebase verifier refuses unsigned Auth emulator mode")
	}
	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: projectID}, option.WithoutAuthentication())
	if err != nil {
		return nil, err
	}
	client, err := app.Auth(ctx)
	if err != nil {
		return nil, err
	}
	return &PublicFirebaseVerifier{verify: client.VerifyIDToken, now: time.Now}, nil
}

func (v *PublicFirebaseVerifier) VerifyGoogle(ctx context.Context, raw string) (Identity, error) {
	if raw == "" || len(raw) > 16<<10 {
		return Identity{}, ErrUnauthorized
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	token, err := v.verify(ctx, raw)
	if err != nil {
		if auth.IsIDTokenInvalid(err) || auth.IsIDTokenExpired(err) {
			return Identity{}, ErrUnauthorized
		}
		return Identity{}, err
	}
	now := v.now()
	if token == nil || token.UID == "" || token.Firebase.SignInProvider != "google.com" || token.Firebase.Tenant != "" || token.Claims["email_verified"] != true || token.AuthTime <= 0 || token.Expires <= now.Unix() || token.IssuedAt > now.Unix()+60 || token.AuthTime > now.Unix()+60 || token.Expires > now.Add(time.Hour+time.Minute).Unix() {
		return Identity{}, ErrUnauthorized
	}
	return Identity{UID: token.UID, AuthTime: time.Unix(token.AuthTime, 0).UTC(), ExpiresAt: time.Unix(token.Expires, 0).UTC()}, nil
}

// CheckAccount implements ONLY the local bounded-session policy. It is not an
// account lookup: Firebase-side disable/revocation becomes effective no later
// than token/session expiry and subsequent token renewal, not instantaneously.
func (v *PublicFirebaseVerifier) CheckAccount(ctx context.Context, identity Identity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := v.now()
	if identity.UID == "" || identity.ExpiresAt.IsZero() || !now.Before(identity.ExpiresAt) || identity.ExpiresAt.After(now.Add(time.Hour+time.Minute)) {
		return ErrUnauthorized
	}
	return nil
}

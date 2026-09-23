package session

import (
	"context"
	"time"

	"firebase.google.com/go/v4/auth"
)

type FirebaseVerifier struct{ Client *auth.Client }

func (v *FirebaseVerifier) VerifyGoogle(ctx context.Context, raw string) (Identity, error) {
	token, err := v.Client.VerifyIDTokenAndCheckRevoked(ctx, raw)
	if err != nil {
		if auth.IsIDTokenInvalid(err) || auth.IsIDTokenExpired(err) || auth.IsIDTokenRevoked(err) || auth.IsUserDisabled(err) || auth.IsUserNotFound(err) {
			return Identity{}, ErrUnauthorized
		}
		return Identity{}, err
	}
	if token.Firebase.SignInProvider != "google.com" {
		return Identity{}, ErrUnauthorized
	}
	return Identity{UID: token.UID, AuthTime: time.Unix(token.AuthTime, 0).UTC()}, nil
}

func (v *FirebaseVerifier) CheckAccount(ctx context.Context, identity Identity) error {
	user, err := v.Client.GetUser(ctx, identity.UID)
	if err != nil {
		if auth.IsUserNotFound(err) {
			return ErrUnauthorized
		}
		return err
	}
	if user.Disabled || identity.AuthTime.UnixMilli() < user.TokensValidAfterMillis {
		return ErrUnauthorized
	}
	return nil
}

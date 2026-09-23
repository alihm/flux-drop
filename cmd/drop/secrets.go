package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/runonflux/flux-drop/internal/firebaseconfig"
)

// Protected Flux environment configuration can carry the credential where file
// mounts are unavailable. Decode only at runtime outside the replicated volume.
// Base64 is encoding, NOT encryption: never use it in a public application spec.
func runtimeCredentials() (func(), error) {
	raw := os.Getenv("DROP_FIREBASE_CREDENTIALS_B64")
	if raw == "" {
		return func() {}, nil
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" || len(raw) > 128<<10 {
		return nil, errors.New("ambiguous or oversized Firebase runtime credentials")
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, errors.New("invalid base64 Firebase credentials")
	}
	var account struct {
		Type    string `json:"type"`
		Project string `json:"project_id"`
		Key     string `json:"private_key"`
		Email   string `json:"client_email"`
	}
	if json.Unmarshal(decoded, &account) != nil || account.Type != "service_account" || account.Project != firebaseconfig.ProjectFromEnv(os.Getenv) || account.Key == "" || account.Email == "" {
		return nil, errors.New("Firebase credentials do not match the configured project")
	}
	directory, err := os.MkdirTemp("", "drop-credentials-")
	if err != nil {
		return nil, err
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	file := filepath.Join(directory, "firebase.json")
	if err := os.WriteFile(file, decoded, 0600); err != nil {
		cleanup()
		return nil, err
	}
	if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", file); err != nil {
		cleanup()
		return nil, err
	}
	_ = os.Unsetenv("DROP_FIREBASE_CREDENTIALS_B64")
	return cleanup, nil
}

package main

import (
	"errors"
	"strings"
)

// Production has exactly one authority: Raft. Legacy Firestore is retained only
// for isolated emulator regression tests, never as an outage fallback.
func legacyMetadata(get func(string) string) (bool, error) {
	if get("GOOGLE_APPLICATION_CREDENTIALS") != "" || get("DROP_FIREBASE_CREDENTIALS_B64") != "" {
		return false, errors.New("Firebase service credentials are no longer used; remove them from Drop configuration")
	}
	switch get("DROP_METADATA_BACKEND") {
	case "", "raft":
		if get("FIRESTORE_EMULATOR_HOST") != "" || get("FIREBASE_AUTH_EMULATOR_HOST") != "" {
			return false, errors.New("Raft runtime refuses Firebase emulators")
		}
		return false, nil
	case "firestore-emulator":
		if get("DROP_ENV") != "development" || !strings.HasPrefix(get("FIREBASE_PROJECT_ID"), "demo-") || get("FIRESTORE_EMULATOR_HOST") == "" || get("FIREBASE_AUTH_EMULATOR_HOST") == "" {
			return false, errors.New("legacy backend requires isolated development emulators")
		}
		return true, nil
	default:
		return false, errors.New("unknown metadata backend")
	}
}

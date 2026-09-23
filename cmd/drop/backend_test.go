package main

import "testing"

func TestMetadataBackendSelection(t *testing.T) {
	for _, tc := range []struct {
		values       map[string]string
		legacy, fail bool
	}{
		{map[string]string{}, false, false},
		{map[string]string{"DROP_FIREBASE_CREDENTIALS_B64": "secret"}, false, true},
		{map[string]string{"GOOGLE_APPLICATION_CREDENTIALS": "/secret"}, false, true},
		{map[string]string{"DROP_METADATA_BACKEND": "firestore"}, false, true},
		{map[string]string{"DROP_METADATA_BACKEND": "firestore-emulator"}, false, true},
		{map[string]string{"FIREBASE_AUTH_EMULATOR_HOST": "localhost:9099"}, false, true},
		{map[string]string{"DROP_METADATA_BACKEND": "firestore-emulator", "DROP_ENV": "development", "FIREBASE_PROJECT_ID": "demo-drop", "FIRESTORE_EMULATOR_HOST": "localhost:8080", "FIREBASE_AUTH_EMULATOR_HOST": "localhost:9099"}, true, false},
	} {
		legacy, err := legacyMetadata(func(k string) string { return tc.values[k] })
		if legacy != tc.legacy || (err != nil) != tc.fail {
			t.Fatalf("selection legacy=%v err=%v", legacy, err)
		}
	}
}

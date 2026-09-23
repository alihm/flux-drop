package main

import (
	"encoding/base64"
	"os"
	"strings"
	"testing"
)

func TestRuntimeCredentials(t *testing.T) {
	t.Setenv("FIREBASE_PROJECT_ID", "demo-test")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("DROP_FIREBASE_CREDENTIALS_B64", base64.StdEncoding.EncodeToString([]byte(`{"type":"service_account","project_id":"demo-test","private_key":"test-key","client_email":"test@example.test"}`)))
	cleanup, err := runtimeCredentials()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	path := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 || !strings.HasPrefix(path, os.TempDir()) {
		t.Fatal(path, err)
	}
	if os.Getenv("DROP_FIREBASE_CREDENTIALS_B64") != "" {
		t.Fatal("environment not cleared")
	}
	cleanup()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("credential file retained")
	}
}

func TestRuntimeCredentialsDefaultProject(t *testing.T) {
	t.Setenv("FIREBASE_PROJECT_ID", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	for _, project := range []string{"wrong-project", "fluxcore-prod"} {
		t.Setenv("DROP_FIREBASE_CREDENTIALS_B64", base64.StdEncoding.EncodeToString([]byte(`{"type":"service_account","project_id":"`+project+`","private_key":"test-key","client_email":"test@example.test"}`)))
		cleanup, err := runtimeCredentials()
		if project == "wrong-project" {
			if err == nil {
				cleanup()
				t.Fatal("accepted credentials for unrelated project")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		cleanup()
	}
}

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublishingConfiguration(t *testing.T) {
	env := map[string]string{"DROP_PUBLISHING_ENABLED": "true", "FIREBASE_PROJECT_ID": "demo-drop", "DROP_DATA_DIR": "/data", "DROP_STAGING_USER": "tester", "DROP_STAGING_PASSWORD": strings.Repeat("x", 24)}
	get := func(k string) string { return env[k] }
	if c, err := publishingFromEnv(get); err != nil || !c.enabled {
		t.Fatal(c, err)
	}
	delete(env, "FIREBASE_PROJECT_ID")
	if _, err := publishingFromEnv(get); err != nil {
		t.Fatal("default Firebase project rejected", err)
	}
	for key, value := range map[string]string{"DROP_PUBLISHING_ENABLED": "yes", "DROP_DATA_DIR": "/", "DROP_STAGING_PASSWORD": "short", "DROP_ANONYMOUS_BYTE_LIMIT": "-1", "DROP_ACCOUNT_BYTE_LIMIT": "999999999999999999999"} {
		old := env[key]
		env[key] = value
		if _, err := publishingFromEnv(get); err == nil {
			t.Fatal("accepted", key)
		}
		env[key] = old
	}
}
func TestStorageRejectsSymlinkRoot(t *testing.T) {
	root := t.TempDir()
	linked := filepath.Join(t.TempDir(), "linked-data")
	if err := os.Symlink(root, linked); err != nil {
		t.Fatal(err)
	}
	if err := validateStorage(publishingConfig{root: linked}); err == nil {
		t.Fatal("symlink data root allowed")
	}
}

func TestPublishingDefaults(t *testing.T) {
	get := func(key string) string {
		if key == "DROP_STAGING_PASSWORD" {
			return strings.Repeat("x", 24)
		}
		return ""
	}
	c, err := publishingFromEnv(get)
	if err != nil || !c.enabled || c.user != "tester" || c.root != "/data" {
		t.Fatal(c, err)
	}
	if _, err := publishingFromEnv(func(string) string { return "" }); err == nil {
		t.Fatal("default shared password must not exist")
	}
	c, err = publishingFromEnv(func(key string) string {
		if key == "DROP_PUBLISHING_ENABLED" {
			return "false"
		}
		return ""
	})
	if err != nil || c.enabled {
		t.Fatal("explicit disable ignored", err)
	}
}

func TestSessionCreationBudget(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int64
		bad   bool
	}{{"", 60, false}, {"250", 250, false}, {"0", 0, true}, {"10001", 0, true}, {"nope", 0, true}} {
		got, err := sessionCreationBudget(func(string) string { return tc.value })
		if (err != nil) != tc.bad || (!tc.bad && got != tc.want) {
			t.Fatalf("%q: %d, %v", tc.value, got, err)
		}
	}
}

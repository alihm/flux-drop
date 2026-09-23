package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRuntimeEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name        string
		env         map[string]string
		origin, app string
		probe       bool
	}{
		{"injected", map[string]string{"FLUX_APP_NAME": "MyDrop"}, "https://mydrop.app.runonflux.io", "MyDrop", false},
		{"explicit", map[string]string{"APP_NAME": "drop-test"}, "https://drop-test.app.runonflux.io", "drop-test", false},
		{"hostinfo", map[string]string{}, "https://discovered.app.runonflux.io", "discovered", true},
		{"custom", map[string]string{"DROP_PUBLIC_ORIGIN": "https://custom.example"}, "https://custom.example", "", false},
		{"precedence", map[string]string{"FLUX_APP_NAME": "injected", "APP_NAME": "other", "DROP_PUBLIC_ORIGIN": "https://custom.example"}, "https://custom.example", "injected", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			get, err := runtimeEnvironment(context.Background(), func(k string) string { return tc.env[k] }, func(context.Context) (string, error) { called = true; return "discovered", nil })
			if err != nil {
				t.Fatal(err)
			}
			if called != tc.probe || get("DROP_PUBLIC_ORIGIN") != tc.origin || get("FLUX_APP_NAME") != tc.app {
				t.Fatal("wrong resolution")
			}
			for k, v := range map[string]string{"DROP_ENV": "production", "DROP_DATA_DIR": "/data", "DROP_PEERS_ENABLED": "false", "DROP_MAINTENANCE_ENABLED": "false"} {
				if get(k) != v {
					t.Fatal(k)
				}
			}
		})
	}
}

func TestRuntimeEnvironmentRejectsUnsafeInputs(t *testing.T) {
	for _, env := range []map[string]string{
		{"FLUX_APP_NAME": "evil.example/path"}, {"APP_NAME": "-invalid"}, {"FLUX_APP_NAME": strings.Repeat("a", 64)},
		{"DROP_PUBLIC_ORIGIN": "http://insecure.example"}, {"DROP_PUBLIC_ORIGIN": "https://example.com/path"},
	} {
		_, err := runtimeEnvironment(context.Background(), func(k string) string { return env[k] }, func(context.Context) (string, error) { t.Fatal("unexpected probe"); return "", nil })
		if err == nil {
			t.Fatal("accepted unsafe configuration", env)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtimeEnvironment(ctx, func(string) string { return "" }, func(context.Context) (string, error) { return "", errors.New("unavailable") }); err == nil {
		t.Fatal("guessed origin on discovery failure")
	}
}

func TestRuntimeEnvironmentRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	calls := 0
	get, err := runtimeEnvironment(ctx, func(string) string { return "" }, func(context.Context) (string, error) {
		calls++
		if calls == 1 {
			return "", errors.New("not ready")
		}
		return "recovered", nil
	})
	if err != nil || calls != 2 || get("FLUX_APP_NAME") != "recovered" {
		t.Fatal(calls, err)
	}
}

func TestParseHostInfo(t *testing.T) {
	for _, body := range []string{`{"appName":"drop"}`, `{"status":"success","data":{"appName":"drop"}}`} {
		if app, err := parseHostInfo(strings.NewReader(body)); err != nil || app != "drop" {
			t.Fatal(app, err)
		}
	}
	for _, body := range []string{`{}`, `{"appName":"evil/path"}`, `{"status":"error","appName":"drop"}`, `{"appName":"drop"} {}`, `not json`} {
		if _, err := parseHostInfo(strings.NewReader(body)); err == nil {
			t.Fatal("accepted", body)
		}
	}
}

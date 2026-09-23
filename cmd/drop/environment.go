package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/httpserver"
)

var fluxAppLabel = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)

// Resolve the origin from trusted runtime configuration, never request headers.
func runtimeEnvironment(ctx context.Context, get func(string) string, probe func(context.Context) (string, error)) (func(string) string, error) {
	values := map[string]string{"DROP_ENV": "production", "DROP_DATA_DIR": "/data", "DROP_PEERS_ENABLED": "false", "DROP_MAINTENANCE_ENABLED": "false"}
	if get("DROP_CLUSTER_PASSPHRASE") != "" {
		// Automatic peer configuration is injected from the generated manifest,
		// not parsed as a partially configured manual peer environment.
		values["DROP_PEERS_ENABLED"] = ""
		values["DROP_MAINTENANCE_ENABLED"] = "true"
	}
	app := get("FLUX_APP_NAME")
	if app == "" {
		app = get("APP_NAME")
	}
	origin := get("DROP_PUBLIC_ORIGIN")
	if app == "" && (origin == "" || get("DROP_PEERS_ENABLED") == "true") {
		for {
			var err error
			app, err = probe(ctx)
			if err == nil && app != "" {
				break
			}
			select {
			case <-ctx.Done():
				return nil, errors.New("Flux app discovery failed; set FLUX_APP_NAME or DROP_PUBLIC_ORIGIN (and FLUX_APP_NAME for peers)")
			case <-time.After(time.Second):
			}
		}
	}
	if app != "" && !fluxAppLabel.MatchString(app) {
		return nil, errors.New("invalid Flux application name")
	}
	if origin == "" {
		origin = "https://" + strings.ToLower(app) + ".app.runonflux.io"
	}
	if err := (httpserver.Config{PublicOrigin: origin, Limits: content.DefaultLimits()}).Validate(); err != nil {
		return nil, err
	}
	values["FLUX_APP_NAME"] = app
	values["DROP_PUBLIC_ORIGIN"] = origin
	return func(key string) string {
		if value := get(key); value != "" {
			return value
		}
		return values[key]
	}, nil
}

func hostInfoAppName(ctx context.Context) (string, error) {
	// Bypass ambient HTTP proxies and redirects: this is a local Flux service.
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://fluxnode.service:16101/hostinfo", nil)
	if err != nil {
		return "", err
	}
	response, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", errors.New("hostinfo unavailable")
	}
	return parseHostInfo(io.LimitReader(response.Body, 64<<10))
}

func parseHostInfo(reader io.Reader) (string, error) {
	var info struct {
		Status  string `json:"status"`
		AppName string `json:"appName"`
		Data    struct {
			AppName string `json:"appName"`
		} `json:"data"`
	}
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(&info); err != nil {
		return "", errors.New("invalid Flux hostinfo JSON")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return "", errors.New("trailing Flux hostinfo data")
	}
	if info.Status != "" && info.Status != "success" {
		return "", errors.New("Flux hostinfo request failed")
	}
	app := info.AppName
	if app == "" {
		app = info.Data.AppName
	}
	if !fluxAppLabel.MatchString(app) {
		return "", errors.New("invalid Flux hostinfo application name")
	}
	return app, nil
}

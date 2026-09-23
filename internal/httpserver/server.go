// Package httpserver owns the public API and project-delivery boundaries.
package httpserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/password"
)

type Config struct {
	PublicOrigin string
	Limits       content.Limits
}

func (c Config) Validate() error {
	u, err := url.Parse(c.PublicOrigin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || u.Opaque != "" || u.ForceQuery || strings.ContainsAny(c.PublicOrigin, "\r\n\\") {
		return fmt.Errorf("DROP_PUBLIC_ORIGIN must be an HTTPS origin without a path, credentials, query, or fragment")
	}
	return c.Limits.Validate()
}

func New(c Config) (http.Handler, error) {
	return NewWithDependencies(c, Dependencies{})
}

func NewWithDependencies(c Config, dependencies Dependencies) (http.Handler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if err := dependencies.FirebaseWeb.Validate(); err != nil {
		return nil, err
	}
	if dependencies.FirebaseWeb != nil && dependencies.Sessions == nil {
		return nil, fmt.Errorf("Google browser login requires server sessions")
	}
	if dependencies.Sessions != nil && (dependencies.Sessions.Store == nil || dependencies.Sessions.Verifier == nil) {
		return nil, fmt.Errorf("session storage and identity verifier must both be configured")
	}
	mux := http.NewServeMux()
	mux.Handle("/{$}", HomePageWithAuth(dependencies.FirebaseWeb))
	mux.Handle("/unlock/", UnlockPage())
	var privateAccess PrivateAccess
	if dependencies.Sessions != nil {
		registerSessions(mux, c.PublicOrigin, dependencies.Sessions)
	}
	if dependencies.Projects != nil {
		if dependencies.Sessions == nil || dependencies.Projects.Repository == nil || dependencies.Projects.DataRoot == "" {
			return nil, fmt.Errorf("project APIs require sessions, repository, and storage")
		}
		hasher := password.NewHasher()
		registerProjects(mux, c, dependencies, hasher)
		if grants, ok := dependencies.Projects.Repository.(grantRepository); ok {
			privateAccess = registerUnlock(mux, c.PublicOrigin, dependencies, grants, hasher)
		}
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if dependencies.Projects != nil && dependencies.Readiness != nil {
			if err := dependencies.Readiness(r.Context()); err == nil {
				respond(w, http.StatusOK, map[string]string{"status": "ready"})
				return
			}
			respond(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "reason": "dependency_unavailable"})
			return
		}
		respond(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "reason": "publishing_not_configured"})
	})
	mux.HandleFunc("GET /api/config", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, struct {
			PublicOrigin          string             `json:"publicOrigin"`
			PublishingEnabled     bool               `json:"publishingEnabled"`
			AuthenticationEnabled bool               `json:"authenticationEnabled"`
			Limits                content.Limits     `json:"limits"`
			Firebase              *FirebaseWebConfig `json:"firebase,omitempty"`
		}{PublicOrigin: c.PublicOrigin, Limits: c.Limits, PublishingEnabled: dependencies.Projects != nil, AuthenticationEnabled: dependencies.Sessions != nil, Firebase: dependencies.FirebaseWeb})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if dependencies.Projects != nil && deliverySlug.MatchString(strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")[0]) {
			ProjectDeliveryWithAccess(dependencies.Projects.Repository, dependencies.Projects.DataRoot, dependencies.Fallback, privateAccess).ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
		mux.ServeHTTP(w, r)
	}), nil
}

func respond(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// RequireBrowserMutation is the first gate for future cookie-authenticated
// mutations. Session authorization and synchronizer CSRF verification must still
// happen inside next. It must not be used as a substitute for either check.
func RequireBrowserMutation(origin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodPatch && r.Method != http.MethodDelete {
			respond(w, http.StatusMethodNotAllowed, map[string]string{"error": "method_not_allowed"})
			return
		}
		origins := r.Header.Values("Origin")
		if len(origins) != 1 || origins[0] != origin || origin == "" || origin == "null" {
			respond(w, http.StatusForbidden, map[string]string{"error": "untrusted_origin"})
			return
		}
		if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" {
			respond(w, http.StatusForbidden, map[string]string{"error": "untrusted_origin"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

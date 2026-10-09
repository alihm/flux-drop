package main

import (
	"context"
	"net/http"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/httpserver"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

// Real consent rendering, cookies and grant transactions, with test-only CAS
// metadata and signed Firebase fixtures. No real Firebase identity is used.
func registerAgentFixtures(mux *http.ServeMux) {
	registerAgentFirebaseFixtures(mux)
	config, err := httpserver.AgentAuthFromEnv(func(key string) string {
		if key == "DROP_AGENT_AUTHORIZE_URL" {
			return "https://127.0.0.1:18443/apps/oauth/authorize"
		}
		return ""
	})
	if err != nil {
		panic(err)
	}
	firebase, err := httpserver.FirebaseWebFromEnv(func(string) string { return "" })
	if err != nil {
		panic(err)
	}
	repo := &project.RaftRepository{Store: &metadata.Store{Backend: &testmetadata.Backend{}}}
	service, err := httpserver.NewAgentAuth(context.Background(), config, "https://localhost:18443", "a private browser fixture secret 1234567890", repo, firebase)
	if err != nil {
		panic(err)
	}
	handler, err := httpserver.NewWithDependencies(httpserver.Config{PublicOrigin: "https://localhost:18443", Limits: content.DefaultLimits()}, httpserver.Dependencies{AgentAuth: service})
	if err != nil {
		panic(err)
	}
	mux.Handle("/oauth/", handler)
	mux.Handle("/.well-known/", handler)
	// The website changes only the routing path. Preserve query, body, browser
	// headers and host-only cookie exactly as the production forwarder does.
	mux.HandleFunc("/apps/oauth/authorize", func(w http.ResponseWriter, r *http.Request) {
		forward := r.Clone(r.Context())
		forward.URL.Path = "/oauth/authorize"
		forward.URL.RawPath = ""
		handler.ServeHTTP(w, forward)
	})
}

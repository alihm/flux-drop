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

// Real consent rendering, cookies and denial transactions, with a test-only CAS
// backend. Firebase sign-in is intentionally not simulated as a real identity.
func registerAgentFixtures(mux *http.ServeMux) {
	config, err := httpserver.AgentAuthFromEnv(func(string) string { return "" })
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
}

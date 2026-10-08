package main

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/httpserver"
	"github.com/runonflux/flux-drop/internal/project"
)

// Exercise the real delivery filter behind Nginx; storage is a deterministic
// fixture, so the browser checks do not depend on Flux or Firebase availability.
type watermarkFixture struct{}

const watermarkDocument = `<!doctype html><html><head><title>Published site</title><style>*{font-size:40px;color:red}body{margin:0;background:#eef2ff}</style></head><body><main><h1>My deployed site</h1><button onclick="this.textContent='Works'">Try it</button></main></body></html>`

func (watermarkFixture) Resolve(_ context.Context, slug string) (project.Project, error) {
	if slug != "watermark-demo" && slug != "watermark-disabled" {
		return project.Project{}, project.ErrNotFound
	}
	return project.Project{ID: strings.Repeat("a", 32), Slug: slug, ActiveDigest: strings.Repeat("b", 64), StorageApp: "fixture", Status: "active", WatermarkDisabled: slug == "watermark-disabled"}, nil
}
func (watermarkFixture) ServeProject(w http.ResponseWriter, r *http.Request, _ project.Project, name string) {
	if name != "index.html" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	http.ServeContent(w, r, name, time.Time{}, strings.NewReader(watermarkDocument))
}
func registerWatermarkFixtures(mux *http.ServeMux) {
	fixture := watermarkFixture{}
	h := httpserver.ProjectDeliveryWithFallback(fixture, "", fixture)
	mux.Handle("/watermark-demo/", h)
	mux.Handle("/watermark-disabled/", h)
}

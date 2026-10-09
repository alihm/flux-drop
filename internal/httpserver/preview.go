package httpserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/runonflux/flux-drop/internal/preview"
	"github.com/runonflux/flux-drop/internal/project"
)

const previewPlaceholder = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 360"><defs><linearGradient id="a" x2="1" y2="1"><stop stop-color="#e9f5f1"/><stop offset="1" stop-color="#eaf0fb"/></linearGradient></defs><path fill="url(#a)" d="M0 0h640v360H0z"/><rect x="120" y="62" width="400" height="236" rx="16" fill="#fff" opacity=".8"/><path stroke="#c2ded7" stroke-width="8" stroke-linecap="round" d="M150 88h40m16 0h4m16 0h4"/><path stroke="#dfebe7" stroke-width="12" stroke-linecap="round" d="M150 128h196m-196 30h134m-134 32h338m-338 30h290m-290 30h216"/></svg>`

func registerPreviews(mux *http.ServeMux, deps Dependencies) {
	mux.HandleFunc("GET /api/explore", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Expose-Headers", "*")
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		rows, err := measure(r, "metadata", func() ([]preview.Card, error) { return deps.Previews.CachedExplore(ctx) })
		if err != nil {
			respond(w, 503, map[string]string{"error": "explore_unavailable"})
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=20, stale-while-revalidate=120")
		respond(w, 200, map[string]any{"projects": rows})
	})
	mux.HandleFunc("GET /api/projects/{id}/thumbnail", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Expose-Headers", "*")
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		// All public preview policy and file IO share one consistent transaction.
		// A concurrent policy change retries before any body or 304 is sent.
		var data []byte
		pending := false
		p, err := measure(r, "metadata", func() (project.Project, error) {
			return deps.Previews.ReadProject(ctx, r.PathValue("id"), func(p project.Project) error {
				data = nil
				pending = false
				if p.Private {
					if deps.Sessions == nil || deps.Projects == nil {
						return project.ErrNotFound
					}
					token, e := cookieToken(r)
					if e != nil {
						return project.ErrNotFound
					}
					view, e := deps.Sessions.Read(ctx, token)
					if e != nil {
						return project.ErrNotFound
					}
					actor, e := project.ActorFrom(token, view)
					if e != nil {
						return project.ErrNotFound
					}
					owned, e := deps.Projects.Repository.GetOwned(ctx, actor, p.ID)
					if e != nil || owned.PolicyRevision != p.PolicyRevision {
						return project.ErrNotFound
					}
				}
				if version := r.URL.Query().Get("v"); version != "" && version != p.ActiveDigest {
					return project.ErrNotFound
				}
				_, e := measure(r, "file", func() (bool, error) {
					root, e := os.OpenRoot(deps.Previews.Root)
					if e != nil {
						return false, e
					}
					defer root.Close()
					file, e := root.Open(p.ID + "-" + p.ActiveDigest + ".jpg")
					if os.IsNotExist(e) {
						pending = true
						return true, nil
					}
					if e != nil {
						return false, e
					}
					defer file.Close()
					info, e := file.Stat()
					if e != nil {
						return false, e
					}
					if !info.Mode().IsRegular() || info.Size() > 64<<10 {
						return false, project.ErrStorage
					}
					data, e = io.ReadAll(io.LimitReader(file, (64<<10)+1))
					if e != nil {
						return false, e
					}
					if int64(len(data)) != info.Size() {
						return false, project.ErrStorage
					}
					return true, nil
				})
				return e
			})
		})
		if err != nil {
			if errors.Is(err, project.ErrNotFound) {
				http.NotFound(w, r)
			} else {
				w.WriteHeader(503)
			}
			return
		}
		if !p.Private {
			w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
		}
		if pending {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "image/svg+xml")
			w.Header().Set("X-Drop-Preview", "pending")
			w.Header().Set("Retry-After", "10")
			if r.Method != "HEAD" {
				_, _ = io.WriteString(w, previewPlaceholder)
			}
			return
		}
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("X-Drop-Preview", "ready")
		if !p.Private {
			w.Header().Set("Cache-Control", "public, max-age=300, stale-while-revalidate=86400")
			w.Header().Set("ETag", `"`+p.ActiveDigest+`"`)
			w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
			if notModified(r, w.Header().Get("ETag")) {
				w.WriteHeader(304)
				return
			}
		} else {
			w.Header().Set("Cache-Control", "no-store")
		}
		if r.Method != "HEAD" {
			_, _ = w.Write(data)
		}
	})
}

package httpserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/runonflux/flux-drop/internal/project"
)

const previewPlaceholder = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 360"><defs><linearGradient id="a" x2="1" y2="1"><stop stop-color="#e9f5f1"/><stop offset="1" stop-color="#eaf0fb"/></linearGradient></defs><path fill="url(#a)" d="M0 0h640v360H0z"/><rect x="120" y="62" width="400" height="236" rx="16" fill="#fff" opacity=".8"/><path stroke="#c2ded7" stroke-width="8" stroke-linecap="round" d="M150 88h40m16 0h4m16 0h4"/><path stroke="#dfebe7" stroke-width="12" stroke-linecap="round" d="M150 128h196m-196 30h134m-134 32h338m-338 30h290m-290 30h216"/></svg>`

func registerPreviews(mux *http.ServeMux, deps Dependencies) {
	mux.HandleFunc("GET /api/explore", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		rows, err := deps.Previews.Explore(ctx)
		if err != nil {
			respond(w, 503, map[string]string{"error": "explore_unavailable"})
			return
		}
		respond(w, 200, map[string]any{"projects": rows})
	})
	mux.HandleFunc("GET /api/projects/{id}/thumbnail", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		authorize := func() (project.Project, error) {
			p, err := deps.Previews.Lookup(ctx, r.PathValue("id"))
			if err != nil {
				return p, err
			}
			if p.Private {
				if deps.Sessions == nil || deps.Projects == nil {
					return p, project.ErrNotFound
				}
				token, e := cookieToken(r)
				if e != nil {
					return p, project.ErrNotFound
				}
				view, e := deps.Sessions.Read(ctx, token)
				if e != nil {
					return p, project.ErrNotFound
				}
				actor, e := project.ActorFrom(token, view)
				if e != nil {
					return p, project.ErrNotFound
				}
				p, e = deps.Projects.Repository.GetOwned(ctx, actor, p.ID)
				if e != nil {
					return p, project.ErrNotFound
				}
			}
			if version := r.URL.Query().Get("v"); version != "" && version != p.ActiveDigest {
				return p, project.ErrNotFound
			}
			return p, nil
		}
		p, err := authorize()
		if err != nil {
			if errors.Is(err, project.ErrNotFound) {
				http.NotFound(w, r)
			} else {
				w.WriteHeader(503)
			}
			return
		}
		root, err := os.OpenRoot(deps.Previews.Root)
		if err != nil {
			w.WriteHeader(503)
			return
		}
		defer root.Close()
		filename := p.ID + "-" + p.ActiveDigest + ".jpg"
		file, err := root.Open(filename)
		if os.IsNotExist(err) {
			w.Header().Set("Content-Type", "image/svg+xml")
			w.Header().Set("X-Drop-Preview", "pending")
			w.Header().Set("Retry-After", "10")
			if r.Method != "HEAD" {
				_, _ = io.WriteString(w, previewPlaceholder)
			}
			return
		}
		if err != nil {
			w.WriteHeader(503)
			return
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<10 {
			w.WriteHeader(503)
			return
		}
		current, err := authorize()
		if err != nil || current.ActiveDigest != p.ActiveDigest || current.PolicyRevision != p.PolicyRevision {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("X-Drop-Preview", "ready")
		if r.Method != "HEAD" {
			_, _ = io.CopyN(w, file, info.Size())
		}
	})
}

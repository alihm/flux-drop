package httpserver

import (
	"net/http"

	"github.com/runonflux/flux-drop/internal/project"
)

func registerProjectRevisions(mux *http.ServeMux, deps Dependencies,
	browser, bearer func(*http.Request, bool) (project.Actor, error),
	mutate func(http.HandlerFunc) http.Handler, reply func(http.ResponseWriter, project.Project)) {
	repo, supported := deps.Projects.Repository.(project.RevisionRepository)
	list := func(actor func(*http.Request, bool) (project.Actor, error)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			a, err := actor(r, false)
			if err != nil {
				projectError(w, err)
				return
			}
			if !supported {
				projectError(w, project.ErrStorage)
				return
			}
			versions, next, err := repo.ListRevisions(r.Context(), a, r.PathValue("id"), r.URL.Query().Get("cursor"))
			if err != nil {
				projectError(w, err)
				return
			}
			respond(w, 200, map[string]any{"versions": versions, "nextCursor": next})
		}
	}
	change := func(actor func(*http.Request, bool) (project.Actor, error), remove bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			a, err := actor(r, true)
			if err != nil {
				projectError(w, err)
				return
			}
			if !supported {
				projectError(w, project.ErrStorage)
				return
			}
			revision, err := expectedRevision(r)
			if err != nil {
				revisionError(w, err)
				return
			}
			var p project.Project
			if remove {
				p, err = repo.RemoveRevision(r.Context(), a, r.PathValue("id"), r.PathValue("version"), revision)
			} else {
				p, err = repo.SelectRevision(r.Context(), a, r.PathValue("id"), r.PathValue("version"), revision)
			}
			if err != nil {
				projectError(w, err)
				return
			}
			if deps.Previews != nil {
				deps.Previews.InvalidateExplore()
			}
			reply(w, p)
		}
	}
	mux.HandleFunc("GET /api/projects/{id}/versions", list(browser))
	mux.Handle("POST /api/projects/{id}/versions/{version}/activate", mutate(change(browser, false)))
	mux.Handle("DELETE /api/projects/{id}/versions/{version}", mutate(change(browser, true)))
	mux.HandleFunc("GET /api/agent/projects/{id}/versions", list(bearer))
	mux.HandleFunc("POST /api/agent/projects/{id}/versions/{version}/activate", change(bearer, false))
	mux.HandleFunc("DELETE /api/agent/projects/{id}/versions/{version}", change(bearer, true))
}

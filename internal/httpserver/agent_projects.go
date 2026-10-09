package httpserver

import (
	"context"
	"net/http"
	"strings"

	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
)

// These endpoints use only an explicitly supplied bearer. Browser cookies,
// Origin and CSRF never contribute authority, and no CORS wrapper is installed.
func projectBearerActor(deps Dependencies, publish bool) func(*http.Request, bool) (project.Actor, error) {
	return func(r *http.Request, _ bool) (project.Actor, error) {
		secret := agentBearer(r)
		if strings.HasPrefix(secret, "drop_") {
			if !publish {
				return project.Actor{}, project.ErrForbidden
			}
			keys, ok := deps.Projects.Repository.(interface {
				AuthenticateAgentKey(context.Context, string) (project.Actor, error)
			})
			if !ok {
				return project.Actor{}, session.ErrUnauthorized
			}
			return keys.AuthenticateAgentKey(r.Context(), secret)
		}
		if strings.Count(secret, ".") != 2 {
			return project.Actor{}, session.ErrUnauthorized
		}
		identity, err := deps.ProjectBearerVerifier.VerifyBearer(r.Context(), secret)
		if err != nil {
			return project.Actor{}, session.ErrUnauthorized
		}
		return project.ActorFromFirebase(identity)
	}
}

// A supplied key retains the publisher's normal retry semantics. Without one,
// the agent request starts a fresh operation; browser upload rules stay intact.
func withAgentUploadKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(r.Header.Values("Idempotency-Key")) == 0 {
			r = r.Clone(r.Context())
			r.Header.Set("Idempotency-Key", agentRandom())
		}
		next(w, r)
	}
}

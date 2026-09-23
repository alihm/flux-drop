package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/runonflux/flux-drop/internal/password"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
)

type grantRepository interface {
	project.UnlockRepository
	ValidateGrant(context.Context, project.Actor, string, string) error
}
type PrivateAccess func(*http.Request, project.Project) bool

func grantCookie(id string) string { return "__Host-drop-grant-" + id }

func registerUnlock(mux *http.ServeMux, origin string, deps Dependencies, repo grantRepository, hasher *password.Hasher) PrivateAccess {
	service := &project.UnlockService{Repository: repo, DataRoot: deps.Projects.DataRoot, Hasher: hasher}
	mux.Handle("POST /api/unlock", RequireBrowserMutation(origin, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		r = r.WithContext(ctx)
		token, csrf, err := mutationCredentials(r)
		if err != nil {
			sessionError(w, err)
			return
		}
		view, err := deps.Sessions.Read(ctx, token)
		if err != nil {
			sessionError(w, err)
			return
		}
		if err := session.CheckCSRF(view.Record, csrf); err != nil {
			sessionError(w, err)
			return
		}
		a, err := project.ActorFrom(token, view)
		if err != nil {
			sessionError(w, err)
			return
		}
		kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || kind != "application/json" {
			projectError(w, project.ErrInvalid)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 8192)
		var input struct {
			Slug     string `json:"slug"`
			Password string `json:"password"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			projectError(w, errors.Join(project.ErrInvalid, err))
			return
		}
		if decoder.Decode(new(any)) != io.EOF {
			projectError(w, project.ErrInvalid)
			return
		}
		raw, grant, err := service.Unlock(ctx, a, input.Slug, input.Password)
		if errors.Is(err, project.ErrUnlockDenied) {
			respond(w, 403, map[string]string{"error": "unlock_denied"})
			return
		}
		if errors.Is(err, project.ErrUnlockLimited) {
			w.Header().Set("Retry-After", "60")
			respond(w, 429, map[string]string{"error": "unlock_limited"})
			return
		}
		if err != nil {
			projectError(w, err)
			return
		}
		maxAge := int(time.Until(grant.ExpiresAt).Seconds())
		if maxAge < 1 {
			respond(w, 403, map[string]string{"error": "unlock_denied"})
			return
		}
		http.SetCookie(w, &http.Cookie{Name: grantCookie(grant.ProjectID), Value: raw, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: maxAge, Expires: grant.ExpiresAt})
		respond(w, 200, map[string]any{"unlocked": true, "expiresAt": grant.ExpiresAt})
	})))
	return func(r *http.Request, p project.Project) bool {
		// The initial private profile permits only top-level document navigation.
		// No credentialed opaque-origin assets, fetches or frames are authorized.
		if r.Header.Get("Sec-Fetch-Mode") != "navigate" || r.Header.Get("Sec-Fetch-Dest") != "document" {
			return false
		}
		token, err := cookieToken(r)
		if err != nil {
			return false
		}
		view, err := deps.Sessions.Read(r.Context(), token)
		if err != nil {
			return false
		}
		a, err := project.ActorFrom(token, view)
		if err != nil {
			return false
		}
		cookies := r.CookiesNamed(grantCookie(p.ID))
		if len(cookies) != 1 {
			return false
		}
		return repo.ValidateGrant(r.Context(), a, p.ID, cookies[0].Value) == nil
	}
}

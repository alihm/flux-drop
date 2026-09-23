package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
)

const sessionCookie = "__Host-drop-session"

type Dependencies struct {
	Sessions    *session.Service
	Projects    *project.Publisher
	Fallback    ProjectFallback
	FirebaseWeb *FirebaseWebConfig
	Readiness   func(context.Context) error
}

func registerSessions(mux *http.ServeMux, origin string, service *session.Service) {
	mux.Handle("POST /api/session", RequireBrowserMutation(origin, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		token, err := cookieToken(r)
		if errors.Is(err, http.ErrNoCookie) {
			token, view, err := service.Create(ctx)
			if err != nil {
				sessionError(w, err)
				return
			}
			setSessionCookie(w, token, view.Record.ExpiresAt)
			sessionResponse(w, view)
			return
		}
		if err != nil {
			sessionError(w, err)
			return
		}
		view, err := service.Read(ctx, token)
		if err != nil {
			sessionError(w, err)
			return
		}
		sessionResponse(w, view)
	})))
	mux.HandleFunc("GET /api/session", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		token, err := cookieToken(r)
		if err != nil {
			sessionError(w, session.ErrUnauthorized)
			return
		}
		view, err := service.Read(ctx, token)
		if err != nil {
			sessionError(w, err)
			return
		}
		sessionResponse(w, view)
	})
	mux.Handle("POST /api/auth/google", RequireBrowserMutation(origin, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, csrf, err := mutationCredentials(r)
		if err != nil {
			sessionError(w, err)
			return
		}
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			respond(w, 415, map[string]string{"error": "json_required"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		var input struct {
			IDToken string `json:"idToken"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil || input.IDToken == "" {
			respond(w, 400, map[string]string{"error": "invalid_request"})
			return
		}
		if decoder.Decode(new(any)) != io.EOF {
			respond(w, 400, map[string]string{"error": "invalid_request"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		newToken, view, err := service.Login(ctx, token, csrf, input.IDToken)
		if err != nil {
			sessionError(w, err)
			return
		}
		setSessionCookie(w, newToken, view.Record.ExpiresAt)
		sessionResponse(w, view)
	})))
	mux.Handle("POST /api/auth/logout", RequireBrowserMutation(origin, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, csrf, err := mutationCredentials(r)
		if err != nil {
			sessionError(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		newToken, view, err := service.Logout(ctx, token, csrf)
		if err != nil {
			sessionError(w, err)
			return
		}
		setSessionCookie(w, newToken, view.Record.ExpiresAt)
		sessionResponse(w, view)
	})))
}

func cookieToken(r *http.Request) (string, error) {
	var value string
	count := 0
	for _, cookie := range r.Cookies() {
		if cookie.Name == sessionCookie {
			value = cookie.Value
			count++
		}
	}
	if count == 0 {
		return "", http.ErrNoCookie
	}
	if count != 1 {
		return "", session.ErrUnauthorized
	}
	if _, err := session.Digest(value); err != nil {
		return "", err
	}
	return value, nil
}

func mutationCredentials(r *http.Request) (string, string, error) {
	token, err := cookieToken(r)
	if err != nil {
		return "", "", session.ErrUnauthorized
	}
	values := r.Header.Values("X-CSRF-Token")
	if len(values) != 1 || len(values[0]) != 43 {
		return "", "", session.ErrCSRF
	}
	return token, values[0], nil
}

func setSessionCookie(w http.ResponseWriter, token string, expires time.Time) {
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: token, Path: "/", Secure: true, HttpOnly: true,
		SameSite: http.SameSiteLaxMode, MaxAge: int(time.Until(expires).Seconds()), Expires: expires})
}

func sessionResponse(w http.ResponseWriter, view session.View) {
	result := map[string]any{"csrfToken": view.Record.CSRF, "authenticated": view.Authenticated,
		"reauthenticationRequired": view.Record.UID != "" && !view.Authenticated, "expiresAt": view.Record.ExpiresAt}
	if view.Authenticated {
		result["user"] = map[string]string{"uid": view.Record.UID}
		result["authExpiresAt"] = view.Record.AuthUntil
	}
	respond(w, http.StatusOK, result)
}

func sessionError(w http.ResponseWriter, err error) {
	code, message := http.StatusServiceUnavailable, "authentication_unavailable"
	switch {
	case errors.Is(err, session.ErrUnauthorized):
		code, message = 401, "authentication_required"
	case errors.Is(err, session.ErrCSRF):
		code, message = 403, "invalid_csrf"
	case errors.Is(err, session.ErrAccountSwitch):
		code, message = 409, "logout_required"
	case errors.Is(err, session.ErrRateLimited):
		code, message = 429, "rate_limited"
		w.Header().Set("Retry-After", "60")
	}
	respond(w, code, map[string]string{"error": message})
}

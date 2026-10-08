package admin

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"io"
	"mime"
	"net/http"
	"time"
)

type Source struct {
	Snapshot func(context.Context) (any, error)
	Change   func(*metadata.Tx, string, string) error
}

const sessionCookie = "__Host-drop-admin-session"
const browserCookie = "__Host-drop-admin-challenge"

func cookie(r *http.Request, name string) string {
	var result string
	count := 0
	for _, c := range r.Cookies() {
		if c.Name == name {
			result = c.Value
			count++
		}
	}
	if count != 1 {
		return ""
	}
	return result
}
func setCookie(w http.ResponseWriter, name, value string, expires time.Time) {
	c := &http.Cookie{Name: name, Value: value, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode, Expires: expires}
	if value == "" {
		c.MaxAge = -1
	}
	http.SetCookie(w, c)
}
func reply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func failure(w http.ResponseWriter, err error) {
	status, code := 503, "admin_unavailable"
	if errors.Is(err, ErrUnauthorized) {
		status, code = 401, "unauthorized"
	}
	if errors.Is(err, ErrLimited) {
		status, code = 429, "login_rate_limited"
		w.Header().Set("Retry-After", "60")
	}
	reply(w, status, map[string]string{"error": code})
}

type input struct {
	ID          string `json:"id"`
	PollToken   string `json:"pollToken"`
	Address     string `json:"address"`
	Signature   string `json:"signature"`
	Message     string `json:"message"`
	LoginPhrase string `json:"loginPhrase"`
	Action      string `json:"action"`
}

func decode(w http.ResponseWriter, r *http.Request, callback bool) (input, bool) {
	var in input
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	media, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if callback && media == "application/x-www-form-urlencoded" {
		if err := r.ParseForm(); err != nil {
			reply(w, 400, map[string]string{"error": "invalid_request"})
			return in, false
		}
		in.Message, in.LoginPhrase, in.Signature = r.PostForm.Get("message"), r.PostForm.Get("loginPhrase"), r.PostForm.Get("signature")
	} else {
		if media != "application/json" {
			reply(w, 415, map[string]string{"error": "json_required"})
			return in, false
		}
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		if d.Decode(&in) != nil || d.Decode(new(any)) != io.EOF {
			reply(w, 400, map[string]string{"error": "invalid_request"})
			return in, false
		}
	}
	return in, true
}
func (s *Service) Handler(source Source) http.Handler {
	mux := http.NewServeMux()
	mutation := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			origins := r.Header.Values("Origin")
			if len(origins) != 1 || origins[0] != s.Origin || (r.Header.Get("Sec-Fetch-Site") != "" && r.Header.Get("Sec-Fetch-Site") != "same-origin") {
				reply(w, 403, map[string]string{"error": "untrusted_origin"})
				return
			}
			h(w, r)
		}
	}
	mux.HandleFunc("GET /admin/{$}", page)
	mux.HandleFunc("GET /admin", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("POST /admin/api/challenge", mutation(func(w http.ResponseWriter, r *http.Request) {
		c, b, err := s.Issue(r.Context())
		if err != nil {
			failure(w, err)
			return
		}
		setCookie(w, browserCookie, b, c.Expires)
		reply(w, 200, c)
	}))
	redeem := func(manual bool) http.HandlerFunc {
		return mutation(func(w http.ResponseWriter, r *http.Request) {
			in, ok := decode(w, r, false)
			if !ok {
				return
			}
			if manual && in.Signature == "" {
				failure(w, ErrUnauthorized)
				return
			}
			if !manual {
				in.Signature = ""
			}
			token, session, ready, err := s.Redeem(r.Context(), in.ID, in.PollToken, cookie(r, browserCookie), in.Signature)
			if err != nil {
				failure(w, err)
				return
			}
			if !ready {
				reply(w, 200, map[string]string{"status": "pending"})
				return
			}
			setCookie(w, sessionCookie, token, session.Expires)
			setCookie(w, browserCookie, "", time.Unix(1, 0))
			reply(w, 200, map[string]any{"status": "authenticated", "session": session})
		})
	}
	mux.HandleFunc("POST /admin/api/login", redeem(true))
	mux.HandleFunc("POST /admin/api/wallet-status", redeem(false))
	mux.HandleFunc("POST /admin/api/wallet-callback", func(w http.ResponseWriter, r *http.Request) {
		in, ok := decode(w, r, true)
		if !ok {
			return
		}
		if in.Message == "" {
			in.Message = in.LoginPhrase
		}
		if err := s.Approve(r.Context(), in.Message, in.Signature); err != nil {
			failure(w, err)
			return
		}
		reply(w, 200, map[string]string{"status": "approved"})
	})
	mux.HandleFunc("GET /admin/api/session", func(w http.ResponseWriter, r *http.Request) {
		session, err := s.Read(r.Context(), cookie(r, sessionCookie))
		if err != nil {
			failure(w, err)
			return
		}
		reply(w, 200, session)
	})
	mux.HandleFunc("GET /admin/api/projects", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		data, err := s.Projects(r.Context(), cookie(r, sessionCookie), q.Get("cursor"), q.Get("q"), q.Get("app"))
		if errors.Is(err, project.ErrInvalid) {
			reply(w, 400, map[string]string{"error": "invalid_search"})
			return
		}
		if err != nil {
			failure(w, err)
			return
		}
		reply(w, 200, data)
	})
	mux.HandleFunc("POST /admin/api/logout", mutation(func(w http.ResponseWriter, r *http.Request) {
		if err := s.Logout(r.Context(), cookie(r, sessionCookie), r.Header.Get("X-CSRF-Token")); err != nil {
			failure(w, err)
			return
		}
		setCookie(w, sessionCookie, "", time.Unix(1, 0))
		reply(w, 200, map[string]bool{"ok": true})
	}))
	mux.HandleFunc("GET /admin/api/apps", func(w http.ResponseWriter, r *http.Request) {
		if _, err := s.Read(r.Context(), cookie(r, sessionCookie)); err != nil {
			failure(w, err)
			return
		}
		data, err := source.Snapshot(r.Context())
		if err != nil {
			failure(w, err)
			return
		}
		reply(w, 200, data)
	})
	mux.HandleFunc("POST /admin/api/apps/{appName}", mutation(func(w http.ResponseWriter, r *http.Request) {
		in, ok := decode(w, r, false)
		if !ok {
			return
		}
		if in.Action != "remove" && in.Action != "drain" && in.Action != "restore" {
			reply(w, 400, map[string]string{"error": "invalid_action"})
			return
		}
		err := s.RunAuthorized(r.Context(), cookie(r, sessionCookie), r.Header.Get("X-CSRF-Token"), func(tx *metadata.Tx) error { return source.Change(tx, r.PathValue("appName"), in.Action) })
		if err != nil {
			if errors.Is(err, ErrUnauthorized) {
				failure(w, err)
			} else if errors.Is(err, ErrAppInUse) {
				reply(w, 409, map[string]string{"error": "app_in_use", "message": "This app has retained data. Drain it to stop uploads while keeping files available."})
			} else if errors.Is(err, ErrUnknownApp) {
				reply(w, 404, map[string]string{"error": "unknown_app"})
			} else {
				failure(w, err)
			}
			return
		}
		reply(w, 200, map[string]bool{"ok": true})
	}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
		mux.ServeHTTP(w, r)
	})
}

// Test-only authorization fixture. Never include this server in the product
// image. It does NOT implement Firebase login or production session semantics.
package main

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/runonflux/flux-drop/internal/httpserver"
)

type session struct {
	CSRF      string
	Mutations int
	Unlocked  bool
}

var sessions = struct {
	sync.Mutex
	values map[string]*session
}{values: make(map[string]*session)}

func randomToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func sessionFor(r *http.Request) *session {
	c, err := r.Cookie("__Host-fixture")
	if err != nil {
		return nil
	}
	return sessions.values[c.Value]
}

func main() {
	mux := http.NewServeMux()
	mux.Handle("/{$}", httpserver.HomePage())
	mux.Handle("/unlock/", httpserver.UnlockPage())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("GET /manage", func(w http.ResponseWriter, r *http.Request) {
		sessions.Lock()
		defer sessions.Unlock()
		s := sessionFor(r)
		if s == nil {
			token := randomToken()
			s = &session{CSRF: randomToken()}
			sessions.values[token] = s
			http.SetCookie(w, &http.Cookie{Name: "__Host-fixture", Value: token, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
		fmt.Fprintf(w, `<!doctype html><title>Management fixture</title><meta name="csrf" content="%s"><h1>Management</h1>`, s.CSRF)
	})
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		sessions.Lock()
		defer sessions.Unlock()
		s := sessionFor(r)
		if s == nil {
			http.Error(w, "unauthorized", 401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"secret": "management-secret", "csrf": s.CSRF, "mutations": s.Mutations})
	})
	mutation := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessions.Lock()
		defer sessions.Unlock()
		s := sessionFor(r)
		if s == nil {
			http.Error(w, "unauthorized", 401)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-CSRF-Token")), []byte(s.CSRF)) != 1 {
			http.Error(w, "csrf denied", 403)
			return
		}
		if r.URL.Path == "/api/unlock" {
			s.Unlocked = true
		} else {
			s.Mutations++
		}
		w.WriteHeader(204)
	})
	mux.Handle("/api/mutate", httpserver.RequireBrowserMutation("https://localhost:18443", mutation))
	mux.Handle("/api/unlock", httpserver.RequireBrowserMutation("https://localhost:18443", mutation))
	// An explicit allowlist maps URLs to fixture files. Nginx serves file bytes
	// through an internal redirect after this authorization check, including HEAD,
	// range requests, and conditional requests.
	files := map[string]string{
		"index.html": "index.html", "react.html": "react.html", "vue.html": "vue.html",
		"hostile.svg": "hostile.svg", "worker.js": "worker.js", "probe.js": "probe.js",
		"react.js": "react.js", "vue.js": "vue.js", "private.html": "private.html", "private-modules.html": "private-modules.html",
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			http.Error(w, "method denied", 405)
			return
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if len(parts) != 2 || (parts[0] != "public-123456" && parts[0] != "private-123456") {
			http.NotFound(w, r)
			return
		}
		name := parts[1]
		if name == "" {
			name = "index.html"
		}
		file, ok := files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		visibility := "public"
		if parts[0] == "private-123456" {
			sessions.Lock()
			s := sessionFor(r)
			permitted := s != nil && s.Unlocked
			sessions.Unlock()
			if !permitted {
				http.Error(w, "not found", 404)
				return
			}
			visibility = "private"
		}
		w.Header().Set("X-Accel-Redirect", "/_files/"+visibility+"/"+file)
	})
	log.Fatal(http.ListenAndServe(":8081", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
		mux.ServeHTTP(w, r)
	})))
}

package httpserver

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// StagingAccess restricts the management UI and APIs. Public site assets remain
// public so opaque-origin scripts can load them without management credentials.
// Origin/CSRF/project authorization remain mandatory inside this outer gate.
func StagingAccess(next http.Handler, user, password string) http.Handler {
	wantUser, wantPassword := sha256.Sum256([]byte(user)), sha256.Sum256([]byte(password))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && !strings.HasPrefix(r.URL.Path, "/api/") && !strings.HasPrefix(r.URL.Path, "/unlock/") {
			next.ServeHTTP(w, r)
			return
		}
		u, p, ok := r.BasicAuth()
		gotUser, gotPassword := sha256.Sum256([]byte(u)), sha256.Sum256([]byte(p))
		valid := subtle.ConstantTimeCompare(gotUser[:], wantUser[:]) & subtle.ConstantTimeCompare(gotPassword[:], wantPassword[:])
		if !ok || valid != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Flux Drop staging", charset="UTF-8"`)
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
			http.Error(w, "Staging access required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

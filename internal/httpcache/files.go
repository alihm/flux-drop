// Package httpcache defines validators for authorized public representations.
package httpcache

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"
)

type originalKey struct{}

func Original(r *http.Request) *http.Request {
	if original, ok := r.Context().Value(originalKey{}).(*http.Request); ok {
		return original
	}
	return r
}
func WithOriginal(r *http.Request) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), originalKey{}, r))
}

// Bump this identity whenever the injected watermark bytes change.
const WatermarkVersion = "runonflux-badge-v1"

func ETag(hash string, branded bool) string {
	if branded {
		hash = fmt.Sprintf("%x-w", sha256.Sum256([]byte(hash+"/"+WatermarkVersion)))
	}
	return `"` + hash + `"`
}
func Match(r *http.Request, tag string) bool {
	for _, v := range strings.Split(Original(r).Header.Get("If-None-Match"), ",") {
		v = strings.TrimSpace(v)
		if v == "*" || strings.TrimPrefix(v, "W/") == tag {
			return true
		}
	}
	return false
}
func File(w http.ResponseWriter, r *http.Request, name, hash string, private, branded bool) bool {
	if private {
		w.Header().Set("Cache-Control", "no-store")
		return false
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
	html := strings.HasSuffix(strings.ToLower(name), ".html") || strings.HasSuffix(strings.ToLower(name), ".htm")
	if html {
		w.Header().Set("Cache-Control", "no-cache")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=600")
	}
	w.Header().Set("ETag", ETag(hash, branded && html))
	if Match(r, w.Header().Get("ETag")) {
		w.WriteHeader(304)
		return true
	}
	return false
}

package httpserver

import (
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"html/template"
	"net/http"
	"strings"
)

//go:embed ui/unlock.html ui/unlock.css ui/unlock.js
var unlockUI embed.FS

// UnlockPage renders only built-in management UI, never uploaded HTML. It does
// not query metadata, so the page itself does not confirm project existence.
func UnlockPage() http.Handler {
	html, _ := unlockUI.ReadFile("ui/unlock.html")
	css, _ := unlockUI.ReadFile("ui/unlock.css")
	js, _ := unlockUI.ReadFile("ui/unlock.js")
	page := template.Must(template.New("unlock").Parse(string(html)))
	hash := func(b []byte) string { sum := sha256.Sum256(b); return base64.StdEncoding.EncodeToString(sum[:]) }
	csp := "default-src 'none'; script-src 'sha256-" + hash(js) + "'; style-src 'sha256-" + hash(css) + "'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slug := strings.TrimPrefix(r.URL.Path, "/unlock/")
		if !strings.HasPrefix(r.URL.Path, "/unlock/") || !deliverySlug.MatchString(slug) {
			http.NotFound(w, r)
			return
		}
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(405)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", csp)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
		if r.Method == "HEAD" {
			return
		}
		_ = page.Execute(w, struct {
			Slug string
			CSS  template.CSS
			JS   template.JS
		}{slug, template.CSS(css), template.JS(js)})
	})
}

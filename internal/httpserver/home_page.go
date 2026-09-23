package httpserver

import (
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"html/template"
	"net/http"
)

//go:embed ui/home.html ui/home.css ui/home.js ui/auth.bundle.js ui/agents.html
var homeUI embed.FS

func AgentGuide() http.Handler {
	html, _ := homeUI.ReadFile("ui/agents.html")
	css, _ := homeUI.ReadFile("ui/home.css")
	page := template.Must(template.New("agents").Parse(string(html)))
	sum := sha256.Sum256(css)
	csp := "default-src 'none'; style-src 'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", csp)
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method != "HEAD" {
			_ = page.Execute(w, struct{ CSS template.CSS }{template.CSS(css)})
		}
	})
}

func HomePage() http.Handler {
	return HomePageWithAuth(nil)
}

func HomePageWithAuth(auth *FirebaseWebConfig) http.Handler {
	html, _ := homeUI.ReadFile("ui/home.html")
	css, _ := homeUI.ReadFile("ui/home.css")
	js, _ := homeUI.ReadFile("ui/home.js")
	if auth != nil {
		bundle, _ := homeUI.ReadFile("ui/auth.bundle.js")
		js = append(append(bundle, '\n'), js...)
	}
	page := template.Must(template.New("home").Parse(string(html)))
	hash := func(b []byte) string { sum := sha256.Sum256(b); return base64.StdEncoding.EncodeToString(sum[:]) }
	csp := "default-src 'none'; script-src 'sha256-" + hash(js) + "'; style-src 'sha256-" + hash(css) + "'; img-src data:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
	if auth != nil {
		csp = "default-src 'none'; script-src 'sha256-" + hash(js) + "' https://apis.google.com; style-src 'sha256-" + hash(css) + "'; img-src data:; connect-src 'self' https://identitytoolkit.googleapis.com https://securetoken.googleapis.com https://apis.google.com https://" + auth.AuthDomain + "; frame-src https://" + auth.AuthDomain + "; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
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
			CSS template.CSS
			JS  template.JS
		}{template.CSS(css), template.JS(js)})
	})
}

package httpserver

import (
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/json"
	"html/template"
	"net/http"
	"net/url"

	"github.com/runonflux/flux-drop/internal/project"
)

//go:embed ui/consent.html ui/consent.css ui/consent.js
var agentUI embed.FS

func (a *AgentAuth) consentPage(w http.ResponseWriter, status int, p project.AgentPending, handle, csrf, message string) {
	html, _ := agentUI.ReadFile("ui/consent.html")
	// The consent page carries its own dark theme; home.css follows the system theme and would fight it.
	css, _ := agentUI.ReadFile("ui/consent.css")
	bundle, _ := homeUI.ReadFile("ui/auth.bundle.js")
	js, _ := agentUI.ReadFile("ui/consent.js")
	js = append(append(bundle, '\n'), js...)
	hash := func(b []byte) string { sum := sha256.Sum256(b); return base64.StdEncoding.EncodeToString(sum[:]) }
	auth := a.Firebase
	csp := "default-src 'none'; script-src 'sha256-" + hash(js) + "' https://apis.google.com; style-src 'sha256-" + hash(css) + "'; img-src 'self' data:; connect-src 'self' https://identitytoolkit.googleapis.com https://securetoken.googleapis.com https://apis.google.com https://" + auth.AuthDomain + "; frame-src https://" + auth.AuthDomain + "; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
	w.Header().Set("Content-Security-Policy", csp)
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin-allow-popups")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	config, _ := json.Marshal(auth)
	redirect, _ := url.Parse(p.RedirectURI)
	host := ""
	loopback := false
	if redirect != nil {
		host = redirect.Host
		if host == "" {
			host = redirect.Scheme + ":"
		}
		loopback = agentLoopback(redirect)
	}
	page := template.Must(template.New("consent").Parse(string(html)))
	_ = page.Execute(w, struct {
		CSS                                                               template.CSS
		JS                                                                template.JS
		Firebase, Handle, CSRF, Name, Domain, RedirectHost, Error, Origin string
		Loopback, Orbit, Drop                                             bool
	}{template.CSS(css), template.JS(js), string(config), handle, csrf, p.Client.Name, p.Client.Domain, host, message, a.Origin, loopback, agentHasScope(project.AgentGrant{Scopes: p.Scopes}, "orbit"), agentHasScope(project.AgentGrant{Scopes: p.Scopes}, "drop")})
}
func (a *AgentAuth) renderConsent(w http.ResponseWriter, p project.AgentPending, handle, csrf string) {
	a.consentPage(w, 200, p, handle, csrf, "")
}
func (a *AgentAuth) consentError(w http.ResponseWriter, status int, message string) {
	a.consentPage(w, status, project.AgentPending{}, "", "", message)
}

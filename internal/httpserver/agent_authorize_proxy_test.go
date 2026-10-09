package httpserver

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestAgentAuthorizeURLConfigurationAndMetadata(t *testing.T) {
	for _, invalid := range []string{"http://runonflux.com/apps/oauth/authorize", "https://user:secret@runonflux.com/authorize", "https://runonflux.com/authorize?query=1", "https://runonflux.com/authorize?", "https://runonflux.com/authorize#fragment", "https://runonflux.com/authorize#", "https:///authorize"} {
		if _, err := AgentAuthFromEnv(func(key string) string {
			if key == "DROP_AGENT_AUTHORIZE_URL" {
				return invalid
			}
			return ""
		}); err == nil {
			t.Fatal("unsafe authorize URL accepted", invalid)
		}
	}
	for _, endpoint := range []string{defaultAgentAuthorizeURL, "https://sign-in.example:8443/custom/subpath/authorize"} {
		h := newOAuthHarnessWithAuthorizeURL(t, endpoint)
		for _, path := range []string{"/.well-known/oauth-authorization-server", "/.well-known/openid-configuration"} {
			w := h.request("GET", path, "", "", "", "", nil)
			m := oauthDecode[map[string]any](t, w)
			if w.Code != 200 || m["authorization_endpoint"] != endpoint || m["issuer"] != h.auth.Origin || m["token_endpoint"] != h.auth.Origin+"/oauth/token" || m["registration_endpoint"] != h.auth.Origin+"/oauth/register" || m["revocation_endpoint"] != h.auth.Origin+"/oauth/revoke" {
				t.Fatal("authorization URL changed another endpoint", m)
			}
		}
	}
	if origin, err := agentAuthorizeOrigin("https://RUNONFLUX.com:443/apps/oauth/authorize"); err != nil || origin != "https://runonflux.com" {
		t.Fatal("incorrect browser origin", origin, err)
	}
}

func TestAgentProxiedConsentKeepsBrowserProtections(t *testing.T) {
	const website = "https://sign-in.example:8443"
	for _, tc := range []struct {
		name, origin, site, invalid string
		status                      int
	}{
		{"website allow", website, "same-origin", "", 200},
		{"drop allow", "https://drop.example.com", "same-origin", "", 200},
		{"missing fetch metadata unchanged", website, "", "", 200},
		{"cross-site website", website, "cross-site", "", 403},
		{"same-site website", website, "same-site", "", 403},
		{"cross-site drop", "https://drop.example.com", "cross-site", "", 403},
		{"unconfigured default website", "https://runonflux.com", "same-origin", "", 403},
		{"foreign", "https://evil.example", "same-origin", "", 403},
		{"similar host", "https://sign-in.example.evil.example:8443", "same-origin", "", 403},
		{"origin with path", website + "/custom/authorize", "same-origin", "", 403},
		{"null origin", "null", "same-origin", "", 403},
		{"missing origin", "", "same-origin", "", 403},
		{"duplicate origin", website, "same-origin", "origin", 403},
		{"missing cookie", website, "same-origin", "missing-cookie", 403},
		{"wrong cookie", website, "same-origin", "wrong-cookie", 403},
		{"duplicate cookie", website, "same-origin", "duplicate-cookie", 403},
		{"missing csrf", website, "same-origin", "missing-csrf", 400},
		{"wrong csrf", website, "same-origin", "wrong-csrf", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOAuthHarnessWithAuthorizeURL(t, website+"/custom/subpath/authorize")
			p := h.pending("")
			switch tc.invalid {
			case "missing-cookie":
				p.cookie = nil
			case "wrong-cookie":
				p.cookie.Value = agentRandom()
			case "missing-csrf":
				p.csrf = ""
			case "wrong-csrf":
				p.csrf = agentRandom()
			}
			body, _ := json.Marshal(map[string]string{"handle": p.handle, "csrf": p.csrf, "action": "allow", "idToken": h.idToken("alice", "google.com", true), "refreshToken": "firebase-original-alice"})
			r := httptest.NewRequest("POST", "/oauth/authorize", strings.NewReader(string(body)))
			r.Header.Set("Content-Type", "application/json")
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			r.Header.Set("Sec-Fetch-Site", tc.site)
			if tc.invalid == "origin" {
				r.Header.Add("Origin", "https://drop.example.com")
			}
			if p.cookie != nil {
				r.AddCookie(p.cookie)
				if tc.invalid == "duplicate-cookie" {
					r.AddCookie(p.cookie)
				}
			}
			w := httptest.NewRecorder()
			h.handler.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
			if tc.status == 200 {
				callback, err := url.Parse(oauthDecode[map[string]string](t, w)["redirect"])
				if err != nil || callback.Query().Get("iss") != h.auth.Origin || callback.Query().Get("code") == "" || callback.Query().Get("state") != "test-state" {
					t.Fatal("proxied consent changed issuer/state or failed to issue code", callback, err)
				}
			} else if h.refreshes.Load() != 0 {
				t.Fatal("invalid browser request reached Firebase")
			}
		})
	}
}

func TestAgentConsentPagePathsAndPopupPolicy(t *testing.T) {
	h := newOAuthHarness(t)
	p := h.pending("")
	query := url.Values{"client_id": {p.client.ID}, "redirect_uri": {p.redirect}, "response_type": {"code"}, "state": {"test-state"}, "code_challenge": {agentRandom()}, "code_challenge_method": {"S256"}}
	w := h.request("GET", "/oauth/authorize?"+query.Encode(), "", "", "", "", nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `href="https://drop.example.com/"`) || !strings.Contains(w.Body.String(), "fetch(window.location.pathname,") || strings.Contains(w.Body.String(), "fetch('/oauth/authorize'") {
		t.Fatal("consent assets depend on the host/path")
	}
	if w.Header().Get("Cross-Origin-Opener-Policy") != "same-origin-allow-popups" || w.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatal("popup/frame policy changed", w.Header())
	}
	for _, directive := range []string{"connect-src 'self'", "https://identitytoolkit.googleapis.com", "https://securetoken.googleapis.com", "https://apis.google.com", "frame-src https://fluxcore-prod.firebaseapp.com", "frame-ancestors 'none'"} {
		if !strings.Contains(w.Header().Get("Content-Security-Policy"), directive) {
			t.Fatal("missing Firebase/consent CSP", directive)
		}
	}
}

func TestAgentAuthorizeSharedWebsiteIPBudget(t *testing.T) {
	h := newOAuthHarness(t)
	p := h.pending("") // One initiation already used by this website server IP.
	r := httptest.NewRequest("GET", "/oauth/authorize", nil)
	r.RemoteAddr = "203.1.2.3:1234"
	for i := 1; i < 599; i++ {
		if err := h.auth.rate(r, "authorize", 600); err != nil {
			t.Fatal(err)
		}
	}
	query := url.Values{"client_id": {p.client.ID}, "redirect_uri": {p.redirect}, "response_type": {"code"}, "state": {"test-state"}, "code_challenge": {agentRandom()}, "code_challenge_method": {"S256"}}
	for _, status := range []int{200, 429} {
		w := h.request("GET", "/oauth/authorize?"+query.Encode(), "", "", "", "", nil)
		if w.Code != status || status == 429 && w.Header().Get("Retry-After") != "60" {
			t.Fatal("incorrect shared-IP initiation budget", w.Code, w.Header())
		}
	}
}

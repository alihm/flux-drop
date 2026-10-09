package httpserver

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/testmetadata"
)

type oauthHarness struct {
	t                    *testing.T
	auth                 *AgentAuth
	handler              http.Handler
	key                  *rsa.PrivateKey
	clock                atomic.Int64
	refreshes            atomic.Int64
	refused              atomic.Bool
	mismatch             atomic.Bool
	jwksRequests         atomic.Int64
	upstreamCalls        atomic.Int64
	seenMu               sync.Mutex
	seenAuth, seenClient string
}

func newOAuthHarness(t *testing.T) *oauthHarness {
	t.Helper()
	h := &oauthHarness{t: t}
	h.clock.Store(time.Now().Unix())
	key, e := rsa.GenerateKey(rand.Reader, 2048)
	if e != nil {
		t.Fatal(e)
	}
	h.key = key
	keys := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.jwksRequests.Add(1)
		respond(w, 200, map[string]any{"keys": []any{map[string]string{"kty": "RSA", "alg": "RS256", "use": "sig", "kid": "test-key", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
	}))
	t.Cleanup(keys.Close)
	secure := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.refreshes.Add(1)
		if h.refused.Load() {
			respond(w, 400, map[string]any{"error": map[string]string{"message": "TOKEN_EXPIRED"}})
			return
		}
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" || r.URL.Query().Get("key") == "" {
			t.Error("invalid securetoken request")
		}
		if r.ParseForm() != nil || r.PostForm.Get("grant_type") != "refresh_token" {
			t.Error("invalid securetoken body")
		}
		uid := "alice"
		if strings.Contains(r.PostForm.Get("refresh_token"), "bob") {
			uid = "bob"
		}
		if h.mismatch.Load() {
			uid = "other"
		}
		respond(w, 200, map[string]any{"id_token": "fresh-firebase-" + uid, "refresh_token": "firebase-rotated-" + uid, "user_id": uid, "expires_in": "3600"})
	}))
	t.Cleanup(secure.Close)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.upstreamCalls.Add(1)
		h.seenMu.Lock()
		h.seenAuth = r.Header.Get("Authorization")
		h.seenClient = r.Header.Get("X-Flux-Agent-Client")
		h.seenMu.Unlock()
		if r.Header.Get("Cookie") != "" || r.Header.Get("X-Flux-Agent-Forwarded") != "" {
			t.Error("private headers leaked")
		}
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Mcp-Session-Id", "session-1")
			w.Header().Set("Connection", "X-Secret-Hop")
			w.Header().Set("X-Secret-Hop", "secret")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "event: message\ndata: one\n\n")
			w.(http.Flusher).Flush()
			_, _ = io.WriteString(w, "event: message\ndata: two\n\n")
			return
		}
		if r.Method == "DELETE" {
			w.WriteHeader(204)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.Copy(w, r.Body)
	}))
	t.Cleanup(upstream.Close)
	config, e := AgentAuthFromEnv(func(string) string { return "" })
	if e != nil {
		t.Fatal(e)
	}
	web, e := FirebaseWebFromEnv(func(string) string { return "" })
	if e != nil {
		t.Fatal(e)
	}
	store := &metadata.Store{Backend: &testmetadata.Backend{}}
	repo := &project.RaftRepository{Store: store, Now: func() time.Time { return time.Unix(h.clock.Load(), 0).UTC() }}
	a, e := NewAgentAuth(context.Background(), config, "https://drop.example.com", "a private test cluster passphrase 0123456789", repo, web)
	if e != nil {
		t.Fatal(e)
	}
	h.auth = a
	a.now = repo.Now
	a.verifier = &session.AgentFirebaseVerifier{ProjectID: web.ProjectID, JWKSURL: keys.URL, Now: a.now}
	a.secureTokenURL = secure.URL
	a.Config.Upstream = upstream.URL
	server, e := NewWithDependencies(Config{"https://drop.example.com", content.DefaultLimits()}, Dependencies{AgentAuth: a, Projects: &project.Publisher{Repository: repo, DataRoot: t.TempDir()}, StagingRoot: t.TempDir(), Sessions: &session.Service{Store: &session.RaftStore{Store: store}, Verifier: identityVerifier{}}})
	if e != nil {
		t.Fatal(e)
	}
	h.handler = server
	return h
}
func (h *oauthHarness) idToken(uid, provider string, verified bool) string {
	h.t.Helper()
	now := h.clock.Load()
	claims := jwt.MapClaims{"iss": "https://securetoken.google.com/fluxcore-prod", "aud": "fluxcore-prod", "sub": uid, "iat": now, "auth_time": now, "exp": now + 3600, "email": uid + "@example.com", "email_verified": verified, "firebase": map[string]string{"sign_in_provider": provider}}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key"
	raw, e := token.SignedString(h.key)
	if e != nil {
		h.t.Fatal(e)
	}
	return raw
}
func (h *oauthHarness) request(method, path, body, media, bearer, origin string, cookie *http.Cookie) *httptest.ResponseRecorder {
	h.t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.RemoteAddr = "203.1.2.3:1234"
	if media != "" {
		r.Header.Set("Content-Type", media)
	}
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	h.handler.ServeHTTP(w, r)
	return w
}
func oauthDecode[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil {
		t.Fatal(w.Code, w.Body.String(), e)
	}
	return out
}
func (h *oauthHarness) register(redirect, name string) project.AgentClient {
	h.t.Helper()
	b, _ := json.Marshal(map[string]any{"redirect_uris": []string{redirect}, "client_name": name, "token_endpoint_auth_method": "none"})
	w := h.request("POST", "/oauth/register", string(b), "application/json", "", "", nil)
	if w.Code != 201 {
		h.t.Fatal(w.Code, w.Body.String())
	}
	return oauthDecode[project.AgentClient](h.t, w)
}

type oauthPending struct {
	handle, csrf       string
	cookie             *http.Cookie
	client             project.AgentClient
	redirect, verifier string
}

func (h *oauthHarness) pending(scopes string) oauthPending {
	h.t.Helper()
	p := oauthPending{client: h.register("http://127.0.0.1:5000/callback", "Test agent"), redirect: "http://127.0.0.1:6000/callback", verifier: agentRandom()}
	sum := sha256.Sum256([]byte(p.verifier))
	q := url.Values{"client_id": {p.client.ID}, "redirect_uri": {p.redirect}, "response_type": {"code"}, "state": {"test-state"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(sum[:])}, "code_challenge_method": {"S256"}, "scope": {scopes}}
	w := h.request("GET", "/oauth/authorize?"+q.Encode(), "", "", "", "", nil)
	if w.Code != 200 {
		h.t.Fatal(w.Code, w.Body.String())
	}
	for attr, dest := range map[string]*string{"handle": &p.handle, "csrf": &p.csrf} {
		match := regexp.MustCompile(`data-` + attr + `="([^"]+)"`).FindStringSubmatch(w.Body.String())
		if len(match) != 2 {
			h.t.Fatal("missing", attr)
		}
		*dest = match[1]
	}
	p.cookie = w.Result().Cookies()[0]
	return p
}
func (h *oauthHarness) consent(p oauthPending, uid, provider string, verified bool, action string) *httptest.ResponseRecorder {
	h.t.Helper()
	b, _ := json.Marshal(map[string]string{"handle": p.handle, "csrf": p.csrf, "action": action, "idToken": h.idToken(uid, provider, verified), "refreshToken": "firebase-original-" + uid})
	return h.request("POST", "/oauth/authorize", string(b), "application/json", "", "https://drop.example.com", p.cookie)
}
func (h *oauthHarness) code(p oauthPending) string {
	h.t.Helper()
	w := h.consent(p, "alice", "google.com", true, "allow")
	if w.Code != 200 {
		h.t.Fatal(w.Code, w.Body.String())
	}
	out := oauthDecode[map[string]string](h.t, w)
	u, e := url.Parse(out["redirect"])
	if e != nil {
		h.t.Fatal(e)
	}
	if u.Query().Get("iss") != h.auth.Origin || u.Query().Get("state") != "test-state" {
		h.t.Fatal("missing callback binding")
	}
	return u.Query().Get("code")
}
func (h *oauthHarness) exchange(p oauthPending, code string) *httptest.ResponseRecorder {
	f := url.Values{"grant_type": {"authorization_code"}, "code": {code}, "client_id": {p.client.ID}, "redirect_uri": {p.redirect}, "code_verifier": {p.verifier}}
	return h.request("POST", "/oauth/token", f.Encode(), "application/x-www-form-urlencoded", "", "", nil)
}

type oauthTokens struct {
	Access  string `json:"access_token"`
	Refresh string `json:"refresh_token"`
	Scope   string `json:"scope"`
	Expires int    `json:"expires_in"`
	Type    string `json:"token_type"`
}

func (h *oauthHarness) connected(scope string) (oauthPending, oauthTokens) {
	h.t.Helper()
	p := h.pending(scope)
	w := h.exchange(p, h.code(p))
	if w.Code != 200 {
		h.t.Fatal(w.Code, w.Body.String())
	}
	return p, oauthDecode[oauthTokens](h.t, w)
}
func (h *oauthHarness) refresh(p oauthPending, token string) *httptest.ResponseRecorder {
	f := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {token}, "client_id": {p.client.ID}}
	return h.request("POST", "/oauth/token", f.Encode(), "application/x-www-form-urlencoded", "", "", nil)
}

const oauthOrbitCall = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"orbit_list_apps"}}`
const oauthDropCall = `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"drop_list_projects"}}`

func (h *oauthHarness) gateway(token, body string) *httptest.ResponseRecorder {
	return h.request("POST", "/agent/mcp", body, "application/json", token, "", nil)
}

func TestAgentOAuthMetadataAndDefaults(t *testing.T) {
	h := newOAuthHarness(t)
	for _, path := range []string{"/.well-known/oauth-authorization-server", "/.well-known/openid-configuration"} {
		w := h.request("GET", path, "", "", "", "", nil)
		m := oauthDecode[map[string]any](t, w)
		if w.Code != 200 || m["issuer"] != h.auth.Origin || m["authorization_endpoint"] != h.auth.Origin+"/oauth/authorize" || m["client_id_metadata_document_supported"] != true || m["authorization_response_iss_parameter_supported"] != true {
			t.Fatal(m)
		}
	}
	for _, key := range []string{"DROP_AGENT_AUTH_ENABLED", "DROP_AGENT_RESOURCE", "DROP_AGENT_MCP_UPSTREAM", "DROP_AGENT_RESOURCE_METADATA", "DROP_AGENT_WEBSITE_ORIGINS"} {
		_, e := AgentAuthFromEnv(func(k string) string {
			if k == key {
				return "invalid"
			}
			return ""
		})
		if e == nil {
			t.Fatal("invalid setting accepted", key)
		}
	}
	config, e := AgentAuthFromEnv(func(k string) string {
		if k == "DROP_AGENT_AUTH_ENABLED" {
			return "false"
		}
		return ""
	})
	if e != nil || config.Enabled {
		t.Fatal(config, e)
	}
}
func TestAgentOAuthRegistrationRedirects(t *testing.T) {
	h := newOAuthHarness(t)
	for _, uri := range []string{"https://client.example/callback", "http://127.0.0.1:5000/cb", "http://[::1]:1234/cb", "http://localhost/cb", "cursor://agent/cb", "vscode://extension/callback", "com.example.agent:/cb"} {
		h.register(uri, "Agent")
	}
	for _, uri := range []string{"http://example.com/cb", "http://127.0.0.2/cb", "https://user:pass@example.com/cb", "https://example.com/cb#fragment", "javascript:alert(1)", "file:///tmp/cb", "ftp://example.com/cb", "/callback", "https://", "cursor:opaque"} {
		b, _ := json.Marshal(map[string]any{"redirect_uris": []string{uri}})
		if w := h.request("POST", "/oauth/register", string(b), "application/json", "", "", nil); w.Code != 400 {
			t.Fatal("redirect accepted", uri, w.Code)
		}
	}
	w := h.request("POST", "/oauth/register", `{"redirect_uris":["https://client.example/callback"],"token_endpoint_auth_method":"client_secret_basic"}`, "application/json", "", "", nil)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	c := h.register("https://client.example/callback", "Short lived")
	h.clock.Add(int64(91 * 24 * time.Hour / time.Second))
	if _, e := h.auth.clientFor(context.Background(), c.ID); e == nil {
		t.Fatal("expired registration accepted")
	}
	if !agentRedirectMatches("http://[::1]:1234/cb?x=1", "http://[::1]:5678/cb?x=1") || agentRedirectMatches("http://localhost/cb", "http://127.0.0.1/cb") || agentRedirectMatches("https://example.com:1234/cb", "https://example.com:5678/cb") {
		t.Fatal("loopback port rule")
	}
}
func TestAgentOAuthRegistrationRateLimit(t *testing.T) {
	h := newOAuthHarness(t)
	for i := 0; i < 20; i++ {
		h.register("https://example.com/cb", "Agent")
	}
	if w := h.request("POST", "/oauth/register", `{}`, "application/json", "", "", nil); w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestAgentOAuthAuthorizeValidation(t *testing.T) {
	h := newOAuthHarness(t)
	c := h.register("https://client.example/callback", "<img src=x onerror=alert(1)>")
	sum := sha256.Sum256([]byte(agentRandom()))
	base := url.Values{"response_type": {"code"}, "client_id": {c.ID}, "redirect_uri": {"https://client.example/callback"}, "state": {"opaque state"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(sum[:])}, "code_challenge_method": {"S256"}}
	for _, tc := range []struct {
		key, value string
		redirect   bool
	}{{"client_id", "missing", false}, {"redirect_uri", "https://evil.example/callback", false}, {"response_type", "token", true}, {"state", "", true}, {"code_challenge", "bad", true}, {"code_challenge_method", "plain", true}, {"scope", "admin", true}, {"resource", "https://evil.example/mcp", true}} {
		q := url.Values{}
		for k, v := range base {
			q[k] = append([]string{}, v...)
		}
		q.Set(tc.key, tc.value)
		w := h.request("GET", "/oauth/authorize?"+q.Encode(), "", "", "", "", nil)
		if tc.redirect {
			u, e := url.Parse(w.Header().Get("Location"))
			if w.Code != 303 || e != nil || u.Host != "client.example" || u.Query().Get("iss") != h.auth.Origin || u.Query().Get("error") == "" {
				t.Fatal(tc, w.Code, w.Header())
			}
		} else if w.Code != 400 || w.Header().Get("Location") != "" || !strings.Contains(w.Body.String(), "Unable to connect") {
			t.Fatal(tc, w.Code)
		}
	}
	w := h.request("GET", "/oauth/authorize?"+base.Encode(), "", "", "", "", nil)
	if w.Code != 200 || strings.Contains(w.Body.String(), "<img src=x") || !strings.Contains(w.Body.String(), "&lt;img") || !strings.Contains(w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") || w.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatal(w.Code, w.Header())
	}
	cookie := w.Result().Cookies()[0]
	if cookie.Name != agentConsentCookie || !cookie.HttpOnly || !cookie.Secure || cookie.Path != "/" || cookie.Domain != "" || cookie.SameSite != http.SameSiteLaxMode || cookie.MaxAge != 600 {
		t.Fatal(cookie)
	}
	if !strings.Contains(w.Body.String(), "This app named itself") || !strings.Contains(w.Body.String(), "email") || !strings.Contains(w.Body.String(), "It cannot pay") {
		t.Fatal("missing consent information")
	}
}
func TestAgentOAuthConsentBindingAndPolicy(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutate   func(*oauthHarness, *oauthPending)
		provider string
		verified bool
		status   int
	}{
		{"missing-cookie", func(h *oauthHarness, p *oauthPending) { p.cookie = nil }, "google.com", true, 403},
		{"wrong-cookie", func(h *oauthHarness, p *oauthPending) {
			p.cookie = &http.Cookie{Name: agentConsentCookie, Value: agentRandom()}
		}, "google.com", true, 403},
		{"wrong-csrf", func(h *oauthHarness, p *oauthPending) { p.csrf = agentRandom() }, "google.com", true, 403},
		{"expired", func(h *oauthHarness, p *oauthPending) { h.clock.Add(601) }, "google.com", true, 403},
		{"unverified", nil, "google.com", false, 401}, {"wrong-provider", nil, "github.com", true, 401},
		{"uid-mismatch", func(h *oauthHarness, p *oauthPending) { h.mismatch.Store(true) }, "google.com", true, 401},
		{"password", nil, "password", true, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOAuthHarness(t)
			p := h.pending("")
			if tc.mutate != nil {
				tc.mutate(h, &p)
			}
			w := h.consent(p, "alice", tc.provider, tc.verified, "allow")
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
	h := newOAuthHarness(t)
	p := h.pending("")
	w := h.consent(p, "alice", "google.com", true, "deny")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "access_denied") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = h.consent(p, "alice", "google.com", true, "allow"); w.Code != 403 {
		t.Fatal("consent reused", w.Code)
	}
	if h.refreshes.Load() != 0 {
		t.Fatal("denial refreshed Firebase")
	}
	p = h.pending("")
	_ = h.code(p)
	if w = h.consent(p, "alice", "google.com", true, "allow"); w.Code != 403 {
		t.Fatal("allow reused", w.Code)
	}
	b, _ := json.Marshal(map[string]string{"handle": p.handle, "csrf": p.csrf, "action": "deny"})
	if w = h.request("POST", "/oauth/authorize", string(b), "application/json", "", "https://evil.example", p.cookie); w.Code != 403 {
		t.Fatal("origin bypass", w.Code)
	}
}
func TestAgentOAuthCodePKCEReplayAndExpiry(t *testing.T) {
	h := newOAuthHarness(t)
	p := h.pending("")
	code := h.code(p)
	bad := p
	bad.verifier = agentRandom()
	if w := h.exchange(bad, code); w.Code != 400 {
		t.Fatal("PKCE bypass", w.Code)
	}
	bad = p
	bad.redirect = "http://127.0.0.1:7000/callback"
	if w := h.exchange(bad, code); w.Code != 400 {
		t.Fatal("redirect changed at token exchange", w.Code)
	}
	w := h.exchange(p, code)
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	}
	tokens := oauthDecode[oauthTokens](t, w)
	if tokens.Access == tokens.Refresh || !agentChallenge(tokens.Access) || tokens.Expires != 3600 || tokens.Type != "Bearer" || tokens.Scope != "orbit drop" {
		t.Fatal("invalid token response")
	}
	if w = h.exchange(p, code); w.Code != 400 {
		t.Fatal("code replay accepted")
	}
	if w = h.gateway(tokens.Access, oauthOrbitCall); w.Code != 401 {
		t.Fatal("replay did not revoke grant", w.Code)
	}
	p = h.pending("")
	code = h.code(p)
	h.clock.Add(61)
	if w = h.exchange(p, code); w.Code != 400 {
		t.Fatal("expired code accepted")
	}
}
func TestAgentOAuthRefreshRotationAndRefusal(t *testing.T) {
	h := newOAuthHarness(t)
	p, tokens := h.connected("")
	before := h.refreshes.Load()
	w := h.refresh(p, tokens.Refresh)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	rotated := oauthDecode[oauthTokens](t, w)
	if rotated.Refresh == tokens.Refresh || h.refreshes.Load() != before+1 {
		t.Fatal("refresh was not rotated and Firebase refreshed")
	}
	if w = h.refresh(p, tokens.Refresh); w.Code != 400 {
		t.Fatal("refresh reuse accepted")
	}
	if w = h.gateway(rotated.Access, oauthOrbitCall); w.Code != 401 {
		t.Fatal("reuse did not revoke grant")
	}
	h = newOAuthHarness(t)
	p, tokens = h.connected("")
	h.refused.Store(true)
	w = h.refresh(p, tokens.Refresh)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "invalid_grant") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = h.gateway(tokens.Access, oauthOrbitCall); w.Code != 401 {
		t.Fatal("Firebase refusal did not revoke grant")
	}
}
func TestAgentOAuthRevoke(t *testing.T) {
	for _, kind := range []string{"access", "refresh"} {
		t.Run(kind, func(t *testing.T) {
			h := newOAuthHarness(t)
			p, tokens := h.connected("")
			value := tokens.Access
			if kind == "refresh" {
				value = tokens.Refresh
			}
			f := url.Values{"token": {value}, "client_id": {p.client.ID}, "token_type_hint": {kind + "_token"}}
			w := h.request("POST", "/oauth/revoke", f.Encode(), "application/x-www-form-urlencoded", "", "", nil)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if w = h.gateway(tokens.Access, oauthOrbitCall); w.Code != 401 {
				t.Fatal("revoke ignored")
			}
			f.Set("token", "unknown")
			if w = h.request("POST", "/oauth/revoke", f.Encode(), "application/x-www-form-urlencoded", "", "", nil); w.Code != 200 {
				t.Fatal("unknown token leaks existence")
			}
		})
	}
}
func TestAgentOAuthGateway(t *testing.T) {
	h := newOAuthHarness(t)
	for _, token := range []string{"", agentRandom(), h.idToken("alice", "google.com", true)} {
		w := h.gateway(token, oauthOrbitCall)
		want := `Bearer resource_metadata="https://runonflux.com/apps/.well-known/oauth-protected-resource", scope="orbit drop"`
		if token != "" {
			want += `, error="invalid_token"`
		}
		if w.Code != 401 || w.Header().Get("WWW-Authenticate") != want {
			t.Fatal(w.Code, w.Header())
		}
	}
	_, tokens := h.connected("")
	before := h.refreshes.Load()
	// Remove the consent cache to force gateway renewal and then prove reuse.
	h.auth.cacheMu.Lock()
	h.auth.cache = map[string]agentCachedSession{}
	h.auth.cacheMu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := h.gateway(tokens.Access, oauthOrbitCall)
			if w.Code != 200 {
				t.Error(w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	if h.refreshes.Load() != before+1 {
		t.Fatal("concurrent gateway refreshes", h.refreshes.Load(), before)
	}
	h.seenMu.Lock()
	got, client := h.seenAuth, h.seenClient
	h.seenMu.Unlock()
	if got != "Bearer fresh-firebase-alice" || strings.Contains(got, tokens.Access) || client != "Test agent" {
		t.Fatal("wrong upstream identity", client)
	}
	w := h.request("GET", "/agent/mcp", "", "", tokens.Access, "", nil)
	if w.Code != 200 || !w.Flushed || !strings.Contains(w.Body.String(), "data: two") || w.Header().Get("Mcp-Session-Id") != "session-1" || w.Header().Get("Content-Type") != "text/event-stream" || w.Header().Get("X-Secret-Hop") != "" {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	if w = h.request("DELETE", "/agent/mcp", "", "", tokens.Access, "", nil); w.Code != 204 {
		t.Fatal(w.Code)
	}
	h.clock.Add(3601)
	if w = h.gateway(tokens.Access, oauthOrbitCall); w.Code != 401 {
		t.Fatal("expired access accepted")
	}
}
func TestAgentOAuthGatewayScopesAndBatchGuard(t *testing.T) {
	h := newOAuthHarness(t)
	_, tokens := h.connected("drop")
	w := h.gateway(tokens.Access, oauthOrbitCall)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"code":-32003`) || h.upstreamCalls.Load() != 0 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = h.gateway(tokens.Access, oauthDropCall); w.Code != 200 || h.upstreamCalls.Load() != 1 {
		t.Fatal(w.Code, w.Body.String())
	}
	_, tokens = h.connected("orbit")
	if w = h.gateway(tokens.Access, oauthDropCall); w.Code != 200 || !strings.Contains(w.Body.String(), "Insufficient scope") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = h.gateway(tokens.Access, "["+oauthDropCall+"]"); w.Code != 400 {
		t.Fatal("batch scope bypass", w.Code)
	}
	if w = h.gateway(tokens.Access, strings.Repeat("x", 10<<20+1)); w.Code != 413 {
		t.Fatal("oversized MCP body accepted", w.Code)
	}
}
func TestAgentOAuthConnectionsIsolationAndCORS(t *testing.T) {
	h := newOAuthHarness(t)
	_, tokens := h.connected("")
	alice, bob := h.idToken("alice", "google.com", true), h.idToken("bob", "google.com", true)
	w := h.request("GET", "/api/agent-grants", "", "", alice, "https://runonflux.com", nil)
	if w.Code != 200 || w.Header().Get("Access-Control-Allow-Origin") != "https://runonflux.com" || w.Header().Get("Access-Control-Allow-Credentials") != "" || strings.Contains(w.Body.String(), "firebase-rotated") {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	list := oauthDecode[struct {
		Agents []struct {
			ID string `json:"id"`
		}
	}](t, w)
	if len(list.Agents) != 1 {
		t.Fatal(w.Body.String())
	}
	id := list.Agents[0].ID
	w = h.request("GET", "/api/agent-grants", "", "", bob, "", nil)
	if w.Code != 200 || w.Body.String() != "{\"agents\":[]}\n" {
		t.Fatal("other user's grants leaked", w.Body.String())
	}
	if w = h.request("DELETE", "/api/agent-grants/"+id, "", "", bob, "", nil); w.Code != 404 {
		t.Fatal("other user revoked grant", w.Code)
	}
	if w = h.request("GET", "/api/agent-grants", "", "", alice, "https://evil.example", nil); w.Code != 403 || w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("untrusted CORS", w.Code)
	}
	r := httptest.NewRequest("OPTIONS", "/api/agent-grants/"+id, nil)
	r.Header.Set("Origin", "https://runonflux.com")
	r.Header.Set("Access-Control-Request-Method", "DELETE")
	r.Header.Set("Access-Control-Request-Headers", "authorization, content-type")
	w = httptest.NewRecorder()
	h.handler.ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	r.Header.Set("Access-Control-Request-Headers", "cookie")
	w = httptest.NewRecorder()
	h.handler.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("cookie CORS allowed")
	}
	if w = h.request("GET", "/api/agent-grants", "", "", "", "", &http.Cookie{Name: sessionCookie, Value: agentRandom()}); w.Code != 401 {
		t.Fatal("cookie authentication allowed")
	}
	if w = h.request("DELETE", "/api/agent-grants/"+id, "", "", alice, "https://runonflux.com", nil); w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = h.gateway(tokens.Access, oauthOrbitCall); w.Code != 401 {
		t.Fatal("disconnect ignored")
	}
}
func TestAgentOAuthCIMDAndSSRF(t *testing.T) {
	h := newOAuthHarness(t)
	for _, ip := range []string{"127.0.0.1", "::1", "10.0.0.1", "169.254.169.254", "100.64.0.1", "192.0.2.1", "224.0.0.1", "::ffff:127.0.0.1", "2001:db8::1", "64:ff9b::7f00:1"} {
		if agentPublicIP(netip.MustParseAddr(ip)) {
			t.Fatal("unsafe IP allowed", ip)
		}
	}
	for _, id := range []string{"https://127.0.0.1/client.json", "https://[::1]/client.json", "https://localhost/client.json", "https://169.254.169.254/client.json", "https://example.com"} {
		if _, e := h.auth.clientFor(context.Background(), id); e == nil {
			t.Fatal("unsafe metadata accepted", id)
		}
	}
	calls := atomic.Int64{}
	bad := atomic.Bool{}
	id := "https://agent.example/oauth/client.json"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		clientID := id
		if bad.Load() {
			clientID = "https://other.example/client.json"
		}
		respond(w, 200, map[string]any{"client_id": clientID, "redirect_uris": []string{"https://client.example/cb"}, "client_name": "Verified agent"})
	}))
	defer server.Close()
	// Only this test client dials a local fixture; the production transport above
	// has already refused localhost/private/metadata destinations.
	h.auth.cimdClient = server.Client()
	transport := server.Client().Transport.(*http.Transport).Clone()
	original := server.URL
	h.auth.cimdClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		copy := req.Clone(req.Context())
		u, _ := url.Parse(original)
		u.Path = req.URL.Path
		copy.URL = u
		return transport.RoundTrip(copy)
	})}
	c, e := h.auth.clientFor(context.Background(), id)
	if e != nil || c.Domain != "agent.example" {
		t.Fatal(c, e)
	}
	_, e = h.auth.clientFor(context.Background(), id)
	if e != nil || calls.Load() != 1 {
		t.Fatal("CIMD not cached")
	}
	h.clock.Add(301)
	bad.Store(true)
	if _, e = h.auth.clientFor(context.Background(), id); e == nil {
		t.Fatal("CIMD client ID mismatch accepted")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAgentOAuthGatewayCaseSensitiveJSONAndClientResponses(t *testing.T) {
	h := newOAuthHarness(t)
	_, tokens := h.connected("orbit")
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","Method":"tools/list","params":{"name":"drop_delete"}}`,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"drop_delete","Name":"orbit_list"}}`,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"drop_delete"},"Params":{"name":"orbit_list"}}`,
	} {
		w := h.gateway(tokens.Access, body)
		if w.Code != 200 || !strings.Contains(w.Body.String(), "Insufficient scope") || h.upstreamCalls.Load() != 0 {
			t.Fatal("case alias bypassed scope check", w.Code, w.Body.String())
		}
	}
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","method":"tools/list","params":{"name":"drop_delete"}}`,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"drop_delete","name":"orbit_list"}}`,
	} {
		w := h.gateway(tokens.Access, body)
		if w.Code != 400 || h.upstreamCalls.Load() != 0 {
			t.Fatal("ambiguous JSON forwarded", w.Code)
		}
	}
	body := `{"jsonrpc":"2.0","id":"server-question","result":{"answer":"yes"}}`
	if w := h.gateway(tokens.Access, body); w.Code != 200 || w.Body.String() != body {
		t.Fatal("client JSON-RPC response rejected or changed", w.Code, w.Body.String())
	}
}

func TestAgentOAuthRefreshIdleAbsoluteAndResourceBinding(t *testing.T) {
	h := newOAuthHarness(t)
	p, tokens := h.connected("")
	f := url.Values{"grant_type": {"refresh_token"}, "client_id": {p.client.ID}, "refresh_token": {tokens.Refresh}, "resource": {"https://other.example/mcp"}}
	if w := h.request("POST", "/oauth/token", f.Encode(), "application/x-www-form-urlencoded", "", "", nil); w.Code != 400 || !strings.Contains(w.Body.String(), "invalid_target") {
		t.Fatal("wrong resource accepted", w.Code)
	}
	wrong := p
	wrong.client.ID = "another-client"
	if w := h.refresh(wrong, tokens.Refresh); w.Code != 400 {
		t.Fatal("wrong client accepted")
	}
	h.clock.Add(int64(29 * 24 * time.Hour / time.Second))
	w := h.refresh(p, tokens.Refresh)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	tokens = oauthDecode[oauthTokens](t, w)
	h.clock.Add(int64(29 * 24 * time.Hour / time.Second))
	w = h.refresh(p, tokens.Refresh)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	tokens = oauthDecode[oauthTokens](t, w)
	h.clock.Add(int64(29 * 24 * time.Hour / time.Second))
	w = h.refresh(p, tokens.Refresh)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	tokens = oauthDecode[oauthTokens](t, w)
	h.clock.Add(int64(4 * 24 * time.Hour / time.Second))
	if w = h.refresh(p, tokens.Refresh); w.Code != 400 {
		t.Fatal("absolute lifetime extended by rotation")
	}
	h = newOAuthHarness(t)
	p, tokens = h.connected("")
	h.clock.Add(int64(30 * 24 * time.Hour / time.Second))
	if w = h.refresh(p, tokens.Refresh); w.Code != 400 {
		t.Fatal("idle lifetime ignored")
	}
}
func TestAgentOAuthMetadataCORS(t *testing.T) {
	h := newOAuthHarness(t)
	for _, path := range []string{"/.well-known/oauth-authorization-server", "/.well-known/openid-configuration"} {
		w := h.request("GET", path, "", "", "", "https://browser-agent.example", nil)
		if w.Code != 200 || w.Header().Get("Access-Control-Allow-Origin") != "https://browser-agent.example" || w.Header().Get("Access-Control-Allow-Credentials") != "" {
			t.Fatal(w.Code, w.Header())
		}
	}
	w := h.request("OPTIONS", "/oauth/token", "", "", "", "https://browser-agent.example", nil)
	if w.Code != 204 || w.Header().Get("Access-Control-Allow-Origin") != "https://browser-agent.example" {
		t.Fatal(w.Code, w.Header())
	}
}

func TestAgentOAuthCallbackRemovesConflictingResponseParameters(t *testing.T) {
	p := project.AgentPending{RedirectURI: "https://client.example/cb?code=old&error=old&error_description=old&state=old&iss=old&keep=1", State: "fresh-state"}
	for _, allow := range []bool{true, false} {
		code, errorCode := "", "access_denied"
		if allow {
			code = "new-code"
			errorCode = ""
		}
		u, e := url.Parse(agentCallbackURL("https://drop.example.com", p, code, errorCode))
		if e != nil {
			t.Fatal(e)
		}
		q := u.Query()
		if q.Get("code") != code || q.Get("error") != errorCode || q.Get("state") != "fresh-state" || q.Get("iss") != "https://drop.example.com" || q.Get("error_description") != "" || q.Get("keep") != "1" {
			t.Fatal(q)
		}
	}
}

func TestAgentOAuthGatewaySSEArrivesBeforeUpstreamFinishes(t *testing.T) {
	h := newOAuthHarness(t)
	_, tokens := h.connected("")
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var finished atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: second\n\n")
		finished.Store(true)
	}))
	defer upstream.Close()
	defer unblock()
	h.auth.Config.Upstream = upstream.URL
	gateway := httptest.NewServer(h.handler)
	defer gateway.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	req, _ := http.NewRequest("GET", gateway.URL+"/agent/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+tokens.Access)
	response, err := client.Do(req)
	if err != nil {
		t.Fatal("SSE headers were buffered", err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != "data: first\n" || finished.Load() {
		t.Fatal("SSE was buffered until completion", line, err)
	}
	unblock()
	rest, err := io.ReadAll(reader)
	if err != nil || !strings.Contains(string(rest), "data: second") {
		t.Fatal(string(rest), err)
	}
}
func TestAgentOAuthGatewayHeaderIsolationAndActivityBudget(t *testing.T) {
	h := newOAuthHarness(t)
	_, tokens := h.connected("")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, name := range []string{"Cookie", "Origin", "X-Flux-Agent-Forwarded", "X-Private-Header", "Connection"} {
			if r.Header.Get(name) != "" {
				t.Errorf("header leaked: %s", name)
			}
		}
		if r.Header.Get("Authorization") != "Bearer fresh-firebase-alice" || r.Header.Get("Mcp-Session-Id") != "mcp-session" || r.Header.Get("MCP-Protocol-Version") != "2025-11-25" || r.Header.Get("Last-Event-ID") != "event-1" || r.Header.Get("Accept") != "application/json" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("MCP headers lost or identity not replaced")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != oauthOrbitCall {
			t.Error("body changed")
		}
		w.Header().Set("WWW-Authenticate", `Bearer error="upstream-example"`)
		w.WriteHeader(202)
	}))
	defer upstream.Close()
	h.auth.Config.Upstream = upstream.URL
	request := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/agent/mcp", strings.NewReader(oauthOrbitCall))
		r.Header.Set("Authorization", "Bearer "+tokens.Access)
		for key, value := range map[string]string{"Cookie": "session=private", "Origin": "https://runonflux.com", "X-Flux-Agent-Forwarded": "1", "X-Private-Header": "private", "Content-Type": "application/json", "Accept": "application/json", "Mcp-Session-Id": "mcp-session", "MCP-Protocol-Version": "2025-11-25", "Last-Event-ID": "event-1"} {
			r.Header.Set(key, value)
		}
		w := httptest.NewRecorder()
		h.handler.ServeHTTP(w, r)
		return w
	}
	w := request()
	if w.Code != 202 || w.Header().Get("WWW-Authenticate") != `Bearer error="upstream-example"` {
		t.Fatal(w.Code, w.Header())
	}
	list := h.request("GET", "/api/agent-grants", "", "", h.idToken("alice", "google.com", true), "", nil)
	id := oauthDecode[struct {
		Agents []struct {
			ID string `json:"id"`
		}
	}](t, list).Agents[0].ID
	key := "agent_grants/" + id
	before, err := h.auth.Repository.Store.Backend.Read(context.Background(), []string{key})
	if err != nil {
		t.Fatal(err)
	}
	h.clock.Add(30)
	if w = request(); w.Code != 202 {
		t.Fatal(w.Code)
	}
	after, err := h.auth.Repository.Store.Backend.Read(context.Background(), []string{key})
	if err != nil || after[key].Version != before[key].Version {
		t.Fatal("activity write happened more than once/minute", err)
	}
	h.clock.Add(31)
	_ = request()
	after, err = h.auth.Repository.Store.Backend.Read(context.Background(), []string{key})
	if err != nil || after[key].Version == before[key].Version {
		t.Fatal("activity was not updated after a minute", err)
	}
}
func TestAgentOAuthGatewayFirebaseRefusalAfterCacheMargin(t *testing.T) {
	h := newOAuthHarness(t)
	_, tokens := h.connected("")
	before := h.refreshes.Load()
	h.refused.Store(true)
	if w := h.gateway(tokens.Access, oauthOrbitCall); w.Code != 200 || h.refreshes.Load() != before {
		t.Fatal("valid cached Firebase ID was not reused")
	}
	h.clock.Add(56 * 60)
	if w := h.gateway(tokens.Access, oauthOrbitCall); w.Code != 401 || h.refreshes.Load() != before+1 {
		t.Fatal("Firebase refusal was not detected at cache margin", w.Code)
	}
	if w := h.gateway(tokens.Access, oauthOrbitCall); w.Code != 401 || h.refreshes.Load() != before+1 {
		t.Fatal("revoked grant reused cache or refreshed again", w.Code)
	}
}

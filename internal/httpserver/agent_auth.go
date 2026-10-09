package httpserver

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
	"golang.org/x/crypto/hkdf"
)

// AgentAuthConfig contains optional deployment settings; credentials reuse the
// primary's existing secret and public Firebase browser configuration.
type AgentAuthConfig struct {
	Enabled                              bool
	Resource, Upstream, ResourceMetadata string
	WebsiteOrigins                       []string
}

func AgentAuthFromEnv(get func(string) string) (AgentAuthConfig, error) {
	c := AgentAuthConfig{Enabled: true, Resource: "https://runonflux.com/apps/mcp", Upstream: "https://runonflux.com/apps/mcp", ResourceMetadata: "https://runonflux.com/apps/.well-known/oauth-protected-resource", WebsiteOrigins: []string{"https://runonflux.com"}}
	switch get("DROP_AGENT_AUTH_ENABLED") {
	case "", "true":
	case "false":
		c.Enabled = false
	default:
		return c, errors.New("DROP_AGENT_AUTH_ENABLED must be true or false")
	}
	for key, dst := range map[string]*string{"DROP_AGENT_RESOURCE": &c.Resource, "DROP_AGENT_MCP_UPSTREAM": &c.Upstream, "DROP_AGENT_RESOURCE_METADATA": &c.ResourceMetadata} {
		if s := get(key); s != "" {
			*dst = s
		}
	}
	if s := get("DROP_AGENT_WEBSITE_ORIGINS"); s != "" {
		c.WebsiteOrigins = strings.Split(s, ",")
	}
	for _, s := range []string{c.Resource, c.Upstream, c.ResourceMetadata} {
		if !agentHTTPSURL(s) {
			return c, errors.New("agent URLs must be HTTPS without credentials or fragments")
		}
	}
	for i, s := range c.WebsiteOrigins {
		c.WebsiteOrigins[i] = strings.TrimSpace(s)
		u, e := url.Parse(c.WebsiteOrigins[i])
		if e != nil || !agentHTTPSURL(c.WebsiteOrigins[i]) || u.Path != "" || u.RawQuery != "" {
			return c, errors.New("agent website origins must be HTTPS origins")
		}
	}
	return c, nil
}
func agentHTTPSURL(s string) bool {
	u, e := url.Parse(s)
	return e == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Fragment == "" && !u.ForceQuery && !strings.ContainsAny(s, "\r\n\\")
}

type agentVerifier interface {
	Verify(context.Context, string) (session.AgentIdentity, error)
}
type agentFirebaseSession struct {
	IDToken, RefreshToken, UID string
	ExpiresAt                  time.Time
}
type agentCachedSession struct {
	session agentFirebaseSession
}
type AgentAuth struct {
	Config         AgentAuthConfig
	Origin         string
	Repository     *project.RaftRepository
	Firebase       *FirebaseWebConfig
	verifier       agentVerifier
	client         *http.Client
	secureTokenURL string
	now            func() time.Time
	cipher         cipher.AEAD
	cacheMu        sync.Mutex
	cache          map[string]agentCachedSession
	// CIMD documents share a bounded cache; outbound dialing has its own SSRF gate.
	cimdMu         sync.Mutex
	cimd           map[string]project.AgentClient
	cimdClient     *http.Client
	uploadsEnabled bool
}

func NewAgentAuth(ctx context.Context, c AgentAuthConfig, origin, secret string, repo *project.RaftRepository, web *FirebaseWebConfig) (*AgentAuth, error) {
	if !c.Enabled {
		return nil, nil
	}
	if repo == nil || repo.Store == nil || web == nil {
		return nil, errors.New("agent authentication needs Raft metadata and Firebase browser configuration")
	}
	a := &AgentAuth{Config: c, Origin: origin, Repository: repo, Firebase: web, verifier: &session.AgentFirebaseVerifier{ProjectID: web.ProjectID}, client: &http.Client{Timeout: 10 * time.Second}, secureTokenURL: "https://securetoken.googleapis.com/v1/token", now: func() time.Time { return time.Now().UTC() }, cache: map[string]agentCachedSession{}, cimd: map[string]project.AgentClient{}}
	var key []byte
	if secret != "" {
		key = make([]byte, 32)
		if _, err := io.ReadFull(hkdf.New(sha256.New, []byte(secret), nil, []byte("flux-agent-grants-v1")), key); err != nil {
			return nil, err
		}
	} else {
		var err error
		key, err = agentPrivateEncryptionKey(ctx, repo)
		if err != nil {
			return nil, err
		}
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	a.cipher, err = cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	a.cimdClient = newAgentMetadataClient()
	return a, nil
}

func agentPrivateEncryptionKey(ctx context.Context, repo *project.RaftRepository) ([]byte, error) {
	// The supervisor starts HTTP and the coordinator together. A deployment
	// without a passphrase must wait for the coordinator and its first election
	// before initializing the replicated key, rather than crash at every start.
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	candidate := make([]byte, 32)
	if _, err := rand.Read(candidate); err != nil {
		return nil, err
	}
	for {
		var key []byte
		err := repo.AgentTransaction(ctx, func(tx *metadata.Tx) error {
			err := tx.Get("agent_private/encryption-v1", &key)
			if errors.Is(err, metadata.ErrNotFound) {
				key = candidate
				return tx.Create("agent_private/encryption-v1", key)
			}
			return err
		})
		if err == nil {
			return key, nil
		}
		// This initialization is explicitly convergent: after an ambiguous
		// commit, re-read the authoritative key and never overwrite it. Do not
		// apply this retry policy to ordinary OAuth or project mutations.
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func agentRandom() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("cryptographic random source unavailable")
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}
func agentHash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func (a *AgentAuth) encrypt(value, context string) ([]byte, error) {
	nonce := make([]byte, a.cipher.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return a.cipher.Seal(nonce, nonce, []byte(value), []byte(context)), nil
}
func (a *AgentAuth) decrypt(value []byte, context string) (string, error) {
	n := a.cipher.NonceSize()
	if len(value) < n {
		return "", errors.New("invalid encrypted credential")
	}
	v, e := a.cipher.Open(nil, value[:n], value[n:], []byte(context))
	return string(v), e
}
func agentJSON(w http.ResponseWriter, r *http.Request, limit int64, out any) bool {
	kind, _, e := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if e != nil || kind != "application/json" {
		agentError(w, 400, "invalid_request", "JSON required")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		agentError(w, 400, "invalid_request", "Invalid JSON request")
		return false
	}
	return true
}
func agentError(w http.ResponseWriter, status int, code, description string) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	respond(w, status, map[string]string{"error": code, "error_description": description})
}
func agentBearer(r *http.Request) string {
	if len(r.Header.Values("Authorization")) != 1 {
		return ""
	}
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return parts[1]
}
func (a *AgentAuth) register(mux *http.ServeMux) {
	for _, path := range []string{"/.well-known/oauth-authorization-server", "/.well-known/openid-configuration"} {
		mux.Handle("GET "+path, agentCORS(http.HandlerFunc(a.metadata), nil, "GET, OPTIONS", true))
		mux.Handle("OPTIONS "+path, agentCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }), nil, "GET, OPTIONS", true))
	}
	mux.Handle("POST /oauth/register", a.oauthCORS(http.HandlerFunc(a.registerClient)))
	mux.HandleFunc("GET /oauth/authorize", a.authorize)
	mux.Handle("POST /oauth/authorize", RequireBrowserMutation(a.Origin, http.HandlerFunc(a.consent)))
	mux.Handle("POST /oauth/token", a.oauthCORS(http.HandlerFunc(a.token)))
	mux.Handle("POST /oauth/revoke", a.oauthCORS(http.HandlerFunc(a.revoke)))
	for _, p := range []string{"/oauth/register", "/oauth/token", "/oauth/revoke"} {
		mux.Handle("OPTIONS "+p, a.oauthCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })))
	}
	for _, m := range []string{"POST", "GET", "DELETE"} {
		mux.HandleFunc(m+" /agent/mcp", a.gateway)
	}
	mux.Handle("GET /api/agent-grants", a.websiteCORS(http.HandlerFunc(a.listGrants)))
	mux.Handle("DELETE /api/agent-grants/{id}", a.websiteCORS(http.HandlerFunc(a.deleteGrant)))
	mux.Handle("POST /api/agent/upload-links", a.websiteCORS(http.HandlerFunc(a.uploadLink)))
	for _, p := range []string{"/api/agent-grants", "/api/agent-grants/{id}", "/api/agent/upload-links"} {
		mux.Handle("OPTIONS "+p, a.websiteCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })))
	}
}
func (a *AgentAuth) metadata(w http.ResponseWriter, r *http.Request) {
	respond(w, 200, map[string]any{
		"issuer": a.Origin, "authorization_endpoint": a.Origin + "/oauth/authorize", "token_endpoint": a.Origin + "/oauth/token", "registration_endpoint": a.Origin + "/oauth/register", "revocation_endpoint": a.Origin + "/oauth/revoke", "response_types_supported": []string{"code"}, "grant_types_supported": []string{"authorization_code", "refresh_token"}, "code_challenge_methods_supported": []string{"S256"}, "token_endpoint_auth_methods_supported": []string{"none"}, "scopes_supported": []string{"orbit", "drop"}, "client_id_metadata_document_supported": true, "authorization_response_iss_parameter_supported": true,
	})
}

// Public OAuth endpoints must also work for browser-based MCP clients on origins
// not known in advance. They use no cookies; consent alone is same-origin/CSRF.
func (a *AgentAuth) oauthCORS(next http.Handler) http.Handler {
	return agentCORS(next, nil, "POST, OPTIONS", true)
}
func (a *AgentAuth) websiteCORS(next http.Handler) http.Handler {
	return agentCORS(next, append([]string{a.Origin}, a.Config.WebsiteOrigins...), "GET, POST, DELETE, OPTIONS", false)
}
func agentCORS(next http.Handler, allowed []string, methods string, public bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		w.Header().Add("Vary", "Origin")
		if origin != "" {
			ok := false
			if public && origin != "null" {
				u, e := url.Parse(origin)
				ok = e == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.Path == "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
			} else {
				for _, s := range allowed {
					if origin == s {
						ok = true
					}
				}
			}
			if len(r.Header.Values("Origin")) != 1 || !ok {
				agentError(w, 403, "untrusted_origin", "Origin is not allowed")
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", methods)
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Max-Age", "600")
		}
		if r.Method == "OPTIONS" {
			requested := r.Header.Get("Access-Control-Request-Method")
			if requested != "" && !strings.Contains(", "+methods+",", ", "+requested+",") {
				agentError(w, 403, "invalid_request", "Method is not allowed")
				return
			}
			for _, h := range strings.Split(r.Header.Get("Access-Control-Request-Headers"), ",") {
				h = strings.TrimSpace(h)
				if h != "" && !strings.EqualFold(h, "Authorization") && !strings.EqualFold(h, "Content-Type") {
					agentError(w, 403, "invalid_request", "Header is not allowed")
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

func (a *AgentAuth) agentUnauthorized(w http.ResponseWriter, r *http.Request) {
	challenge := `Bearer resource_metadata="` + a.Config.ResourceMetadata + `", scope="orbit drop"`
	if len(r.Header.Values("Authorization")) > 0 {
		challenge += `, error="invalid_token"`
	}
	w.Header().Set("WWW-Authenticate", challenge)
	agentError(w, 401, "invalid_token", "Connect your agent using browser sign-in")
}
func agentHasScope(g project.AgentGrant, required string) bool {
	for _, s := range g.Scopes {
		if s == required {
			return true
		}
	}
	return false
}
func (a *AgentAuth) gateway(w http.ResponseWriter, r *http.Request) {
	secret := agentBearer(r)
	// Opaque tokens cannot be JWTs. In particular never proxy a Firebase JWT
	// back to the website, even if it forwarded one accidentally.
	if !agentChallenge(secret) || strings.Count(secret, ".") == 2 {
		a.agentUnauthorized(w, r)
		return
	}
	var grant project.AgentGrant
	invalid := false
	e := a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		invalid = false
		var c project.AgentCredential
		err := tx.Get("agent_credentials/"+agentHash(secret), &c)
		if errors.Is(err, metadata.ErrNotFound) {
			invalid = true
			return nil
		}
		if err != nil {
			return err
		}
		if c.Kind != "access" || !a.now().Before(c.ExpiresAt) {
			invalid = true
			return nil
		}
		if err := tx.Get("agent_grants/"+c.GrantID, &grant); err != nil {
			return err
		}
		invalid = !agentGrantLive(grant, a.now()) || grant.Resource != a.Config.Resource
		return nil
	})
	if e != nil {
		agentError(w, 503, "temporarily_unavailable", "Authorization unavailable")
		return
	}
	if invalid {
		a.agentUnauthorized(w, r)
		return
	}
	var body []byte
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
		body, e = io.ReadAll(r.Body)
		if e != nil {
			agentError(w, 413, "invalid_request", "MCP request exceeds 10 MiB")
			return
		}
	}
	if r.Method == "POST" {
		// MCP streamable HTTP sends one JSON-RPC message per POST. Reject batches
		// rather than allow calls concealed in an array to bypass the scope check.

		// Maps preserve JSON's case-sensitive member names. Go struct decoding
		// would let a trailing "Method"/"Name" hide a differently scoped call
		// from this gateway while a JavaScript upstream reads "method"/"name".
		message, parseErr := agentJSONObject(body)
		var version, method string
		versionErr := json.Unmarshal(message["jsonrpc"], &version)
		methodErr := error(nil)
		if raw, ok := message["method"]; ok {
			methodErr = json.Unmarshal(raw, &method)
			if method == "" {
				methodErr = errAgentInvalid
			}
		}
		_, hasResult := message["result"]
		_, hasError := message["error"]
		_, hasID := message["id"]
		response := method == "" && hasID && (hasResult != hasError)
		if parseErr != nil || versionErr != nil || version != "2.0" || methodErr != nil || (method == "" && !response) {
			respond(w, 400, map[string]any{"jsonrpc": "2.0", "id": nil, "error": map[string]any{"code": -32600, "message": "A single JSON-RPC 2.0 message is required"}})
			return
		}
		if method == "tools/call" {
			params, paramsErr := agentJSONObject(message["params"])
			var name string
			if paramsErr != nil || json.Unmarshal(params["name"], &name) != nil || name == "" {
				respond(w, 400, map[string]any{"jsonrpc": "2.0", "id": nil, "error": map[string]any{"code": -32602, "message": "Tool name required"}})
				return
			}
			required := "orbit"
			if strings.HasPrefix(name, "drop_") {
				required = "drop"
			}
			if !agentHasScope(grant, required) {
				id := message["id"]
				if len(id) == 0 {
					id = json.RawMessage("null")
				}
				respond(w, 200, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32003, "message": "Insufficient scope", "data": map[string]string{"requiredScope": required}}})
				return
			}
		}
	}
	fresh, e := a.firebaseFor(r.Context(), grant.ID, false)
	if e != nil {
		if errors.Is(e, errAgentInvalid) || errors.Is(e, errFirebaseRefused) {
			a.agentUnauthorized(w, r)
		} else {
			agentError(w, 503, "temporarily_unavailable", "Firebase session refresh unavailable")
		}
		return
	}
	// Updating activity shares the grant CAS, but writes at most once a minute
	// across all instances, rather than trusting a per-node clock/cache.
	e = a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		var current project.AgentGrant
		if err := tx.Get("agent_grants/"+grant.ID, &current); err != nil {
			return err
		}
		if !agentGrantLive(current, a.now()) {
			return errAgentInvalid
		}
		if current.LastUsedAt.IsZero() || a.now().Sub(current.LastUsedAt) >= time.Minute {
			current.LastUsedAt = a.now()
			return tx.Set("agent_grants/"+current.ID, current)
		}
		return nil
	})
	if e != nil {
		if errors.Is(e, errAgentInvalid) {
			a.agentUnauthorized(w, r)
		} else {
			agentError(w, 503, "temporarily_unavailable", "Authorization unavailable")
		}
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, r.Method, a.Config.Upstream, bytes.NewReader(body))
	if e != nil {
		agentError(w, 502, "upstream_error", "MCP upstream unavailable")
		return
	}
	blocked := agentHopHeaders(r.Header)
	for _, h := range []string{"Content-Type", "Accept", "Mcp-Session-Id", "MCP-Protocol-Version", "Last-Event-ID"} {
		if !blocked[http.CanonicalHeaderKey(h)] {
			for _, v := range r.Header.Values(h) {
				req.Header.Add(h, v)
			}
		}
	}
	req.Header.Set("Authorization", "Bearer "+fresh.IDToken)
	req.Header.Set("X-Flux-Agent-Client", grant.Name)
	// A dedicated client has no overall read timeout so SSE streams can progress
	// for the request's full ten-minute deadline. Never follow upstream redirects:
	// neither the Firebase credential nor the body may leave the configured URL.
	upstream := &http.Client{Transport: agentGatewayTransport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, e := upstream.Do(req)
	if e != nil {
		agentError(w, 502, "upstream_error", "MCP upstream unavailable")
		return
	}
	defer resp.Body.Close()
	blocked = agentHopHeaders(resp.Header)
	for _, h := range []string{"Content-Type", "Mcp-Session-Id", "WWW-Authenticate"} {
		if !blocked[http.CanonicalHeaderKey(h)] {
			for _, v := range resp.Header.Values(h) {
				w.Header().Add(h, v)
			}
		}
	}
	if resp.Header.Get("Content-Type") != "" && !blocked["Content-Type"] {
		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	}
	w.Header().Set("X-Accel-Buffering", "no")
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Minute))
	w.WriteHeader(resp.StatusCode)
	// Flush headers immediately, and each chunk as it arrives (including SSE).
	controller := http.NewResponseController(w)
	_ = controller.Flush()
	buf := make([]byte, 32<<10)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, e := w.Write(buf[:n]); e != nil {
				return
			}
			_ = controller.Flush()
		}
		if err != nil {
			return
		}
	}
}

var agentGatewayTransport = func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.ResponseHeaderTimeout = 5 * time.Minute
	t.DisableCompression = true
	return t
}()

func agentHopHeaders(h http.Header) map[string]bool {
	blocked := map[string]bool{}
	for _, s := range []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		blocked[http.CanonicalHeaderKey(s)] = true
	}
	for _, v := range h.Values("Connection") {
		for _, s := range strings.Split(v, ",") {
			blocked[http.CanonicalHeaderKey(strings.TrimSpace(s))] = true
		}
	}
	return blocked
}

// Reject ambiguous duplicate members without changing the forwarded bytes.
func agentJSONObject(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errAgentInvalid
	}
	result := map[string]json.RawMessage{}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, errAgentInvalid
		}
		name, ok := key.(string)
		if !ok {
			return nil, errAgentInvalid
		}
		if _, exists := result[name]; exists {
			return nil, errAgentInvalid
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, errAgentInvalid
		}
		result[name] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errAgentInvalid
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errAgentInvalid
	}
	return result, nil
}

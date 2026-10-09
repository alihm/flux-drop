package httpserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

var errAgentInvalid = errors.New("invalid agent grant")
var errFirebaseRefused = errors.New("Firebase session refused")

func (a *AgentAuth) exchangeFirebase(ctx context.Context, refresh, uid string) (agentFirebaseSession, error) {
	endpoint, e := url.Parse(a.secureTokenURL)
	if e != nil {
		return agentFirebaseSession{}, e
	}
	q := endpoint.Query()
	q.Set("key", a.Firebase.APIKey)
	endpoint.RawQuery = q.Encode()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	form := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {refresh}}
	req, e := http.NewRequestWithContext(ctx, "POST", endpoint.String(), strings.NewReader(form.Encode()))
	if e != nil {
		return agentFirebaseSession{}, e
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, e := a.client.Do(req)
	if e != nil {
		return agentFirebaseSession{}, errors.New("Firebase refresh unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode == 400 || resp.StatusCode == 401 || resp.StatusCode == 403 {
		return agentFirebaseSession{}, errFirebaseRefused
	}
	if resp.StatusCode != 200 {
		return agentFirebaseSession{}, errors.New("Firebase refresh unavailable")
	}
	data, e := io.ReadAll(io.LimitReader(resp.Body, 64<<10+1))
	var out struct {
		IDToken      string      `json:"id_token"`
		RefreshToken string      `json:"refresh_token"`
		UID          string      `json:"user_id"`
		ExpiresIn    json.Number `json:"expires_in"`
		ProjectID    string      `json:"project_id"`
	}
	if e != nil || len(data) > 64<<10 || json.Unmarshal(data, &out) != nil || out.UID != uid || out.IDToken == "" {
		return agentFirebaseSession{}, errFirebaseRefused
	}
	expires, e := strconv.Atoi(string(out.ExpiresIn))
	if e != nil || expires <= 0 || expires > 3660 {
		return agentFirebaseSession{}, errFirebaseRefused
	}
	if out.RefreshToken == "" {
		out.RefreshToken = refresh
	}
	return agentFirebaseSession{out.IDToken, out.RefreshToken, out.UID, a.now().Add(time.Duration(expires) * time.Second)}, nil
}
func agentForm(w http.ResponseWriter, r *http.Request) bool {
	kind, _, e := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if e != nil || kind != "application/x-www-form-urlencoded" {
		agentError(w, 400, "invalid_request", "Form URL encoding required")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	if r.ParseForm() != nil || !agentSingleValues(r.PostForm) || r.URL.RawQuery != "" {
		agentError(w, 400, "invalid_request", "Invalid or duplicate parameters")
		return false
	}
	return true
}
func (a *AgentAuth) token(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	if !agentForm(w, r) {
		return
	}
	f := r.PostForm
	if r.Header.Get("Authorization") != "" || f.Get("client_secret") != "" {
		agentError(w, 401, "invalid_client", "Only public clients without a secret are supported")
		return
	}
	if f.Get("client_id") == "" {
		agentError(w, 400, "invalid_request", "client_id is required")
		return
	}
	if target := f.Get("resource"); target != "" && target != a.Config.Resource {
		agentError(w, 400, "invalid_target", "Unsupported resource")
		return
	}
	kind, secret := "", ""
	switch f.Get("grant_type") {
	case "authorization_code":
		kind = "code"
		secret = f.Get("code")
	case "refresh_token":
		kind = "refresh"
		secret = f.Get("refresh_token")
	default:
		agentError(w, 400, "unsupported_grant_type", "Use authorization_code or refresh_token")
		return
	}
	if !agentChallenge(secret) {
		agentError(w, 400, "invalid_grant", "Unknown or expired credential")
		return
	}
	var credential project.AgentCredential
	var grant project.AgentGrant
	invalid := false
	key := "agent_credentials/" + agentHash(secret)
	// A known replay must commit its revocation even though the HTTP result is an
	// error. Returning an error from a transaction would discard that write.
	e := a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		invalid = false
		err := tx.Get(key, &credential)
		if errors.Is(err, metadata.ErrNotFound) {
			invalid = true
			return nil
		}
		if err != nil {
			return err
		}
		if credential.Kind != kind || credential.ClientID != f.Get("client_id") {
			invalid = true
			return nil
		}
		if err := tx.Get("agent_grants/"+credential.GrantID, &grant); err != nil {
			return err
		}
		if credential.Used {
			invalid = true
			grant.Revoked = true
			grant.FirebaseRefresh = nil
			return tx.Set("agent_grants/"+grant.ID, grant)
		}
		if !a.now().Before(credential.ExpiresAt) || !agentGrantLive(grant, a.now()) || grant.Resource != a.Config.Resource {
			invalid = true
			return nil
		}
		if kind == "code" {
			verifier := f.Get("code_verifier")
			if len(verifier) < 43 || len(verifier) > 128 || strings.IndexFunc(verifier, func(r rune) bool {
				return !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '.' || r == '_' || r == '~')
			}) >= 0 {
				invalid = true
				return nil
			}
			sum := sha256.Sum256([]byte(verifier))
			challenge := base64.RawURLEncoding.EncodeToString(sum[:])
			if credential.RedirectURI != f.Get("redirect_uri") || subtle.ConstantTimeCompare([]byte(challenge), []byte(credential.Challenge)) != 1 {
				invalid = true
				return nil
			}
		}
		credential.Used = true
		return tx.Set(key, credential)
	})
	if e != nil {
		agentError(w, 503, "temporarily_unavailable", "Token service unavailable")
		return
	}
	if invalid {
		agentError(w, 400, "invalid_grant", "Credential expired, reused, or does not match the request")
		return
	}
	if kind == "refresh" {
		_, e = a.firebaseFor(r.Context(), grant.ID, true)
		if e != nil {
			if errors.Is(e, errAgentInvalid) || errors.Is(e, errFirebaseRefused) {
				agentError(w, 400, "invalid_grant", "Firebase session is no longer valid")
			} else {
				// A consumed credential cannot be returned to circulation after an
				// indeterminate Firebase call. The user can safely reconnect instead.
				_ = a.revokeGrant(r.Context(), grant.ID)
				agentError(w, 503, "temporarily_unavailable", "Firebase refresh unavailable; reconnect this agent")
			}
			return
		}
	}
	access, refresh := agentRandom(), agentRandom()
	now := a.now()
	e = a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		if err := tx.Get("agent_grants/"+grant.ID, &grant); err != nil {
			return err
		}
		if !agentGrantLive(grant, now) {
			return errAgentInvalid
		}
		grant.IdleUntil = now.Add(30 * 24 * time.Hour)
		if grant.IdleUntil.After(grant.ExpiresAt) {
			grant.IdleUntil = grant.ExpiresAt
		}
		if err := tx.Set("agent_grants/"+grant.ID, grant); err != nil {
			return err
		}
		if err := tx.Create("agent_credentials/"+agentHash(access), project.AgentCredential{Kind: "access", GrantID: grant.ID, ClientID: grant.ClientID, ExpiresAt: now.Add(time.Hour), RetainUntil: grant.ExpiresAt.Add(24 * time.Hour)}); err != nil {
			return err
		}
		return tx.Create("agent_credentials/"+agentHash(refresh), project.AgentCredential{Kind: "refresh", GrantID: grant.ID, ClientID: grant.ClientID, ExpiresAt: grant.ExpiresAt, RetainUntil: grant.ExpiresAt.Add(24 * time.Hour)})
	})
	if e != nil {
		agentError(w, 400, "invalid_grant", "The connection is no longer valid")
		return
	}
	respond(w, 200, map[string]any{"access_token": access, "token_type": "Bearer", "expires_in": 3600, "refresh_token": refresh, "scope": strings.Join(grant.Scopes, " ")})
}
func (a *AgentAuth) revokeGrant(ctx context.Context, id string) error {
	err := a.Repository.AgentTransaction(ctx, func(tx *metadata.Tx) error {
		var g project.AgentGrant
		if err := tx.Get("agent_grants/"+id, &g); err != nil {
			return err
		}
		g.Revoked = true
		g.FirebaseRefresh = nil
		return tx.Set("agent_grants/"+id, g)
	})
	a.cacheMu.Lock()
	delete(a.cache, id)
	a.cacheMu.Unlock()
	return err
}
func (a *AgentAuth) revoke(w http.ResponseWriter, r *http.Request) {
	if !agentForm(w, r) {
		return
	}
	if r.PostForm.Get("token") == "" {
		agentError(w, 400, "invalid_request", "token is required")
		return
	}
	if r.Header.Get("Authorization") != "" {
		agentError(w, 401, "invalid_client", "Public clients only")
		return
	}
	// Unknown tokens intentionally have the same success response (RFC 7009).
	e := a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		var c project.AgentCredential
		err := tx.Get("agent_credentials/"+agentHash(r.PostForm.Get("token")), &c)
		if errors.Is(err, metadata.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if c.Kind != "access" && c.Kind != "refresh" {
			return nil
		}
		if id := r.PostForm.Get("client_id"); id != "" && id != c.ClientID {
			return nil
		}
		var g project.AgentGrant
		if err := tx.Get("agent_grants/"+c.GrantID, &g); err != nil {
			return err
		}
		g.Revoked = true
		g.FirebaseRefresh = nil
		return tx.Set("agent_grants/"+g.ID, g)
	})
	if e != nil {
		agentError(w, 503, "temporarily_unavailable", "Revocation unavailable")
		return
	}
	w.WriteHeader(200)
}

// firebaseFor uses a replicated, bounded lease; local cached ID tokens are never
// trusted without re-reading the grant. Refresh calls happen outside CAS retries.
func (a *AgentAuth) firebaseFor(ctx context.Context, id string, force bool) (agentFirebaseSession, error) {
	lease := agentRandom()
	deadline := a.now().Add(15 * time.Second)
	var grant project.AgentGrant
	for {
		acquired, invalid := false, false
		e := a.Repository.AgentTransaction(ctx, func(tx *metadata.Tx) error {
			acquired, invalid = false, false
			if err := tx.Get("agent_grants/"+id, &grant); err != nil {
				return err
			}
			if !agentGrantLive(grant, a.now()) {
				invalid = true
				return nil
			}
			if grant.Lease != "" && a.now().Before(grant.LeaseUntil) {
				return nil
			}
			if !force {
				a.cacheMu.Lock()
				cached, ok := a.cache[id]
				a.cacheMu.Unlock()
				if ok && a.now().Before(cached.session.ExpiresAt.Add(-5*time.Minute)) {
					return nil
				}
			}
			grant.Lease, grant.LeaseUntil = lease, a.now().Add(15*time.Second)
			acquired = true
			return tx.Set("agent_grants/"+id, grant)
		})
		if e != nil {
			return agentFirebaseSession{}, e
		}
		if invalid {
			return agentFirebaseSession{}, errAgentInvalid
		}
		if acquired {
			break
		}
		if !force {
			a.cacheMu.Lock()
			cached, ok := a.cache[id]
			a.cacheMu.Unlock()
			if ok && a.now().Before(cached.session.ExpiresAt.Add(-5*time.Minute)) {
				return cached.session, nil
			}
		}
		if !a.now().Before(deadline) {
			return agentFirebaseSession{}, errors.New("Firebase refresh busy")
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return agentFirebaseSession{}, ctx.Err()
		case <-timer.C:
		}
	}
	refresh, e := a.decrypt(grant.FirebaseRefresh, "grant:"+id)
	if e != nil {
		_ = a.revokeGrant(ctx, id)
		return agentFirebaseSession{}, errAgentInvalid
	}
	fresh, e := a.exchangeFirebase(ctx, refresh, grant.UID)
	var encrypted []byte
	if e == nil {
		encrypted, e = a.encrypt(fresh.RefreshToken, "grant:"+id)
	}
	// Finish/release even if the caller disconnected after Firebase rotated. This
	// small cleanup is independent of the HTTP context and cannot extend the lease.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	refused := errors.Is(e, errFirebaseRefused)
	commitErr := a.Repository.AgentTransaction(cleanup, func(tx *metadata.Tx) error {
		var current project.AgentGrant
		if err := tx.Get("agent_grants/"+id, &current); err != nil {
			return err
		}
		if current.Lease != lease || !agentGrantLive(current, a.now()) {
			return errAgentInvalid
		}
		current.Lease = ""
		current.LeaseUntil = time.Time{}
		if refused {
			current.Revoked = true
			current.FirebaseRefresh = nil
		} else if e == nil {
			current.FirebaseRefresh = encrypted
		}
		return tx.Set("agent_grants/"+id, current)
	})
	if commitErr != nil {
		return agentFirebaseSession{}, commitErr
	}
	if e != nil {
		return agentFirebaseSession{}, e
	}
	a.cacheSession(id, fresh)
	return fresh, nil
}

// Cache only the short-lived ID token; refresh secrets stay encrypted at rest
// and are not retained unnecessarily in the per-instance cache.
func (a *AgentAuth) cacheSession(id string, fresh agentFirebaseSession) {
	fresh.RefreshToken = ""
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if len(a.cache) >= 1024 {
		for k, v := range a.cache {
			if !a.now().Before(v.session.ExpiresAt) {
				delete(a.cache, k)
			}
		}
		if len(a.cache) >= 1024 {
			a.cache = map[string]agentCachedSession{}
		}
	}
	a.cache[id] = agentCachedSession{session: fresh}
}

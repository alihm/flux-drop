package httpserver

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
)

const agentConsentCookie = "__Host-drop-agent"

func agentScopes(s string) ([]string, bool) {
	if s == "" {
		return []string{"orbit", "drop"}, true
	}
	seen := map[string]bool{}
	for _, scope := range strings.Fields(s) {
		if scope != "orbit" && scope != "drop" {
			return nil, false
		}
		seen[scope] = true
	}
	out := []string{}
	for _, scope := range []string{"orbit", "drop"} {
		if seen[scope] {
			out = append(out, scope)
		}
	}
	return out, len(out) > 0
}
func agentSingleValues(v url.Values) bool {
	for _, values := range v {
		if len(values) != 1 {
			return false
		}
	}
	return true
}
func agentChallenge(s string) bool {
	b, e := base64.RawURLEncoding.Strict().DecodeString(s)
	return e == nil && len(b) == 32 && len(s) == 43
}
func agentBrowserCookie(r *http.Request) string {
	var value string
	count := 0
	for _, c := range r.Cookies() {
		if c.Name == agentConsentCookie {
			value = c.Value
			count++
		}
	}
	if count != 1 || !agentChallenge(value) {
		return ""
	}
	return value
}
func agentCallbackURL(origin string, p project.AgentPending, code, errCode string) string {
	u, _ := url.Parse(p.RedirectURI)
	q := u.Query()
	for _, key := range []string{"code", "error", "error_description", "state", "iss"} {
		q.Del(key)
	}
	q.Set("state", p.State)
	q.Set("iss", origin)
	if code != "" {
		q.Set("code", code)
	} else {
		q.Set("error", errCode)
	}
	u.RawQuery = q.Encode()
	return u.String()
}
func (a *AgentAuth) callback(w http.ResponseWriter, r *http.Request, p project.AgentPending, code, errCode string) {
	http.Redirect(w, r, agentCallbackURL(a.Origin, p, code, errCode), http.StatusSeeOther)
}
func (a *AgentAuth) authorize(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.RawQuery) > 16<<10 {
		a.consentError(w, 400, "The authorization request is too large.")
		return
	}
	q, e := url.ParseQuery(r.URL.RawQuery)
	if e != nil || !agentSingleValues(q) {
		a.consentError(w, 400, "Invalid authorization parameters.")
		return
	}
	c, e := a.clientFor(r.Context(), q.Get("client_id"))
	if e != nil {
		a.consentError(w, 400, "Unknown or expired client.")
		return
	}
	redirect := q.Get("redirect_uri")
	valid := false
	for _, uri := range c.RedirectURIs {
		if agentRedirectMatches(uri, redirect) {
			valid = true
		}
	}
	if !valid {
		a.consentError(w, 400, "The redirect URI is not registered for this client.")
		return
	}
	p := project.AgentPending{Client: c, RedirectURI: redirect, State: q.Get("state"), Resource: a.Config.Resource}
	fail := func(code string) { a.callback(w, r, p, "", code) }
	if q.Get("response_type") != "code" {
		fail("unsupported_response_type")
		return
	}
	if p.State == "" || len(p.State) > 2048 || q.Get("code_challenge_method") != "S256" || !agentChallenge(q.Get("code_challenge")) {
		fail("invalid_request")
		return
	}
	scopes, ok := agentScopes(q.Get("scope"))
	if !ok {
		fail("invalid_scope")
		return
	}
	p.Scopes = scopes
	p.Challenge = q.Get("code_challenge")
	if resource := q.Get("resource"); resource != "" && resource != a.Config.Resource {
		fail("invalid_target")
		return
	}
	if err := a.rate(r, "authorize", 60); err != nil {
		if errors.Is(err, session.ErrRateLimited) {
			w.Header().Set("Retry-After", "60")
			a.consentError(w, 429, "Too many authorization requests. Try again shortly.")
		} else {
			a.consentError(w, 503, "Sign-in is temporarily unavailable.")
		}
		return
	}
	handle, csrf := agentRandom(), agentRandom()
	cookie := agentBrowserCookie(r)
	if cookie == "" {
		cookie = agentRandom()
	}
	p.CookieHash, p.CSRFHash, p.ExpiresAt = agentHash(cookie), agentHash(csrf), a.now().Add(10*time.Minute)
	e = a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error { return tx.Create("agent_pending/"+agentHash(handle), p) })
	if e != nil {
		a.consentError(w, 503, "Sign-in is temporarily unavailable.")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: agentConsentCookie, Value: cookie, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 600, Expires: p.ExpiresAt})
	a.renderConsent(w, p, handle, csrf)
}
func (a *AgentAuth) consent(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Handle       string `json:"handle"`
		CSRF         string `json:"csrf"`
		IDToken      string `json:"idToken"`
		RefreshToken string `json:"refreshToken"`
		Action       string `json:"action"`
	}
	if !agentJSON(w, r, 48<<10, &in) {
		return
	}
	if !agentChallenge(in.Handle) || !agentChallenge(in.CSRF) || (in.Action != "allow" && in.Action != "deny") {
		agentError(w, 400, "invalid_request", "Invalid consent request")
		return
	}
	key := "agent_pending/" + agentHash(in.Handle)
	check := func(p project.AgentPending) bool {
		return !p.Used && a.now().Before(p.ExpiresAt) && subtle.ConstantTimeCompare([]byte(p.CookieHash), []byte(agentHash(agentBrowserCookie(r)))) == 1 && subtle.ConstantTimeCompare([]byte(p.CSRFHash), []byte(agentHash(in.CSRF))) == 1 && agentBrowserCookie(r) != ""
	}
	var p project.AgentPending
	e := a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		if err := tx.Get(key, &p); err != nil {
			return err
		}
		if !check(p) {
			return project.ErrForbidden
		}
		return nil
	})
	if e != nil {
		agentError(w, 403, "invalid_request", "Consent expired, used, or does not belong to this browser")
		return
	}
	if in.Action == "deny" {
		e = a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
			if err := tx.Get(key, &p); err != nil {
				return err
			}
			if !check(p) {
				return project.ErrForbidden
			}
			p.Used = true
			return tx.Set(key, p)
		})
		if e != nil {
			agentError(w, 403, "invalid_request", "Consent is no longer valid")
			return
		}
		a.consentResponse(w, p, "", "access_denied")
		return
	}
	identity, e := a.verifier.Verify(r.Context(), in.IDToken)
	if e != nil {
		agentError(w, 401, "invalid_request", "Sign in with a verified Google or email/password account")
		return
	}
	if len(in.RefreshToken) == 0 || len(in.RefreshToken) > 16<<10 {
		agentError(w, 400, "invalid_request", "A Firebase refresh token is required")
		return
	}
	firebase, e := a.exchangeFirebase(r.Context(), in.RefreshToken, identity.UID)
	if e != nil {
		agentError(w, 401, "invalid_request", "The Firebase session does not match this account or is no longer valid")
		return
	}
	grantID, code := agentRandom(), agentRandom()
	now := a.now()
	encrypted, e := a.encrypt(firebase.RefreshToken, "grant:"+grantID)
	if e != nil {
		agentError(w, 503, "temporarily_unavailable", "Could not save connection")
		return
	}
	g := project.AgentGrant{ID: grantID, UID: identity.UID, Email: identity.Email, Provider: identity.Provider, ClientID: p.Client.ID, Name: p.Client.Name, Domain: p.Client.Domain, Resource: p.Resource, Scopes: p.Scopes, CreatedAt: now, ExpiresAt: now.Add(90 * 24 * time.Hour), IdleUntil: now.Add(30 * 24 * time.Hour), FirebaseRefresh: encrypted}
	credential := project.AgentCredential{Kind: "code", GrantID: g.ID, ClientID: g.ClientID, RedirectURI: p.RedirectURI, Challenge: p.Challenge, ExpiresAt: now.Add(time.Minute), RetainUntil: g.ExpiresAt.Add(24 * time.Hour)}
	e = a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		if err := tx.Get(key, &p); err != nil {
			return err
		}
		if !check(p) {
			return project.ErrForbidden
		}
		indexKey := "agent_grant_owners/" + agentHash(g.UID)
		var ids []string
		err := tx.Get(indexKey, &ids)
		if err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		active := []string{}
		for _, id := range ids {
			var previous project.AgentGrant
			if err := tx.Get("agent_grants/"+id, &previous); err != nil {
				return err
			}
			if agentGrantLive(previous, now) {
				active = append(active, id)
			}
		}
		if len(active) >= 50 {
			return project.ErrQuota
		}
		p.Used = true
		if err := tx.Set(key, p); err != nil {
			return err
		}
		if err := tx.Create("agent_grants/"+g.ID, g); err != nil {
			return err
		}
		if err := tx.Create("agent_credentials/"+agentHash(code), credential); err != nil {
			return err
		}
		return tx.Set(indexKey, append(active, g.ID))
	})
	if e != nil {
		if errors.Is(e, project.ErrQuota) {
			agentError(w, 400, "invalid_request", "Disconnect an agent before connecting another (limit 50)")
		} else {
			agentError(w, 409, "invalid_request", "Consent could not be saved or was already used")
		}
		return
	}
	a.cacheSession(g.ID, firebase)
	a.consentResponse(w, p, code, "")
}

// fetch follows no cross-origin redirect: the consent script explicitly navigates
// to this callback after a successful JSON POST.
func (a *AgentAuth) consentResponse(w http.ResponseWriter, p project.AgentPending, code, errorCode string) {
	respond(w, 200, map[string]string{"redirect": agentCallbackURL(a.Origin, p, code, errorCode)})
}
func agentGrantLive(g project.AgentGrant, now time.Time) bool {
	return !g.Revoked && now.Before(g.ExpiresAt) && now.Before(g.IdleUntil)
}

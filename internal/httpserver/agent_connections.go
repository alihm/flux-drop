package httpserver

import (
	"context"
	"encoding/base64"
	"errors"
	"mime"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
)

func (a *AgentAuth) websiteIdentity(w http.ResponseWriter, r *http.Request) (session.AgentIdentity, bool) {
	identity, e := a.verifier.Verify(r.Context(), agentBearer(r))
	if e != nil {
		agentError(w, 401, "invalid_token", "A live, verified Firebase ID token is required")
		return identity, false
	}
	return identity, true
}
func (a *AgentAuth) listGrants(w http.ResponseWriter, r *http.Request) {
	identity, ok := a.websiteIdentity(w, r)
	if !ok {
		return
	}
	agents := []map[string]any{}
	e := a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		agents = []map[string]any{}
		var ids []string
		err := tx.Get("agent_grant_owners/"+agentHash(identity.UID), &ids)
		if errors.Is(err, metadata.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, id := range ids {
			var g project.AgentGrant
			if err := tx.Get("agent_grants/"+id, &g); err != nil {
				return err
			}
			if g.UID == identity.UID && agentGrantLive(g, a.now()) {
				var lastUsed any
				if !g.LastUsedAt.IsZero() {
					lastUsed = g.LastUsedAt
				}
				agents = append(agents, map[string]any{"id": g.ID, "name": g.Name, "domain": g.Domain, "scope": g.Scopes, "connectedAt": g.CreatedAt, "lastUsedAt": lastUsed})
			}
		}
		return nil
	})
	if e != nil {
		agentError(w, 503, "temporarily_unavailable", "Connections unavailable")
		return
	}
	respond(w, 200, map[string]any{"agents": agents})
}
func (a *AgentAuth) deleteGrant(w http.ResponseWriter, r *http.Request) {
	identity, ok := a.websiteIdentity(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if !agentChallenge(id) {
		agentError(w, 404, "not_found", "Connection not found")
		return
	}
	e := a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		var g project.AgentGrant
		if err := tx.Get("agent_grants/"+id, &g); err != nil {
			return err
		}
		if g.UID != identity.UID {
			return project.ErrNotFound
		}
		g.Revoked = true
		g.FirebaseRefresh = nil
		return tx.Set("agent_grants/"+id, g)
	})
	if errors.Is(e, project.ErrNotFound) || errors.Is(e, metadata.ErrNotFound) {
		agentError(w, 404, "not_found", "Connection not found")
		return
	}
	if e != nil {
		agentError(w, 503, "temporarily_unavailable", "Could not disconnect agent")
		return
	}
	a.cacheMu.Lock()
	delete(a.cache, id)
	a.cacheMu.Unlock()
	w.WriteHeader(204)
}
func (a *AgentAuth) uploadLink(w http.ResponseWriter, r *http.Request) {
	if !a.uploadsEnabled {
		agentError(w, 503, "publishing_unavailable", "Drop publishing is disabled")
		return
	}
	identity, ok := a.websiteIdentity(w, r)
	if !ok {
		return
	}
	if identity.Provider != "google.com" {
		agentError(w, 403, "access_denied", "Drop publishing requires a Google account")
		return
	}
	var in struct {
		Name      string `json:"name"`
		ProjectID string `json:"projectId"`
		Password  string `json:"password"`
	}
	if !agentJSON(w, r, 8192, &in) {
		return
	}
	revision := int64(0)
	if in.ProjectID != "" {
		revision = 1
	}
	if project.ValidateInput("upload_ticket", in.ProjectID, in.Name, revision) != nil || (in.ProjectID != "" && in.Password != "") {
		agentError(w, 400, "invalid_request", "Invalid name or projectId; passwords apply to new projects")
		return
	}
	if in.Password != "" && (len(in.Password) > 1024 || !utf8.ValidString(in.Password) || utf8.RuneCountInString(in.Password) < 12) {
		agentError(w, 400, "invalid_request", "Password must contain at least 12 characters")
		return
	}
	ticket := agentRandom()
	digest := agentHash(ticket)
	record := project.AgentUploadTicket{UID: identity.UID, Name: in.Name, ProjectID: in.ProjectID, ExpiresAt: a.now().Add(30 * time.Minute)}
	if in.Password != "" {
		var e error
		record.Password, e = a.encrypt(in.Password, "ticket:"+digest)
		if e != nil {
			agentError(w, 503, "temporarily_unavailable", "Could not prepare upload")
			return
		}
	}
	e := a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		if in.ProjectID != "" {
			var p project.Project
			if err := tx.Get("projects/"+in.ProjectID, &p); err != nil {
				return err
			}
			if p.Owner.Kind != "firebase" || p.Owner.ID != identity.UID || !p.Live(a.now()) {
				return project.ErrNotFound
			}
			record.Revision = p.Revision
		}
		return tx.Create("agent_upload_tickets/"+digest, record)
	})
	if errors.Is(e, project.ErrNotFound) || errors.Is(e, metadata.ErrNotFound) {
		agentError(w, 404, "not_found", "Project not found in your account")
		return
	}
	if e != nil {
		agentError(w, 503, "temporarily_unavailable", "Could not prepare upload")
		return
	}
	respond(w, 201, map[string]any{"uploadUrl": a.Origin + "/api/agent/uploads/" + ticket, "method": "PUT", "expiresAt": record.ExpiresAt})
}

// registerUpload reuses the exact publisher, staging limits, disk/CPU admission,
// private-password activation and error mapping of the existing agent-key path.
func (a *AgentAuth) registerUpload(mux *http.ServeMux, upload func(bool, func(*http.Request, bool) (project.Actor, error)) http.HandlerFunc) {
	a.uploadsEnabled = true
	mux.HandleFunc("PUT /api/agent/uploads/{ticket}", func(w http.ResponseWriter, r *http.Request) {
		ticket := r.PathValue("ticket")
		if !agentChallenge(ticket) {
			agentError(w, 404, "not_found", "Upload link not found or expired")
			return
		}
		digest := agentHash(ticket)
		var record project.AgentUploadTicket
		lease := agentRandom()
		busy := false
		e := a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
			busy = false
			if err := tx.Get("agent_upload_tickets/"+digest, &record); err != nil {
				return err
			}
			if record.Result != nil || !a.now().Before(record.ExpiresAt) {
				return nil
			}
			if record.Lease != "" && a.now().Before(record.LeaseUntil) {
				busy = true
				return nil
			}
			record.Lease = lease
			record.LeaseUntil = a.now().Add(6 * time.Minute)
			return tx.Set("agent_upload_tickets/"+digest, record)
		})
		if errors.Is(e, metadata.ErrNotFound) || e == nil && !a.now().Before(record.ExpiresAt) {
			agentError(w, 404, "not_found", "Upload link not found or expired")
			return
		}
		if e != nil {
			agentError(w, 503, "temporarily_unavailable", "Upload unavailable")
			return
		}
		if record.Result != nil {
			a.uploadResult(w, *record.Result)
			return
		}
		if busy {
			w.Header().Set("Retry-After", "2")
			agentError(w, 409, "upload_in_progress", "This upload is already in progress; retry this link shortly")
			return
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
			defer cancel()
			_ = a.Repository.AgentTransaction(ctx, func(tx *metadata.Tx) error {
				var current project.AgentUploadTicket
				key := "agent_upload_tickets/" + digest
				if err := tx.Get(key, &current); err != nil {
					return err
				}
				if current.Lease != lease {
					return nil
				}
				current.Lease = ""
				current.LeaseUntil = time.Time{}
				return tx.Set(key, current)
			})
		}()

		kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || (kind != "application/zip" && kind != "text/html") {
			agentError(w, 415, "invalid_request", "Content-Type must be application/zip or text/html")
			return
		}
		// Ignore every client-supplied publish parameter: the capability ticket fixes
		// owner, target, revision, password, name and idempotency key.
		clone := r.Clone(r.Context())
		u := *r.URL
		clone.URL = &u
		u.RawQuery = ""
		q := u.Query()
		q.Set("name", record.Name)
		u.RawQuery = q.Encode()
		clone.Header = r.Header.Clone()
		clone.Header.Set("Idempotency-Key", digest)
		clone.Header.Del("X-Drop-Password")
		clone.Header.Set("If-Match", strconv.Quote(strconv.FormatInt(record.Revision, 10)))
		clone.SetPathValue("id", record.ProjectID)
		if len(record.Password) > 0 {
			secret, e := a.decrypt(record.Password, "ticket:"+digest)
			if e != nil {
				agentError(w, 503, "temporarily_unavailable", "Upload unavailable")
				return
			}
			clone.Header.Set("X-Drop-Password", base64.RawURLEncoding.EncodeToString([]byte(secret)))
		}
		actor := project.Actor{UID: record.UID, UploadTicketDigest: digest}
		upload(record.ProjectID != "", func(*http.Request, bool) (project.Actor, error) { return actor, nil })(w, clone)
	})
}
func (a *AgentAuth) uploadResult(w http.ResponseWriter, p project.Project) {
	w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(p.Revision, 10)))
	respond(w, 200, map[string]any{"project": p, "path": "/" + p.Slug + "/", "claimPath": "/?claim=" + p.ID, "url": a.Origin + "/" + p.Slug + "/"})
}

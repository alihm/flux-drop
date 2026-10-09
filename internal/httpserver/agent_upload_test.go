package httpserver

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

func (h *oauthHarness) uploadTicket(body, uid, provider string) *httptest.ResponseRecorder {
	return h.request("POST", "/api/agent/upload-links", body, "application/json", h.idToken(uid, provider, true), "https://runonflux.com", nil)
}
func (h *oauthHarness) ticketURL(body string) string {
	h.t.Helper()
	w := h.uploadTicket(body, "alice", "google.com")
	if w.Code != 201 {
		h.t.Fatal(w.Code, w.Body.String())
	}
	out := oauthDecode[struct {
		URL     string    `json:"uploadUrl"`
		Method  string    `json:"method"`
		Expires time.Time `json:"expiresAt"`
	}](h.t, w)
	if out.Method != "PUT" || !out.Expires.Equal(h.auth.now().Add(30*time.Minute)) {
		h.t.Fatal(out)
	}
	u, e := url.Parse(out.URL)
	if e != nil || u.Host != "drop.example.com" {
		h.t.Fatal(out.URL)
	}
	return u.RequestURI()
}

type agentUploadResult struct {
	Project project.Project `json:"project"`
	URL     string          `json:"url"`
}

func TestAgentUploadTicketsOwnerVersionAndRetry(t *testing.T) {
	h := newOAuthHarness(t)
	path := h.ticketURL(`{"name":"agent-site"}`)
	w := h.request("PUT", path, "<html><title>One</title></html>", "text/html", "", "", nil)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	first := oauthDecode[agentUploadResult](t, w)
	if first.Project.Owner.Kind != "firebase" || first.Project.ExpiresAt != nil || first.Project.Revision != 1 || first.Project.Slug != "agent-site" || first.URL != "https://drop.example.com/agent-site/" {
		t.Fatal(first)
	}
	// JSON deliberately hides UID; confirm the durable owner, not just the kind.
	var stored project.Project
	err := h.auth.Repository.AgentTransaction(context.Background(), func(tx *metadata.Tx) error { return tx.Get("projects/"+first.Project.ID, &stored) })
	if err != nil || stored.Owner.ID != "alice" {
		t.Fatal(stored.Owner, err)
	}
	w = h.request("PUT", path, "<title>different retry body is never published</title>", "text/html", "", "", nil)
	retry := oauthDecode[agentUploadResult](t, w)
	if w.Code != 200 || retry.Project.ID != first.Project.ID || retry.Project.ActiveDigest != first.Project.ActiveDigest || retry.Project.Revision != 1 {
		t.Fatal("ticket republished", w.Code, retry)
	}
	replacement := h.ticketURL(`{"projectId":"` + first.Project.ID + `"}`)
	w = h.request("PUT", replacement, "<html><title>Two</title></html>", "text/html", "", "", nil)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	second := oauthDecode[agentUploadResult](t, w)
	if second.Project.ID != first.Project.ID || second.Project.Revision != 2 || second.Project.ActiveDigest == first.Project.ActiveDigest || second.URL != first.URL {
		t.Fatal("new project created instead of version", second)
	}
	w = h.request("PUT", replacement, "", "", "", "", nil)
	if w.Code != 200 || oauthDecode[agentUploadResult](t, w).Project.Revision != 2 {
		t.Fatal("update retry not idempotent", w.Code)
	}
	// An old ticket's receipt stays the original publication, even after updates.
	w = h.request("PUT", path, "", "", "", "", nil)
	if w.Code != 200 || oauthDecode[agentUploadResult](t, w).Project.Revision != 1 {
		t.Fatal("receipt changed with project")
	}
	if w = h.uploadTicket(`{"projectId":"`+first.Project.ID+`"}`, "bob", "google.com"); w.Code != 404 {
		t.Fatal("other user obtained update ticket", w.Code)
	}
	h.clock.Add(1801)
	if w = h.request("PUT", path, "", "", "", "", nil); w.Code != 404 || !strings.Contains(w.Body.String(), "expired") {
		t.Fatal("expired ticket accepted", w.Code)
	}
	if w = h.request("PUT", "/api/agent/uploads/"+agentRandom(), "", "", "", "", nil); w.Code != 404 {
		t.Fatal("unknown ticket accepted", w.Code)
	}
}
func TestAgentUploadTicketValidationAndPrivatePublish(t *testing.T) {
	h := newOAuthHarness(t)
	for _, body := range []string{`{"name":"Bad Name"}`, `{"name":"../escape"}`, `{"password":"short"}`, `{"projectId":"invalid"}`, `{"unknown":1}`} {
		if w := h.uploadTicket(body, "alice", "google.com"); w.Code != 400 {
			t.Fatal(body, w.Code, w.Body.String())
		}
	}
	if w := h.uploadTicket(`{}`, "alice", "password"); w.Code != 403 {
		t.Fatal("password account published", w.Code)
	}
	if w := h.request("POST", "/api/agent/upload-links", `{}`, "application/json", "", "", nil); w.Code != 401 {
		t.Fatal("unauthenticated ticket")
	}
	path := h.ticketURL(`{"name":"private-agent","password":"a strong secret phrase"}`)
	w := h.request("PUT", path, "<title>Private</title>", "text/html", "", "", nil)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	p := oauthDecode[agentUploadResult](t, w).Project
	if !p.Private || p.ExpiresAt != nil {
		t.Fatal(p)
	}
	if w = h.request("GET", "/"+p.Slug+"/", "", "", "", "", nil); w.Code != 404 {
		t.Fatal("private upload publicly visible", w.Code)
	}
	if w = h.request("PUT", path, "<title>retry</title>", "text/html", "", "", nil); w.Code != 200 {
		t.Fatal("private retry", w.Code, w.Body.String())
	}
}
func TestAgentUploadTicketFailedBodyAndZip(t *testing.T) {
	h := newOAuthHarness(t)
	path := h.ticketURL(`{"name":"zip-agent"}`)
	if w := h.request("PUT", path, "not a zip", "application/zip", "", "", nil); w.Code == 200 {
		t.Fatal("invalid upload succeeded")
	}
	var body bytes.Buffer
	archive := zip.NewWriter(&body)
	file, e := archive.Create("index.html")
	if e != nil {
		t.Fatal(e)
	}
	_, _ = file.Write([]byte("<title>Zip retry after invalid body</title>"))
	if e = archive.Close(); e != nil {
		t.Fatal(e)
	}
	w := h.request("PUT", path, body.String(), "application/zip", "", "", nil)
	if w.Code != 200 {
		t.Fatal("failed upload consumed ticket", w.Code, w.Body.String())
	}
}
func TestAgentUploadTicketConcurrentSinglePublication(t *testing.T) {
	h := newOAuthHarness(t)
	path := h.ticketURL(`{"name":"parallel-ticket"}`)
	var wg sync.WaitGroup
	responses := make(chan *httptest.ResponseRecorder, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses <- h.request("PUT", path, "<title>same upload</title>", "text/html", "", "", nil)
		}()
	}
	wg.Wait()
	close(responses)
	successes := 0
	var id string
	for w := range responses {
		if w.Code == 409 {
			if !strings.Contains(w.Body.String(), "upload_in_progress") {
				t.Fatal(w.Body.String())
			}
			continue
		}
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		successes++
		p := oauthDecode[agentUploadResult](t, w).Project
		if id != "" && id != p.ID {
			t.Fatal("multiple projects from one ticket")
		}
		id = p.ID
		if p.Revision != 1 {
			t.Fatal("multiple versions from one ticket")
		}
	}
	if successes == 0 {
		t.Fatal("no successful upload")
	}
	w := h.request("PUT", path, "", "", "", "", nil)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestAgentOAuthEncryptionAndCrossInstance(t *testing.T) {
	h := newOAuthHarness(t)
	p, tokens := h.connected("")
	alice := h.idToken("alice", "google.com", true)
	w := h.request("GET", "/api/agent-grants", "", "", alice, "", nil)
	var list struct {
		Agents []struct {
			ID string `json:"id"`
		}
	}
	if e := json.Unmarshal(w.Body.Bytes(), &list); e != nil {
		t.Fatal(e)
	}
	id := list.Agents[0].ID
	var grant project.AgentGrant
	var codeCredential project.AgentCredential
	e := h.auth.Repository.AgentTransaction(context.Background(), func(tx *metadata.Tx) error {
		if err := tx.Get("agent_grants/"+id, &grant); err != nil {
			return err
		}
		return tx.Get("agent_credentials/"+agentHash(tokens.Access), &codeCredential)
	})
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(grant.FirebaseRefresh, []byte("firebase-rotated-alice")) || grant.Email != "alice@example.com" || grant.Provider != "google.com" || grant.ClientID != p.client.ID {
		t.Fatal("incorrect private grant")
	}
	plain, e := h.auth.decrypt(grant.FirebaseRefresh, "grant:"+id)
	if e != nil || plain != "firebase-rotated-alice" {
		t.Fatal("credential encryption", e)
	}
	if _, e = h.auth.decrypt(grant.FirebaseRefresh, "grant:other"); e == nil {
		t.Fatal("ciphertext was not bound to grant")
	}
	other, e := NewAgentAuth(context.Background(), h.auth.Config, h.auth.Origin, "a private test cluster passphrase 0123456789", h.auth.Repository, h.auth.Firebase)
	if e != nil {
		t.Fatal(e)
	}
	other.secureTokenURL = h.auth.secureTokenURL
	w = httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/agent/mcp", strings.NewReader(oauthOrbitCall))
	r.Header.Set("Authorization", "Bearer "+tokens.Access)
	other.gateway(w, r)
	if w.Code != 200 {
		t.Fatal("second instance did not see connection", w.Code, w.Body.String())
	}
	first, e := NewAgentAuth(context.Background(), h.auth.Config, h.auth.Origin, "", h.auth.Repository, h.auth.Firebase)
	if e != nil {
		t.Fatal(e)
	}
	second, e := NewAgentAuth(context.Background(), h.auth.Config, h.auth.Origin, "", h.auth.Repository, h.auth.Firebase)
	if e != nil {
		t.Fatal(e)
	}
	ciphertext, e := first.encrypt("private-refresh-token", "grant:example")
	if e != nil {
		t.Fatal(e)
	}
	value, e := second.decrypt(ciphertext, "grant:example")
	if e != nil || value != "private-refresh-token" {
		t.Fatal("fallback encryption key was not replicated", e)
	}
}

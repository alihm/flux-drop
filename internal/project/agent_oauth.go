package project

import (
	"context"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/session"
)

// Agent OAuth records live in the primary's private, replicated metadata. All
// token lookup keys are digests; Firebase refresh tokens/passwords are ciphertext.
type AgentClient struct {
	ID            string    `json:"client_id"`
	RedirectURIs  []string  `json:"redirect_uris"`
	Name          string    `json:"client_name"`
	URI           string    `json:"client_uri,omitempty"`
	Logo          string    `json:"logo_uri,omitempty"`
	AuthMethod    string    `json:"token_endpoint_auth_method"`
	ResponseTypes []string  `json:"response_types,omitempty"`
	GrantTypes    []string  `json:"grant_types,omitempty"`
	IssuedAt      int64     `json:"client_id_issued_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	Domain        string    `json:"-"`
}
type AgentPending struct {
	Client                                  AgentClient
	RedirectURI, Challenge, State, Resource string
	Scopes                                  []string
	CookieHash, CSRFHash                    string
	ExpiresAt                               time.Time
	Used                                    bool
}
type AgentGrant struct {
	ID, UID, Email, Provider, ClientID, Name, Domain, Resource string
	Scopes                                                     []string
	CreatedAt, LastUsedAt, ExpiresAt, IdleUntil                time.Time
	FirebaseRefresh                                            []byte
	Revoked                                                    bool
	// A replicated lease serializes Firebase token rotation across primary nodes.
	Lease      string
	LeaseUntil time.Time
}
type AgentCredential struct {
	Kind, GrantID, ClientID, RedirectURI, Challenge string
	ExpiresAt                                       time.Time
	RetainUntil                                     time.Time
	Used                                            bool
}
type AgentRate struct {
	Count     int
	ExpiresAt time.Time
}

type AgentUploadTicket struct {
	UID, Name, ProjectID string
	Lease                string
	LeaseUntil           time.Time
	Revision             int64
	Password             []byte
	ExpiresAt            time.Time
	Result               *Project
}

// AgentTransaction always uses replicated durability, including standalone
// development configurations. Callbacks must have no external side effects:
// the metadata store can retry a rejected CAS.
func (s *RaftRepository) AgentTransaction(ctx context.Context, fn func(*metadata.Tx) error) error {
	return s.Store.Run(ctx, fn)
}

func (s *RaftRepository) authorizeUploadTicket(tx *raftTx, a Actor) error {
	if !digestRE.MatchString(a.UploadTicketDigest) || a.UID == "" || a.SessionDigest != "" || a.AnonymousID != "" || a.AgentKeyDigest != "" {
		return session.ErrUnauthorized
	}
	ticket, err := raftRead[AgentUploadTicket](tx, "agent_upload_tickets/"+a.UploadTicketDigest)
	if raftMissing(err) {
		return session.ErrUnauthorized
	}
	if err != nil {
		return err
	}
	if ticket.UID != a.UID || ticket.Result != nil || !s.now().Before(ticket.ExpiresAt) {
		return session.ErrUnauthorized
	}
	return nil
}

// GetAgentUploadProject is deliberately narrower than browser management: a
// ticket can only inspect its own update target, and cannot list/change policy.
func (s *RaftRepository) GetAgentUploadProject(ctx context.Context, a Actor, id string) (Project, error) {
	var result Project
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorizeUploadTicket(tx, a); err != nil {
			return err
		}
		ticket, err := raftRead[AgentUploadTicket](tx, "agent_upload_tickets/"+a.UploadTicketDigest)
		if err != nil {
			return err
		}
		if ticket.ProjectID != id {
			return ErrNotFound
		}
		result, err = raftRead[Project](tx, "projects/"+id)
		if raftMissing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !a.Owns(result.Owner) || !result.Live(s.now()) {
			return ErrNotFound
		}
		return nil
	})
	return result, err
}

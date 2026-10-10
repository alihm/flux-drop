// Package project coordinates authoritative metadata and durable content.
package project

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/session"
)

const anonymousProjectLifetime = 7 * 24 * time.Hour

var (
	ErrNotFound  = errors.New("project not found")
	ErrConflict  = errors.New("project revision or reservation conflict")
	ErrForbidden = errors.New("project access denied")
	ErrQuota     = errors.New("project quota exceeded")
	ErrInvalid   = errors.New("invalid project request")
	ErrStorage   = errors.New("project storage unavailable")
)

type Duplicate struct{ Slug string }

func (d *Duplicate) Error() string { return "content already published" }

type Owner struct {
	Kind string `firestore:"kind" json:"kind"`
	ID   string `firestore:"id" json:"-"`
}

// Actor is constructed from verified credentials, never decoded from HTTP input.
// Transactions re-read sessions/keys/tickets or check the verified JWT expiry.
type Actor struct {
	SessionDigest       string
	AnonymousID         string
	UID                 string
	AgentKeyDigest      string
	UploadTicketDigest  string
	firebaseBearerUntil time.Time
}

// ActorFromFirebase must only receive an identity from Firebase JWT verification.
// The private marker cannot be populated by decoding a user-supplied actor/UID.
func ActorFromFirebase(identity session.AgentIdentity) (Actor, error) {
	if identity.UID == "" || identity.ExpiresAt.IsZero() {
		return Actor{}, session.ErrUnauthorized
	}
	if identity.Provider != "google.com" {
		return Actor{}, ErrForbidden
	}
	return Actor{UID: identity.UID, firebaseBearerUntil: identity.ExpiresAt}, nil
}

func (a Actor) authorizeFirebaseBearer(now time.Time) error {
	if a.UID == "" || !now.Before(a.firebaseBearerUntil) || a.SessionDigest != "" || a.AnonymousID != "" || a.AgentKeyDigest != "" || a.UploadTicketDigest != "" {
		return session.ErrUnauthorized
	}
	return nil
}

func ActorFrom(token string, view session.View) (Actor, error) {
	digest, err := session.Digest(token)
	if err != nil {
		return Actor{}, err
	}
	a := Actor{SessionDigest: digest, AnonymousID: view.Record.AnonymousOwner}
	if view.Authenticated {
		a.UID = view.Record.UID
	}
	return a, nil
}
func (a Actor) Owner() Owner {
	if a.UID != "" {
		return Owner{"firebase", a.UID}
	}
	return Owner{"anonymous", a.AnonymousID}
}
func (a Actor) Owns(o Owner) bool {
	return o.ID != "" && ((o.Kind == "anonymous" && o.ID == a.AnonymousID) || (o.Kind == "firebase" && a.UID != "" && o.ID == a.UID))
}

type Project struct {
	ID                string     `firestore:"id" json:"id"`
	StorageApp        string     `firestore:"storageApp" json:"-"`
	StorageGeneration string     `firestore:"storageGeneration,omitempty" json:"-"`
	Owner             Owner      `firestore:"owner" json:"owner"`
	OwnerKey          string     `firestore:"ownerKey" json:"-"`
	Slug              string     `firestore:"slug" json:"slug"`
	InitialSlug       string     `firestore:"initialSlug" json:"-"`
	AliasCount        int        `firestore:"aliasCount" json:"-"`
	InitialSuffix     string     `firestore:"initialSuffix" json:"initialSuffix"`
	ActiveDigest      string     `firestore:"activeDigest" json:"digest"`
	ActiveBytes       int64      `firestore:"activeBytes" json:"bytes"`
	ChargedBytes      int64      `firestore:"chargedBytes" json:"-"`
	Revision          int64      `firestore:"revision" json:"revision"`
	PolicyRevision    int64      `firestore:"policyRevision" json:"-"`
	WatermarkDisabled bool       `firestore:"watermarkDisabled" json:"watermarkDisabled"`
	Private           bool       `firestore:"private" json:"private"`
	PasswordDigest    string     `firestore:"passwordDigest" json:"-"`
	PasswordRevision  int64      `firestore:"passwordRevision" json:"-"`
	Status            string     `firestore:"status" json:"status"`
	PendingOperation  string     `firestore:"pendingOperation" json:"-"`
	CreatedAt         time.Time  `firestore:"createdAt" json:"createdAt"`
	UpdatedAt         time.Time  `firestore:"updatedAt" json:"updatedAt"`
	ExpiresAt         *time.Time `firestore:"expiresAt" json:"expiresAt"`
}

func (p Project) Live(now time.Time) bool {
	return p.Status == "active" && (p.ExpiresAt == nil || now.Before(*p.ExpiresAt))
}

type Reservation struct {
	Key, ProjectID, Name, Digest string
	Bytes, ExpectedRevision      int64
	Files                        int               `json:"-"`
	StorageManifest              *content.Manifest `json:"-"`
	StorageOperationID           string            `json:"-"`
}
type Operation struct {
	ID                string    `firestore:"id"`
	Fingerprint       string    `firestore:"fingerprint"`
	Owner             Owner     `firestore:"owner"`
	ProjectID         string    `firestore:"projectID"`
	Digest            string    `firestore:"digest"`
	Bytes             int64     `firestore:"bytes"`
	BaseRevision      int64     `firestore:"baseRevision"`
	New               bool      `firestore:"new"`
	State             string    `firestore:"state"`
	ExpiresAt         time.Time `firestore:"expiresAt"`
	StorageGeneration string    `firestore:"storageGeneration,omitempty"`
	StorageVersionKey string    `firestore:"storageVersionKey,omitempty"`
}
type Prepared struct {
	Project   Project
	Operation Operation
}
type Repository interface {
	Reserve(context.Context, Actor, Reservation) (Prepared, error)
	Activate(context.Context, Actor, string) (Project, error)
	Abort(context.Context, Actor, string) error
	Resolve(context.Context, string) (Project, error)
	GetOwned(context.Context, Actor, string) (Project, error)
	ListOwned(context.Context, Actor, string, int) ([]Project, string, error)
	Claim(context.Context, Actor, string, int64) (Project, error)
	Tombstone(context.Context, Actor, string, int64) error
	Rename(context.Context, Actor, string, string, int64) (Project, error)
	SetPrivacy(context.Context, Actor, string, int64, string, int64) (Project, error)
	SetWatermark(context.Context, Actor, string, int64, bool) (Project, error)
}

var digestRE = regexp.MustCompile(`^[a-f0-9]{64}$`)
var idRE = regexp.MustCompile(`^[a-f0-9]{32}$`)
var nameRE = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,46}[a-z0-9])?$`)
var slugRE = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,111}[a-z0-9])?$`)
var keyRE = regexp.MustCompile(`^[A-Za-z0-9_-]{8,128}$`)

func hash(value any) string {
	b, _ := json.Marshal(value)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Owner.ID is hidden from JSON: hash explicit fields, never the Owner struct.
func ownerKey(o Owner) string { return hash([]string{o.Kind, o.ID}) }
func operationID(a Actor, key string) string {
	return hash([]string{a.Owner().Kind, a.Owner().ID, key})
}
func validateReservation(r Reservation) error {
	if !keyRE.MatchString(r.Key) || !digestRE.MatchString(r.Digest) || r.Bytes < 0 || r.Bytes > 200<<20 {
		return ErrInvalid
	}
	if r.ProjectID == "" {
		if r.ExpectedRevision != 0 || (r.Name != "" && !nameRE.MatchString(r.Name)) {
			return ErrInvalid
		}
	} else if !idRE.MatchString(r.ProjectID) || r.ExpectedRevision < 1 || r.Name != "" {
		return ErrInvalid
	}
	return nil
}

// ValidateInput checks request metadata before accepting a potentially large body.
func ValidateInput(key, id, name string, revision int64) error {
	return validateReservation(Reservation{Key: key, ProjectID: id, Name: name, ExpectedRevision: revision, Digest: hex.EncodeToString(make([]byte, 32))})
}

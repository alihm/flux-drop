package project

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/runonflux/flux-drop/internal/session"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FirestoreRepository struct {
	Client                               *firestore.Client
	Now                                  func() time.Time
	AnonymousLimit, AccountLimit         int64
	AnonymousByteLimit, AccountByteLimit int64
}
type slugRecord struct {
	ProjectID string `firestore:"projectID"`
}
type digestRecord struct {
	ProjectID   string `firestore:"projectID"`
	OperationID string `firestore:"operationID"`
}
type quota struct {
	Count        int64 `firestore:"count"`
	Schema       int   `firestore:"schema"`
	ChargedBytes int64 `firestore:"chargedBytes"`
}

func (s *FirestoreRepository) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC().Truncate(time.Microsecond)
	}
	return time.Now().UTC().Truncate(time.Microsecond)
}
func (s *FirestoreRepository) limit(o Owner) int64 {
	if o.Kind == "anonymous" {
		if s.AnonymousLimit > 0 {
			return s.AnonymousLimit
		}
		return 10
	}
	if s.AccountLimit > 0 {
		return s.AccountLimit
	}
	return 100
}
func (s *FirestoreRepository) ref(collection, id string) *firestore.DocumentRef {
	return s.Client.Collection("drop_" + collection).Doc(id)
}
func missing(err error) bool { return status.Code(err) == codes.NotFound }
func read[T any](tx *firestore.Transaction, ref *firestore.DocumentRef) (T, error) {
	var value T
	doc, err := tx.Get(ref)
	if err != nil {
		return value, err
	}
	err = doc.DataTo(&value)
	return value, err
}
func (s *FirestoreRepository) authorize(tx *firestore.Transaction, a Actor) error {
	if !digestRE.MatchString(a.SessionDigest) || a.AnonymousID == "" {
		return session.ErrUnauthorized
	}
	r, err := read[session.Record](tx, s.ref("sessions", a.SessionDigest))
	if missing(err) {
		return session.ErrUnauthorized
	}
	if err != nil {
		return err
	}
	if !r.Active(s.now()) || r.AnonymousOwner != a.AnonymousID {
		return session.ErrUnauthorized
	}
	if a.UID != "" && (a.UID != r.UID || !s.now().Before(r.AuthUntil)) {
		return session.ErrUnauthorized
	}
	return nil
}

func (s *FirestoreRepository) Reserve(ctx context.Context, a Actor, r Reservation) (Prepared, error) {
	if err := validateReservation(r); err != nil {
		return Prepared{}, err
	}
	opID := operationID(a, r.Key)
	var result Prepared
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		op, err := read[Operation](tx, s.ref("operations", opID))
		if err == nil {
			if op.Fingerprint != hash(r) || op.Owner != a.Owner() || op.State == "aborted" {
				return ErrConflict
			}
			p, err := read[Project](tx, s.ref("projects", op.ProjectID))
			if err != nil {
				return err
			}
			if !a.Owns(p.Owner) || p.Status == "deleted" || (p.ExpiresAt != nil && !s.now().Before(*p.ExpiresAt)) {
				return ErrNotFound
			}
			if op.State != "complete" && !s.now().Before(op.ExpiresAt) {
				return ErrConflict
			}
			if err := s.checkNotRetired(tx, VersionRef{p.ID, op.Digest}); err != nil {
				return err
			}
			result = Prepared{p, op}
			return nil
		}
		if !missing(err) {
			return err
		}
		now := s.now()
		isNew := r.ProjectID == ""
		var p Project
		slugExists := false
		if isNew {
			name := r.Name
			if name == "" {
				adjectives := []string{"quiet", "bright", "gentle", "bold"}
				nouns := []string{"pine", "river", "cloud", "meadow"}
				name = adjectives[int(opID[0])%4] + "-" + nouns[int(opID[1])%4] + "-" + opID[2:6]
			}
			p = Project{ID: opID[:32], Owner: a.Owner(), OwnerKey: ownerKey(a.Owner()), Slug: name + "-" + r.Digest[:6], InitialSuffix: r.Digest[:6], CreatedAt: now, PolicyRevision: 1, Status: "reserved"}
			if p.Owner.Kind == "anonymous" {
				expiry := now.Add(30 * 24 * time.Hour)
				p.ExpiresAt = &expiry
			}
			if _, err := tx.Get(s.ref("projects", p.ID)); !missing(err) {
				if err != nil {
					return err
				}
				return ErrConflict
			}
			if _, err := tx.Get(s.ref("slugs", p.Slug)); !missing(err) {
				if err != nil {
					return err
				}
				slugExists = true
			}
		} else {
			p, err = read[Project](tx, s.ref("projects", r.ProjectID))
			if missing(err) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			if !a.Owns(p.Owner) || !p.Live(now) {
				return ErrNotFound
			}
			if p.Revision != r.ExpectedRevision || p.PendingOperation != "" {
				return ErrConflict
			}
		}
		if err := s.checkNotRetired(tx, VersionRef{p.ID, r.Digest}); err != nil {
			return err
		}
		idx, err := read[digestRecord](tx, s.ref("digests", r.Digest))
		if err == nil && idx.ProjectID != p.ID {
			other, e := read[Project](tx, s.ref("projects", idx.ProjectID))
			if e != nil && !missing(e) {
				return e
			}
			if e == nil && other.Live(now) && other.ActiveDigest == r.Digest {
				if !other.Private || a.Owns(other.Owner) {
					return &Duplicate{Slug: other.Slug}
				}
				return ErrConflict
			}
			if e == nil && other.Status != "deleted" && other.PendingOperation == idx.OperationID && (other.ExpiresAt == nil || now.Before(*other.ExpiresAt)) {
				return ErrConflict
			}
		} else if err != nil && !missing(err) {
			return err
		}
		if slugExists {
			return ErrConflict
		}
		q, err := s.readQuota(tx, p.Owner)
		if err != nil && !missing(err) {
			return err
		}
		if isNew && q.Count >= s.limit(p.Owner) {
			return ErrQuota
		}
		charge := versionCharge(r.Bytes)
		if !isNew && (p.ChargedBytes <= 0 || p.ChargedBytes > q.ChargedBytes) {
			return ErrConflict
		}
		if !fitsBytes(q.ChargedBytes, charge, s.byteLimit(p.Owner)) {
			return ErrQuota
		}
		q.ChargedBytes += charge
		p.ChargedBytes += charge
		op = Operation{ID: opID, Owner: a.Owner(), Fingerprint: hash(r), ProjectID: p.ID, Digest: r.Digest, Bytes: r.Bytes, BaseRevision: p.Revision, New: isNew, State: "pending", ExpiresAt: now.Add(15 * time.Minute)}
		p.PendingOperation = opID
		if isNew {
			q.Count++
			if err := tx.Create(s.ref("slugs", p.Slug), slugRecord{p.ID}); err != nil {
				return err
			}
		}
		if err := tx.Set(s.ref("quotas", ownerKey(p.Owner)), q); err != nil {
			return err
		}
		if err := tx.Set(s.ref("projects", p.ID), p); err != nil {
			return err
		}
		if err := tx.Create(s.ref("operations", opID), op); err != nil {
			return err
		}
		if err := tx.Set(s.ref("digests", r.Digest), digestRecord{p.ID, opID}); err != nil {
			return err
		}
		result = Prepared{p, op}
		return nil
	})
	return result, err
}

func (s *FirestoreRepository) Activate(ctx context.Context, a Actor, opID string) (Project, error) {
	if !digestRE.MatchString(opID) {
		return Project{}, ErrInvalid
	}
	var result Project
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		op, err := read[Operation](tx, s.ref("operations", opID))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if op.Owner != a.Owner() {
			return ErrNotFound
		}
		p, err := read[Project](tx, s.ref("projects", op.ProjectID))
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) || p.Status == "deleted" || (p.ExpiresAt != nil && !s.now().Before(*p.ExpiresAt)) {
			return ErrNotFound
		}
		if err := s.checkNotRetired(tx, VersionRef{p.ID, op.Digest}); err != nil {
			return err
		}
		if op.State == "complete" {
			result = p
			return nil
		}
		if op.State != "pending" || !s.now().Before(op.ExpiresAt) || p.PendingOperation != op.ID || p.Revision != op.BaseRevision {
			return ErrConflict
		}
		idx, err := read[digestRecord](tx, s.ref("digests", op.Digest))
		if err != nil {
			return err
		}
		if idx.ProjectID != p.ID || idx.OperationID != op.ID {
			return ErrConflict
		}
		var old digestRecord
		if p.ActiveDigest != "" && p.ActiveDigest != op.Digest {
			old, err = read[digestRecord](tx, s.ref("digests", p.ActiveDigest))
			if err != nil && !missing(err) {
				return err
			}
		}
		if old.ProjectID == p.ID {
			if err := tx.Delete(s.ref("digests", p.ActiveDigest)); err != nil {
				return err
			}
		}
		p.ActiveDigest, p.ActiveBytes, p.Status, p.PendingOperation = op.Digest, op.Bytes, "active", ""
		if op.New {
			p.CreatedAt = s.now()
			if p.Owner.Kind == "anonymous" {
				expiry := p.CreatedAt.Add(30 * 24 * time.Hour)
				p.ExpiresAt = &expiry
			}
		}
		p.Revision++
		op.State = "complete"
		if err := tx.Set(s.ref("operations", opID), op); err != nil {
			return err
		}
		if err := tx.Set(s.ref("digests", op.Digest), digestRecord{ProjectID: p.ID}); err != nil {
			return err
		}
		if err := tx.Set(s.ref("projects", p.ID), p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

func (s *FirestoreRepository) Abort(ctx context.Context, a Actor, opID string) error {
	return s.abort(ctx, &a, opID, false)
}

func (s *FirestoreRepository) abort(ctx context.Context, a *Actor, opID string, expiredOnly bool) error {
	if !digestRE.MatchString(opID) {
		return ErrInvalid
	}
	return s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if !expiredOnly {
			if a == nil {
				return ErrForbidden
			}
			if err := s.authorize(tx, *a); err != nil {
				return err
			}
		}
		op, err := read[Operation](tx, s.ref("operations", opID))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !expiredOnly && op.Owner != a.Owner() {
			return ErrNotFound
		}
		if op.State != "pending" {
			return nil
		}
		if expiredOnly && s.now().Before(op.ExpiresAt) {
			return nil
		}
		p, err := read[Project](tx, s.ref("projects", op.ProjectID))
		if err != nil {
			return err
		}
		if (!expiredOnly && !a.Owns(p.Owner)) || p.PendingOperation != op.ID {
			return ErrConflict
		}
		idx, err := read[digestRecord](tx, s.ref("digests", op.Digest))
		if err != nil && !missing(err) {
			return err
		}
		q, err := s.readQuota(tx, p.Owner)
		if err != nil {
			return err
		}
		if idx.ProjectID == p.ID && idx.OperationID == op.ID {
			if p.ActiveDigest == op.Digest {
				if err := tx.Set(s.ref("digests", op.Digest), digestRecord{ProjectID: p.ID}); err != nil {
					return err
				}
			} else if err := tx.Delete(s.ref("digests", op.Digest)); err != nil {
				return err
			}
		}
		p.PendingOperation = ""
		op.State = "aborted"
		if op.New {
			p.Status = "deleted"
			if q.Count <= 0 {
				return ErrConflict
			}
			if err := tx.Set(s.ref("quotas", ownerKey(p.Owner)), quota{Count: q.Count - 1, Schema: q.Schema, ChargedBytes: q.ChargedBytes}); err != nil {
				return err
			}
			if err := tx.Delete(s.ref("slugs", p.Slug)); err != nil {
				return err
			}
		}
		if err := tx.Set(s.ref("projects", p.ID), p); err != nil {
			return err
		}
		return tx.Set(s.ref("operations", opID), op)
	})
}

// RecoverExpired is a maintenance operation, never an unauthenticated HTTP API.
// Each candidate is rechecked transactionally, so a stale query cannot roll back
// a completed operation. File garbage collection is a separate later phase.
func (s *FirestoreRepository) RecoverExpired(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 100 {
		return 0, ErrInvalid
	}
	query := s.Client.Collection("drop_operations").Where("state", "==", "pending").Where("expiresAt", "<=", s.now()).OrderBy("expiresAt", firestore.Asc).Limit(limit)
	iter := query.Documents(ctx)
	defer iter.Stop()
	processed := 0
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			return processed, nil
		}
		if err != nil {
			return processed, err
		}
		if err := s.abort(ctx, nil, doc.Ref.ID, true); err != nil {
			return processed, err
		}
		processed++
	}
}

func (s *FirestoreRepository) Resolve(ctx context.Context, slug string) (Project, error) {
	if !slugRE.MatchString(slug) {
		return Project{}, ErrNotFound
	}
	var result Project
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		idx, err := read[slugRecord](tx, s.ref("slugs", slug))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		p, err := read[Project](tx, s.ref("projects", idx.ProjectID))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !p.Live(s.now()) {
			return ErrNotFound
		}
		result = p
		return nil
	}, firestore.ReadOnly)
	return result, err
}
func (s *FirestoreRepository) GetOwned(ctx context.Context, a Actor, id string) (Project, error) {
	if !idRE.MatchString(id) {
		return Project{}, ErrNotFound
	}
	var result Project
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := read[Project](tx, s.ref("projects", id))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) || !p.Live(s.now()) {
			return ErrNotFound
		}
		result = p
		return nil
	}, firestore.ReadOnly)
	return result, err
}

// Cursor encodes the last scanned ID. Filtering expired/reserved records can
// produce an empty page with a nonempty cursor; callers should follow it.
func (s *FirestoreRepository) ListOwned(ctx context.Context, a Actor, cursor string, limit int) ([]Project, string, error) {
	if limit < 1 || limit > 100 || (cursor != "" && !idRE.MatchString(cursor)) {
		return nil, "", ErrInvalid
	}
	var projects []Project
	var next string
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		projects = make([]Project, 0)
		next = ""
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		owners := []string{ownerKey(Owner{"anonymous", a.AnonymousID})}
		if a.UID != "" {
			owners = append(owners, ownerKey(Owner{"firebase", a.UID}))
		}
		query := s.Client.Collection("drop_projects").Where("ownerKey", "in", owners).OrderBy(firestore.DocumentID, firestore.Asc).Limit(limit)
		if cursor != "" {
			query = query.StartAfter(s.ref("projects", cursor))
		}
		iter := tx.Documents(query)
		defer iter.Stop()
		scanned := 0
		var last string
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}
			scanned++
			last = doc.Ref.ID
			var p Project
			if err := doc.DataTo(&p); err != nil {
				return err
			}
			if p.Live(s.now()) && a.Owns(p.Owner) {
				projects = append(projects, p)
			}
		}
		if scanned == limit {
			next = last
		}
		return nil
	}, firestore.ReadOnly)
	return projects, next, err
}

func (s *FirestoreRepository) Claim(ctx context.Context, a Actor, id string, revision int64) (Project, error) {
	if !idRE.MatchString(id) || revision < 1 {
		return Project{}, ErrInvalid
	}
	if a.UID == "" {
		return Project{}, ErrForbidden
	}
	var result Project
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := read[Project](tx, s.ref("projects", id))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) || !p.Live(s.now()) {
			return ErrNotFound
		}
		if p.Revision != revision || p.PendingOperation != "" {
			return ErrConflict
		}
		if p.Owner.Kind == "firebase" {
			result = p
			return nil
		}
		oldQuota, err := s.readQuota(tx, p.Owner)
		if err != nil {
			return err
		}
		newOwner := Owner{"firebase", a.UID}
		newQuota, err := s.readQuota(tx, newOwner)
		if err != nil && !missing(err) {
			return err
		}
		if newQuota.Count >= s.limit(newOwner) {
			return ErrQuota
		}
		if oldQuota.Count < 1 {
			return ErrConflict
		}
		if p.ChargedBytes <= 0 || p.ChargedBytes > oldQuota.ChargedBytes {
			return ErrConflict
		}
		if !fitsBytes(newQuota.ChargedBytes, p.ChargedBytes, s.byteLimit(newOwner)) {
			return ErrQuota
		}
		oldQuota.Count--
		oldQuota.ChargedBytes -= p.ChargedBytes
		newQuota.Count++
		newQuota.ChargedBytes += p.ChargedBytes
		if err := tx.Set(s.ref("quotas", ownerKey(p.Owner)), oldQuota); err != nil {
			return err
		}
		if err := tx.Set(s.ref("quotas", ownerKey(newOwner)), newQuota); err != nil {
			return err
		}
		p.Owner, p.ExpiresAt = newOwner, nil
		p.OwnerKey = ownerKey(newOwner)
		p.Revision++
		if err := tx.Set(s.ref("projects", id), p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

func (s *FirestoreRepository) Tombstone(ctx context.Context, a Actor, id string, revision int64) error {
	if !idRE.MatchString(id) || revision < 1 {
		return ErrInvalid
	}
	return s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := read[Project](tx, s.ref("projects", id))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) {
			return ErrNotFound
		}
		if p.Status == "deleted" {
			return nil
		}
		if p.Revision != revision || p.PendingOperation != "" {
			return ErrConflict
		}
		q, err := s.readQuota(tx, p.Owner)
		if err != nil {
			return err
		}
		idx, err := read[digestRecord](tx, s.ref("digests", p.ActiveDigest))
		if err != nil && !missing(err) {
			return err
		}
		if idx.ProjectID == p.ID {
			if err := tx.Delete(s.ref("digests", p.ActiveDigest)); err != nil {
				return err
			}
		}
		if q.Count <= 0 {
			return ErrConflict
		}
		if err := tx.Set(s.ref("quotas", ownerKey(p.Owner)), quota{Count: q.Count - 1, Schema: q.Schema, ChargedBytes: q.ChargedBytes}); err != nil {
			return err
		}
		p.Status = "deleted"
		p.Revision++
		p.PolicyRevision++
		return tx.Set(s.ref("projects", id), p) // retain slug tombstone
	})
}

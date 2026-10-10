// Project rules mirror the existing transactional repository; storage is Raft.
package project

import (
	"context"
	"github.com/runonflux/flux-drop/internal/session"
	"math"
	"time"
)

func (s *RaftRepository) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC().Truncate(time.Microsecond)
	}
	return time.Now().UTC().Truncate(time.Microsecond)
}

func (s *RaftRepository) limit(o Owner) int64 {
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

func (s *RaftRepository) authorize(tx *raftTx, a Actor) error {
	if a.AgentKeyDigest != "" || a.UploadTicketDigest != "" {
		return session.ErrUnauthorized
	}
	if !a.firebaseBearerUntil.IsZero() {
		return a.authorizeFirebaseBearer(s.now())
	}
	if !digestRE.MatchString(a.SessionDigest) || a.AnonymousID == "" {
		return session.ErrUnauthorized
	}
	r, err := raftRead[session.Record](tx, s.raftRef("sessions", a.SessionDigest))
	if raftMissing(err) {
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

func (s *RaftRepository) authorizePublish(tx *raftTx, a Actor) error {
	if !a.firebaseBearerUntil.IsZero() {
		return s.authorize(tx, a)
	}
	if a.UploadTicketDigest != "" {
		return s.authorizeUploadTicket(tx, a)
	}
	if a.AgentKeyDigest == "" {
		return s.authorize(tx, a)
	}
	if !digestRE.MatchString(a.AgentKeyDigest) || a.UID == "" || a.AnonymousID != "" || a.SessionDigest != "" {
		return session.ErrUnauthorized
	}
	key, err := raftRead[AgentKey](tx, s.raftRef("agent_keys", a.AgentKeyDigest))
	if raftMissing(err) {
		return session.ErrUnauthorized
	}
	if err != nil {
		return err
	}
	if key.UID != a.UID || !s.now().Before(key.ExpiresAt) {
		return session.ErrUnauthorized
	}
	return nil
}

func (s *RaftRepository) Reserve(ctx context.Context, a Actor, r Reservation) (Prepared, error) {
	if err := validateReservation(r); err != nil {
		return Prepared{}, err
	}
	if a.AgentKeyDigest != "" && r.ProjectID != "" {
		return Prepared{}, ErrForbidden
	}
	var offers []StorageOffer
	if s.StorageOffers != nil {
		offers = s.StorageOffers()
	}
	opID := operationID(a, r.Key)
	r.StorageOperationID = opID
	var result Prepared
	err := s.runContent(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorizePublish(tx, a); err != nil {
			return err
		}
		if a.UploadTicketDigest != "" {
			ticket, err := raftRead[AgentUploadTicket](tx, "agent_upload_tickets/"+a.UploadTicketDigest)
			if err != nil {
				return err
			}
			if ticket.ProjectID != r.ProjectID || ticket.Name != r.Name || ticket.Revision != r.ExpectedRevision || r.Key != a.UploadTicketDigest {
				return ErrForbidden
			}
		}
		op, err := raftRead[Operation](tx, s.raftRef("operations", opID))
		if err == nil {
			if op.Fingerprint != hash(r) || op.Owner != a.Owner() || op.State == "aborted" {
				return ErrConflict
			}
			p, err := raftRead[Project](tx, s.raftRef("projects", op.ProjectID))
			if err != nil {
				return err
			}
			if !a.Owns(p.Owner) || p.Status == "deleted" || (p.ExpiresAt != nil && !s.now().Before(*p.ExpiresAt)) {
				return ErrNotFound
			}
			if op.State != "complete" && !s.now().Before(op.ExpiresAt) {
				return ErrConflict
			}
			if err := s.checkStorageOperation(tx, p, op); err != nil {
				return err
			}
			result = Prepared{p, op}
			return nil
		}
		if !raftMissing(err) {
			return err
		}
		now := s.now()
		isNew := r.ProjectID == ""
		var p Project
		if isNew {
			name := r.Name
			if name == "" {
				adjectives := []string{"quiet", "bright", "gentle", "bold"}
				nouns := []string{"pine", "river", "cloud", "meadow"}
				name = adjectives[int(opID[0])%4] + "-" + nouns[int(opID[1])%4] + "-" + opID[2:6]
			}
			p = Project{ID: opID[:32], Owner: a.Owner(), OwnerKey: ownerKey(a.Owner()), CreatedAt: now, PolicyRevision: 1, Status: "reserved"}
			if p.Owner.Kind == "anonymous" {
				expiry := now.Add(anonymousProjectLifetime)
				p.ExpiresAt = &expiry
			}
			if _, err := tx.Get(s.raftRef("projects", p.ID)); !raftMissing(err) {
				if err != nil {
					return err
				}
				return ErrConflict
			}
			p.Slug, p.InitialSuffix, err = chooseSlug(name, r.Digest, p.ID, func(slug string) (string, error) {
				return s.slugOwner(tx, slug)
			})
			if err != nil {
				return err
			}
		} else {
			p, err = raftRead[Project](tx, s.raftRef("projects", r.ProjectID))
			if raftMissing(err) {
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
		if s.StorageOffers == nil || r.StorageManifest == nil {
			if err := s.checkNotRetired(tx, VersionRef{p.ID, r.Digest}); err != nil {
				return err
			}
		}
		idx, err := raftRead[digestRecord](tx, s.raftRef("digests", r.Digest))
		if err == nil && idx.ProjectID != p.ID {
			other, e := raftRead[Project](tx, s.raftRef("projects", idx.ProjectID))
			if e != nil && !raftMissing(e) {
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
		} else if err != nil && !raftMissing(err) {
			return err
		}
		q, err := s.readQuota(tx, p.Owner)
		if err != nil && !raftMissing(err) {
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
		if err := s.allocateStorage(tx, &p, r, offers); err != nil {
			return err
		}
		q.ChargedBytes += charge
		p.ChargedBytes += charge
		op = Operation{ID: opID, Owner: a.Owner(), Fingerprint: hash(r), ProjectID: p.ID, Digest: r.Digest, Bytes: r.Bytes, BaseRevision: p.Revision, New: isNew, State: "pending", ExpiresAt: now.Add(15 * time.Minute)}
		if s.StorageOffers != nil && r.StorageManifest != nil {
			charge, err := raftRead[StorageOperationCharge](tx, "storage_operation_charges/"+opID)
			if err != nil {
				return err
			}
			op.StorageGeneration, op.StorageVersionKey = charge.Generation, charge.VersionKey
		}
		if err := s.checkStorageOperation(tx, p, op); err != nil {
			return err
		}
		p.PendingOperation = opID
		if isNew {
			q.Count++
			if err := tx.Set(s.raftRef("slugs", p.Slug), slugRecord{p.ID}); err != nil {
				return err
			}
		}
		if err := tx.Set(s.raftRef("quotas", ownerKey(p.Owner)), q); err != nil {
			return err
		}
		if err := tx.Set(s.raftRef("projects", p.ID), p); err != nil {
			return err
		}
		if err := tx.Create(s.raftRef("operations", opID), op); err != nil {
			return err
		}
		if err := tx.Set(s.raftRef("digests", r.Digest), digestRecord{p.ID, opID}); err != nil {
			return err
		}
		result = Prepared{p, op}
		return nil
	})
	return result, err
}

func (s *RaftRepository) Activate(ctx context.Context, a Actor, opID string) (Project, error) {
	return s.activate(ctx, a, opID, "", 0)
}

// ActivatePrivate makes a new project live and password-protected in the same
// transaction, so it is never publicly resolvable. digest must refer to a
// durably prepared password record for passwordRevision (PolicyRevision+1).
func (s *RaftRepository) ActivatePrivate(ctx context.Context, a Actor, opID, digest string, passwordRevision int64) (Project, error) {
	if !digestRE.MatchString(digest) || passwordRevision < 2 {
		return Project{}, ErrInvalid
	}
	return s.activate(ctx, a, opID, digest, passwordRevision)
}

func (s *RaftRepository) activate(ctx context.Context, a Actor, opID, passwordDigest string, passwordRevision int64) (Project, error) {
	if !digestRE.MatchString(opID) {
		return Project{}, ErrInvalid
	}
	var result Project
	err := s.runContent(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorizePublish(tx, a); err != nil {
			return err
		}
		op, err := raftRead[Operation](tx, s.raftRef("operations", opID))
		if raftMissing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if op.Owner != a.Owner() {
			return ErrNotFound
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", op.ProjectID))
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) || p.Status == "deleted" || (p.ExpiresAt != nil && !s.now().Before(*p.ExpiresAt)) {
			return ErrNotFound
		}
		if err := s.checkStorageOperation(tx, p, op); err != nil {
			return err
		}
		if op.State == "complete" {
			// A retried private publish must never silently report a public project.
			if passwordDigest != "" && !p.Private {
				return ErrConflict
			}
			result = p
			return nil
		}
		if op.State != "pending" || !s.now().Before(op.ExpiresAt) || p.PendingOperation != op.ID || p.Revision != op.BaseRevision {
			return ErrConflict
		}
		if passwordDigest != "" && (!op.New || p.PolicyRevision == math.MaxInt64 || passwordRevision != p.PolicyRevision+1) {
			return ErrConflict
		}
		idx, err := raftRead[digestRecord](tx, s.raftRef("digests", op.Digest))
		if err != nil {
			return err
		}
		if idx.ProjectID != p.ID || idx.OperationID != op.ID {
			return ErrConflict
		}
		var old digestRecord
		if p.ActiveDigest != "" && p.ActiveDigest != op.Digest {
			old, err = raftRead[digestRecord](tx, s.raftRef("digests", p.ActiveDigest))
			if err != nil && !raftMissing(err) {
				return err
			}
		}
		if old.ProjectID == p.ID {
			if err := tx.Delete(s.raftRef("digests", p.ActiveDigest)); err != nil {
				return err
			}
		}
		p.ActiveDigest, p.ActiveBytes, p.Status, p.PendingOperation = op.Digest, op.Bytes, "active", ""
		p.StorageGeneration = op.StorageGeneration
		if passwordDigest != "" {
			p.Private, p.PasswordDigest, p.PasswordRevision = true, passwordDigest, passwordRevision
			p.PolicyRevision++
		}
		if op.New {
			p.CreatedAt = s.now()
			if p.Owner.Kind == "anonymous" {
				expiry := p.CreatedAt.Add(anonymousProjectLifetime)
				p.ExpiresAt = &expiry
			}
		}
		p.Revision++
		op.State = "complete"
		op.PublishedAt = s.now()
		p.UpdatedAt = s.now()
		if a.UploadTicketDigest != "" {
			ticket, err := raftRead[AgentUploadTicket](tx, "agent_upload_tickets/"+a.UploadTicketDigest)
			if err != nil {
				return err
			}
			ticket.Result = &p
			ticket.Password = nil
			ticket.Lease = ""
			ticket.LeaseUntil = time.Time{}
			if err := tx.Set("agent_upload_tickets/"+a.UploadTicketDigest, ticket); err != nil {
				return err
			}
		}
		if err := tx.Set(s.raftRef("operations", opID), op); err != nil {
			return err
		}
		if err := tx.Set(s.raftRef("digests", op.Digest), digestRecord{ProjectID: p.ID}); err != nil {
			return err
		}
		if err := tx.Set(s.raftRef("projects", p.ID), p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

func (s *RaftRepository) Abort(ctx context.Context, a Actor, opID string) error {
	return s.abort(ctx, &a, opID, false)
}

func (s *RaftRepository) abort(ctx context.Context, a *Actor, opID string, expiredOnly bool) error {
	if !digestRE.MatchString(opID) {
		return ErrInvalid
	}
	return s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if !expiredOnly {
			if a == nil {
				return ErrForbidden
			}
			if err := s.authorizePublish(tx, *a); err != nil {
				return err
			}
		}
		op, err := raftRead[Operation](tx, s.raftRef("operations", opID))
		if raftMissing(err) {
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
		p, err := raftRead[Project](tx, s.raftRef("projects", op.ProjectID))
		if err != nil {
			return err
		}
		if (!expiredOnly && !a.Owns(p.Owner)) || p.PendingOperation != op.ID {
			return ErrConflict
		}
		idx, err := raftRead[digestRecord](tx, s.raftRef("digests", op.Digest))
		if err != nil && !raftMissing(err) {
			return err
		}
		q, err := s.readQuota(tx, p.Owner)
		if err != nil {
			return err
		}
		if idx.ProjectID == p.ID && idx.OperationID == op.ID {
			if p.ActiveDigest == op.Digest {
				if err := tx.Set(s.raftRef("digests", op.Digest), digestRecord{ProjectID: p.ID}); err != nil {
					return err
				}
			} else if err := tx.Delete(s.raftRef("digests", op.Digest)); err != nil {
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
			if err := tx.Set(s.raftRef("quotas", ownerKey(p.Owner)), quota{Count: q.Count - 1, Schema: q.Schema, ChargedBytes: q.ChargedBytes}); err != nil {
				return err
			}
			if err := tx.Delete(s.raftRef("slugs", p.Slug)); err != nil {
				return err
			}
		}
		if err := tx.Set(s.raftRef("projects", p.ID), p); err != nil {
			return err
		}
		return tx.Set(s.raftRef("operations", opID), op)
	})
}

func (s *RaftRepository) Resolve(ctx context.Context, slug string) (Project, error) {
	if !slugRE.MatchString(slug) {
		return Project{}, ErrNotFound
	}
	var result Project
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		idx, err := raftRead[slugRecord](tx, s.raftRef("slugs", slug))
		if raftMissing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", idx.ProjectID))
		if raftMissing(err) {
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
	})
	return result, err
}

func (s *RaftRepository) GetOwned(ctx context.Context, a Actor, id string) (Project, error) {
	if !idRE.MatchString(id) {
		return Project{}, ErrNotFound
	}
	var result Project
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
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
	})
	return result, err
}

func (s *RaftRepository) Claim(ctx context.Context, a Actor, id string, revision int64) (Project, error) {
	if !idRE.MatchString(id) || revision < 1 {
		return Project{}, ErrInvalid
	}
	if a.UID == "" {
		return Project{}, ErrForbidden
	}
	var result Project
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
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
		if err := s.moveToAccount(tx, &p, a.UID); err != nil {
			return err
		}
		if err := s.clearTransfer(tx, id); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

func (s *RaftRepository) Tombstone(ctx context.Context, a Actor, id string, revision int64) error {
	if !idRE.MatchString(id) || revision < 1 {
		return ErrInvalid
	}
	return s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
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
		idx, err := raftRead[digestRecord](tx, s.raftRef("digests", p.ActiveDigest))
		if err != nil && !raftMissing(err) {
			return err
		}
		if idx.ProjectID == p.ID {
			if err := tx.Delete(s.raftRef("digests", p.ActiveDigest)); err != nil {
				return err
			}
		}
		if q.Count <= 0 {
			return ErrConflict
		}
		if err := tx.Set(s.raftRef("quotas", ownerKey(p.Owner)), quota{Count: q.Count - 1, Schema: q.Schema, ChargedBytes: q.ChargedBytes}); err != nil {
			return err
		}
		p.Status = "deleted"
		p.Revision++
		p.PolicyRevision++
		if err := s.clearTransfer(tx, id); err != nil {
			return err
		}
		return tx.Set(s.raftRef("projects", id), p) // slugOwner releases every alias atomically with this tombstone
	})
}

func (s *RaftRepository) Rename(ctx context.Context, a Actor, id, name string, revision int64) (Project, error) {
	if !idRE.MatchString(id) || !nameRE.MatchString(name) || revision < 1 {
		return Project{}, ErrInvalid
	}
	var result Project
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
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
		seed := p.ActiveDigest
		if p.InitialSuffix != "" {
			seed = p.InitialSuffix + hash(p.ID)
		}
		slug, suffix, err := chooseSlug(name, seed, id, func(slug string) (string, error) {
			return s.slugOwner(tx, slug)
		})
		if err != nil {
			return err
		}
		if slug == p.Slug {
			result = p
			return nil
		}
		alias, err := raftRead[slugRecord](tx, s.raftRef("slugs", slug))
		newAlias := raftMissing(err)
		if err == nil && alias.ProjectID != id {
			owner, e := s.slugOwner(tx, slug)
			if e != nil {
				return e
			}
			newAlias = owner == ""
		}
		if err != nil && !raftMissing(err) {
			return err
		}
		if !newAlias && alias.ProjectID != id {
			return ErrConflict
		}
		// Bound permanent metadata growth while allowing reuse of prior names.
		if newAlias && p.AliasCount >= 100 {
			return ErrQuota
		}
		if p.InitialSlug == "" {
			p.InitialSlug = p.Slug
		}
		p.Slug = slug
		p.InitialSuffix = suffix
		p.Revision++
		p.PolicyRevision++
		if newAlias {
			p.AliasCount++
			if err := tx.Set(s.raftRef("slugs", slug), slugRecord{ProjectID: id}); err != nil {
				return err
			}
		}
		if err := tx.Set(s.raftRef("projects", id), p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

func (s *RaftRepository) SetPrivacy(ctx context.Context, a Actor, id string, revision int64, digest string, passwordRevision int64) (Project, error) {
	if !idRE.MatchString(id) || revision < 1 || (digest != "" && (!digestRE.MatchString(digest) || passwordRevision < 1)) || (digest == "" && passwordRevision != 0) {
		return Project{}, ErrInvalid
	}
	var result Project
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) || !p.Live(s.now()) {
			return ErrNotFound
		}
		if p.Revision != revision || p.PendingOperation != "" || p.PolicyRevision == math.MaxInt64 {
			return ErrConflict
		}
		if digest != "" && passwordRevision != p.PolicyRevision+1 {
			return ErrConflict
		}
		p.Private = digest != ""
		p.PasswordDigest = digest
		p.PasswordRevision = passwordRevision
		p.Revision++
		p.PolicyRevision++
		if err := tx.Set(s.raftRef("projects", id), p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

func (s *RaftRepository) BeginUnlock(ctx context.Context, a Actor, slug string) (Project, error) {
	if !slugRE.MatchString(slug) {
		return Project{}, ErrUnlockDenied
	}
	var result Project
	eligible := false
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		eligible = false
		result = Project{}
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		now := s.now()
		window := now.Truncate(time.Minute)
		keys := []string{hash([]string{"global"}), hash([]string{"session", a.SessionDigest})}
		limits := []int{60, 10}
		idx, err := raftRead[slugRecord](tx, s.raftRef("slugs", slug))
		if err != nil && !raftMissing(err) {
			return err
		}
		if err == nil {
			p, err := raftRead[Project](tx, s.raftRef("projects", idx.ProjectID))
			if err != nil && !raftMissing(err) {
				return err
			}
			if err == nil && p.Live(now) && p.Private && digestRE.MatchString(p.PasswordDigest) && p.PasswordRevision > 0 {
				result = p
				eligible = true
				keys = append(keys, hash([]string{"project", p.ID}))
				limits = append(limits, 10)
			}
		}
		budgets := make([]unlockBudget, len(keys))
		for i, key := range keys {
			b, err := raftRead[unlockBudget](tx, s.raftRef("unlock_budgets", key))
			if err != nil && !raftMissing(err) {
				return err
			}
			if !now.Before(b.ExpiresAt) {
				b.Count = 0
			}
			if b.Count >= limits[i] {
				return ErrUnlockLimited
			}
			expires := window.Add(time.Minute)
			if b.ExpiresAt.After(expires) {
				expires = b.ExpiresAt
			}
			budgets[i] = unlockBudget{Count: b.Count + 1, ExpiresAt: expires}
		}
		for i, key := range keys {
			if err := tx.Set(s.raftRef("unlock_budgets", key), budgets[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Project{}, err
	}
	if !eligible {
		return Project{}, ErrUnlockDenied
	}
	return result, nil
}

func (s *RaftRepository) CompleteUnlock(ctx context.Context, a Actor, verified Project, digest string) (Grant, error) {
	if !idRE.MatchString(verified.ID) || !digestRE.MatchString(digest) {
		return Grant{}, ErrInvalid
	}
	var result Grant
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", verified.ID))
		if raftMissing(err) {
			return ErrUnlockDenied
		}
		if err != nil {
			return err
		}
		if !p.Live(s.now()) || !p.Private || p.PolicyRevision != verified.PolicyRevision || p.PasswordDigest != verified.PasswordDigest || p.PasswordRevision != verified.PasswordRevision {
			return ErrUnlockDenied
		}
		result = Grant{ProjectID: p.ID, SessionDigest: a.SessionDigest, PolicyRevision: p.PolicyRevision, ExpiresAt: s.now().Add(time.Hour)}
		if p.ExpiresAt != nil && p.ExpiresAt.Before(result.ExpiresAt) {
			result.ExpiresAt = *p.ExpiresAt
		}
		return tx.Create(s.raftRef("grants", digest), result)
	})
	return result, err
}

func (s *RaftRepository) ValidateGrant(ctx context.Context, a Actor, id, token string) error {
	digest, err := session.Digest(token)
	if err != nil || !idRE.MatchString(id) {
		return ErrUnlockDenied
	}
	return s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		g, err := raftRead[Grant](tx, s.raftRef("grants", digest))
		if raftMissing(err) {
			return ErrUnlockDenied
		}
		if err != nil {
			return err
		}
		if g.ProjectID != id || g.SessionDigest != a.SessionDigest || !s.now().Before(g.ExpiresAt) {
			return ErrUnlockDenied
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
			return ErrUnlockDenied
		}
		if err != nil {
			return err
		}
		if !p.Live(s.now()) || !p.Private || p.PolicyRevision != g.PolicyRevision {
			return ErrUnlockDenied
		}
		return nil
	})
}

func (s *RaftRepository) byteLimit(o Owner) int64 {
	if o.Kind == "anonymous" {
		if s.AnonymousByteLimit > 0 {
			return s.AnonymousByteLimit
		}
		return 1 << 30
	}
	if s.AccountByteLimit > 0 {
		return s.AccountByteLimit
	}
	return 10 << 30
}

func (s *RaftRepository) readQuota(tx *raftTx, o Owner) (quota, error) {
	q, err := raftRead[quota](tx, s.raftRef("quotas", ownerKey(o)))
	if raftMissing(err) {
		return quota{Schema: 1}, err
	}
	if err == nil && (q.Schema != 1 || q.Count < 0 || q.ChargedBytes < 0) {
		return quota{}, ErrConflict
	}
	return q, err
}

func (s *RaftRepository) checkNotRetired(tx *raftTx, ref VersionRef) error {
	_, err := tx.Get(s.raftRef("retirements", retirementID(ref)))
	if raftMissing(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return ErrConflict
}

func (s *RaftRepository) ResolveForServing(ctx context.Context, slug string) (Project, error) {
	if !slugRE.MatchString(slug) {
		return Project{}, ErrNotFound
	}
	var result Project
	err := s.runServing(ctx, "slug:"+slug, true, func(ctx context.Context, tx *raftTx) error {
		idx, err := raftRead[slugRecord](tx, s.raftRef("slugs", slug))
		if raftMissing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", idx.ProjectID))
		if raftMissing(err) {
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
	})
	return result, err
}

func (s *RaftRepository) GetOwnedForServing(ctx context.Context, a Actor, id string) (Project, error) {
	if !idRE.MatchString(id) {
		return Project{}, ErrNotFound
	}
	var result Project
	err := s.runServing(ctx, "owner:"+hash([]string{a.SessionDigest, a.UID, a.AnonymousID, id}), false, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
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
	})
	return result, err
}

func (s *RaftRepository) ValidateGrantForServing(ctx context.Context, a Actor, id, token string) error {
	digest, err := session.Digest(token)
	if err != nil || !idRE.MatchString(id) {
		return ErrUnlockDenied
	}
	return s.runServing(ctx, "grant:"+hash([]string{a.SessionDigest, id, token}), false, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		g, err := raftRead[Grant](tx, s.raftRef("grants", digest))
		if raftMissing(err) {
			return ErrUnlockDenied
		}
		if err != nil {
			return err
		}
		if g.ProjectID != id || g.SessionDigest != a.SessionDigest || !s.now().Before(g.ExpiresAt) {
			return ErrUnlockDenied
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
			return ErrUnlockDenied
		}
		if err != nil {
			return err
		}
		if !p.Live(s.now()) || !p.Private || p.PolicyRevision != g.PolicyRevision {
			return ErrUnlockDenied
		}
		return nil
	})
}

func (s *RaftRepository) ValidateSelectedGrantForServing(ctx context.Context, a Actor, selected Project, token string) error {
	id := selected.ID
	digest, err := session.Digest(token)
	if err != nil || !idRE.MatchString(id) {
		return ErrUnlockDenied
	}
	return s.runServing(ctx, "grant:"+hash([]string{a.SessionDigest, id, token, hash(selected)}), false, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		g, err := raftRead[Grant](tx, s.raftRef("grants", digest))
		if raftMissing(err) {
			return ErrUnlockDenied
		}
		if err != nil {
			return err
		}
		if g.ProjectID != id || g.SessionDigest != a.SessionDigest || !s.now().Before(g.ExpiresAt) {
			return ErrUnlockDenied
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
			return ErrUnlockDenied
		}
		if err != nil {
			return err
		}
		if !p.Live(s.now()) || !p.Private || p.PolicyRevision != g.PolicyRevision || p.PolicyRevision != selected.PolicyRevision || p.ActiveDigest != selected.ActiveDigest || p.Slug != selected.Slug {
			return ErrUnlockDenied
		}
		return nil
	})
}

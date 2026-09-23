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

func (s *RaftRepository) Reserve(ctx context.Context, a Actor, r Reservation) (Prepared, error) {
	if err := validateReservation(r); err != nil {
		return Prepared{}, err
	}
	opID := operationID(a, r.Key)
	var result Prepared
	err := s.runContent(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
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
			if err := s.checkNotRetired(tx, VersionRef{p.ID, op.Digest}); err != nil {
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
			if _, err := tx.Get(s.raftRef("projects", p.ID)); !raftMissing(err) {
				if err != nil {
					return err
				}
				return ErrConflict
			}
			if _, err := tx.Get(s.raftRef("slugs", p.Slug)); !raftMissing(err) {
				if err != nil {
					return err
				}
				slugExists = true
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
		if err := s.checkNotRetired(tx, VersionRef{p.ID, r.Digest}); err != nil {
			return err
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
		if slugExists {
			return ErrConflict
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
		q.ChargedBytes += charge
		p.ChargedBytes += charge
		op = Operation{ID: opID, Owner: a.Owner(), Fingerprint: hash(r), ProjectID: p.ID, Digest: r.Digest, Bytes: r.Bytes, BaseRevision: p.Revision, New: isNew, State: "pending", ExpiresAt: now.Add(15 * time.Minute)}
		p.PendingOperation = opID
		if isNew {
			q.Count++
			if err := tx.Create(s.raftRef("slugs", p.Slug), slugRecord{p.ID}); err != nil {
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
	if !digestRE.MatchString(opID) {
		return Project{}, ErrInvalid
	}
	var result Project
	err := s.runContent(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
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
		if op.New {
			p.CreatedAt = s.now()
			if p.Owner.Kind == "anonymous" {
				expiry := p.CreatedAt.Add(30 * 24 * time.Hour)
				p.ExpiresAt = &expiry
			}
		}
		p.Revision++
		op.State = "complete"
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
			if err := s.authorize(tx, *a); err != nil {
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
		oldQuota, err := s.readQuota(tx, p.Owner)
		if err != nil {
			return err
		}
		newOwner := Owner{"firebase", a.UID}
		newQuota, err := s.readQuota(tx, newOwner)
		if err != nil && !raftMissing(err) {
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
		if err := tx.Set(s.raftRef("quotas", ownerKey(p.Owner)), oldQuota); err != nil {
			return err
		}
		if err := tx.Set(s.raftRef("quotas", ownerKey(newOwner)), newQuota); err != nil {
			return err
		}
		p.Owner, p.ExpiresAt = newOwner, nil
		p.OwnerKey = ownerKey(newOwner)
		p.Revision++
		if err := tx.Set(s.raftRef("projects", id), p); err != nil {
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
		return tx.Set(s.raftRef("projects", id), p) // retain slug tombstone
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
		slug := name + "-" + p.InitialSuffix
		if !slugRE.MatchString(slug) {
			return ErrInvalid
		}
		if slug == p.Slug {
			result = p
			return nil
		}
		alias, err := raftRead[slugRecord](tx, s.raftRef("slugs", slug))
		newAlias := raftMissing(err)
		if err != nil && !newAlias {
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
		p.Revision++
		p.PolicyRevision++
		if newAlias {
			p.AliasCount++
			if err := tx.Create(s.raftRef("slugs", slug), slugRecord{ProjectID: id}); err != nil {
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

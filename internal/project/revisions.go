package project

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
)

// Revisions identify successful publications, not project/policy CAS revisions.
// Operations already form the durable publication history, including uploads
// made before this API existed. Failed and pending uploads never appear here.
type Revision struct {
	ID        string    `json:"id"`
	Digest    string    `json:"digest"`
	Bytes     int64     `json:"bytes"`
	CreatedAt time.Time `json:"createdAt"`
	Active    bool      `json:"active"`
}

type RevisionRepository interface {
	ListRevisions(context.Context, Actor, string, string) ([]Revision, string, error)
	SelectRevision(context.Context, Actor, string, string, int64) (Project, error)
	RemoveRevision(context.Context, Actor, string, string, int64) (Project, error)
}

func revisionIdentity(op Operation) string {
	return StorageVersionID(op.ProjectID, op.Digest, op.StorageGeneration)
}

func revisionIndexPrefix(id string) string { return "versions_" + id + "/" }

// Maintenance backfills the index in bounded pages. Activations index new
// publications in their own transaction, so completing a migration never
// misses a concurrent publication, even if its ID sorts before the scan cursor.
func (s *RaftRepository) indexRevisions(ctx context.Context, ids []string) error {
	for start := 0; start < len(ids); start += 20 {
		end := min(start+20, len(ids))
		if err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
			keys := make([]string, 0, end-start)
			for _, id := range ids[start:end] {
				keys = append(keys, "operations/"+id)
			}
			if err := tx.tx.Prefetch(keys); err != nil {
				return err
			}
			for _, id := range ids[start:end] {
				op, err := raftRead[Operation](tx, "operations/"+id)
				if err != nil {
					return err
				}
				if op.State != "complete" {
					continue
				}
				if op.ID != id || !idRE.MatchString(op.ProjectID) {
					return ErrStorage
				}
				key := revisionIndexPrefix(op.ProjectID) + id
				var existing string
				if err = tx.tx.Get(key, &existing); err == nil && existing == id {
					continue
				} else if err != nil && !raftMissing(err) {
					return err
				}
				if err = tx.Set(key, id); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *RaftRepository) revisionProject(tx *raftTx, a Actor, id string) (Project, error) {
	if err := s.authorize(tx, a); err != nil {
		return Project{}, err
	}
	p, err := raftRead[Project](tx, "projects/"+id)
	if raftMissing(err) {
		return p, ErrNotFound
	}
	if err != nil {
		return p, err
	}
	if !a.Owns(p.Owner) || !p.Live(s.now()) {
		return Project{}, ErrNotFound
	}
	return p, nil
}

func (s *RaftRepository) availableRevision(tx *raftTx, p Project, id string) (Operation, error) {
	op, err := raftRead[Operation](tx, "operations/"+id)
	if raftMissing(err) {
		return op, ErrNotFound
	}
	if err != nil {
		return op, err
	}
	if op.ID != id || op.ProjectID != p.ID || op.State != "complete" {
		return op, ErrNotFound
	}
	if _, err = tx.Get("revision_removals/" + revisionIdentity(op)); err == nil {
		return op, ErrNotFound
	} else if !raftMissing(err) {
		return op, err
	}
	if p.StorageApp != "" && op.StorageVersionKey != "" {
		v, e := raftRead[StorageVersion](tx, "storage_versions/"+op.StorageVersionKey)
		if e != nil {
			return op, e
		}
		if v.ID != revisionIdentity(op) || op.StorageVersionKey != v.ID || v.ProjectID != p.ID || v.Digest != op.Digest || v.Generation != op.StorageGeneration || v.App != p.StorageApp || v.State != "retained" || v.Unverified {
			return op, ErrNotFound
		}
	} else if err := s.checkNotRetired(tx, VersionRef{p.ID, op.Digest}); err != nil {
		if errors.Is(err, ErrConflict) {
			return op, ErrNotFound
		}
		return op, err
	}
	return op, nil
}

// Bound each scan to 100 candidate operations and each authoritative read to
// 20 matching publications (under Tx's 128-key budget). History is ordered by
// opaque ID, not date. Empty pages can have a continuation cursor. This uses
// project-specific publication index after background backfill. While backfill
// is incomplete, existing history provides a bounded, migration-free fallback.
// Only this owner dashboard path scans history; serving never does.
func (s *RaftRepository) ListRevisions(ctx context.Context, a Actor, id, cursor string) ([]Revision, string, error) {
	if !idRE.MatchString(id) || cursor != "" && !digestRE.MatchString(cursor) {
		return nil, "", ErrInvalid
	}
	if _, err := s.GetOwned(ctx, a, id); err != nil {
		return nil, "", err
	}
	scanner, ok := s.Store.Backend.(metadataScanner)
	if !ok {
		return nil, "", ErrStorage
	}
	indexed := false
	if err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		err := tx.tx.Get("revision_catalog/v1", &indexed)
		if raftMissing(err) {
			return nil
		}
		return err
	}); err != nil {
		return nil, "", err
	}
	prefix, limit := "operations/", 100
	if indexed {
		prefix, limit = revisionIndexPrefix(id), 20
	}
	after := ""
	if cursor != "" {
		after = prefix + cursor
	}
	page, err := scanner.Scan(ctx, prefix, after, limit)
	if err != nil {
		return nil, "", err
	}
	keys := make([]string, 0, len(page.Records))
	for key := range page.Records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ids := []string{}
	next := strings.TrimPrefix(page.Next, prefix)
	for i, key := range keys {
		if indexed {
			var operationID string
			if err := metadata.Decode(page.Records[key].Value, &operationID); err != nil {
				return nil, "", err
			}
			if !digestRE.MatchString(operationID) || key != prefix+operationID {
				return nil, "", ErrStorage
			}
			ids = append(ids, operationID)
			continue
		}
		var op Operation
		if err := metadata.Decode(page.Records[key].Value, &op); err != nil {
			return nil, "", err
		}
		if op.ProjectID == id && op.State == "complete" {
			ids = append(ids, strings.TrimPrefix(key, prefix))
			if len(ids) == 20 && i+1 < len(keys) {
				next = strings.TrimPrefix(key, prefix)
				break
			}
		}
	}
	var result []Revision
	err = s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		result = make([]Revision, 0, len(ids))
		p, err := s.revisionProject(tx, a, id)
		if err != nil {
			return err
		}
		for _, id := range ids {
			op, err := s.availableRevision(tx, p, id)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			created := op.PublishedAt
			if created.IsZero() {
				created = op.ExpiresAt.Add(-15 * time.Minute)
			}
			result = append(result, Revision{op.ID, op.Digest, op.Bytes, created, p.ActiveDigest == op.Digest && p.StorageGeneration == op.StorageGeneration})
		}
		return nil
	})
	return result, next, err
}

func (s *RaftRepository) SelectRevision(ctx context.Context, a Actor, id, revisionID string, revision int64) (Project, error) {
	return s.changeRevision(ctx, a, id, revisionID, revision, false)
}
func (s *RaftRepository) RemoveRevision(ctx context.Context, a Actor, id, revisionID string, revision int64) (Project, error) {
	return s.changeRevision(ctx, a, id, revisionID, revision, true)
}

func (s *RaftRepository) changeRevision(ctx context.Context, a Actor, id, revisionID string, revision int64, remove bool) (Project, error) {
	if !idRE.MatchString(id) || !digestRE.MatchString(revisionID) || revision < 1 {
		return Project{}, ErrInvalid
	}
	var result Project
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		p, err := s.revisionProject(tx, a, id)
		if err != nil {
			return err
		}
		if p.Revision != revision || p.PendingOperation != "" {
			return ErrConflict
		}
		op, err := s.availableRevision(tx, p, revisionID)
		if err != nil {
			// Backends without snapshot reads can observe an old project
			// revision followed by a newly committed removal. Validate before
			// returning a domain error so CAS retries preserve If-Match semantics.
			if check := tx.tx.ValidateReads(); check != nil {
				return check
			}
			return err
		}
		active := p.ActiveDigest == op.Digest && p.StorageGeneration == op.StorageGeneration
		if active {
			if remove {
				return ErrConflict
			}
			result = p
			return nil
		}
		if remove {
			if err = tx.Set("revision_removals/"+revisionIdentity(op), s.now()); err != nil {
				return err
			}
			if p.StorageApp == "" {
				if err = tx.Set("retirements/"+retirementID(VersionRef{p.ID, op.Digest}), struct{ Removed bool }{true}); err != nil {
					return err
				}
			} else if err = tx.Set(StorageReclamationPrefix(p.StorageApp)+revisionIdentity(op), revisionIdentity(op)); err != nil {
				return err
			}
		} else {
			idx, err := raftRead[digestRecord](tx, "digests/"+op.Digest)
			if err != nil && !raftMissing(err) {
				return err
			}
			if idx.ProjectID != "" && idx.ProjectID != p.ID {
				return ErrConflict
			}
			old, err := raftRead[digestRecord](tx, "digests/"+p.ActiveDigest)
			if err != nil && !raftMissing(err) {
				return err
			}
			if old.ProjectID == p.ID && p.ActiveDigest != op.Digest {
				if err = tx.Delete("digests/" + p.ActiveDigest); err != nil {
					return err
				}
			}
			if err = tx.Set("digests/"+op.Digest, digestRecord{ProjectID: p.ID}); err != nil {
				return err
			}
			p.ActiveDigest, p.ActiveBytes, p.StorageGeneration = op.Digest, op.Bytes, op.StorageGeneration
			// A selection is a replicated serving policy change. In-flight
			// cleanup CASes the same project/version records, so it cannot retire
			// a version concurrently with making it active.
			p.PolicyRevision++
		}
		p.Revision++
		p.UpdatedAt = s.now()
		if err = tx.Set("projects/"+p.ID, p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

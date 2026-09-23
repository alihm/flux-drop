package project

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
)

// VersionRef identifies an immutable version, never an arbitrary filesystem path.
type VersionRef struct {
	ProjectID string `json:"projectID"`
	Digest    string `json:"digest"`
}

// RetentionFinding is a read-only observation, NOT permission to delete. A
// candidate can become referenced immediately after this transaction completes.
type RetentionFinding struct {
	VersionRef
	Classification string    `json:"classification"`
	Reason         string    `json:"reason"`
	Revision       int64     `json:"revision"`
	ObservedAt     time.Time `json:"observedAt"`
}

func retentionFinding(ref VersionRef, p Project, now time.Time) RetentionFinding {
	f := RetentionFinding{VersionRef: ref, Classification: "hold", Revision: p.Revision, ObservedAt: now}
	switch {
	case p.ID != ref.ProjectID || p.ChargedBytes <= 0 || p.Revision < 0:
		f.Reason = "metadata_incomplete"
	case p.PendingOperation != "":
		// Hold every version, not just the pending digest: an expired operation may
		// still have an in-flight installer. Recovery/fencing must precede cleanup.
		f.Reason = "publication_pending"
	case p.Status == "reserved":
		f.Reason = "publication_reserved"
	case p.Status == "deleted":
		f.Classification = "candidate"
		f.Reason = "project_deleted"
	case p.Status != "active" || !digestRE.MatchString(p.ActiveDigest) || p.Revision < 1:
		f.Reason = "metadata_incomplete"
	case p.ExpiresAt != nil && !now.Before(*p.ExpiresAt):
		f.Reason = "expiry_not_finalized"
	case p.ActiveDigest == ref.Digest:
		f.Reason = "active_version"
	default:
		f.Classification = "candidate"
		f.Reason = "superseded_version"
	}
	return f
}

// AuditRetention examines at most 100 explicit version identifiers in one
// authoritative read-only snapshot. It neither scans disk nor mutates metadata,
// deletes files, refunds quotas, or infers replica membership from discovery.
// Any read failure discards the entire report; missing metadata is held.
func (s *FirestoreRepository) AuditRetention(ctx context.Context, refs []VersionRef) ([]RetentionFinding, error) {
	if len(refs) == 0 || len(refs) > 100 {
		return nil, ErrInvalid
	}
	seen := make(map[VersionRef]bool, len(refs))
	for _, ref := range refs {
		if !idRE.MatchString(ref.ProjectID) || !digestRE.MatchString(ref.Digest) || seen[ref] {
			return nil, ErrInvalid
		}
		seen[ref] = true
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var report []RetentionFinding
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		report = make([]RetentionFinding, 0, len(refs))
		now := s.now()
		cache := make(map[string]Project)
		absent := make(map[string]bool)
		for _, ref := range refs {
			p, loaded := cache[ref.ProjectID]
			if !loaded && !absent[ref.ProjectID] {
				var err error
				p, err = read[Project](tx, s.ref("projects", ref.ProjectID))
				if missing(err) {
					absent[ref.ProjectID] = true
				} else if err != nil {
					return err
				} else {
					cache[ref.ProjectID] = p
				}
			}
			if absent[ref.ProjectID] {
				report = append(report, RetentionFinding{VersionRef: ref, Classification: "hold", Reason: "metadata_missing", ObservedAt: now})
				continue
			}
			finding := retentionFinding(ref, p, now)
			if _, err := tx.Get(s.ref("retirements", retirementID(ref))); err == nil {
				finding.Classification, finding.Reason = "hold", "retirement_recorded"
			} else if !missing(err) {
				return err
			}
			report = append(report, finding)
		}
		return nil
	}, firestore.ReadOnly)
	if err != nil {
		return nil, err
	}
	return report, nil
}

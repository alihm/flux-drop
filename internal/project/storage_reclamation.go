package project

import (
	"context"
	"slices"
	"sort"
)

// RetireStorageVersion irreversibly fences this physical identity. Restoring
// identical content requires a new generation. Membership is a complete, sorted
// Syncthing folder device set, not a list of currently reachable Flux nodes.
func (s *RaftRepository) RetireStorageVersion(ctx context.Context, id, epoch string, members []string) (StorageVersion, error) {
	if !digestRE.MatchString(id) || !digestRE.MatchString(epoch) || len(members) < 1 || len(members) > 64 {
		return StorageVersion{}, ErrInvalid
	}
	members = slices.Clone(members)
	sort.Strings(members)
	for i, member := range members {
		if member == "" || len(member) > 128 || i > 0 && members[i-1] == member {
			return StorageVersion{}, ErrInvalid
		}
	}
	var result StorageVersion
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		v, err := raftRead[StorageVersion](tx, "storage_versions/"+id)
		if err != nil {
			return err
		}
		if v.ID != id || v.ID != StorageVersionID(v.ProjectID, v.Digest, v.Generation) || v.Unverified {
			return ErrStorage
		}
		if v.State == "reclaimed" {
			if v.MembershipEpoch != epoch || !slices.Equal(v.Members, members) {
				return ErrConflict
			}
			result = v
			return nil
		}
		p, err := raftRead[Project](tx, "projects/"+v.ProjectID)
		if err != nil {
			return err
		}
		if p.StorageApp != v.App || p.Status != "deleted" && p.Status != "active" || p.PendingOperation != "" || p.Status == "active" && p.ActiveDigest == v.Digest && p.StorageGeneration == v.Generation {
			return ErrConflict
		}
		if p.Status != "deleted" && p.ExpiresAt != nil && !s.now().Before(*p.ExpiresAt) {
			return ErrConflict
		}
		if v.State != "retained" && v.State != "retiring" {
			return ErrConflict
		}
		allocation, err := raftRead[StorageAllocation](tx, "storage_allocations/"+v.App)
		if err != nil {
			return err
		}
		q, err := s.readQuota(tx, p.Owner)
		if err != nil {
			return err
		}
		if !idRE.MatchString(v.ProjectID) || !digestRE.MatchString(v.Digest) || v.Generation != "" && !digestRE.MatchString(v.Generation) || allocation.Schema != 2 || allocation.BlockBytes != measuredStorageFence || v.BlockBytes < 1 || v.BlockBytes > allocation.AccountingBlockBytes || v.Bytes <= 0 || v.Bytes > 1<<40 || v.Inodes > 1<<20 || v.ContentBytes < 0 || v.ContentBytes > 200<<20 || v.QuotaBytes <= 0 || p.ChargedBytes < v.QuotaBytes || q.ChargedBytes < p.ChargedBytes {
			if err := tx.tx.ValidateReads(); err != nil {
				return err
			}
			return ErrStorage
		}
		if v.State == "retained" {
			v.State, v.RetiredAt = "retiring", s.now()
		}
		// Deletion is irreversible. A secondary may then fence and remove the
		// entire project namespace, including the shared hash marker/parents.
		v.ProjectDeleted = p.Status == "deleted"
		if v.MembershipEpoch != "" && (v.MembershipEpoch != epoch || !slices.Equal(v.Members, members)) {
			return ErrConflict
		}
		if v.MembershipEpoch == "" {
			v.MembershipEpoch, v.Members, v.Acknowledged = epoch, members, map[string]bool{}
		}
		if v.Generation == "" {
			// Older writers still address content by digest. Their publication
			// fence must exist before deleting a legacy directory.
			if err := tx.tx.Set("retirements/"+retirementID(VersionRef{v.ProjectID, v.Digest}), struct{ StorageVersionID string }{v.ID}); err != nil {
				return err
			}
		}
		if err := tx.tx.Set("storage_versions/"+id, v); err != nil {
			return err
		}
		result = v
		return nil
	})
	return result, err
}

// AcknowledgeStorageReclamation refunds once, only after every authoritative
// replication member has durably fenced writers/replication and removed bytes.
// A failed/unknown response never counts as an acknowledgement.
func (s *RaftRepository) AcknowledgeStorageReclamation(ctx context.Context, id, epoch, member string) error {
	if !digestRE.MatchString(id) || !digestRE.MatchString(epoch) {
		return ErrInvalid
	}
	return s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		v, err := raftRead[StorageVersion](tx, "storage_versions/"+id)
		if err != nil {
			return err
		}
		if v.State == "reclaimed" {
			if v.MembershipEpoch != epoch || !slices.Contains(v.Members, member) {
				return ErrConflict
			}
			return nil
		}
		if v.State != "retiring" || v.MembershipEpoch != epoch || !slices.Contains(v.Members, member) {
			return ErrConflict
		}
		if v.Acknowledged == nil {
			v.Acknowledged = map[string]bool{}
		}
		v.Acknowledged[member] = true
		for _, required := range v.Members {
			if !v.Acknowledged[required] {
				return tx.tx.Set("storage_versions/"+id, v)
			}
		}
		p, err := raftRead[Project](tx, "projects/"+v.ProjectID)
		if err != nil {
			return err
		}
		if p.StorageApp != v.App || v.ProjectDeleted && p.Status != "deleted" || p.Status == "active" && p.ActiveDigest == v.Digest && p.StorageGeneration == v.Generation {
			return ErrConflict
		}
		a, err := raftRead[StorageAllocation](tx, "storage_allocations/"+v.App)
		if err != nil {
			return err
		}
		if a.Schema != 2 || a.BlockBytes != measuredStorageFence || v.BlockBytes < 1 || v.BlockBytes > a.AccountingBlockBytes || a.AccountingBlockBytes > 1<<20 || v.QuotaBytes <= 0 || v.Inodes > 1<<20 || v.Bytes <= 0 || v.Bytes > 1<<40 || v.ContentBytes < 0 || v.ContentBytes > 200<<20 {
			return ErrStorage
		}
		refund := v.Bytes + int64(v.Inodes)*(a.AccountingBlockBytes-v.BlockBytes)
		q, err := s.readQuota(tx, p.Owner)
		if err != nil {
			return err
		}
		if refund <= 0 || a.Bytes < refund || a.Inodes < v.Inodes || a.ContentBytes < v.ContentBytes || a.Versions < 1 || p.ChargedBytes < v.QuotaBytes || q.ChargedBytes < v.QuotaBytes {
			if err := tx.tx.ValidateReads(); err != nil {
				return err
			}
			return ErrStorage
		}
		a.Bytes -= refund
		a.Inodes -= v.Inodes
		a.ContentBytes -= v.ContentBytes
		a.Versions--
		if a.LiveBytes > a.ContentBytes {
			if err := tx.tx.ValidateReads(); err != nil {
				return err
			}
			return ErrStorage
		}
		p.ChargedBytes -= v.QuotaBytes
		q.ChargedBytes -= v.QuotaBytes
		v.State, v.RefundedAt = "reclaimed", s.now()
		if err := tx.tx.Set("storage_allocations/"+v.App, a); err != nil {
			return err
		}
		if err := tx.tx.Set("quotas/"+ownerKey(p.Owner), q); err != nil {
			return err
		}
		if err := tx.Set("projects/"+p.ID, p); err != nil {
			return err
		}
		if err := tx.tx.Delete(StorageReclamationPrefix(v.App) + v.ID); err != nil {
			return err
		}
		return tx.tx.Set("storage_versions/"+id, v)
	})
}

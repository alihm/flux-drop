package project

import (
	"errors"
	"math"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
)

// Negative BlockBytes fences older primary images, which must not overwrite the
// extended accounting schema through an older Gob struct during a rolling update.
const measuredStorageFence = int64(-2)

type StorageVersion struct {
	ID, App, ProjectID, Digest, Generation      string
	ContentBytes, Bytes, QuotaBytes, BlockBytes int64
	Inodes                                      uint64
	Unverified                                  bool
	ProjectDeleted                              bool
	LegacyOperations                            int64
	State                                       string // retained, retiring, reclaimed
	CreatedAt, RetiredAt                        time.Time
	MembershipEpoch                             string
	Members                                     []string
	Acknowledged                                map[string]bool
	RefundedAt                                  time.Time
}

type StorageOperationCharge struct{ VersionKey, Generation string }

func StorageVersionID(projectID, digest, generation string) string {
	return hash([]string{projectID, digest, generation})
}

// Separate bounded queues prevent one app's retained/reclaimed history from
// starving cleanup in another. The immutable version/refund ledger stays forever.
func StorageReclamationPrefix(app string) string { return "storage_gc_" + hash(app)[:32] + "/" }
func StorageAuditPrefix(app string) string       { return "storage_audit_" + hash(app)[:32] + "/" }

func (s *RaftRepository) allocateMeasuredStorage(tx *raftTx, p *Project, r Reservation, offers []StorageOffer) error {
	if !digestRE.MatchString(r.StorageOperationID) {
		return ErrInvalid
	}
	for _, offer := range offers {
		if p.StorageApp != "" && p.StorageApp != offer.App || offer.BlockBytes < 1 || offer.BlockBytes > 1<<20 || offer.LimitBytes <= 0 {
			continue
		}
		key := "storage_allocations/" + offer.App
		var a StorageAllocation
		if err := tx.tx.Get(key, &a); err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		if a.Control.Drain || a.Control.Removed {
			continue
		}
		if a.Schema == 0 && a.Bytes == 0 && a.Inodes == 0 {
			a.Schema, a.BlockBytes, a.AccountingBlockBytes = 2, measuredStorageFence, offer.BlockBytes
		}
		// Existing allocations are upgraded only by the audited reconciliation
		// worker. Never infer zero usage from a missing new-schema field.
		if a.Schema != 2 || a.BlockBytes != measuredStorageFence || a.AccountingBlockBytes < 1 || a.AccountingBlockBytes > 1<<20 {
			continue
		}
		if offer.BlockBytes > a.AccountingBlockBytes {
			// Filesystem block sizes are powers of two. A per-inode delta covers
			// every rounded file/directory block when moving to a larger size.
			if offer.BlockBytes%a.AccountingBlockBytes != 0 {
				return ErrStorage
			}
			delta := offer.BlockBytes - a.AccountingBlockBytes
			if a.Inodes > uint64(math.MaxInt64/delta) || a.Bytes < 0 || int64(a.Inodes)*delta > math.MaxInt64-a.Bytes || a.LegacyInodes > a.Inodes {
				return ErrStorage
			}
			a.Bytes += int64(a.Inodes) * delta
			a.LegacyBytes += int64(a.LegacyInodes) * delta
			a.AccountingBlockBytes = offer.BlockBytes
		}
		usage, err := r.StorageManifest.Footprint(a.AccountingBlockBytes)
		if err != nil || usage.ContentBytes != r.Bytes || len(r.StorageManifest.Files) != r.Files {
			return ErrInvalid
		}
		generation := ""
		if offer.Generations {
			generation = r.StorageOperationID
		}
		id := StorageVersionID(p.ID, r.Digest, generation)
		var v StorageVersion
		err = tx.tx.Get("storage_versions/"+id, &v)
		fresh := errors.Is(err, metadata.ErrNotFound)
		if err != nil && !fresh {
			return err
		}
		if !fresh && (v.ID != id || v.App != offer.App || v.ProjectID != p.ID || v.Digest != r.Digest || v.Generation != generation || v.ContentBytes != r.Bytes || v.State != "retained") {
			return ErrConflict
		}
		if a.LimitBytes > 0 && a.LimitBytes < offer.LimitBytes {
			offer.LimitBytes = a.LimitBytes
		}
		if a.LimitInodes > 0 && a.LimitInodes < offer.LimitInodes {
			offer.LimitInodes = a.LimitInodes
		}
		if fresh {
			if usage.AllocatedBytes <= 0 || a.Bytes < 0 || a.Bytes > offer.LimitBytes || usage.AllocatedBytes > offer.LimitBytes-a.Bytes || offer.AvailableBytes < usage.AllocatedBytes*2 {
				continue
			}
			if offer.TracksInodes && (a.Inodes > offer.LimitInodes || usage.Inodes > offer.LimitInodes-a.Inodes || offer.AvailableInodes < usage.Inodes*2) {
				continue
			}
			v = StorageVersion{ID: id, App: offer.App, ProjectID: p.ID, Digest: r.Digest, Generation: generation, ContentBytes: r.Bytes, Bytes: usage.AllocatedBytes, BlockBytes: a.AccountingBlockBytes, Inodes: usage.Inodes, State: "retained", CreatedAt: s.now()}
			a.Bytes += v.Bytes
			a.Inodes += v.Inodes
			a.ContentBytes += v.ContentBytes
			a.Versions++
		}
		v.QuotaBytes += versionCharge(r.Bytes)
		a.LimitBytes, a.LimitInodes = offer.LimitBytes, offer.LimitInodes
		if err := tx.tx.Set("storage_versions/"+id, v); err != nil {
			return err
		}
		if err := tx.tx.Set(StorageReclamationPrefix(offer.App)+id, id); err != nil {
			return err
		}
		if err := tx.tx.Set("storage_operation_charges/"+r.StorageOperationID, StorageOperationCharge{VersionKey: id, Generation: generation}); err != nil {
			return err
		}
		if err := tx.tx.Set(key, a); err != nil {
			return err
		}
		p.StorageApp = offer.App
		return nil
	}
	return ErrStorage
}

func (s *RaftRepository) checkStorageOperation(tx *raftTx, p Project, op Operation) error {
	if op.StorageGeneration == "" {
		if err := s.checkNotRetired(tx, VersionRef{p.ID, op.Digest}); err != nil {
			return err
		}
	} else if op.StorageGeneration != op.ID || op.StorageVersionKey == "" {
		return ErrConflict
	}
	return s.activateStorage(tx, p, op)
}

func (s *RaftRepository) activateStorage(tx *raftTx, p Project, op Operation) error {
	if op.StorageVersionKey == "" {
		return nil
	}
	v, err := raftRead[StorageVersion](tx, "storage_versions/"+op.StorageVersionKey)
	if err != nil {
		return err
	}
	if v.ID != op.StorageVersionKey || v.App != p.StorageApp || v.ProjectID != p.ID || v.Digest != op.Digest || v.Generation != op.StorageGeneration || v.State != "retained" {
		return ErrConflict
	}
	return nil
}

func storageLive(p Project) int64 {
	if p.Status == "active" {
		return p.ActiveBytes
	}
	return 0
}

func updateStorageQueue(tx *metadata.Tx, old, next Project) error {
	if next.StorageApp == "" || old.Status == next.Status && old.ActiveDigest == next.ActiveDigest && old.StorageGeneration == next.StorageGeneration && old.PendingOperation == next.PendingOperation {
		return nil
	}
	// Only pending and obsolete versions belong in the maintenance queue. A large
	// population of active sites must not delay cleanup of a handful of updates.
	if old.ActiveDigest != "" && (next.Status == "deleted" || old.ActiveDigest != next.ActiveDigest || old.StorageGeneration != next.StorageGeneration) {
		id := StorageVersionID(old.ID, old.ActiveDigest, old.StorageGeneration)
		var v StorageVersion
		if err := tx.Get("storage_versions/"+id, &v); err == nil {
			if v.App != next.StorageApp || v.ProjectID != next.ID {
				return ErrStorage
			}
			if v.State != "reclaimed" {
				if err = tx.Set(StorageReclamationPrefix(next.StorageApp)+id, id); err != nil {
					return err
				}
			}
		} else if !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
	}
	if next.Status == "active" && next.PendingOperation == "" && next.ActiveDigest != "" {
		id := StorageVersionID(next.ID, next.ActiveDigest, next.StorageGeneration)
		return tx.Delete(StorageReclamationPrefix(next.StorageApp) + id)
	}
	return nil
}

func updateStorageLive(tx *metadata.Tx, old, next Project) error {
	if err := updateStorageQueue(tx, old, next); err != nil {
		return err
	}
	if next.StorageApp == "" || storageLive(old) == storageLive(next) {
		return nil
	}
	key := "storage_allocations/" + next.StorageApp
	var a StorageAllocation
	if err := tx.Get(key, &a); err != nil {
		return err
	}
	if a.Schema == 1 && a.BlockBytes == measuredStorageFence {
		a.LiveEpoch++
		return tx.Set(key, a)
	}
	if a.Schema != 2 {
		return nil
	}
	value := a.LiveBytes + storageLive(next) - storageLive(old)
	if value < 0 || value > a.ContentBytes {
		return ErrStorage
	}
	a.LiveBytes = value
	a.LiveEpoch++
	return tx.Set(key, a)
}

package storagepool

import (
	"context"
	"errors"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

// ReconcileUnverified retries conservative legacy allowances. A transient probe
// failure or a not-yet-replicated manifest must not freeze an inflated budget
// forever. This only adjusts a proven estimate; it deletes no bytes and refunds
// no owner quota. The exact historical operation count is kept separately from
// later operations that reuse the same digest.
func (p *Pool) ReconcileUnverified(ctx context.Context, app, cursor string) (string, error) {
	a := p.app(app)
	if p.store == nil || a == nil {
		return cursor, project.ErrStorage
	}
	var allocation project.StorageAllocation
	if err := p.store.Run(ctx, func(tx *metadata.Tx) error { return tx.Get("storage_allocations/"+app, &allocation) }); errors.Is(err, metadata.ErrNotFound) {
		return "", nil
	} else if err != nil {
		return cursor, err
	}
	if allocation.Schema != 2 || allocation.LegacyBytes == 0 {
		return "", nil
	}
	scanner, ok := p.store.Backend.(accountingScanner)
	if !ok {
		return cursor, project.ErrStorage
	}
	prefix := project.StorageAuditPrefix(app)
	page, err := scanner.Scan(ctx, prefix, cursor, 20)
	if err != nil {
		return cursor, err
	}
	for key := range page.Records {
		var v project.StorageVersion
		err = p.store.Run(ctx, func(tx *metadata.Tx) error {
			var id string
			if err := tx.Get(key, &id); err != nil {
				return err
			}
			if !digestRE.MatchString(id) || key != prefix+id {
				return project.ErrStorage
			}
			return tx.Get("storage_versions/"+id, &v)
		})
		if errors.Is(err, metadata.ErrNotFound) {
			continue
		}
		if err != nil {
			return cursor, err
		}
		if !v.Unverified {
			continue
		}
		if v.App != app || v.Generation != "" || v.ID != project.StorageVersionID(v.ProjectID, v.Digest, "") || v.LegacyOperations < 1 || v.LegacyOperations > 50000 || v.State != "retained" {
			return cursor, project.ErrStorage
		}
		manifest, err := p.accountingManifest(ctx, a, v.ProjectID, v.Digest)
		if err != nil {
			continue
		}
		err = p.store.Run(ctx, func(tx *metadata.Tx) error {
			var current project.StorageVersion
			var budget project.StorageAllocation
			if err := tx.Prefetch([]string{"storage_versions/" + v.ID, "storage_allocations/" + app}); err != nil {
				return err
			}
			if err := tx.Get("storage_versions/"+v.ID, &current); err != nil {
				return err
			}
			if !current.Unverified {
				return nil
			}
			if current.ID != v.ID || current.App != app || current.Generation != "" || current.LegacyOperations != v.LegacyOperations || current.ContentBytes != v.ContentBytes || current.State != "retained" {
				return project.ErrStorage
			}
			if err := tx.Get("storage_allocations/"+app, &budget); err != nil {
				return err
			}
			if budget.Schema != 2 || budget.BlockBytes != -2 || budget.AccountingBlockBytes < 1 || budget.AccountingBlockBytes > 1<<20 {
				return project.ErrStorage
			}
			usage, err := manifest.Footprint(budget.AccountingBlockBytes)
			if err != nil || usage.ContentBytes != v.ContentBytes {
				return project.ErrStorage
			}
			priorInodes := uint64(len(manifest.Files)*21+64) * uint64(v.LegacyOperations)
			priorBytes := (v.ContentBytes+8<<20)*v.LegacyOperations + int64(priorInodes)*budget.AccountingBlockBytes
			if priorBytes < usage.AllocatedBytes || budget.LegacyBytes < priorBytes || budget.Bytes < priorBytes || budget.LegacyInodes < priorInodes || budget.Inodes < priorInodes {
				if err := tx.ValidateReads(); err != nil {
					return err
				}
				return project.ErrStorage
			}
			budget.Bytes += usage.AllocatedBytes - priorBytes
			budget.Inodes = budget.Inodes - priorInodes + usage.Inodes
			budget.LegacyBytes -= priorBytes
			budget.LegacyInodes -= priorInodes
			current.Unverified = false
			current.Bytes, current.Inodes, current.BlockBytes = usage.AllocatedBytes, usage.Inodes, budget.AccountingBlockBytes
			if err := tx.Set("storage_versions/"+v.ID, current); err != nil {
				return err
			}
			if err := tx.Set("storage_allocations/"+app, budget); err != nil {
				return err
			}
			return tx.Delete(key)
		})
		if err != nil {
			return cursor, err
		}
	}
	return page.Next, nil
}

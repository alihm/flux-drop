package storagepool

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/netip"
	"sort"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

type accountingScanner interface {
	Scan(context.Context, string, string, int) (kv.Page, error)
}

// ReconcileAllocation is a one-time audited upgrade. A negative block-size fence
// first stops both old and new allocators. The old total must reconcile exactly
// against durable operation history before any allowance is reduced. Missing
// manifests retain their original worst-case allowance; absence is not GC proof.
func (p *Pool) ReconcileAllocation(ctx context.Context, app string) error {
	if p.store == nil || p.app(app) == nil {
		return project.ErrStorage
	}
	scanner, ok := p.store.Backend.(accountingScanner)
	if !ok {
		return project.ErrStorage
	}
	var original project.StorageAllocation
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return err
	}
	migrationID := hex.EncodeToString(token)
	err := p.store.Run(ctx, func(tx *metadata.Tx) error {
		if err := tx.Get("storage_allocations/"+app, &original); errors.Is(err, metadata.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		if original.Schema == 2 || original.Bytes == 0 && original.Inodes == 0 {
			return nil
		}
		if len(p.app(app).nodes()) == 0 {
			return project.ErrStorage
		}
		if original.Schema == 0 {
			if original.BlockBytes == -1 && (original.Control.Drain || original.Control.Removed) {
				original.BlockBytes = original.Control.ResumeBlockBytes
			}
			if original.BlockBytes < 1 || original.BlockBytes > 1<<20 {
				return project.ErrStorage
			}
			original.Schema, original.AccountingBlockBytes, original.BlockBytes = 1, original.BlockBytes, -2
			original.MigrationID = migrationID
			return tx.Set("storage_allocations/"+app, original)
		}
		if original.Schema != 1 || original.BlockBytes != -2 || original.AccountingBlockBytes < 1 || original.AccountingBlockBytes > 1<<20 {
			return project.ErrStorage
		}
		original.MigrationID = migrationID
		return tx.Set("storage_allocations/"+app, original)
	})
	if err != nil || original.Schema != 1 {
		return err
	}
	type historical struct {
		version project.StorageVersion
		count   int64
	}
	versions := map[string]*historical{}
	projects := map[string]project.Project{}
	var payload, operations int64
	cursor := ""
	for {
		page, err := scanner.Scan(ctx, "operations/", cursor, 100)
		if err != nil {
			return err
		}
		for key := range page.Records {
			var op project.Operation
			var pr project.Project
			err := p.store.Run(ctx, func(tx *metadata.Tx) error {
				if err := tx.Get(key, &op); err != nil {
					return err
				}
				return tx.Get("projects/"+op.ProjectID, &pr)
			})
			if err != nil {
				return err
			}
			if pr.StorageApp != app {
				continue
			}
			if !idRE.MatchString(op.ProjectID) || !digestRE.MatchString(op.ID) || key != "operations/"+op.ID || !digestRE.MatchString(op.Digest) || op.StorageVersionKey != "" || op.Bytes < 0 || op.Bytes > 200<<20 {
				return project.ErrStorage
			}
			// A prior-image activation does not update the new live counters.
			// Wait for every pre-fence pending operation to finish or recover;
			// no old allocator can create another while BlockBytes is negative.
			if pr.PendingOperation != "" {
				return project.ErrConflict
			}
			projects[pr.ID] = pr
			payload += op.Bytes
			operations++
			if operations > 50000 {
				return project.ErrStorage
			}
			id := project.StorageVersionID(pr.ID, op.Digest, "")
			h := versions[id]
			if h == nil {
				h = &historical{version: project.StorageVersion{ID: id, App: app, ProjectID: pr.ID, Digest: op.Digest, ContentBytes: op.Bytes, State: "retained", CreatedAt: op.ExpiresAt.Add(-15 * time.Minute)}}
				versions[id] = h
			}
			if h.version.ContentBytes != op.Bytes {
				return project.ErrStorage
			}
			h.count++
			quota := op.Bytes
			if quota < 1<<20 {
				quota = 1 << 20
			}
			h.version.QuotaBytes += quota
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	// Check the historical formula, including block-size uplifts, before changing
	// accounting. An incomplete scan or legacy data never silently becomes zero.
	if payload+operations*(8<<20)+int64(original.Inodes)*original.AccountingBlockBytes != original.Bytes {
		return project.ErrStorage
	}
	ids := make([]string, 0, len(versions))
	for id := range versions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var bytes, inodes, data, oldBytes, oldInodes int64
	for _, id := range ids {
		h := versions[id]
		h.version.LegacyOperations, h.version.BlockBytes = h.count, original.AccountingBlockBytes
		m, err := p.accountingManifest(ctx, p.app(app), h.version.ProjectID, h.version.Digest)
		if err != nil {
			// Without the old per-operation file count, retain the complete
			// historical allowance. Do not refund possible orphaned content.
			h.version.Unverified = true
			data += h.version.ContentBytes
			continue
		}
		usage, err := m.Footprint(original.AccountingBlockBytes)
		if err != nil || usage.ContentBytes != h.version.ContentBytes {
			return project.ErrStorage
		}
		h.version.Bytes, h.version.Inodes, h.version.BlockBytes = usage.AllocatedBytes, usage.Inodes, original.AccountingBlockBytes
		bytes += usage.AllocatedBytes
		inodes += int64(usage.Inodes)
		data += usage.ContentBytes
		priorInodes := int64(len(m.Files)*21 + 64)
		oldInodes += priorInodes * h.count
		oldBytes += (h.version.ContentBytes + priorInodes*original.AccountingBlockBytes + 8<<20) * h.count
	}
	legacyBytes := original.Bytes - oldBytes
	legacyInodes := int64(original.Inodes) - oldInodes
	if legacyBytes < 0 || legacyInodes < 0 {
		return project.ErrStorage
	}
	bytes += legacyBytes
	inodes += legacyInodes
	// Version records are immutable while the allocation is fenced. Restarting
	// after a partial backfill overwrites the same deterministic records.
	for _, id := range ids {
		v := versions[id].version
		if err := p.store.Run(ctx, func(tx *metadata.Tx) error {
			var allocation project.StorageAllocation
			if err := tx.Get("storage_allocations/"+app, &allocation); err != nil {
				return err
			}
			if allocation.Schema != 1 || allocation.MigrationID != migrationID {
				return project.ErrConflict
			}
			if err := tx.Set("storage_versions/"+id, v); err != nil {
				return err
			}
			if v.Unverified {
				if err := tx.Set(project.StorageAuditPrefix(app)+id, id); err != nil {
					return err
				}
			} else if err := tx.Delete(project.StorageAuditPrefix(app) + id); err != nil {
				return err
			}
			var current project.Project
			if err := tx.Get("projects/"+v.ProjectID, &current); err != nil {
				return err
			}
			if current.Status == "active" && current.ActiveDigest == v.Digest && current.StorageGeneration == v.Generation {
				return tx.Delete(project.StorageReclamationPrefix(app) + id)
			}
			return tx.Set(project.StorageReclamationPrefix(app)+id, id)
		}); err != nil {
			return err
		}
	}
	// Read live totals in bounded batches. Each live-content transition bumps
	// LiveEpoch, so a final CAS rejects a sum straddling a concurrent activation
	// or deletion. Reservations are fenced throughout the history backfill.
	var snapshot project.StorageAllocation
	if err := p.store.Run(ctx, func(tx *metadata.Tx) error { return tx.Get("storage_allocations/"+app, &snapshot) }); err != nil {
		return err
	}
	projectIDs := make([]string, 0, len(projects))
	for id := range projects {
		projectIDs = append(projectIDs, id)
	}
	sort.Strings(projectIDs)
	var live int64
	for start := 0; start < len(projectIDs); start += 100 {
		end := min(start+100, len(projectIDs))
		var batch int64
		if err := p.store.Run(ctx, func(tx *metadata.Tx) error {
			batch = 0
			keys := make([]string, 0, end-start)
			for _, id := range projectIDs[start:end] {
				keys = append(keys, "projects/"+id)
			}
			if err := tx.Prefetch(keys); err != nil {
				return err
			}
			for _, id := range projectIDs[start:end] {
				var pr project.Project
				if err := tx.Get("projects/"+id, &pr); err != nil {
					return err
				}
				if pr.StorageApp != app {
					return project.ErrStorage
				}
				if pr.Status == "active" {
					batch += pr.ActiveBytes
				}
			}
			return nil
		}); err != nil {
			return err
		}
		live += batch
	}
	return p.store.Run(ctx, func(tx *metadata.Tx) error {
		var a project.StorageAllocation
		if err := tx.Get("storage_allocations/"+app, &a); err != nil {
			return err
		}
		if a.Schema == 2 {
			return nil
		}
		if a.Schema != 1 || a.MigrationID != migrationID || a.BlockBytes != -2 || a.Bytes != original.Bytes || a.Inodes != original.Inodes || a.AccountingBlockBytes != original.AccountingBlockBytes || a.LiveEpoch != snapshot.LiveEpoch {
			return project.ErrConflict
		}

		if bytes > a.Bytes || data > bytes || live > data {
			return project.ErrStorage
		}
		a.Schema, a.Bytes, a.Inodes, a.ContentBytes, a.LiveBytes, a.Versions = 2, bytes, uint64(inodes), data, live, int64(len(ids))
		a.LegacyBytes, a.LegacyInodes = legacyBytes, uint64(legacyInodes)
		return tx.Set("storage_allocations/"+app, a)
	})
}

func (p *Pool) accountingManifest(ctx context.Context, a *appRuntime, id, digest string) (content.Manifest, error) {
	for _, node := range a.nodes() {
		addr, err := netip.ParseAddrPort(node.Address)
		if err != nil {
			continue
		}
		res, err := a.request(ctx, addr, "GET", apiPrefix+"versions/"+id+"/"+digest+"/manifest", nil, "")
		if err != nil {
			continue
		}
		var m content.Manifest
		err = readResponse(res, &m, 8<<20)
		res.Body.Close()
		encoded, encodeErr := json.Marshal(m)
		sum := sha256.Sum256(encoded)
		if err == nil && encodeErr == nil && hex.EncodeToString(sum[:]) == digest {
			return m, nil
		}
	}
	return content.Manifest{}, project.ErrStorage
}

func (p *Pool) RunAccounting(ctx context.Context) {
	cursors := map[string]string{}
	auditCursors := map[string]string{}
	for ctx.Err() == nil {
		for _, app := range p.apps {
			bounded, cancel := context.WithTimeout(ctx, 5*time.Minute)
			err := p.ReconcileAllocation(bounded, app.config.AppName)
			if err == nil {
				var next string
				next, err = p.ReconcileUnverified(bounded, app.config.AppName, auditCursors[app.config.AppName])
				if err == nil {
					auditCursors[app.config.AppName] = next
				}
			}
			if err == nil {
				var next string
				next, err = p.ReclaimObsolete(bounded, app.config.AppName, cursors[app.config.AppName])
				if err == nil {
					cursors[app.config.AppName] = next
				}
			}
			cancel()
			if err != nil && ctx.Err() == nil {
				slog.Warn("storage accounting or reclamation deferred", "app", app.config.AppName)
			}
		}
		timer := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

package storagepool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"go.etcd.io/bbolt"
)

var retirementBucket = []byte("storage-retirements-v1")

type reclamationReceipt struct {
	Version   project.StorageVersion
	WasPaused bool
	Complete  bool
}

func (s *Secondary) notRetired(o Operation) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		if tx.Bucket(retirementBucket).Get([]byte("project/"+o.ProjectID)) != nil {
			return errConflict
		}
		if tx.Bucket(retirementBucket).Get([]byte(project.StorageVersionID(o.ProjectID, o.Digest, o.Generation))) != nil {
			return errConflict
		}
		return nil
	})
}

func (s *Secondary) reclaim(w http.ResponseWriter, r *http.Request) {
	var v project.StorageVersion
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	d.DisallowUnknownFields()
	var extra any
	if d.Decode(&v) != nil || d.Decode(&extra) != io.EOF || v.State != "retiring" || v.App != s.config.AppName || !idRE.MatchString(v.ProjectID) || !digestRE.MatchString(v.Digest) || v.Generation != "" && !digestRE.MatchString(v.Generation) || v.ID != project.StorageVersionID(v.ProjectID, v.Digest, v.Generation) || !digestRE.MatchString(v.MembershipEpoch) {
		storageError(w, content.ErrInvalid)
		return
	}
	// Serialize folder pause/ignore updates, not uploads or serving. Persistent
	// writer fences are installed under mu; network coordination holds neither
	// the upload mutex nor a file-reader lock.
	s.reclamationMu.Lock()
	defer s.reclamationMu.Unlock()
	snapshot, folder, err := s.replication(r.Context())
	if err != nil {
		storageError(w, err)
		return
	}
	if snapshot.Epoch != v.MembershipEpoch || !slices.Equal(snapshot.Members, v.Members) {
		storageError(w, errConflict)
		return
	}
	var receipt reclamationReceipt
	s.mu.Lock()
	err = s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(retirementBucket)
		if v.ProjectDeleted {
			if err := bucket.Put([]byte("project/"+v.ProjectID), []byte(v.ID)); err != nil {
				return err
			}
		}
		if raw := bucket.Get([]byte(v.ID)); raw != nil {
			if err := json.Unmarshal(raw, &receipt); err != nil {
				return err
			}
			if receipt.Version.MembershipEpoch != v.MembershipEpoch || receipt.Version.ProjectID != v.ProjectID || receipt.Version.Digest != v.Digest || receipt.Version.Generation != v.Generation {
				return errConflict
			}
			return nil
		}
		receipt = reclamationReceipt{Version: v, WasPaused: folder.Paused}
		raw, err := json.Marshal(receipt)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(v.ID), raw)
	})
	s.mu.Unlock()
	if err != nil {
		storageError(w, err)
		return
	}
	// Always reassert the replication fence, even for a completed retry. Local
	// tombstones never expire and every installer checks them again under mu.
	if err = s.syncPaused(r.Context(), true); err != nil {
		storageError(w, err)
		return
	}
	relative := filepath.Join("projects", v.ProjectID, "versions", v.Digest)
	if v.Generation != "" {
		relative, _ = content.GenerationPath(v.ProjectID, v.Generation)
	}
	if v.ProjectDeleted {
		// No project ID is ever resurrected after its metadata tombstone. Fence
		// all delayed installers and replication before removing shared parents;
		// otherwise the last version's refund would leave unaccounted markers.
		relative = filepath.Join("projects", v.ProjectID)
	}
	if err = s.ignoreVersion(r.Context(), relative); err != nil {
		storageError(w, err)
		return
	}
	// Recheck membership while pullers are stopped, before unlinking anything.
	snapshot, folder, err = s.replication(r.Context())
	if err != nil {
		storageError(w, err)
		return
	}
	if !folder.Paused || snapshot.Epoch != v.MembershipEpoch {
		storageError(w, errConflict)
		return
	}
	unlock := s.lockVersionRemoval(v)
	err = removeVersion(s.root, relative)
	unlock()
	if err != nil {
		storageError(w, err)
		return
	}
	// A crash leaves the folder paused rather than resuming without its fence.
	// The persistent receipt remembers whether Drop is responsible for resuming.
	if !receipt.WasPaused {
		if err = s.syncPaused(r.Context(), false); err != nil {
			storageError(w, err)
			return
		}
	}
	receipt.Complete = true
	err = s.db.Update(func(tx *bbolt.Tx) error {
		raw, err := json.Marshal(receipt)
		if err != nil {
			return err
		}
		return tx.Bucket(retirementBucket).Put([]byte(v.ID), raw)
	})
	if err != nil {
		storageError(w, err)
		return
	}
	jsonReply(w, 200, map[string]string{"id": v.ID, "epoch": v.MembershipEpoch, "member": snapshot.Device})
}

// Bounded lock shards coordinate open/stream/delete without retaining one mutex
// forever per historical version. Separate namespace and version locks keep a
// superseded-version cleanup from blocking reads of the current generation.
// Unrelated hash collisions only add conservative waiting; they cannot bypass a
// fence. Local installers serialize with tombstone creation under s.mu and check
// notRetired immediately before installation, so they need no reader lock.
func (s *Secondary) lockVersionRead(id, digest, generation string) func() {
	projectShard := sha256.Sum256([]byte(id))
	identity := generation
	if identity == "" {
		identity = digest
	}
	versionShard := sha256.Sum256([]byte(id + "\x00" + identity))
	p, v := &s.projectLocks[projectShard[0]], &s.versionLocks[versionShard[0]]
	p.RLock()
	v.RLock()
	return func() { v.RUnlock(); p.RUnlock() }
}

func (s *Secondary) lockVersionRemoval(version project.StorageVersion) func() {
	projectShard := sha256.Sum256([]byte(version.ProjectID))
	p := &s.projectLocks[projectShard[0]]
	if version.ProjectDeleted {
		p.Lock()
		return p.Unlock
	}
	identity := version.Generation
	if identity == "" {
		identity = version.Digest
	}
	versionShard := sha256.Sum256([]byte(version.ProjectID + "\x00" + identity))
	v := &s.versionLocks[versionShard[0]]
	p.RLock()
	v.Lock()
	return func() { v.Unlock(); p.RUnlock() }
}

// Rooted deletion rejects symlink ancestors and the exact generation target.
// RemoveAll never follows symlink children. Parent fsync makes the unlink durable.
func removeVersion(dataRoot, relative string) error {
	root, err := os.OpenRoot(dataRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	for p := relative; p != "."; p = filepath.Dir(p) {
		info, err := root.Lstat(p)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errConflict
		}
	}
	if err = root.RemoveAll(relative); err != nil {
		return err
	}
	for p := filepath.Dir(relative); ; p = filepath.Dir(p) {
		f, err := root.Open(p)
		if os.IsNotExist(err) && p != "." {
			continue
		}
		if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return err
		}
		break
	}
	return nil
}

// ReclaimObsolete uses complete Syncthing folder membership, including offline
// devices. Location discovery only supplies current addresses; absence is never
// an acknowledgement. Epoch changes are held for operator investigation.
func (p *Pool) ReclaimObsolete(ctx context.Context, app, cursor string) (string, error) {
	if !p.config.Reclamation || p.store == nil {
		return cursor, nil
	}
	a := p.app(app)
	if a == nil {
		return cursor, project.ErrStorage
	}
	scanner, ok := p.store.Backend.(accountingScanner)
	if !ok {
		return cursor, project.ErrStorage
	}
	nodes := map[string]netip.AddrPort{}
	var cohort ReplicationSnapshot
	for _, node := range a.nodes() {
		if node.Capacity.Protocol < 2 {
			return cursor, project.ErrStorage
		}
		addr, err := netip.ParseAddrPort(node.Address)
		if err != nil {
			return cursor, err
		}
		res, err := a.request(ctx, addr, "GET", apiPrefix+"replication", nil, "")
		if err != nil {
			return cursor, err
		}
		var snapshot ReplicationSnapshot
		err = readResponse(res, &snapshot, 16<<10)
		res.Body.Close()
		if err != nil {
			return cursor, err
		}
		if !digestRE.MatchString(snapshot.Epoch) || !slices.Contains(snapshot.Members, snapshot.Device) {
			return cursor, project.ErrStorage
		}
		if cohort.Epoch == "" {
			cohort = snapshot
		} else if cohort.Epoch != snapshot.Epoch || !slices.Equal(cohort.Members, snapshot.Members) {
			return cursor, project.ErrConflict
		}
		if _, exists := nodes[snapshot.Device]; exists {
			return cursor, project.ErrConflict
		}
		nodes[snapshot.Device] = addr
	}
	if len(cohort.Members) == 0 || len(nodes) != len(cohort.Members) {
		return cursor, project.ErrStorage
	}
	for _, member := range cohort.Members {
		if _, ok := nodes[member]; !ok {
			return cursor, project.ErrStorage
		}
	}
	prefix := project.StorageReclamationPrefix(app)
	page, err := scanner.Scan(ctx, prefix, cursor, 20)
	if err != nil {
		return cursor, err
	}
	repo := &project.RaftRepository{Store: p.store}
	for key := range page.Records {
		var v project.StorageVersion
		if err = p.store.Run(ctx, func(tx *metadata.Tx) error {
			var id string
			if err := tx.Get(key, &id); err != nil {
				return err
			}
			if !digestRE.MatchString(id) || key != prefix+id {
				return project.ErrStorage
			}
			return tx.Get("storage_versions/"+id, &v)
		}); errors.Is(err, metadata.ErrNotFound) {
			continue
		} else if err != nil {
			return cursor, err
		}
		if v.App != app {
			return cursor, project.ErrStorage
		}
		if v.Unverified || v.State == "reclaimed" {
			continue
		}
		// Unreferenced versions must be at least one hour old. Pending uploads
		// are held independently by
		// the retirement transaction, regardless of their wall-clock expiry.
		if v.State == "retained" && time.Since(v.CreatedAt) < time.Hour {
			continue
		}
		retired, err := repo.RetireStorageVersion(ctx, v.ID, cohort.Epoch, cohort.Members)
		if errors.Is(err, project.ErrConflict) {
			continue
		}
		if err != nil {
			return cursor, err
		}
		for _, member := range cohort.Members {
			if retired.Acknowledged[member] {
				continue
			}
			raw, _ := json.Marshal(retired)
			res, err := a.request(ctx, nodes[member], "POST", apiPrefix+"reclaim", bytes.NewReader(raw), "application/json")
			if err != nil {
				return cursor, err
			}
			var ack struct{ ID, Epoch, Member string }
			err = readResponse(res, &ack, 16<<10)
			res.Body.Close()
			if err != nil {
				return cursor, err
			}
			if ack.ID != v.ID || ack.Epoch != cohort.Epoch || ack.Member != member {
				return cursor, project.ErrConflict
			}
			if err = repo.AcknowledgeStorageReclamation(ctx, v.ID, cohort.Epoch, member); err != nil {
				return cursor, err
			}
		}
	}
	return page.Next, nil
}

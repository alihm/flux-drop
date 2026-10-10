package storagepool

import (
	"context"
	"errors"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

var ErrAppInUse = errors.New("app contains retained allocations; drain it instead")
var ErrUnknownApp = errors.New("unknown storage app")
var ErrInvalidAction = errors.New("invalid storage action")

// BindMetadata is called once during startup, before accepting requests.
func (p *Pool) BindMetadata(store *metadata.Store) { p.store = store }
func (p *Pool) controls(ctx context.Context) (map[string]project.StorageControl, error) {
	out := map[string]project.StorageControl{}
	if p.store == nil {
		return out, nil
	}
	err := p.store.Run(ctx, func(tx *metadata.Tx) error {
		keys := make([]string, 0, len(p.apps))
		for _, a := range p.apps {
			keys = append(keys, "storage_allocations/"+a.config.AppName)
		}
		if err := tx.Prefetch(keys); err != nil {
			return err
		}
		for _, a := range p.apps {
			var allocation project.StorageAllocation
			if err := tx.Get("storage_allocations/"+a.config.AppName, &allocation); err != nil && !errors.Is(err, metadata.ErrNotFound) {
				return err
			}
			out[a.config.AppName] = allocation.Control
		}
		return nil
	})
	return out, err
}
func (p *Pool) Dashboard(ctx context.Context) (any, error) {
	allocations := map[string]project.StorageAllocation{}
	controls := map[string]project.StorageControl{}
	err := p.store.Run(ctx, func(tx *metadata.Tx) error {
		keys := make([]string, 0, len(p.apps))
		for _, a := range p.apps {
			keys = append(keys, "storage_allocations/"+a.config.AppName)
		}
		if err := tx.Prefetch(keys); err != nil {
			return err
		}
		for _, a := range p.apps {
			name := a.config.AppName
			var allocation project.StorageAllocation
			if err := tx.Get("storage_allocations/"+name, &allocation); err != nil && !errors.Is(err, metadata.ErrNotFound) {
				return err
			}
			allocations[name], controls[name] = allocation, allocation.Control
		}
		return nil
	})
	return map[string]any{"apps": p.appStatuses(), "allocations": allocations, "controls": controls, "headroomBytes": Headroom, "reclamationEnabled": p.config.Reclamation}, err
}

// ChangeApp runs inside the admin authorization transaction. A removal and an
// upload both check the allocation/control versions, closing the stale-offer race.
// Removal disconnects an empty app from this pool; it never deletes Flux data.
func (p *Pool) ChangeApp(tx *metadata.Tx, name, action string) error {
	found := false
	for _, a := range p.apps {
		if a.config.AppName == name {
			found = true
			break
		}
	}
	if !found {
		return ErrUnknownApp
	}
	var allocation project.StorageAllocation
	if err := tx.Get("storage_allocations/"+name, &allocation); err != nil && !errors.Is(err, metadata.ErrNotFound) {
		return err
	}
	c := allocation.Control
	switch action {
	case "remove":
		if allocation.Bytes != 0 || allocation.Inodes != 0 {
			return ErrAppInUse
		}
		c.Removed, c.Drain = true, true
	case "drain":
		c.Drain = true
	case "restore":
		if allocation.BlockBytes < 0 && allocation.BlockBytes != -2 {
			if allocation.BlockBytes != -1 || c.ResumeBlockBytes < 0 || c.ResumeBlockBytes > 1<<20 {
				return project.ErrStorage
			}
			allocation.BlockBytes = c.ResumeBlockBytes
		}
		c.Removed, c.Drain = false, false
	default:
		return ErrInvalidAction
	}
	if c.Removed || c.Drain {
		// The preceding image already rejects negative allocation block sizes.
		// Keep that fail-closed check effective while old primary nodes roll over;
		// otherwise their older Gob schema could discard the control fields.
		if allocation.BlockBytes >= 0 {
			c.ResumeBlockBytes = allocation.BlockBytes
		} else if allocation.BlockBytes != -1 && allocation.BlockBytes != -2 {
			return project.ErrStorage
		}
		if allocation.Schema < 1 {
			allocation.BlockBytes = -1
		}
	}
	allocation.Control = c
	return tx.Set("storage_allocations/"+name, allocation)
}

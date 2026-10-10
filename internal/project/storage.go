package project

import (
	"context"
	"errors"
	"math"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
)

// StorageOffer is a recent conservative app-wide observation. The authority
// charges retained capacity atomically with project/quota/operation reservation.
// Aborted uploads and deleted projects retain this charge until verified GC.
type StorageOffer struct {
	App                          string
	LimitBytes                   int64
	AvailableBytes               int64
	BlockBytes                   int64
	LimitInodes, AvailableInodes uint64
	TracksInodes                 bool
	Generations                  bool
}
type StorageAllocation struct {
	Schema               int            `json:"schema"`
	AccountingBlockBytes int64          `json:"accountingBlockBytes"`
	ContentBytes         int64          `json:"contentBytes"`
	LiveBytes            int64          `json:"liveBytes"`
	Versions             int64          `json:"versions"`
	LegacyBytes          int64          `json:"legacyBytes"`
	LegacyInodes         uint64         `json:"legacyInodes"`
	MigrationID          string         `json:"-"`
	LiveEpoch            int64          `json:"-"`
	Control              StorageControl `json:"control"`
	BlockBytes           int64          `json:"blockBytes"`
	LimitBytes           int64          `json:"limitBytes"`
	LimitInodes          uint64         `json:"limitInodes"`
	Bytes                int64          `json:"allocatedBytes"`
	Inodes               uint64         `json:"allocatedInodes"`
}

// StorageControl is checked in the allocation transaction, so even an old offer
// cannot race with a wallet-authorized pool removal or drain.
type StorageControl struct {
	Removed          bool  `json:"removed"`
	Drain            bool  `json:"drain"`
	ResumeBlockBytes int64 `json:"-"`
}

type ContentInstaller interface {
	Install(context.Context, Prepared, *content.Staged) error
}

func (s *RaftRepository) allocateStorage(tx *raftTx, p *Project, r Reservation, offers []StorageOffer) error {
	if s.StorageOffers == nil {
		return nil
	}
	if r.StorageManifest != nil {
		return s.allocateMeasuredStorage(tx, p, r, offers)
	}
	for _, offer := range offers {
		if p.StorageApp != "" && offer.App != p.StorageApp {
			continue
		}
		if offer.BlockBytes < 1 || offer.BlockBytes > 1<<20 || offer.LimitBytes <= 0 || r.Files < 1 || r.Files > 5000 {
			continue
		}
		inodes := uint64(r.Files)*21 + 64

		key := "storage_allocations/" + offer.App
		var allocated StorageAllocation
		if err := tx.tx.Get(key, &allocated); err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		if allocated.Control.Removed || allocated.Control.Drain {
			continue
		}
		if allocated.BlockBytes < 0 || allocated.BlockBytes > 1<<20 {
			return ErrStorage
		}
		if allocated.BlockBytes > offer.BlockBytes {
			offer.BlockBytes = allocated.BlockBytes
		}
		if allocated.BlockBytes > 0 && offer.BlockBytes > allocated.BlockBytes {
			delta := uint64(offer.BlockBytes - allocated.BlockBytes)
			if allocated.Inodes > uint64(math.MaxInt64)/delta {
				continue
			}
			extra := int64(allocated.Inodes * delta)
			if allocated.Bytes < 0 || extra > math.MaxInt64-allocated.Bytes {
				continue
			}
			allocated.Bytes += extra
		}
		allocated.BlockBytes = offer.BlockBytes
		charge := r.Bytes + int64(inodes)*offer.BlockBytes + 8<<20
		if charge <= 0 || offer.AvailableBytes < charge*2 || (offer.TracksInodes && offer.AvailableInodes < inodes*2) {
			continue
		}
		if allocated.LimitBytes > 0 && allocated.LimitBytes < offer.LimitBytes {
			offer.LimitBytes = allocated.LimitBytes
		}
		if allocated.LimitInodes > 0 && allocated.LimitInodes < offer.LimitInodes {
			offer.LimitInodes = allocated.LimitInodes
		}
		allocated.LimitBytes, allocated.LimitInodes = offer.LimitBytes, offer.LimitInodes
		if allocated.Bytes < 0 || allocated.Bytes > offer.LimitBytes || charge > offer.LimitBytes-allocated.Bytes || allocated.Inodes > math.MaxUint64-inodes {
			continue
		}
		if offer.TracksInodes && (allocated.Inodes > offer.LimitInodes || inodes > offer.LimitInodes-allocated.Inodes) {
			continue
		}
		allocated.Bytes += charge
		allocated.Inodes += inodes
		// Unknown collections are always replicated at both metadata and coordinator
		// boundaries. This also commits the new project's storage identity.
		if err := tx.tx.Set(key, allocated); err != nil {
			return err
		}
		p.StorageApp = offer.App
		return nil
	}
	return ErrStorage
}

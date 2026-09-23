package httpserver

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/runonflux/flux-drop/internal/content"
	"golang.org/x/sys/unix"
)

var errStorageCapacity = errors.New("insufficient upload storage")

type diskSpace struct {
	bytes, inodes, block uint64
	tracksInodes         bool
}

// diskAdmission is local and conservative: outstanding reservations are deducted
// even when their bytes have already been written. Replication and other processes
// can still consume disk; this is not a filesystem quota or an ENOSPC guarantee.
type diskAdmission struct {
	mu                            sync.Mutex
	reservedBytes, reservedInodes uint64
	probe                         func() (diskSpace, error)
}

func newDiskAdmission(root string) *diskAdmission {
	return &diskAdmission{probe: func() (diskSpace, error) {
		var s unix.Statfs_t
		if err := unix.Statfs(root, &s); err != nil {
			return diskSpace{}, err
		}
		if s.Bsize <= 0 || uint64(s.Bsize) > 1<<20 || s.Bavail > math.MaxUint64/uint64(s.Bsize) {
			return diskSpace{}, fmt.Errorf("invalid filesystem capacity")
		}
		return diskSpace{bytes: s.Bavail * uint64(s.Bsize), inodes: s.Ffree, block: uint64(s.Bsize), tracksInodes: s.Files != 0}, nil
	}}
}

func (g *diskAdmission) acquire(limits content.Limits) (func(), error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	space, err := g.probe()
	if err != nil {
		return nil, err
	}
	if space.block == 0 || space.block > 1<<20 {
		return nil, errStorageCapacity
	}
	// Each path has at most 20 components. Include staged files/directories,
	// multipart spool files, the manifest and installation metadata.
	inodes := uint64(limits.Files)*21 + 64
	bytes := uint64(limits.UploadBytes) + uint64(limits.ExpandedBytes) + (8 << 20) + inodes*space.block
	const floorBytes = 1 << 30
	const floorInodes = 1024
	if space.bytes < floorBytes || g.reservedBytes > space.bytes-floorBytes || bytes > space.bytes-floorBytes-g.reservedBytes {
		return nil, errStorageCapacity
	}
	if space.tracksInodes && (space.inodes < floorInodes || g.reservedInodes > space.inodes-floorInodes || inodes > space.inodes-floorInodes-g.reservedInodes) {
		return nil, errStorageCapacity
	}
	g.reservedBytes += bytes
	g.reservedInodes += inodes
	var once sync.Once
	return func() {
		once.Do(func() { g.mu.Lock(); defer g.mu.Unlock(); g.reservedBytes -= bytes; g.reservedInodes -= inodes })
	}, nil
}

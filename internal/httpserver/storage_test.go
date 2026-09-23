package httpserver

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/runonflux/flux-drop/internal/content"
)

func TestDiskAdmission(t *testing.T) {
	limits := content.DefaultLimits()
	good := diskSpace{bytes: 2 << 30, inodes: 200000, block: 4096, tracksInodes: true}
	g := &diskAdmission{probe: func() (diskSpace, error) { return good, nil }}
	release, err := g.acquire(limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = g.acquire(limits); !errors.Is(err, errStorageCapacity) {
		t.Fatal("concurrent reservation should exhaust budget", err)
	}
	release()
	release()
	release, err = g.acquire(limits)
	if err != nil {
		t.Fatal("reservation was not released", err)
	}
	release()
	for _, space := range []diskSpace{
		{bytes: 1 << 30, inodes: 200000, block: 4096, tracksInodes: true},
		{bytes: 8 << 30, inodes: 1024, block: 4096, tracksInodes: true},
		{bytes: 8 << 30, inodes: 200000, block: 0, tracksInodes: true},
	} {
		g.probe = func() (diskSpace, error) { return space, nil }
		if _, err = g.acquire(limits); !errors.Is(err, errStorageCapacity) {
			t.Fatal("capacity accepted", space, err)
		}
	}
	g.probe = func() (diskSpace, error) { return diskSpace{}, errors.New("probe failed") }
	if _, err = g.acquire(limits); err == nil {
		t.Fatal("probe failure accepted")
	}
	g.probe = func() (diskSpace, error) { return diskSpace{bytes: 8 << 30, block: 4096}, nil }
	release, err = g.acquire(limits)
	if err != nil {
		t.Fatal("filesystem without inode accounting", err)
	}
	release()
}

func TestDiskAdmissionConcurrent(t *testing.T) {
	g := &diskAdmission{probe: func() (diskSpace, error) {
		return diskSpace{bytes: 2 << 30, inodes: 200000, block: 4096, tracksInodes: true}, nil
	}}
	var wg sync.WaitGroup
	var accepted atomic.Int32
	done := make(chan struct{})
	releases := make(chan func(), 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := g.acquire(content.DefaultLimits())
			if err == nil {
				accepted.Add(1)
				releases <- release
			}
		}()
	}
	go func() { wg.Wait(); close(done) }()
	<-done
	if accepted.Load() != 1 {
		t.Fatal("overcommitted", accepted.Load())
	}
	(<-releases)()
	if g.reservedBytes != 0 || g.reservedInodes != 0 {
		t.Fatal("leaked reservation")
	}
}

func TestDiskProbe(t *testing.T) {
	g := newDiskAdmission(t.TempDir())
	s, err := g.probe()
	if err != nil || s.block == 0 {
		t.Fatal(s, err)
	}
	if _, err := newDiskAdmission("/nonexistent-flux-drop-test-root").acquire(content.DefaultLimits()); err == nil {
		t.Fatal("missing storage accepted")
	}
}

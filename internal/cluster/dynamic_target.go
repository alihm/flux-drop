package cluster

import (
	"context"
	"github.com/hashicorp/raft"
	"log/slog"
	"net/netip"
	"strconv"
	"sync"
	"time"
)

func oddTarget(eligible, floor int) int {
	if eligible%2 == 0 {
		eligible--
	}
	return max(floor, min(maxVoters, eligible))
}
func (e *enrollment) adjustTarget(floor, voters, eligible int) int {
	now := e.now()
	term := e.node.raft.CurrentTerm()
	if e.targetTerm != term || e.activeTarget == 0 {
		e.targetTerm = term
		e.activeTarget = oddTarget(voters, floor)
		e.pendingTarget = 0
		e.targetSince = time.Time{}
	}
	if e.activeTarget < floor {
		e.activeTarget = floor
		e.pendingTarget = 0
		e.targetSince = time.Time{}
	}
	desired := oddTarget(eligible, floor)
	if desired == e.activeTarget {
		e.pendingTarget = 0
		e.targetSince = time.Time{}
		return e.activeTarget
	}
	if desired != e.pendingTarget || e.targetSince.IsZero() {
		e.pendingTarget = desired
		e.targetSince = now
	}
	window := e.targetRaiseWindow
	if desired < e.activeTarget {
		window = e.targetLowerWindow
	}
	if now.Sub(e.targetSince) >= window {
		old := e.activeTarget
		e.activeTarget = desired
		e.pendingTarget = 0
		e.targetSince = time.Time{}
		slog.Info("cluster voter target changed", "previous", old, "desired", desired, "floor", floor, "eligible", eligible)
	}
	return e.activeTarget
}

func (e *enrollment) probeMembers(ctx context.Context, configuration raft.Configuration) map[string]bool {
	healthy := map[string]bool{e.node.config.Local.ID: true}
	keys := map[string]string{e.node.config.Local.ID: e.localKeyHash}
	var mu sync.Mutex
	var workers sync.WaitGroup
	slots := make(chan struct{}, 4)
	for _, server := range configuration.Servers {
		if string(server.ID) == e.node.config.Local.ID {
			continue
		}
		workers.Add(1)
		go func(server raft.Server) {
			defer workers.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				return
			}
			m, err := parseAddress(string(server.Address))
			if err != nil {
				return
			}
			address, _ := netip.ParseAddrPort(m.Address)
			p, err := e.observer.probe(ctx, netip.AddrPortFrom(address.Addr(), e.port))
			if err == nil && p.Status.ID == m.ID && (p.Status.Role == "Follower" || p.Status.Role == "Leader") {
				mu.Lock()
				healthy[m.ID] = true
				keys[m.ID] = p.keyHash
				mu.Unlock()
			}
		}(server)
	}
	workers.Wait()
	e.memberKeys = keys
	return healthy
}

// Raft 1.7.3's GetConfiguration exposes the latest (possibly uncommitted)
// configuration and its future.Index() is zero. Read the actual committed
// configuration from the stores; never count an in-flight admission as eligible.
func (e *enrollment) committedConfiguration(ctx context.Context) (raft.Configuration, uint64, error) {
	term := e.node.raft.CurrentTerm()
	if err := e.node.fence(ctx); err != nil {
		return raft.Configuration{}, 0, err
	}
	committed, err := strconv.ParseUint(e.node.raft.Stats()["commit_index"], 10, 64)
	if err != nil {
		return raft.Configuration{}, 0, err
	}
	configuration, index, err := configurationAtIndex(e.node.store, e.node.snapshots, committed)
	if err != nil || e.node.raft.State() != raft.Leader || e.node.raft.CurrentTerm() != term {
		e.pendingTarget = 0
		e.targetSince = time.Time{}
		return raft.Configuration{}, 0, ErrNotCaughtUp
	}
	return configuration, index, nil
}
func configurationAtIndex(store raft.LogStore, snapshots raft.SnapshotStore, through uint64) (configuration raft.Configuration, configurationIndex uint64, err error) {
	defer func() {
		if recover() != nil {
			configuration = raft.Configuration{}
			configurationIndex = 0
			err = ErrInvalid
		}
	}()
	first, err := store.FirstIndex()
	if err != nil {
		return raft.Configuration{}, 0, err
	}
	last, err := store.LastIndex()
	if err != nil {
		return raft.Configuration{}, 0, err
	}
	for index := min(last, through); index >= first && index > 0; index-- {
		var entry raft.Log
		if err := store.GetLog(index, &entry); err != nil {
			return raft.Configuration{}, 0, err
		}
		if entry.Type == raft.LogConfiguration {
			return raft.DecodeConfiguration(entry.Data), index, nil
		}
	}
	metas, err := snapshots.List()
	if err != nil {
		return raft.Configuration{}, 0, err
	}
	for _, meta := range metas {
		if meta.Index <= through {
			return meta.Configuration, meta.ConfigurationIndex, nil
		}
	}
	return raft.Configuration{}, 0, ErrNotCaughtUp
}

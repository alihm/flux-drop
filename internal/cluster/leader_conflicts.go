package cluster

import (
	"context"
	"log/slog"
	"net/netip"
	"sort"
	"sync"
	"time"
)

const leaderConflictClearWindow = 2 * time.Minute

// Updated only when a new Observer refresh round completes. Re-reading a
// snapshot never counts as another confirmation. Live local roles are sampled
// inside that round, never combined later with older peer observations.
type leaderConflict struct {
	consecutive int
	latched     bool
	singleSince time.Time
	lastRound   time.Time
}

func (c *leaderConflict) breakContinuity() {
	c.consecutive = 0
	c.singleSince = time.Time{}
	c.lastRound = time.Time{}
}
func (c *leaderConflict) observe(now time.Time, observations []Observation, clusterID string) {
	if !c.lastRound.IsZero() && !recoverySampleGapValid(c.lastRound, now) {
		c.breakContinuity()
	}
	c.lastRound = now
	leaders := map[string]bool{}
	for _, p := range observations {
		if p.Status.Role == "Leader" {
			leaders[p.Status.ID] = true
		}
	}
	if len(leaders) > 1 {
		c.singleSince = time.Time{}
		c.consecutive++
		if c.consecutive >= 2 && !c.latched {
			c.latched = true
			ids := make([]string, 0, len(leaders))
			for id := range leaders {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			slog.Error("multiple cluster leaders confirmed in consecutive observation rounds; refusing recovery and reset", "cluster", clusterID, "leaders", ids)
		}
		return
	}
	c.consecutive = 0
	if len(leaders) != 1 {
		c.singleSince = time.Time{}
		return
	}
	if !c.latched {
		c.singleSince = time.Time{}
		return
	}
	if c.singleSince.IsZero() {
		c.singleSince = now
		return
	}
	if recoveryWindowElapsed(c.singleSince, now) >= leaderConflictClearWindow {
		c.latched = false
		c.singleSince = time.Time{}
		slog.Info("cluster leader conflict cleared after sustained single-leader observation rounds", "cluster", clusterID)
	}
}
func (o *Observer) clock() time.Time {
	if o.now != nil {
		return o.now()
	}
	return time.Now()
}
func (o *Observer) setLocalStatus(read func() Status) {
	o.mu.Lock()
	o.localStatus = read
	o.mu.Unlock()
}
func (o *Observer) leaderConflictDetected() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.leaders.latched
}

// A transitional snapshot may contain the old and new leader. Re-probe only
// that ambiguous set, rather than selecting arbitrarily or latching on it. Two
// currently live leaders still defer this operation; only the periodic round
// tracker can latch or clear a persistent conflict.
func (e *enrollment) leaderForRequest(ctx context.Context) (Observation, error) {
	peers, fresh := e.observer.Snapshot()
	if !fresh || e.observer.leaderConflictDetected() {
		return Observation{}, ErrNotLeader
	}
	var candidates []Observation
	for _, p := range peers {
		if p.Status.Role == "Leader" {
			candidates = append(candidates, p)
		}
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	if len(candidates) == 0 {
		return Observation{}, ErrNotLeader
	}
	results := make(chan Observation, len(candidates))
	slots := make(chan struct{}, 4)
	var workers sync.WaitGroup
	for _, p := range candidates {
		workers.Add(1)
		go func(p Observation) {
			defer workers.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				return
			}
			address, err := netip.ParseAddrPort(p.Address)
			if err != nil {
				return
			}
			current, err := e.observer.probe(ctx, address)
			if err == nil && current.Status.ID == p.Status.ID && current.Status.Role == "Leader" {
				results <- current
			}
		}(p)
	}
	workers.Wait()
	close(results)
	var current []Observation
	for p := range results {
		current = append(current, p)
	}
	if len(current) != 1 || ctx.Err() != nil || e.observer.leaderConflictDetected() {
		return Observation{}, ErrNotLeader
	}
	return current[0], nil
}

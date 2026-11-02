package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"
)

const autoRecoveryWindow = 30 * time.Minute
const autoRecoveryInterval = 15 * time.Second
const autoRecoveryCooldown = 24 * time.Hour

type recoveryPlan struct {
	winner      RecoveryView
	lost        []string
	fingerprint string
	view        string
}
type recoveryManager struct {
	operationMu       sync.Mutex
	node              *Node
	observer          *Observer
	statusPort        uint16
	source            PeerSource // MUST include self; never use Observer's self-excluding source.
	refresh           func(context.Context) error
	now               func() time.Time
	window            time.Duration
	lastSample        time.Time
	since             time.Time
	missingSince      time.Time
	fingerprint       string
	policyFingerprint string
	reported          bool
	fenceReported     bool
	conflictDetected  bool
}

func (m *recoveryManager) details(view, state, mode string) {
	n := m.node
	n.recoveryDetails.Lock()
	n.recoveryDetails.view = view
	n.recoveryDetails.evaluation = state
	n.recoveryDetails.mode = mode
	n.recoveryDetails.protocol = 1
	n.recoveryDetails.Unlock()
}
func (m *recoveryManager) resetWindow() {
	m.since = time.Time{}
	m.fingerprint = ""
	m.reported = false
}

func (m *recoveryManager) run(ctx context.Context) {
	ticker := time.NewTicker(autoRecoveryInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		bounded, cancel := context.WithTimeout(ctx, 25*time.Second)
		_ = m.tick(bounded)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (m *recoveryManager) tick(ctx context.Context) error {
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	now := m.now()
	policy, signature := loadAutoRecoveryPolicy(m.node.config)
	// Changing even a same-mode signed policy starts a fresh evidence window.
	if signature != m.policyFingerprint || !m.lastSample.IsZero() && !recoverySampleGapValid(m.lastSample, now) {
		m.resetWindow()
		m.missingSince = time.Time{}
		m.fenceReported = false
	}
	m.policyFingerprint = signature
	m.lastSample = now
	if policy.Mode != "on" {
		m.node.selfFenced.Store(false)
	}
	m.observeLeaderConflict(now)
	if policy.Mode == "off" {
		m.resetWindow()
		m.missingSince = time.Time{}
		m.details("", "off", policy.Mode)
		return nil
	}
	// A failed Refresh retains Discovery's previous snapshot. It MUST NOT count
	// as another successful absence sample, even while that snapshot is fresh.
	if m.refresh == nil || m.refresh(ctx) != nil {
		m.resetWindow()
		m.missingSince = time.Time{}
		m.details("", "discovery-failed", policy.Mode)
		return ErrNotLeader
	}
	addresses, fresh := m.source.Snapshot()
	if !fresh || !validRecoveryAddresses(addresses, m.statusPort) {
		m.resetWindow()
		m.missingSince = time.Time{}
		m.details("", "invalid-discovery", policy.Mode)
		return ErrNotLeader
	}
	view := discoveryView(addresses)
	m.details(view, "evaluating", policy.Mode)
	local, _ := netip.ParseAddrPort(m.node.config.Local.Address)
	listed := false
	for _, address := range addresses {
		if address.Addr() == local.Addr() {
			listed = true
		}
	}
	if listed {
		m.missingSince = time.Time{}
		m.fenceReported = false
		m.node.selfFenced.Store(false)
	} else {
		m.resetWindow()
		if m.missingSince.IsZero() {
			m.missingSince = now
		}
		if recoveryWindowElapsed(m.missingSince, now) >= m.window {
			if policy.Valid && policy.Mode == "on" {
				m.node.selfFenced.Store(true)
			}
			if !m.fenceReported {
				slog.Error("primary absent from continuous fresh discovery; authority would be fenced in on mode", "node", m.node.config.Local.ID, "mode", policy.Mode)
				m.fenceReported = true
			}
		}
		m.details(view, "self-absent", policy.Mode)
		return ErrNotLeader
	}
	if !policy.Valid {
		m.resetWindow()
		m.details(view, "unsigned-policy-observe", policy.Mode)
		return ErrNotLeader
	}
	if m.conflictDetected {
		m.resetWindow()
		m.details(view, "multiple-leaders-manual-review", policy.Mode)
		return ErrNotLeader
	}
	plan, err := m.evaluate(ctx, addresses, view, now)
	if err != nil {
		m.resetWindow()
		m.details(view, "waiting", policy.Mode)
		return err
	}
	if m.fingerprint != plan.fingerprint || m.since.IsZero() {
		m.since = now
		m.fingerprint = plan.fingerprint
		m.reported = false
	}
	m.details(view, "collecting-evidence", policy.Mode)
	if recoveryWindowElapsed(m.since, now) < m.window {
		return nil
	}
	m.details(view, "window-satisfied", policy.Mode)
	if !m.reported {
		message := "would recover now"
		if policy.Mode == "on" {
			message = "automatic recovery evidence window satisfied"
		}
		slog.Error(message, "winner", plan.winner.NodeID, "lost_voters", plan.lost, "last_log_index", plan.winner.LastLogIndex, "last_log_term", plan.winner.LastLogTerm, "window_start", m.since)
		m.reported = true
	}
	if policy.Mode == "on" && plan.winner.NodeID == m.node.config.Local.ID && m.node.controlPending.CompareAndSwap(false, true) {
		// Runtime re-fetches discovery and repeats every guard before closing Raft.
		m.node.control <- false
	}
	return nil
}

func validRecoveryAddresses(addresses []netip.AddrPort, port uint16) bool {
	if len(addresses) == 0 || len(addresses) > 64 {
		return false
	}
	seen := map[netip.Addr]bool{}
	for _, a := range addresses {
		if !a.IsValid() || a.Addr().IsUnspecified() || a.Addr().IsMulticast() || a.Addr().Zone() != "" || a.Port() == 0 || port != 0 && a.Port() != port || seen[a.Addr()] {
			return false
		}
		seen[a.Addr()] = true
	}
	return true
}
func (m *recoveryManager) observeLeaderConflict(_ time.Time) {
	m.conflictDetected = m.observer.leaderConflictDetected()
}

func recoveryBlocked(c Config, now time.Time) bool {
	for _, name := range []string{"recover-singleton.json", "restore-singleton.json", generationPendingFile, "rejoin-reset.json"} {
		info, err := os.Lstat(filepath.Join(c.StateDir, name))
		if err == nil {
			// Completed reset journals preserve identity/bootstrap safeguards; they
			// cease blocking once an authenticated generation has caught up.
			if name == "rejoin-reset.json" {
				var intent resetIntent
				if _, err := readOperatorRequest(filepath.Join(c.StateDir, name), &intent, "clusterID", "nodeID", "directory", "moving", "recovery"); err == nil && !intent.Moving {
					continue
				}
			}
			_ = info
			return true
		}
		if !errors.Is(err, os.ErrNotExist) {
			return true
		}
	}
	entries, err := os.ReadDir(c.StateDir)
	if err != nil {
		return true
	}
	for _, entry := range entries {
		name := entry.Name()
		isDone := strings.HasPrefix(name, "recover-singleton.done-") || strings.HasPrefix(name, "restore-singleton.done-")
		if !isDone {
			continue
		}
		at := strings.Index(name, ".done-")
		stamp, err := time.Parse("20060102T150405.000000000Z", name[at+6:])
		if err != nil || now.Before(stamp) || now.Sub(stamp) < autoRecoveryCooldown {
			return true
		}
	}
	return false
}

func (m *recoveryManager) evaluate(ctx context.Context, addresses []netip.AddrPort, hash string, now time.Time) (recoveryPlan, error) {
	var plan recoveryPlan
	if m.node.raftGuard == nil || m.observer.leaderConflictDetected() {
		return plan, ErrNotLeader
	}
	local, _ := netip.ParseAddrPort(m.node.config.Local.Address)
	selfListed := false
	for _, a := range addresses {
		if a.Addr() == local.Addr() {
			selfListed = true
		}
	}
	if !selfListed {
		return plan, ErrNotLeader
	}
	own, err := m.node.RecoveryView()
	if err != nil || own.HasLeader || own.TransportRecovery != nil && *own.TransportRecovery != own.Recovery || own.LastLogIndex == 0 || own.MembershipRole == "none" || m.node.generationPending.Load() || recoveryBlocked(m.node.config, now) {
		return plan, ErrNotLeader
	}
	configuration := m.node.raft.GetConfiguration()
	if err := configuration.Error(); err != nil {
		return plan, err
	}
	// Probe every listed address, including this process, through real mTLS.
	// Unknown routes, timeouts, duplicate identities and stale view hashes all
	// break agreement. Discovery alone never grants recovery authority.
	views := make([]RecoveryView, len(addresses))
	errs := make([]error, len(addresses))
	slots := make(chan struct{}, 4)
	var workers sync.WaitGroup
	for i, a := range addresses {
		workers.Add(1)
		go func(i int, a netip.AddrPort) {
			defer workers.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			views[i], errs[i] = m.observer.probeRecoveryView(ctx, a)
		}(i, a)
	}
	workers.Wait()
	seen := map[string]bool{}
	winner := -1
	for i, v := range views {
		if errs[i] != nil || v.TransportRecovery != nil && *v.TransportRecovery != v.Recovery || seen[v.NodeID] || v.HasLeader || v.DiscoveryViewHash != hash || v.Protocol != 1 || v.Recovery.Generation > own.Recovery.Generation || v.Recovery.Generation == own.Recovery.Generation && v.Recovery.Lineage != own.Recovery.Lineage {
			return plan, ErrNotLeader
		}
		seen[v.NodeID] = true
		if v.NodeID == own.NodeID && v.Recovery != own.Recovery {
			return plan, ErrNotLeader
		}
		if v.LastLogIndex == 0 || v.MembershipRole == "none" {
			continue
		}
		if winner < 0 || recoveryWinner(v, addresses[i], views[winner], addresses[winner]) {
			winner = i
		}
	}
	if !seen[own.NodeID] || winner < 0 {
		return plan, ErrNotLeader
	}
	voters, present := 0, 0
	var absent []Member
	for _, server := range configuration.Configuration().Servers {
		if server.Suffrage != raft.Voter {
			continue
		}
		voters++
		member, err := parseAddress(string(server.Address))
		if err != nil {
			return plan, err
		}
		a, _ := netip.ParseAddrPort(member.Address)
		listed := seen[member.ID]
		for _, address := range addresses {
			if address.Addr() == a.Addr() {
				listed = true
			}
		}
		if listed {
			present++
		} else {
			absent = append(absent, member)
		}
	}
	if voters == 0 || present >= voters/2+1 || len(absent) == 0 {
		return plan, ErrNotLeader
	}
	// Absence evidence must agree with direct, identity-checked probes of all
	// missing voters. Even a reachable candidate means keep waiting.
	reachable := make([]bool, len(absent))
	workers = sync.WaitGroup{}
	for i, member := range absent {
		workers.Add(1)
		go func(i int, member Member) {
			defer workers.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				reachable[i] = true
				return
			}
			a, _ := netip.ParseAddrPort(member.Address)
			peer, err := m.observer.probe(ctx, netip.AddrPortFrom(a.Addr(), addresses[0].Port()))
			reachable[i] = err == nil && peer.Status.ID == member.ID
		}(i, member)
	}
	workers.Wait()
	for i, live := range reachable {
		if live {
			return plan, ErrNotLeader
		}
		plan.lost = append(plan.lost, absent[i].ID)
	}
	if ctx.Err() != nil {
		return plan, ctx.Err()
	}
	sort.Strings(plan.lost)
	// Check again after network I/O: local authority may have reappeared.
	_, leader := m.node.raft.LeaderWithID()
	if leader != "" || m.node.raft.State() == raft.Leader {
		return plan, ErrNotLeader
	}
	plan.winner = views[winner]
	plan.view = hash
	raw, _ := json.Marshal(struct {
		View     string
		Winner   string
		Lost     []string
		Recovery recoveryGeneration
	}{hash, plan.winner.NodeID, plan.lost, own.Recovery})
	plan.fingerprint = string(raw)
	return plan, nil
}
func recoveryWinner(a RecoveryView, aa netip.AddrPort, b RecoveryView, ba netip.AddrPort) bool {
	if (a.MembershipRole == "voter") != (b.MembershipRole == "voter") {
		return a.MembershipRole == "voter"
	}
	if a.LastLogTerm != b.LastLogTerm {
		return a.LastLogTerm > b.LastLogTerm
	}
	if a.LastLogIndex != b.LastLogIndex {
		return a.LastLogIndex > b.LastLogIndex
	}
	return candidateRank(aa.Addr()) < candidateRank(ba.Addr())
}

// Returns stopped=true once Raft was closed, even if the final disk comparison
// fails: Runtime must then exit and restart normally, never run a dead node.
func (m *recoveryManager) performRecovery(ctx context.Context) (stopped bool, err error) {
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	defer func() {
		if !stopped && err != nil {
			m.resetWindow()
		}
	}()
	policy, signature := loadAutoRecoveryPolicy(m.node.config)
	if !policy.Valid || policy.Mode != "on" || signature != m.policyFingerprint || m.since.IsZero() || recoveryWindowElapsed(m.since, m.now()) < m.window || !recoverySampleGapValid(m.lastSample, m.now()) || m.refresh(ctx) != nil {
		return false, ErrNotLeader
	}
	addresses, fresh := m.source.Snapshot()
	if !fresh || !validRecoveryAddresses(addresses, m.statusPort) {
		return false, ErrNotLeader
	}
	plan, err := m.evaluate(ctx, addresses, discoveryView(addresses), m.now())
	if err != nil || plan.fingerprint != m.fingerprint || plan.winner.NodeID != m.node.config.Local.ID {
		return false, ErrNotLeader
	}
	latestPolicy, latestSignature := loadAutoRecoveryPolicy(m.node.config)
	latestAddresses, latestFresh := m.source.Snapshot()
	if !latestPolicy.Valid || latestPolicy.Mode != "on" || latestSignature != signature || !latestFresh || discoveryView(latestAddresses) != plan.view || m.conflictDetected || m.observer.leaderConflictDetected() || !recoverySampleGapValid(m.lastSample, m.now()) {
		return false, ErrNotLeader
	}
	// No concurrent membership/write operation can continue once Close returns.
	if err := m.node.Close(); err != nil {
		return true, err
	}
	index, term := m.node.logPosition()
	if index != plan.winner.LastLogIndex || term != plan.winner.LastLogTerm {
		return true, errors.New("Raft position changed during shutdown; restart without recovery")
	}
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(m.node.config.StateDir, "raft.db"), BoltOptions: &bbolt.Options{ReadOnly: true, Timeout: time.Second}})
	if err != nil {
		return true, err
	}
	last, err := store.LastIndex()
	if err != nil {
		store.Close()
		return true, err
	}
	// The request contract uses the physical log-store index, including zero
	// for snapshot-only histories. The evaluation uses Raft's logical position.
	var tail raft.Log
	if last > 0 {
		err = store.GetLog(last, &tail)
	}
	store.Close()
	if err != nil {
		return true, err
	}
	if last > plan.winner.LastLogIndex || last == plan.winner.LastLogIndex && last > 0 && tail.Term != plan.winner.LastLogTerm {
		return true, errors.New("Raft history changed during recovery shutdown; restart without recovery")
	}
	request := singletonRequest{ClusterID: m.node.config.ClusterID, NodeID: m.node.config.Local.ID, ExpectedLastIndex: last, AllowLearner: plan.winner.MembershipRole == "learner"}
	raw, _ := json.Marshal(request)
	path := filepath.Join(m.node.config.StateDir, "recover-singleton.json")
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return true, errors.New("recovery request already present")
	}
	if err := writeNewRecoveryRequest(path, raw); err != nil {
		return true, err
	}
	slog.Error("automatic singleton recovery requested; container restart required; lost tail of writes may be gone; review security metadata", "winner", request.NodeID, "lost_voters", plan.lost, "last_log_index", plan.winner.LastLogIndex, "last_log_term", plan.winner.LastLogTerm, "window_start", m.since)
	return true, fmt.Errorf("automatic recovery request persisted; restart required")
}

// Link installs a fully fsynced file only if the destination is still absent;
// a concurrently written operator request is never overwritten.
func writeNewRecoveryRequest(path string, raw []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".recovery-request-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(raw)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Link(name, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

// Like authorityTimeValid, evidence requires both clocks. CLOCK_MONOTONIC
// may stop during host suspend while wall time advances; that gap is not a
// continuous sequence of successful samples and must never authorize recovery.
func recoverySampleGapValid(previous, now time.Time) bool {
	if previous.IsZero() {
		return false
	}
	monotonic := now.Sub(previous)
	wall := now.Round(0).Sub(previous.Round(0))
	return monotonic >= 0 && monotonic <= 3*autoRecoveryInterval && wall >= 0 && wall <= 3*autoRecoveryInterval
}
func recoveryWindowElapsed(since, now time.Time) time.Duration {
	return min(now.Sub(since), now.Round(0).Sub(since.Round(0)))
}

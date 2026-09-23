package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type automatic struct {
	node     *Node
	observer *Observer
	source   PeerSource // includes self; an absent self is NOT a singleton
	refresh  func(context.Context) error
	mu       sync.Mutex
	view     string
	since    time.Time
}

func discoveryView(addresses []netip.AddrPort) string {
	values := make([]string, len(addresses))
	for i, a := range addresses {
		values[i] = a.String()
	}
	sort.Strings(values)
	data, _ := json.Marshal(values)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
func candidateRank(ip netip.Addr) string {
	h := sha256.Sum256([]byte(ip.String()))
	return hex.EncodeToString(h[:])
}

func (a *automatic) statusView() string { a.mu.Lock(); defer a.mu.Unlock(); return a.view }

func (a *automatic) run(ctx context.Context) {
	timer := time.NewTicker(5 * time.Second)
	defer timer.Stop()
	for ctx.Err() == nil {
		bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
		if err := a.refresh(bounded); err == nil {
			if err := a.tick(bounded, time.Now()); err != nil && ctx.Err() == nil {
				slog.Warn("automatic cluster initialization waiting for consistent authenticated peer evidence")
			}
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

func (a *automatic) tick(ctx context.Context, now time.Time) error {
	addresses, fresh := a.source.Snapshot()
	if !fresh || len(addresses) == 0 || len(addresses) > 64 {
		return ErrNotLeader
	}
	view := discoveryView(addresses)
	a.mu.Lock()
	if a.view != view {
		a.view = view
		a.since = now
	}
	stable := now.Sub(a.since) >= 30*time.Second
	a.mu.Unlock()
	local, _ := netip.ParseAddrPort(a.node.config.Local.Address)
	self := false
	candidate := true
	for _, address := range addresses {
		if address.Addr() == local.Addr() {
			self = true
		}
		if candidateRank(address.Addr()) < candidateRank(local.Addr()) {
			candidate = false
		}
	}
	if !self {
		return ErrNotLeader
	}
	if a.node.raft.LastIndex() > 0 {
		// Persist evidence on every initialized member, not only the first
		// primary, so replacement cohorts cannot mistake surviving data for new.
		if err := a.recordGenesis(); err != nil {
			return err
		}
		if a.node.raft.State() == raft.Leader {
			configuration := a.node.raft.GetConfiguration()
			if err := configuration.Error(); err != nil {
				return err
			}
			for _, member := range configuration.Configuration().Servers {
				if string(member.ID) == a.node.config.Local.ID && string(member.Address) != a.node.config.Local.raftAddress() {
					if err := a.node.updateAddress(ctx, a.node.config.Local); err != nil {
						return err
					}
				}
			}
		}
		return a.recordHistory()
	}
	if !stable || !candidate {
		return nil
	}
	if _, err := os.Lstat(filepath.Join(a.node.config.StateDir, "primary-history.json")); err == nil {
		return errors.New("previous primary history without durable log requires recovery")
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, address := range addresses {
		if address.Addr() == local.Addr() {
			continue
		}
		observed, err := a.observer.probe(ctx, address)
		if err != nil || observed.Status.LastIndex != 0 || observed.Status.BootstrapView != view {
			return ErrNotLeader
		}
	}
	marker := filepath.Join(a.node.config.ContentDir, "cluster-genesis.json")
	if _, err := os.Lstat(marker); err == nil {
		return errors.New("existing deployment evidence requires surviving quorum or explicit recovery")
	} else if !os.IsNotExist(err) {
		return err
	}
	// A local intent is fsynced before BootstrapCluster. A crash never erases
	// the chosen genesis identity; restarting this node retries the same choice.
	intent := filepath.Join(a.node.config.StateDir, "bootstrap-intent.json")
	data, _ := json.Marshal(a.node.config.Local)
	if raw, err := os.ReadFile(intent); err == nil {
		var member Member
		if decodeStrict(raw, &member) != nil || member.ID != a.node.config.Local.ID {
			return ErrInvalid
		}
	} else if os.IsNotExist(err) {
		if err := replaceCertificate(intent, data); err != nil {
			return err
		}
	} else {
		return err
	}
	configuration := raft.Configuration{Servers: []raft.Server{{ID: raft.ServerID(a.node.config.Local.ID), Address: raft.ServerAddress(a.node.config.Local.raftAddress()), Suffrage: raft.Voter}}}
	if err := a.node.wait(ctx, func() raft.Future { return a.node.raft.BootstrapCluster(configuration) }); err != nil {
		return err
	}
	slog.Info("initial cluster coordinator selected from unanimous fresh discovery; waiting for replica enrollment")
	return a.recordGenesis()
}

type primaryHistory struct {
	Primary         string    `json:"primary"`
	Term            uint64    `json:"term"`
	HighestRevision uint64    `json:"highestRevision"`
	Updated         time.Time `json:"updated"`
}

// Operational loss detection only. A counter is never permission to overwrite
// committed security history or bootstrap a replacement cluster.
func (a *automatic) recordHistory() error {
	path := filepath.Join(a.node.config.StateDir, "primary-history.json")
	var old primaryHistory
	if raw, err := privateFile(path, a.node.config.ContentDir, 2048); err == nil {
		if decodeStrict(raw, &old) != nil {
			return ErrInvalid
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	status := a.node.Status()
	peers, fresh := a.observer.Snapshot()
	highest := max(old.HighestRevision, status.DurableRevision)
	if fresh {
		for _, peer := range peers {
			highest = max(highest, peer.Status.DurableRevision)
		}
	}
	if status.Role == "Leader" && old.Primary != status.ID && old.HighestRevision > status.ReplicatedRevision {
		slog.Warn("new primary is behind previously observed content history; recent uploads may be lost", "known_revision", old.HighestRevision, "replicated_revision", status.ReplicatedRevision)
	}
	if status.LeaderID == "" {
		status.LeaderID = old.Primary
	}
	next := primaryHistory{Primary: status.LeaderID, Term: status.Term, HighestRevision: highest, Updated: time.Now().UTC()}
	data, _ := json.Marshal(next)
	return replaceCertificate(path, data)
}

func (a *automatic) recordGenesis() error {
	path := filepath.Join(a.node.config.ContentDir, "cluster-genesis.json")
	data, _ := json.Marshal(struct {
		ClusterID string `json:"clusterID"`
	}{a.node.config.ClusterID})
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			return ErrInvalid
		}
		raw, err := os.ReadFile(path)
		var marker struct {
			ClusterID string `json:"clusterID"`
		}
		if err != nil || len(raw) > 1024 || decodeStrict(raw, &marker) != nil || marker.ClusterID != a.node.config.ClusterID {
			return errors.New("existing content belongs to a different cluster; refusing implicit recovery")
		}
		return nil
	}
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	dir, err := os.Open(a.node.config.ContentDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

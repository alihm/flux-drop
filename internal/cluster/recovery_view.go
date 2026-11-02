package cluster

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"strconv"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/replica"
)

const recoveryViewPath = "/_drop_cluster/recovery-view"

// Deliberately separate from Status: old peers decode Status strictly. Protocol
// zero advertises observation only; automated recovery requires protocol one.
type RecoveryView struct {
	TransportRecovery *recoveryGeneration `json:"transportRecovery,omitempty"`
	NodeID            string              `json:"nodeID"`
	ClusterID         string              `json:"clusterID"`
	LastLogIndex      uint64              `json:"lastLogIndex"`
	LastLogTerm       uint64              `json:"lastLogTerm"`
	HasLeader         bool                `json:"hasLeader"`
	MembershipRole    string              `json:"membershipRole"`
	DiscoveryViewHash string              `json:"discoveryViewHash"`
	EvaluationState   string              `json:"evaluationState"`
	RecoveryMode      string              `json:"recoveryMode"`
	Recovery          recoveryGeneration  `json:"recovery"`
	Protocol          int                 `json:"protocol"`
}

type recoveryDetails struct {
	sync.RWMutex
	view, evaluation, mode string
	protocol               int
}

func (n *Node) logPosition() (index, term uint64) {
	stats := n.raft.Stats()
	index, _ = strconv.ParseUint(stats["last_log_index"], 10, 64)
	term, _ = strconv.ParseUint(stats["last_log_term"], 10, 64)
	snapshotIndex, _ := strconv.ParseUint(stats["last_snapshot_index"], 10, 64)
	if snapshotIndex > index {
		index = snapshotIndex
		term, _ = strconv.ParseUint(stats["last_snapshot_term"], 10, 64)
	}
	return
}
func (n *Node) RecoveryView() (RecoveryView, error) {
	index, term := n.logPosition()
	_, leader := n.raft.LeaderWithID()
	view := RecoveryView{NodeID: n.config.Local.ID, ClusterID: n.config.ClusterID, LastLogIndex: index, LastLogTerm: term, HasLeader: leader != "" || n.raft.State() == raft.Leader, MembershipRole: "none", RecoveryMode: "observe", EvaluationState: "waiting"}
	configuration := n.raft.GetConfiguration()
	if err := configuration.Error(); err != nil {
		return view, err
	}
	for _, s := range configuration.Configuration().Servers {
		if string(s.ID) == n.config.Local.ID {
			if s.Suffrage == raft.Voter {
				view.MembershipRole = "voter"
			} else {
				view.MembershipRole = "learner"
			}
		}
	}
	var err error
	view.Recovery, err = n.recoveryGeneration()
	if err != nil {
		return view, err
	}
	n.recoveryDetails.RLock()
	view.DiscoveryViewHash = n.recoveryDetails.view
	view.Protocol = n.recoveryDetails.protocol
	if n.recoveryDetails.evaluation != "" {
		view.EvaluationState = n.recoveryDetails.evaluation
	}
	if n.recoveryDetails.mode != "" {
		view.RecoveryMode = n.recoveryDetails.mode
	}
	n.recoveryDetails.RUnlock()
	if view.DiscoveryViewHash == "" && n.auto != nil {
		view.DiscoveryViewHash = n.auto.statusView()
	}
	if n.raftGuard != nil {
		effective, err := n.raftGuard.effective()
		if err != nil {
			return view, err
		}
		if effective != view.Recovery {
			view.TransportRecovery = &effective
		}
	}
	if n.generationPending.Load() {
		view.EvaluationState = "generation-pending"
	}
	return view, nil
}

func (o *Observer) probeRecoveryView(ctx context.Context, address netip.AddrPort) (RecoveryView, error) {
	var view RecoveryView
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+address.String()+recoveryViewPath, nil)
	if err != nil {
		return view, err
	}
	req.Header.Set("X-Drop-Cluster", o.clusterID)
	response, err := o.client.Do(req)
	if err != nil {
		return view, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.TLS == nil || len(response.TLS.VerifiedChains) == 0 || len(response.TLS.PeerCertificates) == 0 {
		return view, errors.New("peer has no agreed recovery view")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 16385))
	if err != nil || len(raw) > 16384 || decodePeerJSON(raw, &view) != nil || view.ClusterID != o.clusterID || view.NodeID == "" || view.NodeID != replica.PeerIdentity(response.TLS.PeerCertificates[0], o.app) || (view.LastLogIndex == 0) != (view.LastLogTerm == 0) || view.Protocol < 0 || len(view.DiscoveryViewHash) > 64 {
		return view, ErrInvalid
	}
	switch view.MembershipRole {
	case "none", "voter", "learner":
	default:
		return view, ErrInvalid
	}
	switch view.RecoveryMode {
	case "off", "observe", "on":
	default:
		return view, ErrInvalid
	}
	if view.TransportRecovery != nil && !validRecoveryGeneration(*view.TransportRecovery) {
		return view, ErrInvalid
	}
	if view.Recovery.Generation == 0 && view.Recovery.Lineage != "" || view.Recovery.Generation > 0 && len(view.Recovery.Lineage) != 32 {
		return view, ErrInvalid
	}
	return view, nil
}

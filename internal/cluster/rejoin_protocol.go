package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/replica"
)

const rejoinPath = "/_drop_cluster/rejoin"

type rejoinRequest struct {
	Member   Member             `json:"member"`
	Recovery recoveryGeneration `json:"recovery"`
}
type rejoinResponse struct {
	Result    string             `json:"result"`
	NodeID    string             `json:"nodeID"`
	ClusterID string             `json:"clusterID"`
	Recovery  recoveryGeneration `json:"recovery"`
}
type rejoinApproval struct {
	key      string
	recovery recoveryGeneration
	expires  time.Time
}
type rejoinApprovals struct {
	sync.Mutex
	values map[string]rejoinApproval
}

func (e *enrollment) candidateAllowed(r *http.Request, m Member) bool {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 || m.validate() != nil || m.ID != replica.PeerIdentity(r.TLS.PeerCertificates[0], e.observer.app) {
		return false
	}
	address, _ := netip.ParseAddrPort(m.Address)
	local, _ := netip.ParseAddrPort(e.node.config.Local.Address)
	peers, fresh := e.observer.Snapshot()
	if !fresh || address.Port() != local.Port() {
		return false
	}
	for _, p := range peers {
		a, err := netip.ParseAddrPort(p.Address)
		if err == nil && a.Addr() == address.Addr() && p.Status.ID == m.ID {
			return true
		}
	}
	return false
}
func (e *enrollment) rejoinServe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.RawPath != "" || r.URL.RawQuery != "" || r.URL.ForceQuery || r.Header.Get("Cookie") != "" || r.Header.Get("Content-Type") != "application/json" || len(r.Header.Values("X-Drop-Cluster")) != 1 || r.Header.Get("X-Drop-Cluster") != e.node.config.ClusterID || len(r.Header.Values("Authorization")) != 1 {
		http.Error(w, "invalid rejoin", 400)
		return
	}
	token, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if err != nil || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare(token, e.credential[:]) != 1 {
		http.Error(w, "forbidden", 403)
		return
	}
	var request rejoinRequest
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2048))
	if err != nil || decodeStrict(data, &request) != nil || !e.candidateAllowed(r, request.Member) {
		http.Error(w, "candidate not verified", 503)
		return
	}
	current, err := e.node.recoveryGeneration()
	if err != nil {
		http.Error(w, "recovery unavailable", 503)
		return
	}
	response := rejoinResponse{Result: "wait", NodeID: e.node.config.Local.ID, ClusterID: e.node.config.ClusterID, Recovery: current}
	if e.node.raft.State() == raft.Leader && !e.node.generationPending.Load() && e.node.fence(r.Context()) == nil {
		if request.Recovery.Generation < current.Generation || (request.Recovery.Generation == current.Generation && request.Recovery.Lineage != current.Lineage) {
			response.Result = "reset_required"
		} else if request.Recovery == current {
			// Candidate credentials and TLS are necessary, but an existing identity's
			// key pin must also match before granting either admission or reset advice.
			key := sha256.Sum256(r.TLS.PeerCertificates[0].RawSubjectPublicKeyInfo)
			hash := hex.EncodeToString(key[:])
			row := e.node.state.read([]string{"cluster_admissions/" + request.Member.ID})["cluster_admissions/"+request.Member.ID]
			var old admission
			if row.Version != 0 && (decodeStrict(row.Value, &old) != nil || old.KeyHash != hash || old.Member.ID != request.Member.ID) {
				http.Error(w, "identity mismatch", 403)
				return
			}
			e.approvals.Lock()
			if e.approvals.values == nil {
				e.approvals.values = map[string]rejoinApproval{}
			}
			// Bounded to the admitted/discovered cohort, with expired entries removed.
			for id, a := range e.approvals.values {
				if !e.now().Before(a.expires) {
					delete(e.approvals.values, id)
				}
			}
			if len(e.approvals.values) >= 64 {
				e.approvals.Unlock()
				http.Error(w, "busy", 503)
				return
			}
			e.approvals.values[request.Member.ID] = rejoinApproval{hash, current, e.now().Add(time.Minute)}
			e.approvals.Unlock()
			response.Result = "ok"
		}
	}
	// Never disclose reset advice to a different key for an existing identity.
	if response.Result == "reset_required" {
		row := e.node.state.read([]string{"cluster_admissions/" + request.Member.ID})["cluster_admissions/"+request.Member.ID]
		var old admission
		hash := sha256.Sum256(r.TLS.PeerCertificates[0].RawSubjectPublicKeyInfo)
		if row.Version != 0 && (decodeStrict(row.Value, &old) != nil || old.KeyHash != hex.EncodeToString(hash[:]) || old.Member.ID != request.Member.ID) {
			http.Error(w, "identity mismatch", 403)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}
func (e *enrollment) approved(a admission, current recoveryGeneration) bool {
	e.approvals.Lock()
	defer e.approvals.Unlock()
	approval, ok := e.approvals.values[a.Member.ID]
	return ok && approval.key == a.KeyHash && approval.recovery == current && e.now().Before(approval.expires)
}

func (e *enrollment) exchangeRejoin(ctx context.Context, p Observation) (rejoinResponse, error) {
	current, err := e.node.recoveryGeneration()
	if err != nil {
		return rejoinResponse{}, err
	}
	raw, _ := json.Marshal(rejoinRequest{e.node.config.Local, current})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+p.Address+rejoinPath, bytes.NewReader(raw))
	if err != nil {
		return rejoinResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Drop-Cluster", e.node.config.ClusterID)
	req.Header.Set("Authorization", "Bearer "+base64.StdEncoding.EncodeToString(e.credential[:]))
	transport := e.client.Transport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	transport.TLSClientConfig = transport.TLSClientConfig.Clone()
	verify := transport.TLSClientConfig.VerifyConnection
	transport.TLSClientConfig.VerifyConnection = func(state tls.ConnectionState) error {
		if err := verify(state); err != nil {
			return err
		}
		if replica.PeerIdentity(state.PeerCertificates[0], e.observer.app) != p.Status.ID {
			return ErrInvalid
		}
		return nil
	}
	client := *e.client
	client.Transport = transport
	response, err := client.Do(req)
	if err != nil {
		return rejoinResponse{}, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	var result rejoinResponse
	if err != nil || len(data) > 4096 || response.StatusCode != 200 || decodePeerJSON(data, &result) != nil || result.NodeID != p.Status.ID || result.ClusterID != e.node.config.ClusterID {
		return result, ErrNotLeader
	}
	if result.Result != "ok" && result.Result != "wait" && result.Result != "reset_required" {
		return result, ErrInvalid
	}
	return result, nil
}

// A reset is permitted only with no local leader and no directly reachable
// majority of this node's old generation. Unknown/legacy generations count as
// potentially live: compatibility must delay recovery, never justify it.
func (e *enrollment) mayReset(ctx context.Context) bool {
	if e.node.raft.State() == raft.Leader {
		return false
	}
	_, leader := e.node.raft.LeaderWithID()
	if leader != "" {
		return false
	}
	configuration := e.node.raft.GetConfiguration()
	if configuration.Error() != nil {
		return false
	}
	current, err := e.node.recoveryGeneration()
	if err != nil {
		return false
	}
	voters, reachable, selfVoter := 0, 0, false
	for _, s := range configuration.Configuration().Servers {
		if s.Suffrage != raft.Voter {
			continue
		}
		voters++
		if string(s.ID) == e.node.config.Local.ID {
			reachable++
			selfVoter = true
			continue
		}
		m, err := parseAddress(string(s.Address))
		if err != nil {
			return false
		}
		a, _ := netip.ParseAddrPort(m.Address)
		peer, err := e.observer.probe(ctx, netip.AddrPortFrom(a.Addr(), e.port))
		if err != nil || peer.Status.ID != m.ID {
			continue
		}
		response, err := e.exchangeRejoin(ctx, peer)
		if err != nil || response.Recovery == current {
			reachable++
		}
	}
	return !selfVoter || reachable < voters/2+1
}

type resetIntent struct {
	ClusterID string             `json:"clusterID"`
	NodeID    string             `json:"nodeID"`
	Directory string             `json:"directory"`
	Moving    bool               `json:"moving"`
	Recovery  recoveryGeneration `json:"recovery"`
}

// Restart is supervised by Runtime, not by an HTTP handler. Journal each move
// so a process death halfway through cannot accidentally reopen old history.
func archiveSuperseded(c Config, recovery ...recoveryGeneration) error {
	intent := resetIntent{ClusterID: c.ClusterID, NodeID: c.Local.ID, Directory: "superseded-" + recoveryTimestamp(), Moving: true}
	if len(recovery) == 1 {
		intent.Recovery = recovery[0]
	}
	raw, _ := json.Marshal(intent)
	if err := replaceCertificate(filepath.Join(c.StateDir, "rejoin-reset.json"), raw); err != nil {
		return err
	}
	return resumeSuperseded(c)
}
func resumeSuperseded(c Config) error {
	var intent resetIntent
	_, err := readOperatorRequest(filepath.Join(c.StateDir, "rejoin-reset.json"), &intent, "clusterID", "nodeID", "directory", "moving", "recovery")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if intent.ClusterID != c.ClusterID || intent.NodeID != c.Local.ID || !strings.HasPrefix(intent.Directory, "superseded-") || filepath.Base(intent.Directory) != intent.Directory {
		return ErrInvalid
	}
	if !intent.Moving {
		return nil
	}
	directory := filepath.Join(c.StateDir, intent.Directory)
	if err := os.Mkdir(directory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe superseded archive directory")
	}
	for _, name := range []string{"raft.db", "snapshots"} {
		source := filepath.Join(c.StateDir, name)
		destination := filepath.Join(directory, name)
		_, srcErr := os.Lstat(source)
		_, dstErr := os.Lstat(destination)
		if srcErr == nil && dstErr == nil {
			return errors.New("ambiguous interrupted state reset")
		}
		if srcErr == nil {
			if err := os.Rename(source, destination); err != nil {
				return err
			}
		} else if !errors.Is(srcErr, os.ErrNotExist) {
			return srcErr
		}
	}
	if err := syncDirectory(directory); err != nil {
		return err
	}
	if err := syncDirectory(c.StateDir); err != nil {
		return err
	}
	intent.Moving = false
	raw, _ := json.Marshal(intent)
	if err := replaceCertificate(filepath.Join(c.StateDir, "rejoin-reset.json"), raw); err != nil {
		return err
	}
	entries, err := filepath.Glob(filepath.Join(c.StateDir, "superseded-*"))
	if err != nil {
		return err
	}
	sort.Strings(entries)
	for _, old := range entries {
		if old != directory {
			stamp := strings.TrimPrefix(filepath.Base(old), "superseded-")
			if _, err := time.Parse("20060102T150405.000000000Z", stamp); err != nil {
				continue
			}
			info, err := os.Lstat(old)
			if err != nil {
				return err
			}
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				continue
			}
			if err := os.RemoveAll(old); err != nil {
				return err
			}
		}
	}
	return syncDirectory(c.StateDir)
}

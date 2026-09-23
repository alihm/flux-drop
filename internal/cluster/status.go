package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/runonflux/flux-drop/internal/replica"
)

const StatusPath = "/_drop_cluster/status"

func parseLeaf(c tls.Certificate) (*x509.Certificate, error) {
	if len(c.Certificate) == 0 {
		return nil, errors.New("missing certificate")
	}
	return x509.ParseCertificate(c.Certificate[0])
}

// StatusHandler belongs ONLY on an mTLS listener. It verifies that boundary
// again so accidentally mounting it on the public server does not expose it.
func (n *Node) StatusHandler(app string) http.Handler {
	return n.statusHandler(app, nil)
}

func (n *Node) statusHandler(app string, observer *Observer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 || replica.PeerIdentity(r.TLS.PeerCertificates[0], app) == "" {
			http.Error(w, "forbidden", 403)
			return
		}
		if r.URL.Path == enrollmentPath && n.enrollment != nil {
			n.enrollment.serve(w, r)
			return
		}
		if r.URL.Path == metadataPath {
			n.rpcHandler(w, r, app)
			return
		}
		if r.Method != http.MethodGet || r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" || len(r.Header.Values("X-Drop-Cluster")) != 1 || r.Header.Get("X-Drop-Cluster") != n.config.ClusterID {
			http.Error(w, "invalid cluster request", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case StatusPath:
			_ = json.NewEncoder(w).Encode(n.Status())
		case "/_drop_cluster/peers":
			if observer == nil {
				http.Error(w, "discovery unavailable", 503)
				return
			}
			peers, fresh := observer.Snapshot()
			if !fresh {
				w.WriteHeader(503)
			}
			_ = json.NewEncoder(w).Encode(struct {
				Fresh      bool          `json:"fresh"`
				Candidates []Observation `json:"candidates"`
			}{fresh, peers})
		case "/healthz":
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
		case "/readyz":
			if err := n.Ready(r.Context()); err != nil {
				w.WriteHeader(503)
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "not_authoritative"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "authoritative"})
		default:
			http.NotFound(w, r)
		}
	})
}

type PeerSource interface {
	Snapshot() ([]netip.AddrPort, bool)
}
type Observation struct {
	Address    string    `json:"address"`
	Status     Status    `json:"status"`
	ObservedAt time.Time `json:"observedAt"`
}

// Observer periodically refreshes authenticated status for fresh Flux candidates.
// It intentionally has NO reference to membership mutation/election methods.
type Observer struct {
	source         PeerSource
	client         *http.Client
	app, clusterID string
	mu             sync.RWMutex
	observations   []Observation
	updated        time.Time
}

func NewObserver(source PeerSource, clusterID, app string, config *tls.Config) (*Observer, error) {
	if source == nil || !clusterIDPattern.MatchString(clusterID) {
		return nil, ErrInvalid
	}
	client, err := replica.NewPeerClient(config)
	if err != nil {
		return nil, err
	}
	client.Timeout = 3 * time.Second
	return &Observer{source: source, client: client, clusterID: clusterID, app: app}, nil
}

func (o *Observer) probe(ctx context.Context, address netip.AddrPort) (Observation, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+address.String()+StatusPath, nil)
	if err != nil {
		return Observation{}, err
	}
	req.Header.Set("X-Drop-Cluster", o.clusterID)
	response, err := o.client.Do(req)
	if err != nil {
		return Observation{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.TLS == nil || len(response.TLS.VerifiedChains) == 0 || len(response.TLS.PeerCertificates) == 0 {
		return Observation{}, errors.New("untrusted peer status")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 16<<10+1))
	if err != nil {
		return Observation{}, err
	}
	var status Status
	if len(body) > 16<<10 || decodeStrict(body, &status) != nil || status.ClusterID != o.clusterID || status.ID != replica.PeerIdentity(response.TLS.PeerCertificates[0], o.app) || status.ID == "" || status.UptimeSeconds < 0 || status.QuorumVerified || status.AppliedIndex > status.LastIndex || status.StateIndex > status.AppliedIndex {
		return Observation{}, errors.New("invalid peer status")
	}
	switch status.Role {
	case "Follower", "Candidate", "Leader", "Shutdown":
	default:
		return Observation{}, errors.New("invalid peer role")
	}
	return Observation{Address: address.String(), Status: status, ObservedAt: time.Now().UTC()}, nil
}

func (o *Observer) Refresh(ctx context.Context) error {
	addresses, fresh := o.source.Snapshot()
	if !fresh {
		o.mu.Lock()
		o.observations = nil
		o.updated = time.Time{}
		o.mu.Unlock()
		return errors.New("discovery is stale")
	}
	if len(addresses) > 64 {
		return errors.New("too many cluster candidates")
	}
	// Small, bounded concurrency; status failure is NOT an eviction vote.
	results := make(chan Observation, len(addresses))
	slots := make(chan struct{}, 4)
	var workers sync.WaitGroup
	for _, address := range addresses {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			workers.Wait()
			return ctx.Err()
		}
		workers.Add(1)
		go func(address netip.AddrPort) {
			defer workers.Done()
			defer func() { <-slots }()
			if observation, err := o.probe(ctx, address); err == nil {
				results <- observation
			}
		}(address)
	}
	workers.Wait()
	close(results)
	observations := make([]Observation, 0, len(addresses))
	seen := map[string]bool{}
	for observation := range results {
		if seen[observation.Status.ID] {
			o.mu.Lock()
			o.observations = nil
			o.updated = time.Time{}
			o.mu.Unlock()
			return errors.New("duplicate peer identity")
		}
		seen[observation.Status.ID] = true
		observations = append(observations, observation)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mu.Lock()
	o.observations = observations
	o.updated = time.Now()
	o.mu.Unlock()
	return nil
}

func (o *Observer) Snapshot() ([]Observation, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.updated.IsZero() || time.Since(o.updated) > 90*time.Second {
		return nil, false
	}
	return append([]Observation(nil), o.observations...), true
}

func (o *Observer) Run(ctx context.Context) {
	defer o.client.CloseIdleConnections()
	for ctx.Err() == nil {
		refreshCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		_ = o.Refresh(refreshCtx)
		cancel()
		timer := time.NewTimer(15 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

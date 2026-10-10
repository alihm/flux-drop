package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/analytics"
	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/replica"
)

const metadataPath = "/_drop_cluster/metadata"

type rpcRequest struct {
	Analytics      *analytics.Snapshot `json:"analytics,omitempty"`
	AnalyticsQuery *analytics.Query    `json:"analyticsQuery,omitempty"`
	Stamp          *readStamp          `json:"stamp,omitempty"`
	Prefix         string              `json:"prefix,omitempty"`
	Cursor         string              `json:"cursor,omitempty"`
	Limit          int                 `json:"limit,omitempty"`
	Method         string              `json:"method"`
	Keys           []string            `json:"keys,omitempty"`
	Checks         []Check             `json:"checks,omitempty"`
	Transaction    *Transaction        `json:"transaction,omitempty"`
}
type rpcResponse struct {
	Analytics *analytics.Result `json:"analytics,omitempty"`
	Stamp     *readStamp        `json:"stamp,omitempty"`
	Next      string            `json:"next,omitempty"`
	Records   map[string]Record `json:"records,omitempty"`
	Error     string            `json:"error,omitempty"`
	Leader    *Member           `json:"leader,omitempty"`
}

// rpcHandler is reachable only through the status listener's mTLS boundary.
// Possession of an enrollment credential is not access to this metadata API.
func (n *Node) rpcHandler(w http.ResponseWriter, r *http.Request, app string) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" || len(r.Header.Values("X-Drop-Cluster")) != 1 || r.Header.Get("X-Drop-Cluster") != n.config.ClusterID || r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" || r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "invalid cluster request", 400)
		return
	}
	id := replica.PeerIdentity(r.TLS.PeerCertificates[0], app)
	var configuration raft.ConfigurationFuture
	if err := n.wait(r.Context(), func() raft.Future { configuration = n.raft.GetConfiguration(); return configuration }); err != nil {
		http.Error(w, "unavailable", 503)
		return
	}
	allowed := false
	for _, s := range configuration.Configuration().Servers {
		if string(s.ID) == id {
			allowed = true
		}
	}
	if !allowed {
		http.Error(w, "forbidden", 403)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxCommand+4096))
	var request rpcRequest
	if err != nil || decodeStrict(data, &request) != nil {
		http.Error(w, "invalid command", 400)
		return
	}
	isAnalytics := request.Method == "analytics_put" || request.Method == "analytics_query"
	if !isAnalytics && (request.Analytics != nil || request.AnalyticsQuery != nil) || isAnalytics && (request.Stamp != nil || request.Transaction != nil || len(request.Checks) != 0 || len(request.Keys) != 0 || request.Prefix != "" || request.Cursor != "" || request.Limit != 0) {
		http.Error(w, "invalid command", 400)
		return
	}
	// Followers return only an authenticated routing hint, never cached data.
	if n.raft.State() != raft.Leader && request.Method != "local_snapshot" {
		response := rpcResponse{Error: "not_leader"}
		address, _ := n.raft.LeaderWithID()
		if member, e := parseAddress(string(address)); e == nil {
			response.Leader = &member
		}
		_ = json.NewEncoder(w).Encode(response)
		return
	}
	var response rpcResponse
	if request.Method != "scan" && (request.Prefix != "" || request.Cursor != "" || request.Limit != 0) {
		http.Error(w, "invalid command", 400)
		return
	}
	if request.Stamp != nil && request.Method != "snapshot_read" && request.Method != "local_snapshot" {
		http.Error(w, "invalid command", 400)
		return
	}
	switch request.Method {
	case "analytics_put":
		if request.Analytics == nil || request.AnalyticsQuery != nil || request.Analytics.Node != id {
			err = ErrInvalid
		} else if n.analytics == nil {
			err = analytics.ErrUnavailable
		} else {
			// This is telemetry, not authorization: no Raft write or quorum fence.
			// Immutable cumulative producer snapshots merge safely across old/new leaders.
			err = analyticsError(n.analytics.Accept(r.Context(), *request.Analytics))
		}
	case "analytics_query":
		if request.AnalyticsQuery == nil || request.Analytics != nil {
			err = ErrInvalid
		} else if n.analytics == nil {
			err = analytics.ErrUnavailable
		} else {
			var result analytics.Result
			result, err = n.analytics.Query(r.Context(), *request.AnalyticsQuery)
			if err == nil {
				response.Analytics = &result
			}
			err = analyticsError(err)
		}

	case "local_snapshot":
		if request.Transaction != nil || len(request.Checks) != 0 {
			err = ErrInvalid
		} else {
			response.Records, response.Stamp, err = n.localSnapshot(r.Context(), request.Keys, request.Stamp)
		}
	case "snapshot_read":
		if request.Transaction != nil || len(request.Checks) != 0 {
			err = ErrInvalid
		} else {
			response.Records, response.Stamp, err = n.readSnapshot(r.Context(), request.Keys, request.Stamp)
		}
	case "scan":
		if request.Transaction != nil || len(request.Keys) != 0 || len(request.Checks) != 0 {
			err = ErrInvalid
		} else {
			var page kv.Page
			page, err = n.Scan(r.Context(), request.Prefix, request.Cursor, request.Limit)
			response.Records, response.Next = page.Records, page.Next
		}
	case "read":
		if request.Transaction != nil || len(request.Checks) != 0 {
			err = ErrInvalid
		} else {
			response.Records, err = n.Read(r.Context(), request.Keys)
		}
	case "check":
		if request.Transaction != nil || len(request.Keys) != 0 {
			err = ErrInvalid
		} else {
			err = n.Check(r.Context(), request.Checks)
		}
	case "commit":
		if request.Transaction == nil || len(request.Keys) != 0 || len(request.Checks) != 0 {
			err = ErrInvalid
		} else {
			err = n.Commit(r.Context(), *request.Transaction)
		}
	default:
		err = ErrInvalid
	}
	switch {
	case err == nil:
	case errors.Is(err, ErrNotLeader):
		response.Error = "not_leader"
	case errors.Is(err, ErrConflict):
		response.Error = "conflict"
	case errors.Is(err, ErrInvalid):
		response.Error = "invalid"
	case errors.Is(err, ErrCapacity):
		response.Error = "capacity"
	default:
		response.Error = "unavailable" // May have committed: never auto-replay.
	}
	_ = json.NewEncoder(w).Encode(response)
}

// Client routes via the local coordinator and follows at most one authenticated
// leader hint. There are no HTTP redirects, environment proxies or blind write
// retries after timeouts/leadership changes.
type Client struct {
	mu                 sync.Mutex
	peers              map[string]*http.Client
	leaderAddress      netip.AddrPort
	leaderID           string
	http               *http.Client
	local              netip.AddrPort
	id, app, clusterID string
	statusPort         uint16
	rpcCounts          map[string]uint64
}

func NewClient(c RuntimeConfig) (*Client, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	material, err := c.tlsMaterial()
	if err != nil {
		return nil, err
	}
	h, err := replica.NewPeerClient(material.Client)
	if err != nil {
		return nil, err
	}
	h.Timeout = 8 * time.Second
	address, _ := netip.ParseAddrPort(c.StatusListen)
	if address.Addr().IsUnspecified() {
		ip := netip.MustParseAddr("127.0.0.1")
		if address.Addr().Is6() {
			ip = netip.IPv6Loopback()
		}
		address = netip.AddrPortFrom(ip, address.Port())
	}
	return &Client{http: h, local: address, id: c.Local.ID, app: c.App, clusterID: c.ClusterID, statusPort: c.StatusPort}, nil
}
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.http.CloseIdleConnections()
	for _, peer := range c.peers {
		peer.CloseIdleConnections()
	}
}
func (c *Client) call(ctx context.Context, request rpcRequest) (rpcResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	data, err := json.Marshal(request)
	if err != nil || len(data) > maxCommand+4096 {
		return rpcResponse{}, ErrInvalid
	}
	address, id := c.local, c.id
	c.mu.Lock()
	if request.Method != "local_snapshot" && c.leaderID != "" {
		address, id = c.leaderAddress, c.leaderID
	}
	c.mu.Unlock()
	for hop := 0; hop < 2; hop++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+address.String()+metadataPath, bytes.NewReader(data))
		if err != nil {
			return rpcResponse{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Drop-Cluster", c.clusterID)
		c.mu.Lock()
		if c.rpcCounts == nil {
			c.rpcCounts = map[string]uint64{}
		}
		destination := "leader"
		if address == c.local {
			destination = "local"
		}
		c.rpcCounts[request.Method+"_"+destination]++
		c.mu.Unlock()
		httpClient := c.peerClient(address, id)
		response, err := httpClient.Do(req)
		if err != nil {
			c.mu.Lock()
			if request.Method != "local_snapshot" {
				c.leaderID = ""
			}
			c.mu.Unlock()
			return rpcResponse{}, err
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 9<<20))
		response.Body.Close()
		if readErr != nil || len(body) >= 9<<20 || response.StatusCode != 200 || response.TLS == nil || len(response.TLS.VerifiedChains) == 0 || len(response.TLS.PeerCertificates) == 0 || replica.PeerIdentity(response.TLS.PeerCertificates[0], c.app) != id {
			return rpcResponse{}, errors.New("untrusted or unavailable metadata response")
		}
		var result rpcResponse
		if decodeStrict(body, &result) != nil {
			return result, ErrInvalid
		}
		switch result.Error {
		case "":
			c.mu.Lock()
			if request.Method != "local_snapshot" {
				c.leaderAddress, c.leaderID = address, id
			}
			c.mu.Unlock()
			return result, nil
		case "conflict":
			return result, ErrConflict
		case "invalid":
			return result, ErrInvalid
		case "capacity":
			return result, ErrCapacity
		case "not_leader":
			if request.Method == "local_snapshot" {
				return result, ErrNotLeader
			}
			c.mu.Lock()
			if request.Method != "local_snapshot" {
				c.leaderID = ""
			}
			c.mu.Unlock()
			if hop == 0 && result.Leader == nil && address != c.local {
				address, id = c.local, c.id
				continue
			}
			if hop != 0 || result.Leader == nil || result.Leader.validate() != nil {
				return result, ErrNotLeader
			}
			ip, _ := netip.ParseAddrPort(result.Leader.Address)
			address = netip.AddrPortFrom(ip.Addr(), c.statusPort)
			id = result.Leader.ID
		default:
			c.mu.Lock()
			if request.Method != "local_snapshot" {
				c.leaderID = ""
			}
			c.mu.Unlock()
			return result, errors.New("metadata operation outcome unavailable")
		}
	}
	return rpcResponse{}, ErrNotLeader
}
func (c *Client) Read(ctx context.Context, keys []string) (map[string]Record, error) {
	r, e := c.call(ctx, rpcRequest{Method: "read", Keys: keys})
	return r.Records, e
}
func (c *Client) Check(ctx context.Context, checks []Check) error {
	_, e := c.call(ctx, rpcRequest{Method: "check", Checks: checks})
	return e
}
func (c *Client) Commit(ctx context.Context, t Transaction) error {
	_, e := c.call(ctx, rpcRequest{Method: "commit", Transaction: &t})
	return e
}

func (c *Client) Scan(ctx context.Context, prefix, cursor string, limit int) (kv.Page, error) {
	r, e := c.call(ctx, rpcRequest{Method: "scan", Prefix: prefix, Cursor: cursor, Limit: limit})
	return kv.Page{Records: r.Records, Next: r.Next}, e
}

// Pool by both destination and verified identity. TLS pinning remains checked
// before sending records; a certificate for another member cannot receive data.
func (c *Client) peerClient(address netip.AddrPort, id string) *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.peers == nil {
		c.peers = map[string]*http.Client{}
	}
	key := address.String() + "/" + id
	if client := c.peers[key]; client != nil {
		return client
	}
	transport := c.http.Transport.(*http.Transport).Clone()
	transport.TLSClientConfig = transport.TLSClientConfig.Clone()
	verify := transport.TLSClientConfig.VerifyConnection
	transport.TLSClientConfig.VerifyConnection = func(state tls.ConnectionState) error {
		if err := verify(state); err != nil {
			return err
		}
		if replica.PeerIdentity(state.PeerCertificates[0], c.app) != id {
			return errors.New("metadata destination identity mismatch")
		}
		return nil
	}
	transport.MaxIdleConnsPerHost = 50
	transport.MaxConnsPerHost = 64
	client := *c.http
	client.Transport = transport
	// Membership churn cannot grow the connection pool without bound.
	if len(c.peers) >= 32 {
		for key, old := range c.peers {
			old.CloseIdleConnections()
			delete(c.peers, key)
			break
		}
	}
	c.peers[key] = &client
	return &client
}

func (c *Client) RPCMetrics() map[string]uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]uint64{}
	for k, v := range c.rpcCounts {
		out[k] = v
	}
	return out
}

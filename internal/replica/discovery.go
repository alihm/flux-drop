// Package replica implements bounded, validated Flux replica discovery.
package replica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/netip"
	"regexp"
	"sort"
	"sync"
	"time"
)

const maxBody = 1 << 20
const maxRecords = 1000
const maxAge = 5 * time.Minute

var ErrDiscovery = errors.New("invalid replica discovery response")
var appName = regexp.MustCompile(`^[A-Za-z0-9]{1,64}$`)

type peer struct {
	address netip.AddrPort
	expires time.Time
}

type Discovery struct {
	app     string
	port    uint16
	self    map[netip.Addr]bool
	client  *http.Client
	now     func() time.Time
	refresh sync.Mutex
	mu      sync.RWMutex
	peers   []peer
	updated time.Time
}

func NewDiscovery(app string, port uint16, self []netip.Addr) (*Discovery, error) {
	if !appName.MatchString(app) || port == 0 {
		return nil, fmt.Errorf("invalid FLUX_APP_NAME or REPLICA_PORT")
	}
	d := &Discovery{app: app, port: port, self: make(map[netip.Addr]bool), now: time.Now}
	for _, ip := range self {
		if !ip.IsValid() || ip.Zone() != "" {
			return nil, fmt.Errorf("invalid replica self address")
		}
		d.self[ip.Unmap()] = true
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxResponseHeaderBytes = 16 << 10
	d.client = &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("discovery redirects are forbidden") }}
	return d, nil
}

// Refresh publishes a complete validated snapshot atomically. Failure retains
// the previous snapshot only until its maximum age or individual record expiry.
func (d *Discovery) Refresh(ctx context.Context) error {
	d.refresh.Lock()
	defer d.refresh.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.runonflux.io/apps/location/"+d.app, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	res, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("discovery HTTP status %d", res.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err != nil {
		return err
	}
	if len(body) > maxBody {
		return ErrDiscovery
	}
	now := d.now()
	peers, err := d.parse(body, now)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.peers = peers
	d.updated = now
	d.mu.Unlock()
	return nil
}

// Snapshot returns an independent list; fresh=false means callers must not
// interpret an empty list as authoritative absence. It never returns stale peers.
func (d *Discovery) Snapshot() (addresses []netip.AddrPort, fresh bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	now := d.now()
	if d.updated.IsZero() || now.Before(d.updated) || now.Sub(d.updated) >= maxAge {
		return nil, false
	}
	for _, p := range d.peers {
		if now.Before(p.expires) {
			addresses = append(addresses, p.address)
		}
	}
	return addresses, true
}

func (d *Discovery) Run(ctx context.Context) {
	defer d.client.CloseIdleConnections()
	for ctx.Err() == nil {
		if err := d.Refresh(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("replica discovery failed", "error", err)
		}
		timer := time.NewTimer(45*time.Second + time.Duration(rand.IntN(30001))*time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (d *Discovery) parse(body []byte, now time.Time) ([]peer, error) {
	var response struct {
		Status string `json:"status"`
		Data   []struct {
			IP      string    `json:"ip"`
			Name    string    `json:"name"`
			Expires time.Time `json:"expireAt"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &response) != nil || response.Status != "success" || response.Data == nil || len(response.Data) > maxRecords {
		return nil, ErrDiscovery
	}
	unique := make(map[netip.Addr]time.Time)
	for _, record := range response.Data {
		if record.Name != d.app || record.Expires.IsZero() {
			return nil, ErrDiscovery
		}
		ip, err := netip.ParseAddr(record.IP)
		if err != nil {
			address, parseErr := netip.ParseAddrPort(record.IP)
			if parseErr != nil || address.Port() == 0 {
				return nil, ErrDiscovery
			}
			ip = address.Addr()
		}
		ip = ip.Unmap()
		if !publicIP(ip) {
			return nil, ErrDiscovery
		}
		if d.self[ip] || !now.Before(record.Expires) {
			continue
		}
		// Earliest expiry wins for duplicate advertisements.
		if old, exists := unique[ip]; !exists || record.Expires.Before(old) {
			unique[ip] = record.Expires
		}
	}
	peers := make([]peer, 0, len(unique))
	for ip, expires := range unique {
		peers = append(peers, peer{netip.AddrPortFrom(ip, d.port), expires})
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].address.Compare(peers[j].address) < 0 })
	return peers, nil
}

// Conservative public-unicast policy: no loopback, LAN, metadata, documentation,
// shared-address space, or IPv6 translation/tunneling destinations.
var denied = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/3"),
	netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("3fff::/20"),
}
var ipv6Global = netip.MustParsePrefix("2000::/3")

func publicIP(ip netip.Addr) bool {
	if !ip.IsValid() || ip.Zone() != "" || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	if ip.Is6() && !ipv6Global.Contains(ip) {
		return false
	}
	for _, prefix := range denied {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}

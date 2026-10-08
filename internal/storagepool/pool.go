package storagepool

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/replica"
)

type peerDiscovery interface {
	Snapshot() ([]netip.AddrPort, bool)
	Run(context.Context)
}

type NodeHealth struct {
	Address   string    `json:"address"`
	CheckedAt time.Time `json:"checkedAt"`
	Healthy   bool      `json:"healthy"`
	Capacity  Capacity  `json:"capacity"`
}
type appRuntime struct {
	config    App
	discovery peerDiscovery
	client    *http.Client
	mu        sync.RWMutex
	health    map[netip.AddrPort]NodeHealth
}
type Pool struct {
	store     *metadata.Store
	config    Config
	apps      []*appRuntime
	cache     *fileCache
	downloads chan struct{}
	probes    chan struct{}
}

func NewPool(c Config, cacheRoot string) (*Pool, error) {
	if c.Role != "primary" {
		return nil, errors.New("primary role required")
	}
	p := &Pool{config: c, downloads: make(chan struct{}, 4), probes: make(chan struct{}, 8)}
	cache, err := newFileCache(cacheRoot, c.CacheBytes)
	if err != nil {
		return nil, err
	}
	p.cache = cache
	for _, app := range c.Apps {
		tlsConfig, err := clientTLS(app, c.AppName)
		if err != nil {
			p.Close()
			return nil, err
		}
		d, err := replica.NewDiscovery(app.AppName, app.Port, nil)
		if err != nil {
			p.Close()
			return nil, err
		}
		transport := &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig, DialContext: (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext, MaxConnsPerHost: 4, MaxIdleConns: 64, MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 15 * time.Second, MaxResponseHeaderBytes: 16 << 10, DisableCompression: true}
		client := &http.Client{Transport: transport, Timeout: 5 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("storage redirects forbidden") }}
		p.apps = append(p.apps, &appRuntime{config: app, discovery: d, client: client, health: map[netip.AddrPort]NodeHealth{}})
	}
	return p, nil
}
func (p *Pool) Close() {
	for _, a := range p.apps {
		a.client.CloseIdleConnections()
	}
	if p.cache != nil {
		p.cache.close()
	}
}
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, a := range p.apps {
		a := a
		wg.Add(2)
		go func() { defer wg.Done(); a.discovery.Run(ctx) }()
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				p.refresh(ctx, a)
				timer := time.NewTimer((12*time.Second + time.Duration(rand.IntN(6001))*time.Millisecond))
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	}
	wg.Wait()
}
func (p *Pool) refresh(ctx context.Context, a *appRuntime) {
	peers, fresh := a.discovery.Snapshot()
	if !fresh {
		return
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, addr := range peers {
		addr := addr
		select {
		case p.probes <- struct{}{}:
		case <-ctx.Done():
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-p.probes }()
			bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			health := NodeHealth{Address: addr.String(), CheckedAt: time.Now()}
			res, err := a.request(bounded, addr, "GET", apiPrefix+"capacity", nil, "")
			if err == nil {
				defer res.Body.Close()
				err = readResponse(res, &health.Capacity, 16<<10)
				health.Healthy = err == nil && health.Capacity.valid(a.config.AppName)
			}
			a.mu.Lock()
			a.health[addr] = health
			a.mu.Unlock()
		}()
	}
	wg.Wait()
	a.mu.Lock()
	for addr := range a.health {
		found := false
		for _, peer := range peers {
			if addr == peer {
				found = true
				break
			}
		}
		if !found {
			delete(a.health, addr)
		}
	}
	a.mu.Unlock()
}
func (c Capacity) valid(app string) bool {
	return idRE.MatchString(c.InstanceID) && c.AppName == app && c.CapacityBytes > Headroom && c.CapacityBytes <= 1<<40 && c.AvailableBytes >= 0 && c.ReservedBytes >= 0 && c.BlockBytes > 0 && c.BlockBytes <= 1<<20 && c.HeadroomBytes == Headroom && c.ReservedInodes <= 1<<40 && (!c.TracksInodes || c.FreeInodes <= c.TotalInodes)
}
func (a *appRuntime) nodes() []NodeHealth {
	peers, fresh := a.discovery.Snapshot()
	if !fresh {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	var nodes []NodeHealth
	for _, addr := range peers {
		n, ok := a.health[addr]
		if ok && n.Healthy && time.Since(n.CheckedAt) >= 0 && time.Since(n.CheckedAt) < 45*time.Second {
			nodes = append(nodes, n)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Capacity.AvailableBytes > nodes[j].Capacity.AvailableBytes })
	return nodes
}
func (p *Pool) Offers() []project.StorageOffer {
	controls, err := p.controls(context.Background())
	if err != nil {
		return nil
	}
	var offers []project.StorageOffer
	for _, a := range p.apps {
		if a.config.Drain || controls[a.config.AppName].Drain || controls[a.config.AppName].Removed {
			continue
		}
		nodes := a.nodes()
		writableNodes := nodes[:0]
		for _, n := range nodes {
			if n.Capacity.Writable {
				writableNodes = append(writableNodes, n)
			}
		}
		nodes = writableNodes
		if len(nodes) == 0 {
			continue
		}
		// A single healthy instance may acknowledge uploads. App-wide retained
		// accounting reserves room for replication even while other nodes are down.
		offer := project.StorageOffer{App: a.config.AppName, LimitBytes: 1 << 40, AvailableBytes: 1 << 62, BlockBytes: 1, LimitInodes: ^uint64(0), AvailableInodes: ^uint64(0)}
		for _, n := range nodes {
			c := n.Capacity
			if c.CapacityBytes-Headroom < offer.LimitBytes {
				offer.LimitBytes = c.CapacityBytes - Headroom
			}
			available := c.AvailableBytes - c.ReservedBytes - Headroom
			if available < offer.AvailableBytes {
				offer.AvailableBytes = available
			}
			if c.BlockBytes > offer.BlockBytes {
				offer.BlockBytes = c.BlockBytes
			}
			if c.TracksInodes {
				offer.TracksInodes = true
				limit := uint64(0)
				free := uint64(0)
				if c.TotalInodes > 1024 {
					limit = c.TotalInodes - 1024
				}
				if c.FreeInodes > 1024+c.ReservedInodes {
					free = c.FreeInodes - 1024 - c.ReservedInodes
				}
				if limit < offer.LimitInodes {
					offer.LimitInodes = limit
				}
				if free < offer.AvailableInodes {
					offer.AvailableInodes = free
				}
			}
		}
		offers = append(offers, offer)
	}
	sort.Slice(offers, func(i, j int) bool {
		if offers[i].AvailableBytes == offers[j].AvailableBytes {
			return offers[i].App < offers[j].App
		}
		return offers[i].AvailableBytes > offers[j].AvailableBytes
	})
	return offers
}
func (p *Pool) app(name string) *appRuntime {
	for _, a := range p.apps {
		if a.config.AppName == name {
			return a
		}
	}
	return nil
}
func (a *appRuntime) request(ctx context.Context, addr netip.AddrPort, method, path string, body io.Reader, contentType string) (*http.Response, error) {
	// Recheck the destination at request time. Never dial arbitrary metadata URLs.
	peers, fresh := a.discovery.Snapshot()
	found := false
	for _, peer := range peers {
		if peer == addr {
			found = true
			break
		}
	}
	if !fresh || !found {
		return nil, project.ErrStorage
	}
	req, err := http.NewRequestWithContext(ctx, method, (&url.URL{Scheme: "https", Host: addr.String(), Path: path}).String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.config.APIKey)
	req.Header.Set("X-Drop-Key-ID", a.config.KeyID)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return a.client.Do(req)
}
func readResponse(res *http.Response, value any, limit int64) error {
	if res.StatusCode != 200 {
		return fmt.Errorf("storage HTTP status %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > limit {
		return content.ErrLimit
	}
	return json.Unmarshal(data, value)
}
func (p *Pool) Install(ctx context.Context, prepared project.Prepared, staged *content.Staged) error {
	a := p.app(prepared.Project.StorageApp)
	if a == nil {
		return project.ErrStorage
	}
	op := Operation{ProjectID: prepared.Project.ID, Digest: staged.Digest, Slug: prepared.Project.InitialSlug, Bytes: prepared.Operation.Bytes, Files: len(staged.Manifest.Files), ExpiresAt: prepared.Operation.ExpiresAt}
	if op.Slug == "" {
		op.Slug = prepared.Project.Slug
	}
	raw, _ := json.Marshal(op)
	var last error = project.ErrStorage
	for _, node := range a.nodes() {
		if !node.Capacity.Writable {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		addr, err := netip.ParseAddrPort(node.Address)
		if err != nil {
			continue
		}
		res, err := a.request(ctx, addr, "PUT", apiPrefix+"operations/"+prepared.Operation.ID, strings.NewReader(string(raw)), "application/json")
		if err != nil {
			last = err
			continue
		}
		var receipt Operation
		err = readResponse(res, &receipt, 16<<10)
		res.Body.Close()
		if err != nil || !op.same(receipt) {
			last = project.ErrStorage
			continue
		}
		if receipt.State != "stored" {
			err = p.transfer(ctx, a, addr, prepared.Operation.ID, staged)
			if err != nil {
				last = err
				continue
			}
		}
		// The commit endpoint confirms that the exact installed tree is fsynced on
		// THIS instance. No wait for another instance or Syncthing propagation.
		res, err = a.request(ctx, addr, "POST", apiPrefix+"operations/"+prepared.Operation.ID+"/commit", nil, "")
		if err != nil {
			last = err
			continue
		}
		err = readResponse(res, &receipt, 16<<10)
		res.Body.Close()
		if err == nil && op.same(receipt) && receipt.State == "stored" {
			return nil
		}
		last = project.ErrStorage
	}
	return errors.Join(project.ErrStorage, last)
}
func (p *Pool) transfer(ctx context.Context, a *appRuntime, addr netip.AddrPort, id string, staged *content.Staged) error {
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() { err := writeZIP(writer, staged); _ = writer.CloseWithError(err); done <- err }()
	res, err := a.request(ctx, addr, "PUT", apiPrefix+"operations/"+id+"/content", reader, "application/zip")
	_ = reader.CloseWithError(errors.New("transfer finished"))
	zipErr := <-done
	if err != nil {
		return err
	}
	defer res.Body.Close()
	var receipt Operation
	if err = readResponse(res, &receipt, 16<<10); err != nil {
		return err
	}
	if zipErr != nil {
		return zipErr
	}
	if receipt.State != "stored" || receipt.Digest != staged.Digest {
		return project.ErrStorage
	}
	return nil
}
func writeZIP(w io.Writer, staged *content.Staged) error {
	root, err := os.OpenRoot(staged.Directory)
	if err != nil {
		return err
	}
	defer root.Close()
	z := zip.NewWriter(w)
	for _, entry := range staged.Manifest.Files {
		f, err := root.Open("public/" + entry.Path)
		if err != nil {
			return err
		}
		out, err := z.CreateHeader(&zip.FileHeader{Name: entry.Path, Method: zip.Store})
		if err == nil {
			_, err = io.Copy(out, io.LimitReader(f, entry.Size+1))
		}
		f.Close()
		if err != nil {
			return err
		}
	}
	return z.Close()
}
func (p *Pool) AdminHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !p.adminAuthorized(r) {
			http.NotFound(w, r)
			return
		}
		result := p.appStatuses()
		jsonReply(w, 200, map[string]any{"apps": result})
	})
}
func (p *Pool) adminAuthorized(r *http.Request) bool {
	if p.config.AdminKey == "" || len(r.Header.Values("Authorization")) != 1 {
		return false
	}
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return false
	}
	a, b := sha256.Sum256([]byte(got)), sha256.Sum256([]byte(p.config.AdminKey))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

type appStatus struct {
	AppName        string       `json:"appName"`
	Drain          bool         `json:"drain"`
	DiscoveryFresh bool         `json:"discoveryFresh"`
	Nodes          []NodeHealth `json:"nodes"`
}

func (p *Pool) appStatuses() []appStatus {
	result := []appStatus{}
	for _, a := range p.apps {
		peers, fresh := a.discovery.Snapshot()
		status := appStatus{AppName: a.config.AppName, Drain: a.config.Drain, DiscoveryFresh: fresh, Nodes: []NodeHealth{}}
		a.mu.RLock()
		for _, peer := range peers {
			node, ok := a.health[peer]
			if !ok {
				node = NodeHealth{Address: peer.String()}
			}
			if !fresh || time.Since(node.CheckedAt) < 0 || time.Since(node.CheckedAt) >= 45*time.Second {
				node.Healthy = false
			}
			status.Nodes = append(status.Nodes, node)
		}
		a.mu.RUnlock()
		result = append(result, status)
	}
	return result
}

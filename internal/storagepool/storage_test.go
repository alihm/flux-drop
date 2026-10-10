package storagepool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"golang.org/x/sys/unix"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/httpserver"
	"github.com/runonflux/flux-drop/internal/project"
)

type testDiscovery struct {
	mu    sync.Mutex
	peers []netip.AddrPort
	fresh bool
}

func (d *testDiscovery) Snapshot() ([]netip.AddrPort, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]netip.AddrPort(nil), d.peers...), d.fresh
}
func (d *testDiscovery) Run(ctx context.Context) { <-ctx.Done() }
func (d *testDiscovery) set(peers []netip.AddrPort, fresh bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.peers = peers
	d.fresh = fresh
}

const testKey = "only-for-tests-32-byte-key-not-deployed"

func secondaryConfig() Config {
	return Config{Role: "secondary", AppName: "storagea", PrimaryApp: "primarya", Capacity: 4 << 30, Port: DefaultPort, Keys: map[string]string{"v1": testKey}}
}
func fixture(t *testing.T, root string, wrap func(http.Handler) http.Handler) (*Secondary, *httptest.Server, *Pool) {
	t.Helper()
	c := secondaryConfig()
	s, err := NewSecondary(c, root, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.primary = &testDiscovery{peers: []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:1")}, fresh: true}
	handler := s.Handler()
	if wrap != nil {
		handler = wrap(handler)
	}
	server := httptest.NewUnstartedServer(handler)
	tlsConfig, err := serverTLS(c)
	if err != nil {
		t.Fatal(err)
	}
	server.TLS = tlsConfig
	server.StartTLS()
	port := uint16(server.Listener.Addr().(*net.TCPAddr).Port)
	config := Config{Role: "primary", AppName: c.PrimaryApp, Apps: []App{{AppName: c.AppName, Port: port, KeyID: "v1", APIKey: testKey}}, CacheBytes: 2 << 20, AdminKey: testKey}
	p, err := NewPool(config, filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	addr := netip.MustParseAddrPort(server.Listener.Addr().String())
	p.apps[0].discovery = &testDiscovery{peers: []netip.AddrPort{addr}, fresh: true}
	p.refresh(context.Background(), p.apps[0])
	if len(p.Offers()) != 1 {
		t.Fatal("no healthy storage offer", p.apps[0].nodes())
	}
	t.Cleanup(func() { server.Close(); p.Close(); s.Close() })
	return s, server, p
}
func stagedHTML(t *testing.T, html string) *content.Staged {
	t.Helper()
	s, err := content.StageHTML(t.TempDir(), strings.NewReader(html), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Discard() })
	return s
}
func prepared(staged *content.Staged) project.Prepared {
	return project.Prepared{Project: project.Project{ID: strings.Repeat("a", 32), Slug: "hello-" + staged.Digest[:6], StorageApp: "storagea", ActiveDigest: staged.Digest, Status: "active", Revision: 1, PolicyRevision: 1}, Operation: project.Operation{ID: strings.Repeat("b", 64), Bytes: staged.Manifest.Files[0].Size, ExpiresAt: time.Now().Add(15 * time.Minute)}}
}
func TestRoleConfiguration(t *testing.T) {
	base := map[string]string{"DROP_ROLE": "secondary", "FLUX_APP_NAME": "storagea", "DROP_PRIMARY_APP_NAME": "primarya", "DROP_STORAGE_CAPACITY_BYTES": "4294967296", "DROP_STORAGE_API_KEYS_JSON": `{"v1":"` + testKey + `"}`}
	valid := func(values map[string]string) (Config, error) {
		return FromEnv(func(k string) string { return values[k] })
	}
	if _, err := valid(base); err != nil {
		t.Fatal(err)
	}
	for _, change := range []map[string]string{{"DROP_ROLE": "bad"}, {"DROP_STORAGE_CAPACITY_BYTES": "0"}, {"DROP_PRIMARY_APP_NAME": "storagea"}, {"DROP_STORAGE_API_KEYS_JSON": `{"v1":"weak"}`}, {"DROP_STORAGE_PORT": "8081"}, {"DROP_CACHE_BYTES": "10"}} {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range change {
			m[k] = v
		}
		if _, err := valid(m); err == nil {
			t.Fatal("accepted invalid configuration", change)
		}
	}
	primary := map[string]string{"DROP_ROLE": "primary", "FLUX_APP_NAME": "primarya", "DROP_STORAGE_APPS_JSON": `[{"appName":"storagea","keyId":"v1","apiKey":"` + testKey + `"}]`}
	c, err := valid(primary)
	if err != nil || c.Apps[0].Port != DefaultPort {
		t.Fatal(c, err)
	}
	primary["DROP_STORAGE_APPS_JSON"] = `[{"appName":"storagea","keyId":"v1","apiKey":"` + testKey + `"},{"appName":"storagea","keyId":"v1","apiKey":"` + testKey + `"}]`
	if _, err := valid(primary); err == nil {
		t.Fatal("accepted duplicate app")
	}
}
func TestTLSIdentityAndRotation(t *testing.T) {
	a := App{AppName: "storagea", KeyID: "v1", APIKey: testKey}
	c := secondaryConfig()
	c.Keys["v2"] = testKey + "rotated"
	tlsConfig, err := serverTLS(c)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	server.TLS = tlsConfig
	server.StartTLS()
	defer server.Close()
	for _, test := range []struct {
		app     App
		primary string
		success bool
	}{{a, "primarya", true}, {App{AppName: "storagea", KeyID: "v2", APIKey: testKey + "rotated"}, "primarya", true}, {a, "wrongprimary", false}, {App{AppName: "storagea", KeyID: "v1", APIKey: testKey + "wrong"}, "primarya", false}, {App{AppName: "storageb", KeyID: "v1", APIKey: testKey}, "primarya", false}} {
		tlsClient, err := clientTLS(test.app, test.primary)
		if err != nil {
			t.Fatal(err)
		}
		transport := &http.Transport{TLSClientConfig: tlsClient}
		client := &http.Client{Transport: transport, Timeout: time.Second}
		res, err := client.Get(server.URL)
		transport.CloseIdleConnections()
		if res != nil {
			res.Body.Close()
		}
		if (err == nil) != test.success {
			t.Fatal("TLS identity isolation failed", test.primary, test.app.AppName, err)
		}
	}
}
func TestSecondaryAPIKeyIPAndPublicBoundary(t *testing.T) {
	s, _, p := fixture(t, t.TempDir(), nil)
	a := p.apps[0]
	addr, _ := netip.ParseAddrPort(a.nodes()[0].Address)
	call := func(headers map[string]string) int {
		req, _ := http.NewRequest("GET", "https://"+addr.String()+apiPrefix+"capacity", nil)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		res, err := a.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		return res.StatusCode
	}
	auth := map[string]string{"Authorization": "Bearer " + testKey, "X-Drop-Key-ID": "v1"}
	if call(auth) != 200 || call(map[string]string{"Authorization": "Bearer wrong", "X-Drop-Key-ID": "v1"}) != 403 {
		t.Fatal("API key boundary")
	}
	s.primary.(*testDiscovery).set([]netip.AddrPort{netip.MustParseAddrPort("8.8.8.8:1")}, true)
	auth["X-Forwarded-For"] = "8.8.8.8"
	if call(auth) != 403 {
		t.Fatal("spoofed IP accepted")
	}
	s.primary.(*testDiscovery).set(nil, false)
	if call(auth) != 403 {
		t.Fatal("stale discovery accepted")
	}
	for _, path := range []string{"/", "/api/projects", apiPrefix + "capacity", "/hello-abcdef/"} {
		rec := httptest.NewRecorder()
		s.PublicHandler().ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != 404 {
			t.Fatal("secondary exposed public route", path, rec.Code)
		}
	}
}
func TestUploadRetryAfterLostCommitResponse(t *testing.T) {
	var lose atomic.Bool
	lose.Store(true)
	var uploads atomic.Int64
	s, _, p := fixture(t, t.TempDir(), func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/content") {
				uploads.Add(1)
			}
			if strings.HasSuffix(r.URL.Path, "/commit") && lose.Swap(false) {
				h, ok := w.(http.Hijacker)
				if !ok {
					t.Fatal("no hijacker")
				}
				conn, _, _ := h.Hijack()
				conn.Close()
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	staged := stagedHTML(t, "<h1>durable</h1>")
	pr := prepared(staged)
	if err := p.Install(context.Background(), pr, staged); err == nil {
		t.Fatal("lost commit incorrectly reported success")
	}
	if err := p.Install(context.Background(), pr, staged); err != nil {
		t.Fatal("retry failed", err)
	}
	if uploads.Load() != 1 {
		t.Fatal("retry unnecessarily retransferred", uploads.Load())
	}
	if _, err := content.VerifyVersion(s.version(pr.Project.ID, staged.Digest), staged.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(p.cache.root, "projects")); !os.IsNotExist(err) {
		t.Fatal("primary installed content locally")
	}
	// An operation's identity cannot be reused with different bytes.
	other := stagedHTML(t, "<h1>different</h1>")
	changed := pr
	changed.Operation.Bytes = other.Manifest.Files[0].Size
	if err := p.Install(context.Background(), changed, other); err == nil {
		t.Fatal("operation identity reused")
	}
}
func TestRejectMismatchedContentAndCancellation(t *testing.T) {
	_, _, p := fixture(t, t.TempDir(), nil)
	a := p.apps[0]
	addr, _ := netip.ParseAddrPort(a.nodes()[0].Address)
	staged := stagedHTML(t, "correct")
	pr := prepared(staged)
	op := Operation{ProjectID: pr.Project.ID, Digest: staged.Digest, Slug: pr.Project.Slug, Bytes: 7, Files: 1, ExpiresAt: pr.Operation.ExpiresAt}
	raw, _ := json.Marshal(op)
	res, err := a.request(context.Background(), addr, "PUT", apiPrefix+"operations/"+pr.Operation.ID, bytes.NewReader(raw), "application/json")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatal(res.StatusCode)
	}
	bad := stagedHTML(t, "wrong")
	if err := p.transfer(context.Background(), a, addr, pr.Operation.ID, bad); err == nil {
		t.Fatal("mismatched bytes accepted")
	}
	res, err = a.request(context.Background(), addr, "DELETE", apiPrefix+"operations/"+pr.Operation.ID, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatal(res.StatusCode)
	}
	if err := p.transfer(context.Background(), a, addr, pr.Operation.ID, staged); err == nil {
		t.Fatal("canceled upload accepted")
	}
}

type deliveryRepository struct {
	mu    sync.Mutex
	value project.Project
	err   error
}

func (d *deliveryRepository) Resolve(context.Context, string) (project.Project, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.value, d.err
}
func TestCacheRespectsCurrentPolicyDeletionAndCorruption(t *testing.T) {
	s, _, p := fixture(t, t.TempDir(), nil)
	staged := stagedHTML(t, "<h1>cached</h1>")
	pr := prepared(staged)
	if err := p.Install(context.Background(), pr, staged); err != nil {
		t.Fatal(err)
	}
	repository := &deliveryRepository{value: pr.Project}
	handler := httpserver.ProjectDeliveryWithAccess(repository, t.TempDir(), p, func(r *http.Request, pr project.Project) bool { return r.Header.Get("Test-Grant") == "valid" })
	call := func(grant bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/"+pr.Project.Slug+"/", nil)
		if grant {
			req.Header.Set("Test-Grant", "valid")
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := call(false); rec.Code != 200 || (!strings.HasPrefix(rec.Body.String(), "<h1>cached</h1>") || !strings.Contains(rec.Body.String(), `data-drop-watermark="runonflux"`)) {
		t.Fatal(rec.Code, rec.Body.String())
	}
	if p.cache.used == 0 {
		t.Fatal("response not cached")
	}
	// Storage disappearance still permits verified cached delivery under current policy.
	if err := os.RemoveAll(s.version(pr.Project.ID, staged.Digest)); err != nil {
		t.Fatal(err)
	}
	if rec := call(false); rec.Code != 200 {
		t.Fatal("cache missed", rec.Code)
	}
	repository.value.Private = true
	repository.value.PolicyRevision++
	if rec := call(false); rec.Code != 404 {
		t.Fatal("cache bypassed private access", rec.Code)
	}
	if rec := call(true); rec.Code != 200 || rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("private cache delivery", rec.Code)
	}
	// Tampering with local cached bytes cannot produce an authenticated response.
	p.cache.mu.Lock()
	for _, entry := range p.cache.entries {
		if err := os.WriteFile(entry.path, []byte("corrupted bytes"), 0600); err != nil {
			t.Fatal(err)
		}
		// Invalidation uses size/mtime; explicitly change mtime on filesystems
		// whose timestamp resolution can coalesce consecutive same-size writes.
		changed := time.Now().Add(time.Second)
		if err := os.Chtimes(entry.path, changed, changed); err != nil {
			t.Fatal(err)
		}
	}
	p.cache.mu.Unlock()
	if rec := call(true); rec.Code != 503 {
		t.Fatal("corrupted cache served", rec.Code)
	}
	repository.err = project.ErrNotFound
	if rec := call(true); rec.Code != 404 {
		t.Fatal("deleted project served", rec.Code)
	}
}
func TestPartialReplicationAndReplicaReadFailover(t *testing.T) {
	root1, root2 := t.TempDir(), t.TempDir()
	s1, _, p := fixture(t, root1, nil)
	s2, server2, _ := fixture(t, root2, nil)
	staged := stagedHTML(t, "replicated bytes")
	pr := prepared(staged)
	if err := p.Install(context.Background(), pr, staged); err != nil {
		t.Fatal(err)
	}
	addr2 := netip.MustParseAddrPort(server2.Listener.Addr().String())
	a := p.apps[0]
	first := a.nodes()[0]
	addr1, _ := netip.ParseAddrPort(first.Address)
	a.discovery.(*testDiscovery).set([]netip.AddrPort{addr1, addr2}, true)
	// Simulate an incomplete Syncthing tree on instance two. It must not be served.
	dir := s2.version(pr.Project.ID, staged.Digest)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	manifest, _ := json.Marshal(staged.Manifest)
	os.WriteFile(filepath.Join(dir, "manifest.json"), manifest, 0600)
	p.refresh(context.Background(), a)
	if err := os.RemoveAll(s1.version(pr.Project.ID, staged.Digest)); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	p.ServeProject(rec, httptest.NewRequest("GET", "/", nil), pr.Project, "index.html")
	if rec.Code != 503 {
		t.Fatal("partial replica served", rec.Code)
	}
	os.RemoveAll(dir)
	copy := stagedHTML(t, "replicated bytes")
	if err := copy.Install(root2, pr.Project.ID, pr.Project.Slug); err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	p.ServeProject(rec, httptest.NewRequest("GET", "/", nil), pr.Project, "index.html")
	if rec.Code != 200 || rec.Body.String() != "replicated bytes" {
		t.Fatal("replica failover failed", rec.Code, rec.Body.String())
	}
}
func TestCacheEvictionAndPins(t *testing.T) {
	c, err := newFileCache(filepath.Join(t.TempDir(), "cache"), 12<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	put := func(key string, size int64) func() {
		f, err := os.CreateTemp(c.root, "cached-")
		if err != nil {
			t.Fatal(err)
		}
		f.Write(bytes.Repeat([]byte("x"), int(size)))
		return c.put(key, f, content.File{Path: key, Size: size})
	}
	one := put("one", 6<<10)
	two := put("two", 6<<10)
	if c.used != 6<<10 || len(c.entries) != 1 {
		t.Fatal("pinned entry evicted or limit exceeded")
	}
	two()
	one()
	three := put("three", 6<<10)
	three()
	if c.used != 6<<10 || c.entries["one"] != nil {
		t.Fatal("eviction failed")
	}
}
func TestSecondaryRestartAndIdentityFence(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	c := secondaryConfig()
	s, err := NewSecondary(c, root, state)
	if err != nil {
		t.Fatal(err)
	}
	o := Operation{ProjectID: strings.Repeat("a", 32), Digest: strings.Repeat("b", 64), Slug: "test-abcdef", Bytes: 1, Files: 1, ExpiresAt: time.Now().Add(time.Minute), State: "reserved"}
	id := strings.Repeat("c", 64)
	if err := s.save(id, o); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = NewSecondary(c, root, state)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.read(id)
	if err != nil || !o.same(got) {
		t.Fatal("reservation lost across restart", got, err)
	}
	s.Close()
	c.PrimaryApp = "otherprimary"
	if _, err := NewSecondary(c, root, state); err == nil {
		t.Fatal("secondary silently reassigned")
	}
}
func TestAdminRequiresDedicatedKeyAndHidesSecrets(t *testing.T) {
	_, _, p := fixture(t, t.TempDir(), nil)
	for _, auth := range []string{"", "Bearer wrong", "Bearer " + testKey} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/storage/apps", nil)
		req.Header.Set("Authorization", auth)
		p.AdminHandler().ServeHTTP(rec, req)
		if auth == "Bearer "+testKey {
			if rec.Code != 200 || strings.Contains(rec.Body.String(), testKey) {
				t.Fatal("admin status leaked key")
			}
		} else if rec.Code != 404 {
			t.Fatal("unauthorized admin access")
		}
	}
}

func TestRevocationDuringSlowFetchIsRechecked(t *testing.T) {
	staged := stagedHTML(t, "must not leak")
	pr := prepared(staged)
	repository := &deliveryRepository{value: pr.Project}
	_, _, p := fixture(t, t.TempDir(), func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/files/") {
				repository.mu.Lock()
				repository.value.Private = true
				repository.value.PolicyRevision++
				repository.mu.Unlock()
			}
			next.ServeHTTP(w, r)
		})
	})
	if err := p.Install(context.Background(), pr, staged); err != nil {
		t.Fatal(err)
	}
	handler := httpserver.ProjectDeliveryWithAccess(repository, t.TempDir(), p, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/"+pr.Project.Slug+"/", nil))
	if rec.Code != 404 || strings.Contains(rec.Body.String(), "must not leak") {
		t.Fatal("policy changed during fetch but bytes were sent", rec.Code, rec.Body.String())
	}
}
func TestSecondaryDiskAndInodeExhaustion(t *testing.T) {
	s, _, p := fixture(t, t.TempDir(), nil)
	a := p.apps[0]
	addr, _ := netip.ParseAddrPort(a.nodes()[0].Address)
	staged := stagedHTML(t, "x")
	pr := prepared(staged)
	op := Operation{ProjectID: pr.Project.ID, Digest: staged.Digest, Slug: pr.Project.Slug, Bytes: 1, Files: 1, ExpiresAt: pr.Operation.ExpiresAt}
	raw, _ := json.Marshal(op)
	for _, space := range []unix.Statfs_t{{Bsize: 4096, Bavail: 1, Files: 10000, Ffree: 10000}, {Bsize: 4096, Bavail: 1 << 25, Files: 10000, Ffree: 1}} {
		s.mu.Lock()
		s.probe = func(_ string, out *unix.Statfs_t) error { *out = space; return nil }
		s.mu.Unlock()
		res, err := a.request(context.Background(), addr, "PUT", apiPrefix+"operations/"+pr.Operation.ID, bytes.NewReader(raw), "application/json")
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 503 {
			t.Fatal("exhausted disk admitted upload", res.StatusCode)
		}
	}
	if _, err := s.read(pr.Operation.ID); !errors.Is(err, errMissing) {
		t.Fatal("failed admission created reservation", err)
	}
}
func TestInventoryPagingAndReplicaObservation(t *testing.T) {
	s, _, p := fixture(t, t.TempDir(), nil)
	projectID := strings.Repeat("a", 32)
	for _, ch := range []string{"a", "b", "c"} {
		os.MkdirAll(s.version(projectID, strings.Repeat(ch, 64)), 0700)
	}
	a := p.apps[0]
	addr, _ := netip.ParseAddrPort(a.nodes()[0].Address)
	get := func(path string) ([]InventoryEntry, string) {
		res, err := a.request(context.Background(), addr, "GET", path, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var page struct {
			Versions []InventoryEntry `json:"versions"`
			Next     string           `json:"nextCursor"`
		}
		if err = readResponse(res, &page, 16<<10); err != nil {
			t.Fatal(err)
		}
		return page.Versions, page.Next
	}
	// appRuntime.request builds a canonical Path without a query; exercise the
	// authenticated endpoint directly to supply the administrative query string.
	req, _ := http.NewRequest("GET", "https://"+addr.String()+apiPrefix+"inventory?limit=2", nil)
	req.Header.Set("Authorization", "Bearer "+testKey)
	req.Header.Set("X-Drop-Key-ID", "v1")
	res, err := a.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		Versions []InventoryEntry `json:"versions"`
		Next     string           `json:"nextCursor"`
	}
	err = readResponse(res, &page, 16<<10)
	res.Body.Close()
	if err != nil || len(page.Versions) != 2 || page.Next == "" {
		t.Fatal(page, err)
	}
	req, _ = http.NewRequest("GET", "https://"+addr.String()+apiPrefix+"inventory?limit=2&cursor="+page.Next, nil)
	req.Header.Set("Authorization", "Bearer "+testKey)
	req.Header.Set("X-Drop-Key-ID", "v1")
	res, err = a.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	err = readResponse(res, &page, 16<<10)
	res.Body.Close()
	if err != nil || len(page.Versions) != 1 || page.Versions[0].Digest != strings.Repeat("c", 64) || page.Next != "" {
		t.Fatal(page, err)
	}
	rows, _ := get(apiPrefix + "inventory")
	if len(rows) != 3 {
		t.Fatal(rows)
	}
}

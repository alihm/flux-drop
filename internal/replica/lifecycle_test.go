package replica

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/httpserver"
	"github.com/runonflux/flux-drop/internal/project"
)

func runtimeCredentials(t *testing.T, cert tls.Certificate) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{"peer.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), "peer.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), "ca.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[1]})}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(dir, "peer.crt"), filepath.Join(dir, "peer.key"), filepath.Join(dir, "ca.crt")
}

func TestMultiInstanceRuntimeLifecycle(t *testing.T) {
	_, issue := peerCertificates(t)
	const body = "<!doctype html><h1>replicated</h1>"
	rootA, rootB := t.TempDir(), t.TempDir()
	staged, err := content.StageHTML(rootB, strings.NewReader(body), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	p := project.Project{ID: strings.Repeat("a", 32), Slug: "runtime-" + staged.Digest[:6], ActiveDigest: staged.Digest, Status: "active", PolicyRevision: 1}
	repo := &localResolver{p: p}
	var mu sync.Mutex
	routes := map[string]string{}
	start := func(instance, ip, root string) *Runtime {
		t.Helper()
		cert, key, ca := runtimeCredentials(t, issue("drop", instance))
		cfg := RuntimeConfig{App: "drop", Instance: instance, DataRoot: root, Certificate: cert, Key: key, CA: ca, Port: 8444, Self: []netip.Addr{netip.MustParseAddr(ip)}}
		r, err := startRuntime(context.Background(), cfg, repo, runtimeHooks{
			listen: func(string, string) (net.Listener, error) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err == nil {
					mu.Lock()
					routes[ip+":8444"] = listener.Addr().String()
					mu.Unlock()
				}
				return listener, err
			},
			configure: func(d *Discovery, f *Fallback) {
				d.client.Transport = roundTrip(func(req *http.Request) (*http.Response, error) {
					expires := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
					data := fmt.Sprintf(`{"status":"success","data":[{"name":"drop","ip":"91.192.45.220:16127","expireAt":%q},{"name":"drop","ip":"74.103.5.187:16187","expireAt":%q}]}`, expires, expires)
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(data)), Header: make(http.Header)}, nil
				})
				f.client.Transport.(*http.Transport).DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
					mu.Lock()
					local, ok := routes[address]
					mu.Unlock()
					if !ok {
						return nil, fmt.Errorf("unmapped test peer")
					}
					return (&net.Dialer{}).DialContext(ctx, network, local)
				}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := r.Close(ctx); err != nil {
				t.Error(err)
			}
		})
		return r
	}
	a := start("one", "91.192.45.220", rootA)
	b := start("two", "74.103.5.187", rootB)
	// Deterministic readiness: a refresh serializes with the background worker.
	if err := a.Discovery.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Discovery.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Ingress deliberately keeps a stale public snapshot when peer policy changes.
	ingressRepo := &localResolver{p: p}
	front := httptest.NewServer(httpserver.ProjectDeliveryWithFallback(ingressRepo, rootA, a.Fallback))
	defer front.Close()
	check := func(want int, wantBody string) {
		t.Helper()
		req, _ := http.NewRequest("GET", front.URL+"/"+p.Slug+"/", nil)
		req.Header.Set("Cookie", "secret=private")
		req.Header.Set("Authorization", "Bearer private")
		res, err := front.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != want || (wantBody != "" && string(data) != wantBody) {
			t.Fatalf("status=%d body=%q", res.StatusCode, data)
		}
	}
	check(503, "") // Neither replica has an installed version; no recursive lookup.
	if err := staged.Install(rootB, p.ID, p.Slug); err != nil {
		t.Fatal(err)
	}
	check(200, body) // A remains empty; B supplies the bytes over actual mTLS.
	index := filepath.Join(rootB, "projects", p.ID, "versions", p.ActiveDigest, "public", "index.html")
	held := filepath.Join(rootB, "held-index.html")
	if err := os.Rename(index, held); err != nil {
		t.Fatal(err)
	}
	check(503, "") // Metadata/manifest arrived, but replication lacks a file.
	if err := os.Rename(held, index); err != nil {
		t.Fatal(err)
	}
	check(200, body)
	if _, err := os.Stat(filepath.Join(rootA, "projects", p.ID)); !os.IsNotExist(err) {
		t.Fatal("fallback cached project files", err)
	}
	repo.Lock()
	repo.p.Private = true
	repo.p.PolicyRevision++
	repo.Unlock()
	check(404, "")
	repo.Lock()
	repo.p.Private = false // Still a new policy revision: do not serve stale binding.
	repo.Unlock()
	check(503, "")
	repo.Lock()
	repo.p = p
	repo.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-b.Errors():
		if err != http.ErrServerClosed {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not stop")
	}
	check(503, "") // Discovery still advertises B, but its listener is gone.
	mu.Lock()
	address := routes["74.103.5.187:8444"]
	mu.Unlock()
	if conn, err := net.DialTimeout("tcp", address, time.Second); err == nil {
		conn.Close()
		t.Fatal("peer listener leaked")
	}
}

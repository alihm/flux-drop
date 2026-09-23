package replica

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
)

type peers struct {
	addresses []netip.AddrPort
	fresh     bool
}

func (p peers) Snapshot() ([]netip.AddrPort, bool) { return p.addresses, p.fresh }

func TestFallbackRealTLS(t *testing.T) {
	root := t.TempDir()
	s, err := content.StageHTML(root, strings.NewReader("<h1>remote</h1>"), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	p := project.Project{ID: strings.Repeat("a", 32), Slug: "remote-" + s.Digest[:6], ActiveDigest: s.Digest, Status: "active", PolicyRevision: 1}
	if err := s.Install(root, p.ID, p.Slug); err != nil {
		t.Fatal(err)
	}
	roots, issue := peerCertificates(t)
	cfg, _, _ := PeerTLS("drop", "one", issue("drop", "one"), roots)
	_, serverCfg, _ := PeerTLS("drop", "two", issue("drop", "two"), roots)
	server := httptest.NewUnstartedServer(LocalDelivery("drop", &localResolver{p: p}, root))
	server.TLS = serverCfg
	server.StartTLS()
	defer server.Close()
	f, err := NewFallback(peers{[]netip.AddrPort{netip.MustParseAddrPort("91.192.45.220:8444")}, true}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// Test-only dial mapping: production destinations remain discovery IPs.
	f.client.Transport.(*http.Transport).DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	for _, method := range []string{"GET", "HEAD"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, "https://drop.example/"+p.Slug+"/", nil)
		r.Header.Set("Cookie", "secret=not-forwarded")
		r.Header.Set("Authorization", "Bearer secret")
		r.Header.Set("Range", "bytes=0-2")
		f.ServeProject(w, r, p, "index.html")
		if w.Code != 200 || w.Header().Get("Access-Control-Allow-Origin") != "*" {
			t.Fatalf("%d %v", w.Code, w.Header())
		}
		if method == "GET" && w.Body.String() != "<h1>remote</h1>" {
			t.Fatal(w.Body.String())
		}
		if method == "HEAD" && w.Body.Len() != 0 {
			t.Fatal("HEAD body")
		}
	}
}

func TestFallbackFailureSelection(t *testing.T) {
	p := project.Project{Slug: "site-abcdef", ActiveDigest: strings.Repeat("a", 64), Status: "active", PolicyRevision: 1}
	addresses := []netip.AddrPort{netip.MustParseAddrPort("91.192.45.220:8444"), netip.MustParseAddrPort("74.103.5.187:8444")}
	for _, tc := range []struct {
		name  string
		codes []int
		fresh bool
		want  int
	}{
		{"all absent", []int{404, 404}, true, 404}, {"unreachable", []int{404, 503}, true, 503}, {"stale", nil, false, 503}, {"redirect", []int{302, 302}, true, 503}, {"wrong binding", []int{200, 200}, true, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			f := &Fallback{source: peers{addresses, tc.fresh}, slots: make(chan struct{}, 8), client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
				code := tc.codes[calls]
				calls++
				return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("unsafe")), ContentLength: 6, Header: http.Header{"Set-Cookie": []string{"bad=1"}, "X-Accel-Redirect": []string{"/unsafe"}}}, nil
			})}}
			w := httptest.NewRecorder()
			f.ServeProject(w, httptest.NewRequest("GET", "/", nil), p, "index.html")
			if w.Code != tc.want || w.Header().Get("Set-Cookie") != "" || w.Header().Get("X-Accel-Redirect") != "" {
				t.Fatal(w.Code, w.Header())
			}
			if !tc.fresh && calls != 0 {
				t.Fatal("stale peers contacted")
			}
		})
	}
}

func TestFallbackRejectsTamperedPeerBytes(t *testing.T) {
	root := t.TempDir()
	staged, err := content.StageHTML(root, strings.NewReader("good"), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Discard()
	manifest, err := os.ReadFile(filepath.Join(staged.Directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	p := project.Project{Slug: "site-" + staged.Digest[:6], ActiveDigest: staged.Digest, Status: "active", PolicyRevision: 1}
	address := netip.MustParseAddrPort("91.192.45.220:8444")
	f := &Fallback{source: peers{[]netip.AddrPort{address}, true}, slots: make(chan struct{}, 1), client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		body := "bad!"
		if strings.Contains(r.URL.Path, "/manifest/") {
			body = string(manifest)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Header: http.Header{"X-Drop-Content-Digest": []string{p.ActiveDigest}, "X-Drop-Policy-Revision": []string{"1"}}}, nil
	})}}
	w := httptest.NewRecorder()
	f.ServeProject(w, httptest.NewRequest("GET", "/", nil), p, "index.html")
	if w.Code != 503 || w.Body.Len() != 0 {
		t.Fatalf("tampered content escaped: %d %q", w.Code, w.Body.String())
	}
}

func TestFallbackDoesNotCallUntriedPeersAbsent(t *testing.T) {
	p := project.Project{Slug: "site-abcdef", ActiveDigest: strings.Repeat("a", 64), Status: "active", PolicyRevision: 1}
	addresses := []netip.AddrPort{
		netip.MustParseAddrPort("91.192.45.220:8444"), netip.MustParseAddrPort("74.103.5.187:8444"),
		netip.MustParseAddrPort("91.192.45.221:8444"), netip.MustParseAddrPort("74.103.5.188:8444"),
	}
	calls := 0
	f := &Fallback{source: peers{addresses, true}, slots: make(chan struct{}, 1), client: &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
	})}}
	w := httptest.NewRecorder()
	f.ServeProject(w, httptest.NewRequest("GET", "/", nil), p, "index.html")
	if w.Code != 503 || calls != 3 {
		t.Fatalf("untried peer incorrectly treated as absent: %d, %d calls", w.Code, calls)
	}
}

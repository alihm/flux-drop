package replica

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRecordedDiscovery(t *testing.T) {
	d, err := NewDiscovery("explorer", 8444, []netip.Addr{netip.MustParseAddr("74.103.5.187")})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/explorer.json")
	if err != nil {
		t.Fatal(err)
	}
	peers, err := d.parse(data, time.Date(2026, 9, 22, 13, 10, 0, 0, time.UTC))
	if err != nil || len(peers) != 1 || peers[0].address.String() != "91.192.45.220:8444" {
		t.Fatalf("%+v %v", peers, err)
	}
}

func TestAddressPolicy(t *testing.T) {
	d, _ := NewDiscovery("explorer", 8444, nil)
	now := time.Date(2026, 9, 22, 13, 0, 0, 0, time.UTC)
	for _, ip := range []string{"127.0.0.1", "10.0.0.1:80", "169.254.169.254", "100.64.0.1", "192.0.2.1", "198.18.0.1", "224.0.0.1", "240.0.0.1", "localhost:80", "example.com", "::1", "::ffff:127.0.0.1", "[fe80::1%eth0]:80", "[2001:db8::1]:80", "64:ff9b::7f00:1", "2002:7f00:1::", "1.1.1.1:0", "1.1.1.1:65536", "1.1.1.1/path"} {
		body := fmt.Sprintf(`{"status":"success","data":[{"name":"explorer","ip":%q,"expireAt":"2026-09-22T14:00:00Z"}]}`, ip)
		if _, err := d.parse([]byte(body), now); err == nil {
			t.Errorf("accepted %s", ip)
		}
	}
	for _, ip := range []string{"91.192.45.220", "91.192.45.220:16187", "2606:4700:4700::1111", "[2606:4700:4700::1111]:16127", "::ffff:91.192.45.220"} {
		body := fmt.Sprintf(`{"status":"success","data":[{"name":"explorer","ip":%q,"expireAt":"2026-09-22T14:00:00Z"}]}`, ip)
		p, err := d.parse([]byte(body), now)
		if err != nil || len(p) != 1 || p[0].address.Port() != 8444 {
			t.Errorf("rejected %s: %v", ip, err)
		}
	}
}

func TestRefreshBoundsAndStaleness(t *testing.T) {
	d, _ := NewDiscovery("explorer", 8444, nil)
	now := time.Date(2026, 9, 22, 13, 0, 0, 0, time.UTC)
	d.now = func() time.Time { return now }
	body := `{"status":"success","data":[{"name":"explorer","ip":"91.192.45.220","expireAt":"2026-09-22T14:00:00Z"},{"name":"explorer","ip":"91.192.45.220:1234","expireAt":"2026-09-22T13:02:00Z"}]}`
	status := 200
	d.client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://api.runonflux.io/apps/location/explorer" {
			t.Fatal(r.URL)
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})
	if _, fresh := d.Snapshot(); fresh {
		t.Fatal("uninitialized snapshot is fresh")
	}
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	p, fresh := d.Snapshot()
	if !fresh || len(p) != 1 {
		t.Fatal(p, fresh)
	}
	p[0] = netip.AddrPort{}
	for _, bad := range []string{`{}`, `{"status":"success","data":null}`, `{"status":"error","data":[]}`, `{"status":"success","data":[{"name":"wrong"}]}`, strings.Repeat("x", maxBody+1), `{"status":"success","data":[]} {}`} {
		body = bad
		if err := d.Refresh(context.Background()); err == nil {
			t.Fatal("invalid response accepted")
		}
		p, fresh = d.Snapshot()
		if !fresh || len(p) != 1 || !p[0].IsValid() {
			t.Fatal("lost or mutable snapshot")
		}
	}
	now = now.Add(2 * time.Minute)
	if p, fresh := d.Snapshot(); !fresh || len(p) != 0 {
		t.Fatal("expired peer retained")
	}
	now = now.Add(3 * time.Minute)
	if _, fresh := d.Snapshot(); fresh {
		t.Fatal("stale discovery retained")
	}
	body = `{"status":"success","data":[]}`
	status = 503
	if err := d.Refresh(context.Background()); err == nil {
		t.Fatal("HTTP error accepted")
	}
	status = 200
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p, fresh := d.Snapshot(); !fresh || len(p) != 0 {
		t.Fatal("empty discovery not applied")
	}
}

func TestDiscoveryConfigAndCancellation(t *testing.T) {
	for _, app := range []string{"", "../explorer", "explorer?x=1", "https://host", "a/b"} {
		if _, err := NewDiscovery(app, 8444, nil); err == nil {
			t.Fatal(app)
		}
	}
	if _, err := NewDiscovery("explorer", 0, nil); err == nil {
		t.Fatal("zero port accepted")
	}
	d, _ := NewDiscovery("explorer", 8444, nil)
	d.client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

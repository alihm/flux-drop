package replica

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
)

type localResolver struct {
	sync.Mutex
	p   project.Project
	err error
}

func (s *localResolver) Resolve(context.Context, string) (project.Project, error) {
	s.Lock()
	defer s.Unlock()
	return s.p, s.err
}

func TestLocalPeerDelivery(t *testing.T) {
	root := t.TempDir()
	const body = "<h1>peer content</h1>"
	s, err := content.StageHTML(root, strings.NewReader(body), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	p := project.Project{ID: strings.Repeat("b", 32), Slug: "peer-" + s.Digest[:6], ActiveDigest: s.Digest, Status: "active", PolicyRevision: 1}
	if err := s.Install(root, p.ID, p.Slug); err != nil {
		t.Fatal(err)
	}
	repo := &localResolver{p: p}
	roots, issue := peerCertificates(t)
	cfg, _, err := PeerTLS("drop", "one", issue("drop", "one"), roots)
	if err != nil {
		t.Fatal(err)
	}
	_, serverCfg, err := PeerTLS("drop", "two", issue("drop", "two"), roots)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(LocalDelivery("drop", repo, root))
	server.TLS = serverCfg
	server.StartTLS()
	defer server.Close()
	client, _ := NewPeerClient(cfg)
	defer client.CloseIdleConnections()
	request := func(method, file string, extra map[string]string) (*http.Response, string) {
		t.Helper()
		r, _ := http.NewRequest(method, server.URL+"/_drop_peer/content/"+p.Slug+"/"+file, nil)
		r.Header.Set("X-Drop-Peer-Hop", "1")
		r.Header.Set("X-Drop-Content-Digest", p.ActiveDigest)
		r.Header.Set("X-Drop-Policy-Revision", "1")
		for k, v := range extra {
			r.Header.Set(k, v)
		}
		res, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return res, string(b)
	}
	res, b := request("GET", "index.html", nil)
	if res.StatusCode != 200 || b != body || res.Header.Get("X-Drop-Content-Digest") != p.ActiveDigest {
		t.Fatalf("delivery %d %q", res.StatusCode, b)
	}
	etag := res.Header.Get("ETag")
	for _, tc := range []struct {
		method  string
		headers map[string]string
		code    int
		body    string
	}{
		{"HEAD", nil, 200, ""}, {"GET", map[string]string{"Range": "bytes=0-3"}, 206, body[:4]},
		{"GET", map[string]string{"If-None-Match": etag}, 304, ""},
		{"GET", map[string]string{"X-Drop-Content-Digest": strings.Repeat("c", 64)}, 409, ""},
		{"GET", map[string]string{"X-Drop-Policy-Revision": "2"}, 409, ""},
		{"GET", map[string]string{"X-Drop-Content-Digest": ""}, 400, ""},
	} {
		res, b := request(tc.method, "index.html", tc.headers)
		if res.StatusCode != tc.code || b != tc.body {
			t.Fatalf("%+v: %d %q", tc, res.StatusCode, b)
		}
	}
	res, _ = request("GET", "manifest.json", nil)
	if res.StatusCode != 404 {
		t.Fatal(res.StatusCode)
	}
	for _, state := range []string{"private", "deleted", "expired", "offline"} {
		repo.Lock()
		repo.p = p
		repo.err = nil
		switch state {
		case "private":
			repo.p.Private = true
		case "deleted":
			repo.p.Status = "deleted"
		case "expired":
			past := time.Now().Add(-time.Second)
			repo.p.ExpiresAt = &past
		case "offline":
			repo.err = errors.New("offline")
		}
		repo.Unlock()
		res, _ := request("HEAD", "index.html", map[string]string{"If-None-Match": etag, "Range": "bytes=0-3"})
		want := 404
		if state == "offline" {
			want = 503
		}
		if res.StatusCode != want {
			t.Fatal(state, res.StatusCode)
		}
	}
	repo.Lock()
	repo.p = p
	repo.err = nil
	repo.Unlock()
	if err := os.WriteFile(filepath.Join(root, "projects", p.ID, "versions", p.ActiveDigest, "public", "index.html"), []byte("bad"), 0600); err != nil {
		t.Fatal(err)
	}
	res, _ = request("GET", "index.html", nil)
	if res.StatusCode != 503 {
		t.Fatal("corruption served", res.StatusCode)
	}
}

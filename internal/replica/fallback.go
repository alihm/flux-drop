package replica

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"net/url"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/runonflux/flux-drop/internal/project"
)

type PeerSource interface {
	Snapshot() ([]netip.AddrPort, bool)
}

type Fallback struct {
	source PeerSource
	client *http.Client
	next   atomic.Uint64
	slots  chan struct{}
}

func NewFallback(source PeerSource, config *tls.Config) (*Fallback, error) {
	if source == nil {
		return nil, errors.New("peer discovery source required")
	}
	client, err := NewPeerClient(config)
	if err != nil {
		return nil, err
	}
	return &Fallback{source: source, client: client, slots: make(chan struct{}, 8)}, nil
}

func (f *Fallback) Close() { f.client.CloseIdleConnections() }

// ServeProject streams a public version from at most three discovered peers.
// Only the version/policy binding is sent: browser credentials and peer-supplied
// response headers never pass through. No files are cached on disk.
func (f *Fallback) ServeProject(w http.ResponseWriter, r *http.Request, p project.Project, file string) {
	if p.Private || !p.Live(time.Now()) || !contentDigest.MatchString(p.ActiveDigest) || p.PolicyRevision < 1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// Local Nginx validators cannot prove a precondition on the remote
	// representation. Fail conservatively instead of ignoring If-Match.
	if r.Header.Get("If-Match") != "" || r.Header.Get("If-Unmodified-Since") != "" {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	u := &url.URL{Path: "/_drop_peer/content/" + p.Slug + "/" + file}
	if validatePeerPath(u) != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	select {
	case f.slots <- struct{}{}:
		defer func() { <-f.slots }()
	default:
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	addresses, fresh := f.source.Snapshot()
	if !fresh || len(addresses) == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	count := len(addresses)
	if count > 3 {
		count = 3
	}
	start := int((f.next.Add(1) - 1) % uint64(len(addresses)))
	absent := 0
	for i := 0; i < count; i++ {
		address := addresses[(start+i)%len(addresses)]
		if !publicIP(address.Addr().Unmap()) || address.Port() == 0 {
			continue
		}
		u.Scheme = "https"
		u.Host = address.String()
		req, err := http.NewRequestWithContext(ctx, r.Method, u.String(), nil)
		if err != nil {
			continue
		}
		req.Header.Set("X-Drop-Peer-Hop", "1")
		req.Header.Set("X-Drop-Content-Digest", p.ActiveDigest)
		req.Header.Set("X-Drop-Policy-Revision", strconv.FormatInt(p.PolicyRevision, 10))
		res, err := f.client.Do(req)
		if err != nil {
			continue
		}
		if res.StatusCode == http.StatusNotFound {
			absent++
			res.Body.Close()
			continue
		}
		valid := res.StatusCode == http.StatusOK && res.ContentLength >= 0 && res.ContentLength <= 200<<20 && res.Header.Get("Content-Encoding") == "" &&
			len(res.Header.Values("X-Drop-Content-Digest")) == 1 && res.Header.Get("X-Drop-Content-Digest") == p.ActiveDigest &&
			len(res.Header.Values("X-Drop-Policy-Revision")) == 1 && res.Header.Get("X-Drop-Policy-Revision") == strconv.FormatInt(p.PolicyRevision, 10)
		if !valid {
			res.Body.Close()
			continue
		}
		// Public fallback deliberately ignores ranges/validators and returns a
		// complete 200 response. This avoids mixing Nginx and peer ETag formats.
		kind := mime.TypeByExtension(filepath.Ext(file))
		if kind == "" {
			kind = "application/octet-stream"
		}
		w.Header().Set("Content-Type", kind)
		w.Header().Set("Content-Length", strconv.FormatInt(res.ContentLength, 10))
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "sandbox allow-scripts; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
		w.Header().Set("Accept-Ranges", "none")
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodHead {
			res.Body.Close()
			return
		}
		_, err = io.CopyN(w, res.Body, res.ContentLength)
		res.Body.Close()
		if err != nil {
			panic(http.ErrAbortHandler)
		} // never retry after bytes escape
		return
	}
	if absent == len(addresses) {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

package replica

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
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

// ServeProject verifies a public version against its metadata digest before
// sending any bytes. The temporary spool is removed after each response.
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
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
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
		manifestURL := &url.URL{Scheme: "https", Host: address.String(), Path: "/_drop_peer/manifest/" + p.Slug}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL.String(), nil)
		if err != nil {
			continue
		}
		req.Header.Set("X-Drop-Peer-Hop", "1")
		req.Header.Set("X-Drop-Content-Digest", p.ActiveDigest)
		req.Header.Set("X-Drop-Policy-Revision", strconv.FormatInt(p.PolicyRevision, 10))
		manifestResponse, err := f.client.Do(req)
		if err != nil {
			continue
		}
		if manifestResponse.StatusCode == http.StatusNotFound {
			absent++
			manifestResponse.Body.Close()
			continue
		}
		if !validPeerResponse(manifestResponse, p, 8<<20) {
			manifestResponse.Body.Close()
			continue
		}
		manifestBytes, readErr := io.ReadAll(io.LimitReader(manifestResponse.Body, (8<<20)+1))
		manifestResponse.Body.Close()
		if readErr != nil || int64(len(manifestBytes)) != manifestResponse.ContentLength {
			continue
		}
		manifest, err := content.ParseManifest(manifestBytes, p.ActiveDigest)
		if err != nil {
			continue
		}
		var selected *content.File
		for i := range manifest.Files {
			if manifest.Files[i].Path == file {
				selected = &manifest.Files[i]
				break
			}
		}
		if selected == nil {
			absent++
			continue
		}
		u.Scheme = "https"
		u.Host = address.String()
		req, err = http.NewRequestWithContext(ctx, r.Method, u.String(), nil)
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
		if !validPeerResponse(res, p, 200<<20) || res.ContentLength != selected.Size {
			res.Body.Close()
			continue
		}
		var spool *os.File
		if r.Method == http.MethodGet {
			spool, err = os.CreateTemp("", "drop-fallback-")
			if err != nil {
				res.Body.Close()
				continue
			}
			hash := sha256.New()
			n, copyErr := io.Copy(io.MultiWriter(spool, hash), io.LimitReader(res.Body, selected.Size+1))
			res.Body.Close()
			if copyErr != nil || n != selected.Size || hex.EncodeToString(hash.Sum(nil)) != selected.SHA256 || spool.Sync() != nil {
				spool.Close()
				os.Remove(spool.Name())
				continue
			}
			if _, err := spool.Seek(0, io.SeekStart); err != nil {
				spool.Close()
				os.Remove(spool.Name())
				continue
			}
			defer func() { spool.Close(); os.Remove(spool.Name()) }()
		} else {
			res.Body.Close()
		}
		// Public fallback deliberately ignores ranges/validators and returns a
		// complete 200 response. This avoids mixing Nginx and peer ETag formats.
		kind := mime.TypeByExtension(filepath.Ext(file))
		if kind == "" {
			kind = "application/octet-stream"
		}
		w.Header().Set("Content-Type", kind)
		w.Header().Set("Content-Length", strconv.FormatInt(selected.Size, 10))
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "sandbox allow-scripts; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
		w.Header().Set("Accept-Ranges", "none")
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodHead {
			return
		}
		_, err = io.Copy(w, spool)
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

func validPeerResponse(res *http.Response, p project.Project, limit int64) bool {
	return res.StatusCode == http.StatusOK && res.ContentLength >= 0 && res.ContentLength <= limit && res.Header.Get("Content-Encoding") == "" &&
		len(res.Header.Values("X-Drop-Content-Digest")) == 1 && res.Header.Get("X-Drop-Content-Digest") == p.ActiveDigest &&
		len(res.Header.Values("X-Drop-Policy-Revision")) == 1 && res.Header.Get("X-Drop-Policy-Revision") == strconv.FormatInt(p.PolicyRevision, 10)
}

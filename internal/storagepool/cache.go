package storagepool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/httpcache"
	"github.com/runonflux/flux-drop/internal/project"
)

func (p *Pool) RemotePrivateContent() bool { return true }
func (p *Pool) ServeProject(w http.ResponseWriter, r *http.Request, pr project.Project, name string) {
	p.serve(w, r, pr, name, nil)
}
func (p *Pool) ServeAuthorizedProject(w http.ResponseWriter, r *http.Request, pr project.Project, name string, authorize func(context.Context) error) {
	p.serve(w, r, pr, name, authorize)
}
func (p *Pool) serve(w http.ResponseWriter, r *http.Request, pr project.Project, name string, authorize func(context.Context) error) {
	check := func() bool {
		if authorize == nil {
			return true
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := authorize(ctx); err != nil {
			w.Header().Del("Access-Control-Allow-Origin")
			w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
			if errors.Is(err, project.ErrNotFound) {
				http.NotFound(w, r)
			} else {
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(503)
			}
			return false
		}
		return true
	}
	if !idRE.MatchString(pr.ID) || !digestRE.MatchString(pr.ActiveDigest) || content.ValidatePath(name) != nil {
		http.NotFound(w, r)
		return
	}
	if !pr.Private {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
	}
	key := cacheKey(pr, name)
	if f, entry, release, ok := p.cache.get(key); ok {
		p.fetch.metrics.Hits.Add(1)
		// get checked the immutable downloaded file's inode, size and mtime.
		defer release()
		// Delivery already authorized this request. A verified immediate cache
		// hit has no intervening remote work and needs no second lookup.
		if httpcache.File(w, r, name, entry.SHA256, pr.Private, !pr.WatermarkDisabled) {
			return
		}
		p.deliverFile(w, r, pr, name, key, f)
		return
	}

	p.fetch.metrics.Misses.Add(1)
	f, entry, release, err := p.fetchFile(r.Context(), pr, name)
	if err != nil {
		if errors.Is(err, project.ErrNotFound) {
			http.NotFound(w, r)
		} else {
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(503)
		}
		return
	}
	defer release()
	if !check() {
		return
	}
	if httpcache.File(w, r, name, entry.SHA256, pr.Private, !pr.WatermarkDisabled) {
		return
	}
	p.deliverFile(w, r, pr, name, key, f)
}

func (p *Pool) download(ctx context.Context, a *appRuntime, addr netip.AddrPort, path string, entry content.File) (file *os.File, release func(), err error) {
	release, err = p.reserveSpoolWait(ctx, entry.Size)
	if err != nil {
		return nil, nil, err
	}
	reserved := release
	var f *os.File
	success := false
	defer func() {
		if !success {
			if f != nil {
				f.Close()
				if e := os.Remove(f.Name()); e != nil && !os.IsNotExist(e) {
					p.fetch.mu.Lock()
					p.fetch.orphans[f.Name()] = reserved
					p.fetch.mu.Unlock()
					return
				}
			}
			reserved()
		}
	}()
	res, err := a.request(ctx, addr, "GET", path, nil, "")
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 || res.Header.Get("Content-Encoding") != "" {
		return nil, nil, project.ErrStorage
	}
	f, err = os.CreateTemp(p.cache.root, "cached-")
	if err != nil {
		return nil, nil, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(res.Body, entry.Size+1))
	if err != nil {
		return nil, nil, err
	}
	if n != entry.Size || hex.EncodeToString(h.Sum(nil)) != entry.SHA256 {
		return nil, nil, content.ErrInvalid
	}
	if _, err = f.Seek(0, 0); err != nil {
		return nil, nil, err
	}
	success = true
	return f, release, nil
}

// EnableNginxCache is selected only by the supervised image runtime, whose
// internal aliases have a fixed private cache root. Standalone constructors and
// test servers continue streaming in Go without assuming nginx is present.
func (p *Pool) EnableNginxCache() {
	p.nginxCache = p.cache.root == "/var/lib/drop-cluster/storage-cache"
}
func (p *Pool) deliverFile(w http.ResponseWriter, r *http.Request, pr project.Project, name, key string, f *os.File) {
	ext := strings.ToLower(filepath.Ext(name))
	branded := !pr.WatermarkDisabled && (ext == ".html" || ext == ".htm")
	if p.nginxCache && !branded {
		if base, ok := p.cache.handoff(key, f); ok {
			kind := mime.TypeByExtension(ext)
			if kind == "" {
				b := make([]byte, 512)
				n, _ := f.ReadAt(b, 0)
				kind = http.DetectContentType(b[:n])
			}
			w.Header().Set("Content-Type", kind)
			prefix := "/_drop_internal/cache-public/"
			if pr.Private {
				prefix = "/_drop_internal/cache-private/"
			}
			w.Header().Set("X-Accel-Redirect", (&url.URL{Path: prefix + base}).EscapedPath())
			w.WriteHeader(200)
			return
		}
	}
	http.ServeContent(w, r, name, time.Time{}, f)
}

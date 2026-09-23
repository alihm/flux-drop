package replica

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
)

type Resolver interface {
	Resolve(context.Context, string) (project.Project, error)
}

var contentDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)
var projectID = regexp.MustCompile(`^[a-f0-9]{32}$`)

// LocalDelivery returns an authenticated, local-only peer handler. It has no
// discovery client and cannot recurse. Public ingress still serves via Nginx;
// this separate mTLS listener streams files only to authenticated replicas.
func LocalDelivery(app string, repository Resolver, dataRoot string) http.Handler {
	slots := make(chan struct{}, 8)
	return PeerBoundary(app, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "sandbox allow-scripts; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("Referrer-Policy", "no-referrer")
		digest := r.Header.Get("X-Drop-Content-Digest")
		policy, err := strconv.ParseInt(r.Header.Get("X-Drop-Policy-Revision"), 10, 64)
		if len(r.Header.Values("X-Drop-Content-Digest")) != 1 || !contentDigest.MatchString(digest) || len(r.Header.Values("X-Drop-Policy-Revision")) != 1 || err != nil || policy < 1 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		slug, file, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/_drop_peer/content/"), "/")
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		p, err := repository.Resolve(ctx, slug)
		if err != nil {
			if errors.Is(err, project.ErrNotFound) {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		if !p.Live(time.Now()) || p.Private || p.Slug != slug {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if p.ActiveDigest != digest || p.PolicyRevision != policy {
			w.WriteHeader(http.StatusConflict)
			return
		}
		if !projectID.MatchString(p.ID) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		directory := filepath.Join(dataRoot, "projects", p.ID, "versions", digest)
		manifest, err := content.VerifyVersion(directory, digest)
		if err != nil {
			// An incomplete/corrupt version is not proof of global absence.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var selected *content.File
		for i := range manifest.Files {
			if manifest.Files[i].Path == file {
				selected = &manifest.Files[i]
				break
			}
		}
		if selected == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		root, err := os.OpenRoot(directory)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer root.Close()
		f, err := root.Open("public/" + file)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer f.Close()
		// Verify the exact descriptor used for transfer, not just a prior path
		// lookup. Published version trees must remain immutable on disk.
		h := sha256.New()
		n, err := io.Copy(h, io.LimitReader(f, selected.Size+1))
		if err != nil || n != selected.Size || hex.EncodeToString(h.Sum(nil)) != selected.SHA256 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil || ctx.Err() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		kind := mime.TypeByExtension(filepath.Ext(file))
		if kind == "" {
			kind = "application/octet-stream"
		}
		w.Header().Set("Content-Type", kind)
		w.Header().Set("ETag", `"`+selected.SHA256+`"`)
		w.Header().Set("X-Drop-Content-Digest", digest)
		w.Header().Set("X-Drop-Policy-Revision", strconv.FormatInt(policy, 10))
		http.ServeContent(w, r, file, time.Time{}, f)
	}))
}

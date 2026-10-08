package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
)

type projectResolver interface {
	Resolve(context.Context, string) (project.Project, error)
}

var deliverySlug = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,111}[a-z0-9])?$`)
var deliveryID = regexp.MustCompile(`^[a-f0-9]{32}$`)
var deliveryDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

// ProjectDelivery authorizes public content before handing it to Nginx. This
// handler must never be used behind a proxy that ignores X-Accel-Redirect.
// This convenience wrapper has no private grant provider, so private projects fail closed.
func ProjectDelivery(repository projectResolver, dataRoot string) http.Handler {
	return ProjectDeliveryWithFallback(repository, dataRoot, nil)
}

type AuthorizedProjectFallback interface {
	RemotePrivateContent() bool
	ServeAuthorizedProject(http.ResponseWriter, *http.Request, project.Project, string, func(context.Context) error)
}

type ProjectFallback interface {
	ServeProject(http.ResponseWriter, *http.Request, project.Project, string)
}

func ProjectDeliveryWithFallback(repository projectResolver, dataRoot string, fallback ProjectFallback) http.Handler {
	return ProjectDeliveryWithAccess(repository, dataRoot, fallback, nil)
}

func ProjectDeliveryWithAccess(repository projectResolver, dataRoot string, fallback ProjectFallback, privateAccess PrivateAccess) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "sandbox allow-scripts; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// Reject alternate encodings rather than letting Go and Nginx disagree
		// on the path that was authorized. Uploaded paths cannot contain '%'.
		if r.URL.EscapedPath() != (&url.URL{Path: r.URL.Path}).EscapedPath() || strings.ContainsAny(r.URL.Path, "%\\") || !strings.HasPrefix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
		slug, file, slash := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if !deliverySlug.MatchString(slug) {
			http.NotFound(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		p, err := repository.Resolve(ctx, slug)
		if err != nil {
			if errors.Is(err, project.ErrNotFound) {
				http.NotFound(w, r)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		if !p.Live(time.Now()) || !deliverySlug.MatchString(p.Slug) || !deliveryID.MatchString(p.ID) || !deliveryDigest.MatchString(p.ActiveDigest) {
			http.NotFound(w, r)
			return
		}
		if p.Private && (privateAccess == nil || !privateAccess(r.WithContext(ctx), p) || (file != "" && file != "index.html")) {
			if privateAccess != nil && p.Slug == slug && (file == "" || file == "index.html") && r.Header.Get("Sec-Fetch-Mode") == "navigate" && r.Header.Get("Sec-Fetch-Dest") == "document" {
				http.Redirect(w, r, "/unlock/"+slug, http.StatusSeeOther)
				return
			}
			http.NotFound(w, r)
			return
		}
		if p.Slug != slug {
			path := "/" + p.Slug + "/" + file
			target := (&url.URL{Path: path, RawQuery: r.URL.RawQuery}).String()
			http.Redirect(w, r, target, http.StatusTemporaryRedirect)
			return
		}
		if !slash {
			http.Redirect(w, r, "/"+slug+"/", http.StatusTemporaryRedirect)
			return
		}
		if file == "" || strings.HasSuffix(file, "/") {
			file += "index.html"
		}
		branded := !p.WatermarkDisabled && htmlFile(file)
		if branded {
			r = watermarkRequest(r)
			writer := &watermarkWriter{ResponseWriter: w, head: r.Method == http.MethodHead}
			w = writer
			defer writer.finish()
		}
		if p.StorageApp != "" {
			if fallback != nil && (!p.Private || remotePrivateFallback(fallback)) {
				if remote, ok := fallback.(AuthorizedProjectFallback); ok {
					remote.ServeAuthorizedProject(w, r, p, file, func(ctx context.Context) error {
						current, err := repository.Resolve(ctx, slug)
						if err != nil {
							return err
						}
						if !current.Live(time.Now()) || current.PolicyRevision != p.PolicyRevision || current.Private != p.Private {
							return project.ErrNotFound
						}
						if current.Slug != p.Slug || current.ActiveDigest != p.ActiveDigest || current.StorageApp != p.StorageApp || current.WatermarkDisabled != p.WatermarkDisabled {
							return project.ErrStorage
						}
						if current.Private && (privateAccess == nil || !privateAccess(r.WithContext(ctx), current)) {
							return project.ErrNotFound
						}
						return nil
					})
				} else {
					fallback.ServeProject(w, r, p, file)
				}
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		// Deliberately verify each request for now: no readiness cache may hide
		// partially replicated or corrupted versions. A bounded immutable
		// readiness cache is a release performance gate, not an authorization cache.
		manifest, err := content.VerifyVersion(filepath.Join(dataRoot, "projects", p.ID, "versions", p.ActiveDigest), p.ActiveDigest)
		if err != nil {
			if fallback != nil && !p.Private {
				fallback.ServeProject(w, r, p, file)
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		for _, entry := range manifest.Files {
			if entry.Path != file {
				continue
			}
			prefix := "/_drop_internal/private/"
			if !p.Private {
				prefix = "/_drop_internal/files/"
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
			}
			if branded {
				root, err := os.OpenRoot(filepath.Join(dataRoot, "projects", p.ID, "versions", p.ActiveDigest, "public"))
				if err != nil {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				defer root.Close()
				f, err := root.Open(file)
				if err != nil {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				defer f.Close()
				http.ServeContent(w, r, file, time.Time{}, f)
				return
			}
			internal := prefix + p.ID + "/versions/" + p.ActiveDigest + "/public/" + file
			w.Header().Set("X-Accel-Redirect", (&url.URL{Path: internal}).EscapedPath())
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	})
}

// Only a remote backend invoked after the public handler's CURRENT access check
// can serve private content. Existing same-app public peer fallback cannot.
func remotePrivateFallback(fallback ProjectFallback) bool {
	remote, ok := fallback.(AuthorizedProjectFallback)
	return ok && remote.RemotePrivateContent()
}

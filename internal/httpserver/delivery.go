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
	"github.com/runonflux/flux-drop/internal/httpcache"
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
	return ProjectDeliveryWithAnalytics(repository, dataRoot, fallback, privateAccess, nil)
}

// ProjectDeliveryWithAnalytics adds optional memory-only page counting after
// authorization. Observer failures must be isolated in its background worker.
func ProjectDeliveryWithAnalytics(repository projectResolver, dataRoot string, fallback ProjectFallback, privateAccess PrivateAccess, analytics ProjectAnalytics) http.Handler {
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
		p, err := measure(r, "metadata", func() (project.Project, error) { return resolveForServing(ctx, repository, slug) })
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
		var accessErr error
		r = r.WithContext(context.WithValue(r.Context(), privateAccessErrorKey{}, &accessErr))
		allowed := false
		if p.Private && privateAccess != nil {
			allowed, _ = measure(r, "metadata", func() (bool, error) { return privateAccess(r.WithContext(ctx), p), nil })
		}
		if accessErr != nil {
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(503)
			return
		}
		if p.Private && (!allowed || (file != "" && file != "index.html")) {
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
		if content.ValidatePath(file) != nil {
			http.NotFound(w, r)
			return
		}
		if analytics != nil && countPageView(r, file) {
			viewAt := time.Now()
			recorder := &pageViewWriter{ResponseWriter: w}
			w = recorder
			defer func() {
				// Nginx completes internal redirects after Go returns. This records
				// successful authorized handoffs, not proof of final client receipt.
				if (recorder.status == 200 || recorder.status == 304) && r.Context().Err() == nil {
					analytics.Record(p.ID, viewAt)
				}
			}()
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
					_, _ = measure(r, "file", func() (bool, error) {
						remote.ServeAuthorizedProject(w, r, p, file, func(ctx context.Context) error {
							current, err := measure(r, "metadata", func() (project.Project, error) { return resolveForServing(ctx, repository, slug) })
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
								if accessErr != nil {
									return accessErr
								}
								return project.ErrNotFound
							}
							return nil
						})
						return true, nil
					})
				} else {
					_, _ = measure(r, "file", func() (bool, error) { fallback.ServeProject(w, r, p, file); return true, nil })
				}
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		// Integrity is cached; the project authorization above is never cached.
		manifest, err := measure(r, "verify", func() (content.Manifest, error) {
			verifyCtx, verifyCancel := context.WithTimeout(r.Context(), 2*time.Minute)
			defer verifyCancel()
			return content.ServingVersions.VerifyContext(verifyCtx, filepath.Join(dataRoot, "projects", p.ID, "versions", p.ActiveDigest), p.ActiveDigest, file)
		})
		if r.Context().Err() != nil {
			w.WriteHeader(503)
			return
		}
		if err != nil {
			if fallback != nil && !p.Private {
				_, _ = measure(r, "file", func() (bool, error) { fallback.ServeProject(w, r, p, file); return true, nil })
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
			if httpcache.File(w, r, file, entry.SHA256, p.Private, branded) {
				return
			}
			if branded {
				root, err := os.OpenRoot(filepath.Join(dataRoot, "projects", p.ID, "versions", p.ActiveDigest, "public"))
				if err != nil {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				defer root.Close()
				f, err := measure(r, "file", func() (*os.File, error) { return root.Open(file) })
				if err != nil {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				defer f.Close()
				_, _ = measure(r, "file", func() (bool, error) { http.ServeContent(w, r, file, time.Time{}, f); return true, nil })
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

func resolveForServing(ctx context.Context, repo projectResolver, slug string) (project.Project, error) {
	if serving, ok := repo.(interface {
		ResolveForServing(context.Context, string) (project.Project, error)
	}); ok {
		return serving.ResolveForServing(ctx, slug)
	}
	return repo.Resolve(ctx, slug)
}

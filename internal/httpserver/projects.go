package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/password"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
)

func registerProjects(mux *http.ServeMux, config Config, deps Dependencies, hasher *password.Hasher) {
	// Bounded per-instance disk/CPU concurrency; shared project quotas are in
	// Firestore. Ingress bandwidth/IP rate limits remain a deployment requirement.
	slots := make(chan struct{}, 4)
	disk := newDiskAdmission(deps.Projects.DataRoot)
	mutate := func(handler http.HandlerFunc) http.Handler {
		return RequireBrowserMutation(config.PublicOrigin, handler)
	}
	actor := func(r *http.Request, mutation bool) (project.Actor, error) {
		token, err := cookieToken(r)
		if err != nil {
			return project.Actor{}, session.ErrUnauthorized
		}
		view, err := deps.Sessions.Read(r.Context(), token)
		if err != nil {
			return project.Actor{}, err
		}
		if mutation {
			_, csrf, err := mutationCredentials(r)
			if err != nil {
				return project.Actor{}, err
			}
			if err := session.CheckCSRF(view.Record, csrf); err != nil {
				return project.Actor{}, err
			}
		}
		return project.ActorFrom(token, view)
	}
	bounded := func(r *http.Request) (*http.Request, context.CancelFunc) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
		return r.WithContext(ctx), cancel
	}
	upload := func(update bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			r, cancel := bounded(r)
			defer cancel()
			a, err := actor(r, true)
			if err != nil {
				projectError(w, err)
				return
			}
			keys := r.Header.Values("Idempotency-Key")
			if len(keys) != 1 {
				projectError(w, project.ErrInvalid)
				return
			}
			request := project.Reservation{Key: keys[0], Name: r.URL.Query().Get("name")}
			if update {
				request.ProjectID = r.PathValue("id")
				request.ExpectedRevision, err = expectedRevision(r)
				if err != nil {
					respond(w, 428, map[string]string{"error": "revision_required"})
					return
				}
			}
			if project.ValidateInput(request.Key, request.ProjectID, request.Name, request.ExpectedRevision) != nil {
				projectError(w, project.ErrInvalid)
				return
			}
			if update {
				if _, err := deps.Projects.Repository.GetOwned(r.Context(), a, request.ProjectID); err != nil {
					projectError(w, err)
					return
				}
			}
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			default:
				w.Header().Set("Retry-After", "5")
				respond(w, 429, map[string]string{"error": "upload_busy"})
				return
			}
			release, err := disk.acquire(config.Limits)
			if err != nil {
				w.Header().Set("Retry-After", "30")
				respond(w, http.StatusServiceUnavailable, map[string]string{"error": "storage_unavailable"})
				return
			}
			defer release()
			staged, err := stageRequest(w, r, filepath.Join(deps.Projects.DataRoot, "staging"), config.Limits)
			if err != nil {
				projectError(w, err)
				return
			}
			result, err := deps.Projects.Publish(r.Context(), a, request, staged)
			if err != nil {
				projectError(w, err)
				return
			}
			projectResponse(w, result)
		}
	}
	mux.Handle("POST /api/projects", mutate(upload(false)))
	mux.Handle("POST /api/projects/{id}/versions", mutate(upload(true)))
	privacy := &project.PrivacyService{Repository: deps.Projects.Repository, DataRoot: deps.Projects.DataRoot, Hasher: hasher}
	mux.Handle("PUT /api/projects/{id}/privacy", mutate(func(w http.ResponseWriter, r *http.Request) {
		r, cancel := bounded(r)
		defer cancel()
		a, err := actor(r, true)
		if err != nil {
			projectError(w, err)
			return
		}
		revision, err := expectedRevision(r)
		if err != nil {
			respond(w, 428, map[string]string{"error": "revision_required"})
			return
		}
		kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || kind != "application/json" {
			projectError(w, project.ErrInvalid)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 8192)
		var input struct {
			Private  *bool  `json:"private"`
			Password string `json:"password"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			projectError(w, errors.Join(project.ErrInvalid, err))
			return
		}
		if input.Private == nil || decoder.Decode(new(any)) != io.EOF {
			projectError(w, project.ErrInvalid)
			return
		}
		p, err := privacy.Change(r.Context(), a, r.PathValue("id"), revision, *input.Private, input.Password)
		if err != nil {
			projectError(w, err)
			return
		}
		projectResponse(w, p)
	}))
	mux.Handle("PATCH /api/projects/{id}", mutate(func(w http.ResponseWriter, r *http.Request) {
		r, cancel := bounded(r)
		defer cancel()
		a, err := actor(r, true)
		if err != nil {
			projectError(w, err)
			return
		}
		revision, err := expectedRevision(r)
		if err != nil {
			respond(w, 428, map[string]string{"error": "revision_required"})
			return
		}
		kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || kind != "application/json" {
			projectError(w, project.ErrInvalid)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		var input struct {
			Name string `json:"name"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			projectError(w, errors.Join(project.ErrInvalid, err))
			return
		}
		if decoder.Decode(new(any)) != io.EOF {
			projectError(w, project.ErrInvalid)
			return
		}
		p, err := deps.Projects.Repository.Rename(r.Context(), a, r.PathValue("id"), input.Name, revision)
		if err != nil {
			projectError(w, err)
			return
		}
		projectResponse(w, p)
	}))
	mux.HandleFunc("GET /api/projects", func(w http.ResponseWriter, r *http.Request) {
		r, cancel := bounded(r)
		defer cancel()
		a, err := actor(r, false)
		if err != nil {
			projectError(w, err)
			return
		}
		projects, next, err := deps.Projects.Repository.ListOwned(r.Context(), a, r.URL.Query().Get("cursor"), 50)
		if err != nil {
			projectError(w, err)
			return
		}
		respond(w, 200, map[string]any{"projects": projects, "nextCursor": next})
	})
	mux.HandleFunc("GET /api/projects/{id}", func(w http.ResponseWriter, r *http.Request) {
		r, cancel := bounded(r)
		defer cancel()
		a, err := actor(r, false)
		if err != nil {
			projectError(w, err)
			return
		}
		p, err := deps.Projects.Repository.GetOwned(r.Context(), a, r.PathValue("id"))
		if err != nil {
			projectError(w, err)
			return
		}
		projectResponse(w, p)
	})
	mux.Handle("POST /api/projects/{id}/claim", mutate(func(w http.ResponseWriter, r *http.Request) {
		r, cancel := bounded(r)
		defer cancel()
		a, err := actor(r, true)
		if err != nil {
			projectError(w, err)
			return
		}
		revision, err := expectedRevision(r)
		if err != nil {
			respond(w, 428, map[string]string{"error": "revision_required"})
			return
		}
		p, err := deps.Projects.Repository.Claim(r.Context(), a, r.PathValue("id"), revision)
		if err != nil {
			projectError(w, err)
			return
		}
		projectResponse(w, p)
	}))
	mux.Handle("DELETE /api/projects/{id}", mutate(func(w http.ResponseWriter, r *http.Request) {
		r, cancel := bounded(r)
		defer cancel()
		a, err := actor(r, true)
		if err != nil {
			projectError(w, err)
			return
		}
		revision, err := expectedRevision(r)
		if err != nil {
			respond(w, 428, map[string]string{"error": "revision_required"})
			return
		}
		if err := deps.Projects.Repository.Tombstone(r.Context(), a, r.PathValue("id"), revision); err != nil {
			projectError(w, err)
			return
		}
		w.WriteHeader(204)
	}))
}

func expectedRevision(r *http.Request) (int64, error) {
	values := r.Header.Values("If-Match")
	if len(values) != 1 {
		return 0, project.ErrInvalid
	}
	value := values[0]
	if len(value) < 3 || value[0] != '"' || value[len(value)-1] != '"' {
		return 0, project.ErrInvalid
	}
	revision, err := strconv.ParseInt(value[1:len(value)-1], 10, 64)
	if err != nil || revision < 1 {
		return 0, project.ErrInvalid
	}
	return revision, nil
}
func projectResponse(w http.ResponseWriter, p project.Project) {
	w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(p.Revision, 10)))
	respond(w, 200, map[string]any{"project": p, "path": "/" + p.Slug + "/", "claimPath": "/?claim=" + p.ID})
}
func projectError(w http.ResponseWriter, err error) {
	var duplicate *project.Duplicate
	var tooLarge *http.MaxBytesError
	switch {
	case errors.Is(err, password.ErrBusy):
		w.Header().Set("Retry-After", "2")
		respond(w, 429, map[string]string{"error": "password_busy"})
	case errors.Is(err, password.ErrInvalid):
		respond(w, 400, map[string]string{"error": "invalid_password"})
	case errors.Is(err, project.ErrStorage):
		respond(w, 503, map[string]string{"error": "storage_unavailable"})
	case errors.As(err, &duplicate):
		respond(w, 409, map[string]string{"error": "duplicate_content", "path": "/" + duplicate.Slug + "/"})
	case errors.Is(err, project.ErrNotFound):
		respond(w, 404, map[string]string{"error": "project_not_found"})
	case errors.Is(err, project.ErrConflict):
		respond(w, 409, map[string]string{"error": "project_conflict"})
	case errors.Is(err, project.ErrQuota):
		respond(w, 409, map[string]string{"error": "project_quota"})
	case errors.Is(err, project.ErrForbidden):
		respond(w, 403, map[string]string{"error": "account_required"})
	case errors.Is(err, content.ErrLimit), errors.As(err, &tooLarge):
		respond(w, 413, map[string]string{"error": "upload_limit"})
	case errors.Is(err, content.ErrInvalid), errors.Is(err, project.ErrInvalid):
		respond(w, 400, map[string]string{"error": "invalid_project"})
	default:
		sessionError(w, err)
	}
}

func stageRequest(w http.ResponseWriter, r *http.Request, parent string, limits content.Limits) (*content.Staged, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limits.UploadBytes)
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, content.ErrInvalid
	}
	if mediaType == "text/html" {
		return content.StageHTML(parent, r.Body, limits)
	}
	if mediaType != "application/zip" && mediaType != "multipart/form-data" {
		return nil, content.ErrInvalid
	}
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(parent, "request-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	type part struct {
		name, path string
		size       int64
	}
	var parts []part
	spool := func(name string, reader io.Reader) error {
		if err := content.ValidatePath(name); err != nil {
			return err
		}
		if len(parts) >= limits.Files {
			return content.ErrLimit
		}
		file, err := os.CreateTemp(dir, "part-")
		if err != nil {
			return err
		}
		n, copyErr := io.Copy(file, reader)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		parts = append(parts, part{name, file.Name(), n})
		return nil
	}
	if mediaType == "application/zip" {
		if err := spool("upload.zip", r.Body); err != nil {
			return nil, err
		}
	} else {
		reader, err := r.MultipartReader()
		if err != nil {
			return nil, content.ErrInvalid
		}
		for {
			item, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, errors.Join(content.ErrInvalid, err)
			}
			_, parameters, err := mime.ParseMediaType(item.Header.Get("Content-Disposition"))
			// Part.FileName strips directories; use the original filename and
			// validate it so folder paths survive and traversal is not sanitized away.
			if err != nil || parameters["name"] != "files" || parameters["filename"] == "" {
				_ = item.Close()
				return nil, content.ErrInvalid
			}
			err = spool(parameters["filename"], item)
			_ = item.Close()
			if err != nil {
				return nil, err
			}
		}
	}
	if len(parts) == 1 {
		file, err := os.Open(parts[0].path)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		switch strings.ToLower(filepath.Ext(parts[0].name)) {
		case ".zip":
			return content.StageZIP(parent, file, parts[0].size, limits)
		case ".html", ".htm":
			if !strings.Contains(parts[0].name, "/") {
				return content.StageHTML(parent, file, limits)
			}
		}
	}
	sources := make([]content.Source, 0, len(parts))
	for _, item := range parts {
		file := item.path
		sources = append(sources, content.Source{Name: item.name, Open: func() (io.ReadCloser, error) { return os.Open(file) }})
	}
	return content.StageFolder(parent, sources, limits)
}

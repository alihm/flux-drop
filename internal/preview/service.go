// Package preview owns primary-only screenshot jobs and the public discovery view.
package preview

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"image/jpeg"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"golang.org/x/sync/singleflight"
)

var idPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

const maxImage = 64 << 10
const maxRecent = 100

type Renderer func(context.Context, project.Project) ([]byte, error)
type State struct {
	Digest     string
	Private    bool
	Listed     bool
	Token      string
	LeaseUntil time.Time
	Ready      bool
	RetryAfter time.Time
}
type Recent struct{ IDs []string }
type Card struct {
	ID            string    `json:"id"`
	Slug          string    `json:"slug"`
	InitialSuffix string    `json:"initialSuffix"`
	UpdatedAt     time.Time `json:"updatedAt"`
	Thumbnail     string    `json:"thumbnail"`
	Claimed       bool      `json:"claimed"`
}
type Service struct {
	exploreMu         sync.Mutex
	exploreRows       []Card
	exploreUntil      time.Time
	exploreGeneration uint64
	exploreFlight     singleflight.Group
	Store             *metadata.Store
	Root              string
	Render            Renderer
	queue             chan string
	mu                sync.Mutex
	pending           map[string]bool
}

func New(store *metadata.Store, root string, render Renderer) (*Service, error) {
	if store == nil || store.Backend == nil || !filepath.IsAbs(root) || filepath.Clean(root) != root || render == nil {
		return nil, errors.New("invalid preview configuration")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	st, err := os.Lstat(root)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid preview directory")
	}
	return &Service{Store: store, Root: root, Render: render, queue: make(chan string, 128), pending: map[string]bool{}}, nil
}

// Notify never blocks upload completion. A bounded background scan recovers
// notifications lost to overload, process death, or a lost upload response.
func (s *Service) Notify(p project.Project) {
	s.InvalidateExplore()
	if !idPattern.MatchString(p.ID) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending[p.ID] {
		return
	}
	select {
	case s.queue <- p.ID:
		s.pending[p.ID] = true
	default:
	}
}
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	cursor := ""
	s.scan(ctx, &cursor)
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-s.queue:
			s.process(ctx, id)
			s.mu.Lock()
			delete(s.pending, id)
			s.mu.Unlock()
		case <-ticker.C:
			s.scan(ctx, &cursor)
		}
	}
}

type scanner interface {
	Scan(context.Context, string, string, int) (kv.Page, error)
}

func (s *Service) scan(ctx context.Context, cursor *string) {
	backend, ok := s.Store.Backend.(scanner)
	if !ok {
		return
	}
	page, err := backend.Scan(ctx, "projects/", *cursor, 100)
	if err != nil {
		return
	}
	*cursor = page.Next
	for _, r := range page.Records {
		var p project.Project
		if metadata.Decode(r.Value, &p) == nil && p.Live(time.Now()) {
			s.Notify(p)
		}
	}
}
func (s *Service) Lookup(ctx context.Context, id string) (project.Project, error) {
	return s.ReadProject(ctx, id, nil)
}

// ReadProject checks policy and performs bounded preview IO in one optimistic
// transaction. The final revision check retries if privacy/deletion changed
// during IO; no second full authorization lookup is needed.
func (s *Service) ReadProject(ctx context.Context, id string, fn func(project.Project) error) (project.Project, error) {
	var p project.Project
	if !idPattern.MatchString(id) {
		return p, project.ErrNotFound
	}
	err := s.Store.Run(ctx, func(tx *metadata.Tx) error {
		if err := tx.Get("projects/"+id, &p); err != nil {
			return err
		}
		if !p.Live(time.Now()) || !digestPattern.MatchString(p.ActiveDigest) {
			return project.ErrNotFound
		}
		if fn != nil {
			return fn(p)
		}
		return nil
	})
	if errors.Is(err, metadata.ErrNotFound) {
		err = project.ErrNotFound
	}
	return p, err
}
func updated(p project.Project) time.Time {
	if p.UpdatedAt.IsZero() {
		return p.CreatedAt
	}
	return p.UpdatedAt
}

func discoverable(p project.Project) bool {
	return p.Live(time.Now()) && !p.Private && p.Owner.Kind == "firebase" && p.Owner.ID != "" && p.ExpiresAt == nil
}
func (s *Service) Explore(ctx context.Context) ([]Card, error) {
	rows := []Card{}
	err := s.Store.Run(ctx, func(tx *metadata.Tx) error {
		rows = []Card{}
		var index Recent
		if err := tx.Get("preview_recent/public", &index); errors.Is(err, metadata.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		if len(index.IDs) > maxRecent {
			return project.ErrInvalid
		}
		keys := make([]string, 0, len(index.IDs))
		for _, id := range index.IDs {
			if idPattern.MatchString(id) {
				keys = append(keys, "projects/"+id)
			}
		}
		if err := tx.Prefetch(keys); err != nil {
			return err
		}
		for _, key := range keys {
			var p project.Project
			if err := tx.Get(key, &p); errors.Is(err, metadata.ErrNotFound) {
				continue
			} else if err != nil {
				return err
			}
			if discoverable(p) && digestPattern.MatchString(p.ActiveDigest) {
				rows = append(rows, Card{ID: p.ID, Slug: p.Slug, InitialSuffix: p.InitialSuffix, UpdatedAt: updated(p), Thumbnail: "/api/projects/" + p.ID + "/thumbnail?v=" + p.ActiveDigest, Claimed: true})
			}
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].UpdatedAt.Equal(rows[j].UpdatedAt) {
				return rows[i].ID < rows[j].ID
			}
			return rows[i].UpdatedAt.After(rows[j].UpdatedAt)
		})
		if len(rows) > 24 {
			rows = rows[:24]
		}
		return nil
	})
	return rows, err
}

// Remember fences candidates against current policy. No public response trusts
// the index, and the index contains neither identities nor credentials.
func (s *Service) remember(ctx context.Context, id string) error {
	return s.Store.Run(ctx, func(tx *metadata.Tx) error {
		var p project.Project
		if err := tx.Get("projects/"+id, &p); err != nil {
			return err
		}
		var index Recent
		if err := tx.Get("preview_recent/public", &index); err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		ids := []string{}
		for _, old := range index.IDs {
			if old != id {
				ids = append(ids, old)
			}
		}
		if discoverable(p) {
			// Compare timestamps rather than scan order, so backfill and updates do
			// not displace newer deployments with older projects.
			ids = append(ids, id)
		}
		if len(ids) > maxRecent+1 {
			return project.ErrInvalid
		}
		keys := make([]string, 0, len(ids))
		for _, v := range ids {
			keys = append(keys, "projects/"+v)
		}
		if err := tx.Prefetch(keys); err != nil {
			return err
		}
		projects := map[string]project.Project{}
		live := []string{}
		for _, v := range ids {
			var candidate project.Project
			if err := tx.Get("projects/"+v, &candidate); errors.Is(err, metadata.ErrNotFound) {
				continue
			} else if err != nil {
				return err
			}
			if discoverable(candidate) {
				live = append(live, v)
				projects[v] = candidate
			}
		}
		sort.Slice(live, func(i, j int) bool {
			a, b := updated(projects[live[i]]), updated(projects[live[j]])
			if a.Equal(b) {
				return live[i] < live[j]
			}
			return a.After(b)
		})
		if len(live) > maxRecent {
			live = live[:maxRecent]
		}
		if equalIDs(index.IDs, live) {
			return nil
		}
		return tx.Set("preview_recent/public", Recent{live})
	})
}
func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func (s *Service) ImagePath(p project.Project) string {
	return filepath.Join(s.Root, p.ID+"-"+p.ActiveDigest+".jpg")
}
func (s *Service) process(ctx context.Context, id string) {
	defer s.InvalidateExplore()
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return
	}
	token := hex.EncodeToString(tokenBytes)
	var p project.Project
	acquired := false
	indexOnly := false
	err := s.Store.Run(ctx, func(tx *metadata.Tx) error {
		acquired = false
		indexOnly = false
		if err := tx.Get("projects/"+id, &p); err != nil {
			return err
		}
		if !p.Live(time.Now()) {
			return nil
		}
		var state State
		if err := tx.Get("previews/"+id, &state); err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		if state.Digest == p.ActiveDigest && state.Ready {
			indexOnly = state.Private != p.Private || state.Listed != discoverable(p)
			return nil
		}
		if state.Digest == p.ActiveDigest && (state.Private != p.Private || state.Listed != discoverable(p)) {
			indexOnly = true
			return nil
		}
		if state.Digest == p.ActiveDigest && state.Private == p.Private && time.Now().Before(state.RetryAfter) {
			return nil
		}
		if time.Now().Before(state.LeaseUntil) {
			return nil
		}
		acquired = true
		return tx.Set("previews/"+id, State{Digest: p.ActiveDigest, Private: p.Private, Listed: discoverable(p), Token: token, LeaseUntil: time.Now().Add(time.Minute)})
	})
	if err != nil {
		return
	}
	if indexOnly {
		if err := s.remember(ctx, id); err != nil {
			return
		}
		_ = s.Store.Run(ctx, func(tx *metadata.Tx) error {
			var current project.Project
			var state State
			if err := tx.Get("projects/"+id, &current); err != nil {
				return err
			}
			if err := tx.Get("previews/"+id, &state); err != nil {
				return err
			}
			if current.ActiveDigest != p.ActiveDigest || current.Private != p.Private || discoverable(current) != discoverable(p) || state.Digest != current.ActiveDigest {
				return nil
			}
			state.Listed = discoverable(current)
			state.Private = current.Private
			return tx.Set("previews/"+id, state)
		})
		return
	}
	if !acquired {
		return
	}
	if err := s.remember(ctx, id); err != nil {
		return
	}
	raw, renderErr := s.Render(ctx, p)
	if renderErr == nil {
		config, e := jpeg.DecodeConfig(bytes.NewReader(raw))
		if e != nil || config.Width != 320 || config.Height != 180 || len(raw) > maxImage {
			renderErr = errors.New("invalid preview image")
		}
	}
	if renderErr == nil {
		renderErr = s.save(p, raw)
	}
	err = s.Store.Run(ctx, func(tx *metadata.Tx) error {
		var current project.Project
		var state State
		if err := tx.Get("projects/"+id, &current); err != nil {
			return err
		}
		if err := tx.Get("previews/"+id, &state); err != nil {
			return err
		}
		if state.Token != token {
			return nil
		}
		if current.ActiveDigest != p.ActiveDigest || !current.Live(time.Now()) || current.Private != p.Private {
			return tx.Delete("previews/" + id)
		}
		state.Token = ""
		state.LeaseUntil = time.Time{}
		state.Ready = renderErr == nil
		if renderErr != nil {
			state.RetryAfter = time.Now().Add(5 * time.Minute)
		}
		return tx.Set("previews/"+id, state)
	})
	if renderErr != nil {
		slog.Warn("thumbnail rendering failed", "project", id, "error", renderErr)
	}
	if err != nil {
		slog.Warn("thumbnail metadata update failed", "project", id, "error", err)
	}
}
func (s *Service) save(p project.Project, raw []byte) error {
	if !idPattern.MatchString(p.ID) || !digestPattern.MatchString(p.ActiveDigest) {
		return project.ErrInvalid
	}
	// A fixed ceiling bounds generated storage even when retained project records
	// grow. Old versions are safe to discard; authorization never relies on files.
	files, err := os.ReadDir(s.Root)
	if err != nil {
		return err
	}
	var total int64
	type oldFile struct {
		path string
		size int64
		when time.Time
	}
	old := []oldFile{}
	for _, f := range files {
		if !f.Type().IsRegular() {
			continue
		}
		info, e := f.Info()
		if e != nil {
			continue
		}
		total += info.Size()
		if filepath.Ext(f.Name()) == ".jpg" {
			old = append(old, oldFile{filepath.Join(s.Root, f.Name()), info.Size(), info.ModTime()})
		}
	}
	sort.Slice(old, func(i, j int) bool { return old[i].when.Before(old[j].when) })
	for _, f := range old {
		if total+int64(len(raw)) <= 512<<20 && len(files) < 10000 {
			break
		}
		if os.Remove(f.path) == nil {
			total -= f.size
			files = files[1:]
		}
	}
	if total+int64(len(raw)) > 512<<20 {
		return errors.New("thumbnail storage full")
	}
	file, err := os.CreateTemp(s.Root, ".preview-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(name, s.ImagePath(p)); err != nil {
		return err
	}
	dir, err := os.Open(s.Root)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

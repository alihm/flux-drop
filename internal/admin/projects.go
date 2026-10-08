package admin

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/kv"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

type projectScanner interface {
	Scan(context.Context, string, string, int) (kv.Page, error)
}
type ProjectView struct {
	ID         string     `json:"id"`
	Slug       string     `json:"slug"`
	OwnerKind  string     `json:"ownerKind"`
	OwnerID    string     `json:"ownerId"`
	StorageApp string     `json:"storageApp"`
	Bytes      int64      `json:"bytes"`
	Status     string     `json:"status"`
	Private    bool       `json:"private"`
	Revision   int64      `json:"revision"`
	CreatedAt  time.Time  `json:"createdAt"`
	ExpiresAt  *time.Time `json:"expiresAt,omitempty"`
}
type ProjectPage struct {
	Projects   []ProjectView `json:"projects"`
	NextCursor string        `json:"nextCursor"`
}

// Scan provides candidate IDs only. Authorization and the current project
// records are reread and fenced together before returning an admin-only view.
// Password digests, session credentials, and transfer tokens never leave Go.
func (s *Service) Projects(ctx context.Context, token, cursor, query, app string) (ProjectPage, error) {
	result := ProjectPage{Projects: []ProjectView{}}
	if len(query) > 200 || len(app) > 64 || (cursor != "" && (len(cursor) != 32 || !tokenPattern.MatchString(cursor+strings.Repeat("0", 32)))) {
		return result, project.ErrInvalid
	}
	if _, err := s.Read(ctx, token); err != nil {
		return result, err
	}
	scanner, ok := s.Store.Backend.(projectScanner)
	if !ok {
		return result, errors.New("project scan unavailable")
	}
	after := ""
	if cursor != "" {
		after = "projects/" + cursor
	}
	page, err := scanner.Scan(ctx, "projects/", after, 100)
	if err != nil {
		return result, err
	}
	keys := make([]string, 0, len(page.Records))
	for key := range page.Records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	query = strings.ToLower(strings.TrimSpace(query))
	err = s.Store.Run(ctx, func(tx *metadata.Tx) error {
		if _, err := s.authorize(tx, token, ""); err != nil {
			return err
		}
		if err := tx.Prefetch(keys); err != nil {
			return err
		}
		rows := []ProjectView{}
		for _, key := range keys {
			var p project.Project
			if err := tx.Get(key, &p); errors.Is(err, metadata.ErrNotFound) {
				continue
			} else if err != nil {
				return err
			}
			if app != "" && p.StorageApp != app {
				continue
			}
			status := p.Status
			if status == "active" && p.ExpiresAt != nil && !s.now().Before(*p.ExpiresAt) {
				status = "expired"
			}
			if query != "" && !strings.Contains(strings.ToLower(p.ID+" "+p.Slug+" "+p.Owner.ID+" "+p.Owner.Kind+" "+p.StorageApp+" "+status), query) {
				continue
			}
			rows = append(rows, ProjectView{ID: p.ID, Slug: p.Slug, OwnerKind: p.Owner.Kind, OwnerID: p.Owner.ID, StorageApp: p.StorageApp, Bytes: p.ActiveBytes, Status: status, Private: p.Private, Revision: p.Revision, CreatedAt: p.CreatedAt, ExpiresAt: p.ExpiresAt})
		}
		result.Projects = rows
		return nil
	})
	result.NextCursor = strings.TrimPrefix(page.Next, "projects/")
	return result, err
}

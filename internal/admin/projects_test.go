package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

func TestProjectListingAuthorizationPaginationSearchAndRedaction(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	if _, err := s.Projects(ctx, "", "", "", ""); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("anonymous project access", err)
	}
	c, b, err := s.Issue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	token, session, ready, err := s.Redeem(ctx, c.ID, c.PollToken, b, sign(c.Message))
	if err != nil || !ready {
		t.Fatal(err)
	}
	expired := time.Now().Add(-time.Hour)
	if err := s.Store.Run(ctx, func(tx *metadata.Tx) error {
		for i := 0; i < 105; i++ {
			id := fmt.Sprintf("%032x", i)
			p := project.Project{ID: id, Slug: fmt.Sprintf("demo-%06x", i), Owner: project.Owner{Kind: "firebase", ID: "owner-demo"}, StorageApp: "storagea", ActiveBytes: 1024, Private: true, PasswordDigest: "hidden-password-digest", PendingOperation: "hidden-operation-token", Status: "active", Revision: 3, CreatedAt: time.Now()}
			if i == 0 {
				p.ExpiresAt = &expired
			}
			if err := tx.Set("projects/"+id, p); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	first, err := s.Projects(ctx, token, "", "", "")
	if err != nil || len(first.Projects) != 100 || first.NextCursor == "" {
		t.Fatal("bounded page", len(first.Projects), err)
	}
	if first.Projects[0].Status != "expired" || first.Projects[0].OwnerID != "owner-demo" || first.Projects[0].StorageApp != "storagea" {
		t.Fatal(first.Projects[0])
	}
	raw, _ := json.Marshal(first)
	if strings.Contains(string(raw), "hidden-") || strings.Contains(string(raw), "passwordDigest") || strings.Contains(string(raw), s.Address) {
		t.Fatal("private credentials exposed")
	}
	last, err := s.Projects(ctx, token, first.NextCursor, "OWNER-DEMO", "storagea")
	if err != nil || len(last.Projects) != 5 || last.NextCursor != "" {
		t.Fatal("search continuation", last, err)
	}
	filtered, err := s.Projects(ctx, token, "", "missing", "")
	if err != nil || len(filtered.Projects) != 0 || filtered.NextCursor == "" {
		t.Fatal("search cursor lost", filtered, err)
	}
	expiredPage, err := s.Projects(ctx, token, "", "expired", "")
	if err != nil || len(expiredPage.Projects) != 1 {
		t.Fatal("expiry search", expiredPage, err)
	}
	if _, err := s.Projects(ctx, token, "../invalid", "", ""); !errors.Is(err, project.ErrInvalid) {
		t.Fatal("invalid cursor", err)
	}
	if err := s.Logout(ctx, token, session.CSRF); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Projects(ctx, token, "", "", ""); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked admin saw projects", err)
	}
}

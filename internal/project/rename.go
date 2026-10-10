package project

import (
	"cloud.google.com/go/firestore"
	"context"
)

// Rename retains old slug mappings as aliases/tombstones. They always resolve
// directly to the project, so repeated renames cannot create redirect chains.
func (s *FirestoreRepository) Rename(ctx context.Context, a Actor, id, name string, revision int64) (Project, error) {
	if !idRE.MatchString(id) || !nameRE.MatchString(name) || revision < 1 {
		return Project{}, ErrInvalid
	}
	var result Project
	err := s.Client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := read[Project](tx, s.ref("projects", id))
		if missing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) || !p.Live(s.now()) {
			return ErrNotFound
		}
		if p.Revision != revision || p.PendingOperation != "" {
			return ErrConflict
		}
		seed := p.ActiveDigest
		if p.InitialSuffix != "" {
			seed = p.InitialSuffix + hash(p.ID)
		}
		slug, suffix, err := chooseSlug(name, seed, id, func(slug string) (string, error) {
			return s.slugOwner(tx, slug)
		})
		if err != nil {
			return err
		}
		if slug == p.Slug {
			result = p
			return nil
		}
		alias, err := read[slugRecord](tx, s.ref("slugs", slug))
		newAlias := missing(err)
		if err == nil && alias.ProjectID != id {
			owner, e := s.slugOwner(tx, slug)
			if e != nil {
				return e
			}
			newAlias = owner == ""
		}
		if err != nil && !missing(err) {
			return err
		}
		if !newAlias && alias.ProjectID != id {
			return ErrConflict
		}
		// Bound permanent metadata growth while allowing reuse of prior names.
		if newAlias && p.AliasCount >= 100 {
			return ErrQuota
		}
		if p.InitialSlug == "" {
			p.InitialSlug = p.Slug
		}
		p.Slug = slug
		p.InitialSuffix = suffix
		p.Revision++
		p.PolicyRevision++
		if newAlias {
			p.AliasCount++
			if err := tx.Set(s.ref("slugs", slug), slugRecord{ProjectID: id}); err != nil {
				return err
			}
		}
		if err := tx.Set(s.ref("projects", id), p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

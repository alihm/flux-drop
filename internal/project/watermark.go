package project

import (
	"cloud.google.com/go/firestore"
	"context"
	"math"
)

// SetWatermark changes presentation only. Existing projects default to enabled;
// access grants, immutable content and storage accounting are unaffected.
func (s *FirestoreRepository) SetWatermark(ctx context.Context, a Actor, id string, revision int64, enabled bool) (Project, error) {
	if !idRE.MatchString(id) || revision < 1 {
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
		if p.Revision != revision || p.PendingOperation != "" || p.Revision == math.MaxInt64 {
			return ErrConflict
		}
		if p.WatermarkDisabled == !enabled {
			result = p
			return nil
		}
		p.WatermarkDisabled = !enabled
		p.Revision++
		p.UpdatedAt = s.now()
		if err := tx.Set(s.ref("projects", id), p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

func (s *RaftRepository) SetWatermark(ctx context.Context, a Actor, id string, revision int64, enabled bool) (Project, error) {
	if !idRE.MatchString(id) || revision < 1 {
		return Project{}, ErrInvalid
	}
	var result Project
	err := s.run(ctx, func(ctx context.Context, tx *raftTx) error {
		if err := s.authorize(tx, a); err != nil {
			return err
		}
		p, err := raftRead[Project](tx, s.raftRef("projects", id))
		if raftMissing(err) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !a.Owns(p.Owner) || !p.Live(s.now()) {
			return ErrNotFound
		}
		if p.Revision != revision || p.PendingOperation != "" || p.Revision == math.MaxInt64 {
			return ErrConflict
		}
		if p.WatermarkDisabled == !enabled {
			result = p
			return nil
		}
		p.WatermarkDisabled = !enabled
		p.Revision++
		p.UpdatedAt = s.now()
		if err := tx.Set(s.raftRef("projects", id), p); err != nil {
			return err
		}
		result = p
		return nil
	})
	return result, err
}

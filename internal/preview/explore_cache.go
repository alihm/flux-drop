package preview

import (
	"context"
	"time"
)

const exploreTTL = 20 * time.Second

type exploreResult struct {
	rows       []Card
	generation uint64
}

func (s *Service) InvalidateExplore() {
	s.exploreMu.Lock()
	s.exploreGeneration++
	s.exploreUntil = time.Time{}
	s.exploreRows = nil
	s.exploreMu.Unlock()
}
func (s *Service) CachedExplore(ctx context.Context) ([]Card, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.exploreMu.Lock()
		if time.Now().Before(s.exploreUntil) {
			rows := append([]Card{}, s.exploreRows...)
			s.exploreMu.Unlock()
			return rows, nil
		}
		s.exploreMu.Unlock()
		result := s.exploreFlight.DoChan("public", func() (any, error) {
			bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			s.exploreMu.Lock()
			if time.Now().Before(s.exploreUntil) {
				rows := append([]Card{}, s.exploreRows...)
				generation := s.exploreGeneration
				s.exploreMu.Unlock()
				return exploreResult{rows, generation}, nil
			}
			generation := s.exploreGeneration
			s.exploreMu.Unlock()
			rows, err := s.Explore(bounded)
			if err != nil {
				return nil, err
			}
			s.exploreMu.Lock()
			if generation == s.exploreGeneration {
				s.exploreRows = append([]Card{}, rows...)
				s.exploreUntil = time.Now().Add(exploreTTL)
			}
			s.exploreMu.Unlock()
			return exploreResult{rows, generation}, nil
		})
		select {
		case result := <-result:
			if result.Err != nil {
				return nil, result.Err
			}
			built := result.Val.(exploreResult)
			s.exploreMu.Lock()
			current := s.exploreGeneration
			s.exploreMu.Unlock()
			if built.generation != current {
				continue
			}
			return append([]Card{}, built.rows...), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}

	}
}

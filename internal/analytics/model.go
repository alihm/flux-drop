// Package analytics records approximate page views independently of serving.
// No IP addresses, cookies, URLs, credentials or visitor identifiers are stored.
package analytics

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"
)

const (
	Shards            = 16
	ProjectsPerShard  = 1024
	RetentionDays     = 180
	MaxSnapshotBytes  = 768 << 10
	MaxArchiveBytes   = 1 << 30
	MaxArchiveFiles   = 65536
	MaxDirectoryFiles = 512
)

var (
	ErrInvalid     = errors.New("invalid analytics request")
	ErrUnavailable = errors.New("analytics unavailable")
	ErrBusy        = errors.New("analytics capacity reached")
	idPattern      = regexp.MustCompile(`^[a-f0-9]{32}$`)
	nodePattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

// A fresh Producer is generated per process. Sequence is monotonic within each
// (Node, Producer, Day, Shard). Higher snapshots REPLACE lower ones, never add to
// them. Different producers add. Thus retries, lost ACKs and competing leaders
// cannot double count, without placing analytics history in Raft.
type Snapshot struct {
	Node      string                `json:"node"`
	Producer  string                `json:"producer"`
	Day       string                `json:"day"`
	Shard     int                   `json:"shard"`
	Sequence  uint64                `json:"sequence"`
	UpdatedAt time.Time             `json:"updatedAt"`
	Projects  map[string][24]uint64 `json:"projects"`
}

func Day(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
func Cutoff(now time.Time) time.Time { return Day(now).AddDate(0, 0, 1-RetentionDays) }
func shard(id string) int            { return strings.IndexByte("0123456789abcdef", id[0]) }
func (s Snapshot) Validate(now time.Time) error {
	d, e := time.Parse("2006-01-02", s.Day)
	if e != nil || !nodePattern.MatchString(s.Node) || !idPattern.MatchString(s.Producer) || s.Shard < 0 || s.Shard >= Shards || s.Sequence == 0 || len(s.Projects) == 0 || len(s.Projects) > ProjectsPerShard || s.UpdatedAt.IsZero() || s.UpdatedAt.After(now.Add(time.Minute)) || Day(s.UpdatedAt).Before(d) || d.After(Day(now)) {
		return ErrInvalid
	}
	for id := range s.Projects {
		if !idPattern.MatchString(id) || shard(id) != s.Shard {
			return ErrInvalid
		}
	}
	return nil
}

type Query struct {
	ProjectID string    `json:"projectId"`
	From      time.Time `json:"from"`
	To        time.Time `json:"to"`
	Interval  string    `json:"interval"`
}

func (q Query) Validate(now time.Time) error {
	if !idPattern.MatchString(q.ProjectID) || (q.Interval != "hour" && q.Interval != "day") || !q.From.Equal(q.From.Truncate(time.Hour)) || !q.To.Equal(q.To.Truncate(time.Hour)) || !q.From.Before(q.To) || q.From.Before(Cutoff(now)) || q.To.After(Day(now).AddDate(0, 0, 1)) || q.To.Sub(q.From) > RetentionDays*24*time.Hour {
		return ErrInvalid
	}
	if q.Interval == "day" && !q.From.Equal(Day(q.From)) {
		return ErrInvalid
	}
	return nil
}

type Bucket struct {
	Start     time.Time `json:"start"`
	PageViews uint64    `json:"pageViews"`
}
type Result struct {
	ProjectID   string     `json:"projectId"`
	Timezone    string     `json:"timezone"`
	Approximate bool       `json:"approximate"`
	Interval    string     `json:"interval"`
	From        time.Time  `json:"from"`
	To          time.Time  `json:"to"`
	UpdatedAt   *time.Time `json:"updatedAt"`
	PageViews   uint64     `json:"pageViews"`
	Buckets     []Bucket   `json:"buckets"`
}
type Source interface {
	SubmitPageViews(context.Context, Snapshot) error
	PageViews(context.Context, Query) (Result, error)
}

func add(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}

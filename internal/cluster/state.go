// Package cluster implements the metadata coordination layer. Flux file discovery
// is deliberately not an election, membership or authorization mechanism.
package cluster

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"regexp"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/kv"
)

var (
	ErrConflict  = kv.ErrConflict
	ErrInvalid   = kv.ErrInvalid
	ErrCapacity  = kv.ErrCapacity
	ErrNotLeader = kv.ErrNotLeader
)

const (
	maxCommand    = 1 << 20
	maxValue      = 64 << 10
	maxChanges    = 128
	maxRecords    = 100000
	maxStateBytes = 128 << 20
	maxSnapshot   = 256 << 20
)

var documentKey = regexp.MustCompile(`^[a-z][a-z0-9_]{0,47}/[A-Za-z0-9_-]{1,128}$`)

type Record = kv.Record
type Check = kv.Check
type Write = kv.Write
type Transaction = kv.Transaction

func validateTransaction(t Transaction) error {
	if t.Schema != 1 || !t.Durability.Valid() || len(t.Checks) == 0 || len(t.Checks) > maxChanges || len(t.Writes) == 0 || len(t.Writes) > maxChanges {
		return ErrInvalid
	}
	checked, written := map[string]bool{}, map[string]bool{}
	for _, c := range t.Checks {
		if !documentKey.MatchString(c.Key) || checked[c.Key] {
			return ErrInvalid
		}
		checked[c.Key] = true
	}
	for _, w := range t.Writes {
		if !checked[w.Key] || written[w.Key] || (w.Delete && len(w.Value) != 0) || (!w.Delete && (len(w.Value) > maxValue || !json.Valid(w.Value))) {
			return ErrInvalid
		}
		written[w.Key] = true
	}
	return nil
}

type state struct {
	mu             sync.RWMutex
	clusterID      string
	records        map[string]Record
	bytes          int
	index          uint64
	versionCeiling uint64
	historyDigest  string
}

func newState(id string) *state { return &state{clusterID: id, records: make(map[string]Record)} }

func decodeStrict(data []byte, into any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(into); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ErrInvalid
	}
	return nil
}

func (s *state) Apply(log *raft.Log) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index = log.Index
	var t Transaction
	if len(log.Data) > maxCommand || decodeStrict(log.Data, &t) != nil || validateTransaction(t) != nil {
		return ErrInvalid
	}
	if t.Revision != 0 && (t.Revision>>32 != log.Term || uint32(t.Revision) == 0) {
		return ErrInvalid
	}
	for _, c := range t.Checks {
		if s.records[c.Key].Version != c.Version {
			return ErrConflict
		}
	}
	count, size := len(s.records), s.bytes
	for _, w := range t.Writes {
		if old, ok := s.records[w.Key]; ok {
			count--
			size -= len(w.Key) + len(old.Value)
		}
		if !w.Delete {
			count++
			size += len(w.Key) + len(w.Value)
		}
	}
	if count > maxRecords || size > maxStateBytes {
		return ErrCapacity
	}
	version := log.Index
	if t.Revision != 0 {
		version = t.Revision
	}
	if version > s.versionCeiling {
		s.versionCeiling = version
	}
	digest := sha256.Sum256(append([]byte(s.historyDigest), log.Data...))
	s.historyDigest = hex.EncodeToString(digest[:])
	for _, w := range t.Writes {
		if w.Delete {
			delete(s.records, w.Key)
		} else {
			version := log.Index
			if t.Revision != 0 {
				version = t.Revision
			}
			s.records[w.Key] = Record{Version: version, Value: bytes.Clone(w.Value)}
			if version > s.versionCeiling {
				s.versionCeiling = version
			}
		}
	}
	s.bytes = size
	return nil
}

func (s *state) read(keys []string) map[string]Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]Record, len(keys))
	for _, key := range keys {
		if r, ok := s.records[key]; ok {
			r.Value = bytes.Clone(r.Value)
			result[key] = r
		}
	}
	return result
}

func (s *state) appliedIndex() uint64 { s.mu.RLock(); defer s.mu.RUnlock(); return s.index }

type snapshot struct {
	Schema         int               `json:"schema"`
	ClusterID      string            `json:"clusterID"`
	Index          uint64            `json:"index"`
	Records        map[string]Record `json:"records"`
	VersionCeiling uint64            `json:"versionCeiling,omitempty"`
	HistoryDigest  string            `json:"historyDigest,omitempty"`
}

func (s *state) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copy := &snapshot{Schema: 1, ClusterID: s.clusterID, Index: s.index, Records: make(map[string]Record, len(s.records))}
	copy.VersionCeiling = s.versionCeiling
	copy.HistoryDigest = s.historyDigest
	// Values are immutable once installed by Apply; only copy map entries here.
	for k, v := range s.records {
		copy.Records[k] = v
	}
	return copy, nil
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s); err != nil {
		_ = sink.Cancel()
		return err
	}
	if err := sink.Close(); err != nil {
		_ = sink.Cancel()
		return err
	}
	return nil
}
func (*snapshot) Release() {}

func (s *state) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, maxSnapshot+1))
	if err != nil {
		return err
	}
	var copy snapshot
	if len(data) > maxSnapshot || decodeStrict(data, &copy) != nil || copy.Schema != 1 || copy.ClusterID != s.clusterID || copy.Records == nil || len(copy.Records) > maxRecords {
		return ErrInvalid
	}
	if copy.HistoryDigest != "" {
		if digest, err := hex.DecodeString(copy.HistoryDigest); err != nil || len(digest) != sha256.Size {
			return ErrInvalid
		}
	}
	size := 0
	for k, r := range copy.Records {
		if !documentKey.MatchString(k) || r.Version == 0 || r.Version > max(copy.Index, copy.VersionCeiling) || len(r.Value) > maxValue || !json.Valid(r.Value) {
			return ErrInvalid
		}
		size += len(k) + len(r.Value)
		if size > maxStateBytes {
			return ErrCapacity
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records, s.index, s.bytes = copy.Records, copy.Index, size
	s.versionCeiling = copy.VersionCeiling
	s.historyDigest = copy.HistoryDigest
	return nil
}

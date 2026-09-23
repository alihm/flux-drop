package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

const testClusterID = "0123456789abcdef0123456789abcdef"

func transaction(key string, version uint64, value string) Transaction {
	return Transaction{Schema: 1, Checks: []Check{{Key: key, Version: version}}, Writes: []Write{{Key: key, Value: json.RawMessage(value)}}}
}

func apply(t *testing.T, s *state, index uint64, tx Transaction) error {
	t.Helper()
	data, err := json.Marshal(tx)
	if err != nil {
		t.Fatal(err)
	}
	if result := s.Apply(&raft.Log{Index: index, Data: data}); result != nil {
		return result.(error)
	}
	return nil
}

func TestStateAtomicChecksAndValidation(t *testing.T) {
	s := newState(testClusterID)
	if err := apply(t, s, 1, transaction("sessions/a", 0, `{"owner":"a"}`)); err != nil {
		t.Fatal(err)
	}
	tx := Transaction{Schema: 1, Checks: []Check{{Key: "sessions/a", Version: 0}, {Key: "projects/b", Version: 0}}, Writes: []Write{{Key: "sessions/a", Delete: true}, {Key: "projects/b", Value: json.RawMessage(`{}`)}}}
	if err := apply(t, s, 2, tx); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	got := s.read([]string{"sessions/a", "projects/b"})
	if len(got) != 1 || got["sessions/a"].Version != 1 {
		t.Fatal("partial transaction applied", got)
	}
	got["sessions/a"].Value[0] = '!'
	if !json.Valid(s.read([]string{"sessions/a"})["sessions/a"].Value) {
		t.Fatal("caller mutated state")
	}
	tx.Checks[0].Version = 1
	if err := apply(t, s, 3, tx); err != nil {
		t.Fatal(err)
	}
	if got := s.read([]string{"sessions/a", "projects/b"}); len(got) != 1 || got["projects/b"].Version != 3 {
		t.Fatal(got)
	}
	for _, bad := range []Transaction{
		{Schema: 1, Durability: "unknown", Checks: tx.Checks, Writes: tx.Writes},
		{Schema: 2, Checks: tx.Checks, Writes: tx.Writes},
		transaction("../outside", 0, `{}`),
		{Schema: 1, Checks: []Check{{Key: "projects/a"}}, Writes: []Write{{Key: "sessions/b", Value: json.RawMessage(`{}`)}}},
		{Schema: 1, Checks: []Check{{Key: "projects/a"}, {Key: "projects/a"}}, Writes: []Write{{Key: "projects/a", Value: json.RawMessage(`{}`)}}},
		{Schema: 1, Checks: []Check{{Key: "projects/a"}}, Writes: []Write{{Key: "projects/a", Value: json.RawMessage(`{}`), Delete: true}}},
		transaction("projects/a", 0, `"`+strings.Repeat("a", maxValue)+`"`),
	} {
		if err := apply(t, s, 4, bad); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid command accepted", err)
		}
	}
}

type memorySink struct {
	bytes.Buffer
	cancelled, closed bool
	fail              bool
}

func (*memorySink) ID() string      { return "test" }
func (s *memorySink) Cancel() error { s.cancelled = true; return nil }
func (s *memorySink) Close() error  { s.closed = true; return nil }
func (s *memorySink) Write(data []byte) (int, error) {
	if s.fail {
		return 0, io.ErrClosedPipe
	}
	return s.Buffer.Write(data)
}

func TestSnapshotRestoresOnlyMatchingValidState(t *testing.T) {
	s := newState(testClusterID)
	if err := apply(t, s, 7, transaction("projects/a", 0, `{"private":true}`)); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()
	if err := apply(t, s, 8, transaction("projects/a", 7, `{"private":false}`)); err != nil {
		t.Fatal(err)
	}
	sink := &memorySink{}
	if err := snap.Persist(sink); err != nil || !sink.closed || sink.cancelled {
		t.Fatal(err)
	}
	restored := newState(testClusterID)
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if got := restored.read([]string{"projects/a"})["projects/a"]; got.Version != 7 || string(got.Value) != `{"private":true}` {
		t.Fatal("snapshot was mutated", got)
	}
	wrong := newState(strings.Repeat("a", 32))
	if err := wrong.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err == nil {
		t.Fatal("cross-cluster snapshot accepted")
	}
	if err := restored.Restore(io.NopCloser(strings.NewReader(`{"schema":1,"clusterID":"` + testClusterID + `","index":1,"records":{"projects/a":{"version":2,"value":{}}}}`))); err == nil {
		t.Fatal("future revision restored")
	}
	if restored.read([]string{"projects/a"})["projects/a"].Version != 7 {
		t.Fatal("failed restore changed state")
	}
	failure := &memorySink{fail: true}
	if err := snap.Persist(failure); err == nil || !failure.cancelled {
		t.Fatal("snapshot failure not cancelled")
	}
}

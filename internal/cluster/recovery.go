package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	"golang.org/x/sys/unix"
)

type singletonRequest struct {
	ClusterID         string `json:"clusterID"`
	NodeID            string `json:"nodeID"`
	ExpectedLastIndex uint64 `json:"expectedLastIndex"`
	AllowLearner      bool   `json:"allowLearner,omitempty"`
}

// Only an operator-owned file authorizes recovery. O_NOFOLLOW and fstat bind
// validation to the opened inode, not to a potentially replaced pathname.
func readOperatorRequest(path string, into any, fields ...string) (os.FileInfo, error) {
	return readOperatorRequestOptional(path, into, fields, nil)
}

func readOperatorRequestOptional(path string, into any, fields, optional []string) (os.FileInfo, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Geteuid()) || !info.Mode().IsRegular() || info.Size() > 4096 {
		return nil, errors.New("recovery request must be a small regular file owned by Drop's user")
	}
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return nil, err
	}
	// Reject duplicate fields too: a reviewed request must have one meaning.
	dec := json.NewDecoder(bytes.NewReader(data))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("invalid recovery request JSON")
	}
	seen := map[string]bool{}
	allowed := map[string]bool{}
	for _, field := range fields {
		allowed[field] = true
	}
	for _, field := range optional {
		allowed[field] = true
	}
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok || !allowed[key] || seen[key] {
			return nil, errors.New("unknown or duplicate recovery request field")
		}
		seen[key] = true
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, errors.New("null recovery request field")
		}
	}
	for _, field := range fields {
		if !seen[field] {
			return nil, fmt.Errorf("recovery request requires %s", field)
		}
	}
	if err := decodeStrict(data, into); err != nil {
		return nil, fmt.Errorf("invalid recovery request: %w", err)
	}
	return info, nil
}

func singletonRequestFile(c Config) (*singletonRequest, os.FileInfo, error) {
	var request singletonRequest
	info, err := readOperatorRequestOptional(filepath.Join(c.StateDir, "recover-singleton.json"), &request, []string{"clusterID", "nodeID", "expectedLastIndex"}, []string{"allowLearner"})
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if request.ClusterID != c.ClusterID || request.NodeID != c.Local.ID {
		return nil, nil, errors.New("singleton recovery identity or expectedLastIndex mismatch")
	}
	// FileSnapshotStore's constructor creates snapshots/. Refuse a damaged
	// history before that constructor can change a rejected recovery's tree;
	// an absent source also prevents the required pre-recovery backup.
	snapshotInfo, err := os.Lstat(filepath.Join(c.StateDir, "snapshots"))
	if err != nil {
		return nil, nil, fmt.Errorf("recovery requires the original snapshots directory: %w", err)
	}
	if !snapshotInfo.IsDir() {
		return nil, nil, errors.New("recovery snapshots path is not a directory")
	}
	return &request, info, nil
}

// Inspect the latest configuration without NewRaft/GetConfiguration: even
// skipStartup NewRaft writes the stable term. Never mutate rejected histories.
func latestRecoveryConfiguration(store raft.LogStore, snapshots raft.SnapshotStore) (configuration raft.Configuration, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("invalid persisted Raft configuration")
		}
	}()
	metas, err := snapshots.List()
	if err != nil {
		return configuration, err
	}
	var after uint64
	if len(metas) > 0 {
		configuration = metas[0].Configuration
		after = metas[0].Index
	}
	first, err := store.FirstIndex()
	if err != nil {
		return configuration, err
	}
	last, err := store.LastIndex()
	if err != nil {
		return configuration, err
	}
	for index := max(first, after+1); index > 0 && index <= last; index++ {
		var entry raft.Log
		if err := store.GetLog(index, &entry); err != nil {
			return configuration, err
		}
		if entry.Type == raft.LogConfiguration {
			configuration = raft.DecodeConfiguration(entry.Data)
		}
	}
	return configuration, nil
}

func validateSingleton(c Config, request *singletonRequest, existing bool, store raft.LogStore, snapshots raft.SnapshotStore) error {
	if !existing {
		return errors.New("singleton recovery requires existing Raft state")
	}
	last, err := store.LastIndex()
	if err != nil {
		return err
	}
	if last != request.ExpectedLastIndex {
		return fmt.Errorf("singleton recovery last index mismatch: have %d, expected %d", last, request.ExpectedLastIndex)
	}
	configuration, err := latestRecoveryConfiguration(store, snapshots)
	if err != nil {
		return err
	}
	for _, member := range configuration.Servers {
		if string(member.ID) == c.Local.ID && (member.Suffrage == raft.Voter || request.AllowLearner && member.Suffrage == raft.Nonvoter) {
			return nil
		}
	}
	return errors.New("singleton recovery requires this node to be a voter in the latest configuration")
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func copyRecoveryTree(source, destination string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Geteuid()) || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe recovery backup source")
	}
	if info.IsDir() {
		if err := os.Mkdir(destination, 0700); err != nil {
			return err
		}
		entries, err := os.ReadDir(source)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := copyRecoveryTree(filepath.Join(source, entry.Name()), filepath.Join(destination, entry.Name())); err != nil {
				return err
			}
		}
		return syncDirectory(destination)
	}
	if !info.Mode().IsRegular() {
		return errors.New("unsafe recovery backup file")
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func backupBeforeRecovery(c Config, stamp string) error {
	directory := filepath.Join(c.StateDir, "pre-recovery-"+stamp)
	if err := os.Mkdir(directory, 0700); err != nil {
		return err
	}
	for _, name := range []string{"raft.db", "snapshots", "identity.json"} {
		if err := copyRecoveryTree(filepath.Join(c.StateDir, name), filepath.Join(directory, name)); err != nil {
			return fmt.Errorf("pre-recovery copy %s: %w", name, err)
		}
	}
	if err := syncDirectory(directory); err != nil {
		return err
	}
	return syncDirectory(c.StateDir)
}

func consumeRecoveryRequest(c Config, name, stamp string, info os.FileInfo) error {
	path := filepath.Join(c.StateDir, name+".json")
	current, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !os.SameFile(info, current) {
		return errors.New("recovery request changed during recovery")
	}
	if err := os.Rename(path, filepath.Join(c.StateDir, name+".done-"+stamp)); err != nil {
		return err
	}
	return syncDirectory(c.StateDir)
}

func recoverSingleton(c Config, rc *raft.Config, store interface {
	raft.LogStore
	raft.StableStore
}, snapshots raft.SnapshotStore, transport raft.Transport, request *singletonRequest, info os.FileInfo, stamp string) error {
	configuration := raft.Configuration{Servers: []raft.Server{{ID: raft.ServerID(c.Local.ID), Address: raft.ServerAddress(c.Local.raftAddress()), Suffrage: raft.Voter}}}
	// RecoverCluster commits the survivor's entire log, including its uncommitted
	// tail. Only permanent loss confirmation by an operator makes this acceptable.
	if err := raft.RecoverCluster(rc, newState(c.ClusterID), store, store, snapshots, transport, configuration); err != nil {
		return fmt.Errorf("singleton recovery failed (request retained): %w", err)
	}
	if err := consumeRecoveryRequest(c, "recover-singleton", stamp, info); err != nil {
		return err
	}
	term, _ := store.GetUint64([]byte("CurrentTerm"))
	slog.Warn("operator majority-loss singleton recovery completed; surviving log tail implicitly committed", "node", c.Local.ID, "last_index", request.ExpectedLastIndex, "term", term)
	return nil
}

func recoveryTimestamp() string { return time.Now().UTC().Format("20060102T150405.000000000Z") }

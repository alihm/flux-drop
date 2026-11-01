package cluster

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"
)

type restoreSingletonRequest struct {
	ClusterID  string `json:"clusterID"`
	NodeID     string `json:"nodeID"`
	BackupFile string `json:"backupFile"`
}
type verifiedRestore struct {
	header      backupHeader
	plaintext   []byte
	requestInfo os.FileInfo
}

// This preflight runs before any writable store or new snapshot directory is
// opened. Even a valid request can only restore an empty node, never overwrite
// usable state. Corrupt remnants require an explicit operator decision too.
func refuseExistingRestoreState(c Config) error {
	path := filepath.Join(c.StateDir, "raft.db")
	if _, err := os.Lstat(path); err == nil {
		store, err := raftboltdb.New(raftboltdb.Options{Path: path, BoltOptions: &bbolt.Options{ReadOnly: true, Timeout: time.Second}})
		if err != nil {
			return err
		}
		defer store.Close()
		index, err := store.LastIndex()
		if err != nil {
			return err
		}
		term, err := store.GetUint64([]byte("CurrentTerm"))
		if err != nil && err.Error() != "not found" {
			return err
		}
		if index != 0 || term != 0 {
			return errors.New("backup restore refuses existing Raft state")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	entries, err := os.ReadDir(filepath.Join(c.StateDir, "snapshots"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(entries) != 0 {
		return errors.New("backup restore refuses existing Raft snapshots or incomplete recovery")
	}
	return nil
}

func readMetadataBackup(_ Config, path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("restore backupFile must be an exact clean absolute filename")
	}
	// The operator may stage an off-node copy under private recovery-backup/.
	// Authenticate its contents, not its location or filename extension.
	directory, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if directory != filepath.Dir(path) {
		return nil, errors.New("restore backup directory must not contain symlinks")
	}
	return readBackupCandidate(path)
}

func prepareSingletonRestore(c Config, hasRecovery bool) (*verifiedRestore, error) {
	var request restoreSingletonRequest
	info, err := readOperatorRequest(filepath.Join(c.StateDir, "restore-singleton.json"), &request, "clusterID", "nodeID", "backupFile")
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if hasRecovery {
		return nil, errors.New("recover-singleton and restore-singleton requests are mutually exclusive")
	}
	if request.ClusterID != c.ClusterID || request.NodeID != c.Local.ID || request.BackupFile == "" {
		return nil, errors.New("backup restore request identity mismatch")
	}
	if err := refuseExistingRestoreState(c); err != nil {
		return nil, err
	}
	data, err := readMetadataBackup(c, request.BackupFile)
	if err != nil {
		return nil, err
	}
	header, plaintext, err := decryptMetadataBackup(data, c.ClusterID, os.Getenv(PassphraseEnv))
	if err != nil {
		return nil, err
	}
	return &verifiedRestore{header: header, plaintext: plaintext, requestInfo: info}, nil
}

func installSingletonRestore(c Config, store raft.StableStore, snapshots raft.SnapshotStore, transport raft.Transport, restore *verifiedRestore) error {
	configuration := raft.Configuration{Servers: []raft.Server{{ID: raft.ServerID(c.Local.ID), Address: raft.ServerAddress(c.Local.raftAddress()), Suffrage: raft.Voter}}}
	// This is already a fully validated FSM snapshot. Install it with a new
	// sole-voter configuration, retaining the original revision ceiling and term
	// so subsequent async revisions cannot collide with pre-backup revisions.
	sink, err := snapshots.Create(raft.SnapshotVersionMax, restore.header.Index, restore.header.Term, configuration, 1, transport)
	if err != nil {
		return err
	}
	if _, err := sink.Write(restore.plaintext); err != nil {
		sink.Cancel()
		return err
	}
	if err := sink.Close(); err != nil {
		sink.Cancel()
		return err
	}
	if err := store.SetUint64([]byte("CurrentTerm"), restore.header.Term); err != nil {
		return err
	}
	if err := consumeRecoveryRequest(c, "restore-singleton", recoveryTimestamp(), restore.requestInfo); err != nil {
		return fmt.Errorf("restore completed but request consumption failed: %w", err)
	}
	slog.Warn("operator backup singleton restore completed; all metadata writes after this backup are lost; do not restore another node", "node", c.Local.ID, "backup_created_at", restore.header.CreatedAt, "last_index", restore.header.Index, "term", restore.header.Term)
	return nil
}

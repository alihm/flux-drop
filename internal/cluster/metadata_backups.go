package cluster

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/sys/unix"
)

const metadataBackupInterval = 15 * time.Minute
const metadataBackupMagic = "DROP-METADATA-BACKUP-V1\n"
const metadataBackupLimit = maxSnapshot + 8192

// Keep more than the default upload readiness floor (~1.7 GiB), including
// staging/block overhead. Backup success must not consume that admission margin.
const metadataBackupReserve = 2 << 30
const metadataBackupKeep = 6

type backupHeader struct {
	Schema    int       `json:"schema"`
	ClusterID string    `json:"clusterID"`
	Index     uint64    `json:"index"`
	Term      uint64    `json:"term"` // leader term at capture; >= every included command term
	CreatedAt time.Time `json:"createdAt"`
}

type limitedSnapshotBuffer struct{ bytes.Buffer }

func (b *limitedSnapshotBuffer) Write(p []byte) (int, error) {
	if len(p) > maxSnapshot-b.Len() {
		return 0, ErrCapacity
	}
	return b.Buffer.Write(p)
}

func metadataBackupCipher(passphrase, clusterID string) (cipher.AEAD, error) {
	if len(passphrase) < 32 || len(passphrase) > 1024 {
		return nil, errors.New("metadata backups require a 32–1024 byte cluster passphrase")
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, []byte(passphrase), []byte(clusterID), []byte("flux-drop/metadata-backups/v1")), key); err != nil {
		return nil, err
	}
	defer clear(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func encryptMetadataBackup(header backupHeader, plaintext []byte, passphrase string) ([]byte, error) {
	aead, err := metadataBackupCipher(passphrase, header.ClusterID)
	if err != nil {
		return nil, err
	}
	if len(plaintext) > maxSnapshot {
		return nil, ErrCapacity
	}
	raw, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	aad := append([]byte(metadataBackupMagic), append(raw, '\n')...)
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	result := append(aad, nonce...)
	return aead.Seal(result, nonce, plaintext, aad), nil
}

func decryptMetadataBackup(data []byte, clusterID, passphrase string) (backupHeader, []byte, error) {
	var header backupHeader
	invalid := errors.New("metadata backup authentication or format invalid")
	if len(data) > metadataBackupLimit || !bytes.HasPrefix(data, []byte(metadataBackupMagic)) {
		return header, nil, invalid
	}
	start := len(metadataBackupMagic)
	end := bytes.IndexByte(data[start:], '\n')
	if end < 0 || end > 4096 {
		return header, nil, invalid
	}
	end += start + 1
	if decodeStrict(data[start:end], &header) != nil || header.Schema != 1 || header.ClusterID != clusterID || header.Index == 0 || header.Term == 0 || header.CreatedAt.IsZero() {
		return header, nil, invalid
	}
	aead, err := metadataBackupCipher(passphrase, clusterID)
	if err != nil {
		return header, nil, err
	}
	if len(data) < end+aead.NonceSize()+aead.Overhead() {
		return header, nil, invalid
	}
	plaintext, err := aead.Open(nil, data[end:end+aead.NonceSize()], data[end+aead.NonceSize():], data[:end])
	if err != nil {
		return header, nil, invalid
	}
	// Restore performs full FSM validation before any node-local Raft mutation.
	scratch := newState(clusterID)
	if err := scratch.Restore(io.NopCloser(bytes.NewReader(plaintext))); err != nil {
		return header, nil, invalid
	}
	if scratch.index != header.Index || scratch.versionCeiling>>32 > header.Term {
		return header, nil, invalid
	}
	return header, plaintext, nil
}

// The unchanged-state hint is authenticated before use, including across leader
// restarts. A missing/corrupt older backup never prevents writing a fresh one.
func newestBackupHistory(c Config, passphrase string) (string, time.Time) {
	directory := filepath.Join(c.ContentDir, "metadata-backups")
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", time.Time{}
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return "", time.Time{}
	}
	var names []string
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), "metadata-"+c.ClusterID+"-") && strings.HasSuffix(entry.Name(), ".enc") {
			names = append(names, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	for _, name := range names[:min(len(names), metadataBackupKeep)] {
		data, err := readBackupCandidate(filepath.Join(directory, name))
		if err != nil {
			continue
		}
		header, plain, err := decryptMetadataBackup(data, c.ClusterID, passphrase)
		if err != nil {
			continue
		}
		var view struct {
			HistoryDigest string `json:"historyDigest"`
		}
		err = json.Unmarshal(plain, &view)
		clear(plain)
		if err == nil {
			return view.HistoryDigest, header.CreatedAt
		}
	}
	return "", time.Time{}
}

func readBackupCandidate(path string) ([]byte, error) {
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
	if !info.Mode().IsRegular() || info.Size() > metadataBackupLimit {
		return nil, errors.New("invalid metadata backup file size or type")
	}
	return io.ReadAll(io.LimitReader(f, metadataBackupLimit+1))
}

func metadataBackupDirectory(c Config) (string, error) {
	directory := filepath.Join(c.ContentDir, "metadata-backups")
	// Refuse even an operator-misconfigured content root below an image alias.
	// Encryption is defense in depth, not permission to expose backup files.
	for _, alias := range []string{"/data/projects", "/var/lib/drop-cluster/storage-cache"} {
		if overlap(alias, directory) {
			return "", errors.New("metadata backups must be outside every Nginx content alias")
		}
	}
	if err := os.Mkdir(directory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("metadata backup directory must not be a symlink")
	}
	return directory, nil
}

func backupDiskHeadroom(directory string, size uint64) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(directory, &stat); err != nil {
		return err
	}
	if uint64(stat.Bsize)*stat.Bavail < metadataBackupReserve+2*size {
		return errors.New("insufficient disk headroom for encrypted metadata backup")
	}
	return nil
}

func writeEncryptedBackup(directory, name string, data []byte) error {
	if len(data) > metadataBackupLimit {
		return ErrCapacity
	}
	if err := backupDiskHeadroom(directory, uint64(len(data))); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".metadata-backup-*.tmp")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(temporary, filepath.Join(directory, name)); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func retainMetadataBackups(directory, clusterID string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	names := []string{}
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), "metadata-"+clusterID+"-") && strings.HasSuffix(entry.Name(), ".enc") {
			names = append(names, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	if len(names) <= metadataBackupKeep {
		return nil
	}
	for _, name := range names[metadataBackupKeep:] {
		if err := os.Remove(filepath.Join(directory, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return syncDirectory(directory)
}

// Runs only in its own worker: failures never propagate to Raft, serving or
// readiness. Snapshot() copies committed immutable map entries under a brief
// read lock, then encoding/encryption/fsync proceed without that lock. Never use
// async.view: it may contain content acknowledged before replication.
func (n *Node) writeMetadataBackup(passphrase string, previous string) (string, error) {
	if n.raft.State() != raft.Leader {
		return previous, nil
	}
	term := n.raft.CurrentTerm()
	view, err := n.state.Snapshot()
	if err != nil {
		return previous, err
	}
	defer view.Release()
	snapshot := view.(*snapshot)
	if previous == "" {
		digest, created := newestBackupHistory(n.config, passphrase)
		previous = digest
		if !created.IsZero() {
			n.lastBackupSuccess.Store(created.Unix())
		}
	}
	if snapshot.Index == 0 || snapshot.HistoryDigest == previous {
		return previous, nil
	}
	var plaintext limitedSnapshotBuffer
	if err := json.NewEncoder(&plaintext).Encode(snapshot); err != nil {
		return previous, err
	}
	defer clear(plaintext.Bytes())
	if n.raft.State() != raft.Leader || n.raft.CurrentTerm() != term {
		return previous, nil
	}
	header := backupHeader{Schema: 1, ClusterID: n.config.ClusterID, Index: snapshot.Index, Term: term, CreatedAt: time.Now().UTC()}
	encrypted, err := encryptMetadataBackup(header, plaintext.Bytes(), passphrase)
	if err != nil {
		return previous, err
	}
	directory, err := metadataBackupDirectory(n.config)
	if err != nil {
		return previous, err
	}
	name := fmt.Sprintf("metadata-%s-%s-%s.enc", n.config.ClusterID, header.CreatedAt.Format("20060102T150405.000000000Z"), n.localEpoch)
	if err := writeEncryptedBackup(directory, name, encrypted); err != nil {
		return previous, err
	}
	n.lastBackupSuccess.Store(header.CreatedAt.Unix())
	if err := retainMetadataBackups(directory, n.config.ClusterID); err != nil {
		return previous, err
	}
	return snapshot.HistoryDigest, nil
}

func (n *Node) metadataBackupTick(passphrase, previous string, lastLog *time.Time) string {
	next, err := n.writeMetadataBackup(passphrase, previous)
	if err != nil {
		n.backupFailures.Add(1)
		if time.Since(*lastLog) >= 5*time.Minute {
			slog.Warn("encrypted metadata backup failed; metadata service unaffected", "error", err)
			*lastLog = time.Now()
		}
		return previous
	}
	return next
}

func (n *Node) startMetadataBackups() {
	ctx, cancel := context.WithCancel(context.Background())
	n.backupCancel = cancel
	n.backupDone = make(chan struct{})
	go func() {
		defer close(n.backupDone)
		timer := time.NewTicker(metadataBackupInterval)
		defer timer.Stop()
		var previous string
		var lastLog time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				previous = n.metadataBackupTick(os.Getenv(PassphraseEnv), previous, &lastLog)
			}
		}
	}()
}

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEncryptedMetadataBackups(t *testing.T) {
	g := newRecoveryTLSGroup(t, false)
	leader := g.nodes[g.leader(t, -1)]
	passphrase := strings.Repeat("backup-private-", 4)
	if err := leader.Commit(context.Background(), transaction("sessions/secret", 0, `{"secret":"never-on-replicated-disk"}`)); err != nil {
		t.Fatal(err)
	}
	previous, err := leader.writeMetadataBackup(passphrase, "")
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(leader.config.ContentDir, "metadata-backups")
	files, _ := filepath.Glob(filepath.Join(directory, "*.enc"))
	if len(files) != 1 {
		t.Fatal(files)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("never-on-replicated-disk")) || bytes.Contains(data, []byte("sessions/secret")) {
		t.Fatal("plaintext on replicated volume")
	}
	header, plain, err := decryptMetadataBackup(data, testClusterID, passphrase)
	if err != nil {
		t.Fatal(err)
	}
	if header.Index == 0 || header.Term == 0 || !bytes.Contains(plain, []byte("sessions/secret")) {
		t.Fatal(header)
	}
	if _, err := leader.writeMetadataBackup(passphrase, previous); err != nil {
		t.Fatal(err)
	}
	// A fresh worker/leader also recognizes the last authenticated history.
	if _, err := leader.writeMetadataBackup(passphrase, ""); err != nil {
		t.Fatal(err)
	}
	unchanged, _ := filepath.Glob(filepath.Join(directory, "*.enc"))
	if len(unchanged) != 1 {
		t.Fatal("unchanged metadata backed up again")
	}
	for label, mutate := range map[string]func([]byte) []byte{
		"ciphertext": func(b []byte) []byte { b[len(b)-1] ^= 1; return b },
		"cluster": func(b []byte) []byte {
			return bytes.Replace(b, []byte(testClusterID), []byte(strings.Repeat("e", 32)), 1)
		},
		"term":  func(b []byte) []byte { return bytes.Replace(b, []byte(`"term":`), []byte(`"term":9`), 1) },
		"index": func(b []byte) []byte { return bytes.Replace(b, []byte(`"index":`), []byte(`"index":9`), 1) },
	} {
		t.Run(label, func(t *testing.T) {
			if _, _, err := decryptMetadataBackup(mutate(bytes.Clone(data)), testClusterID, passphrase); err == nil {
				t.Fatal("tampering accepted")
			}
		})
	}
	if _, _, err := decryptMetadataBackup(data, testClusterID, strings.Repeat("wrong-key", 5)); err == nil {
		t.Fatal("wrong key accepted")
	}
	if _, _, err := decryptMetadataBackup(data, strings.Repeat("d", 32), passphrase); err == nil {
		t.Fatal("wrong cluster accepted")
	}
	if _, err := metadataBackupCipher("", testClusterID); err == nil {
		t.Fatal("missing secret accepted")
	}
	if err := writeEncryptedBackup(directory, "oversized.enc", make([]byte, metadataBackupLimit+1)); err == nil {
		t.Fatal("oversized backup accepted")
	}
	if err := backupDiskHeadroom(directory, ^uint64(0)/4); err == nil {
		t.Fatal("unbounded disk use permitted")
	}
	// Keep six newest validly named files; unrelated files are not our retention policy.
	for i := 0; i < 8; i++ {
		if err := leader.Commit(context.Background(), transaction("projects/p"+fmtInt(i), 0, `{}`)); err != nil {
			t.Fatal(err)
		}
		previous, err = leader.writeMetadataBackup(passphrase, previous)
		if err != nil {
			t.Fatal(err)
		}
	}
	files, _ = filepath.Glob(filepath.Join(directory, "*.enc"))
	if len(files) != metadataBackupKeep {
		t.Fatal("retention", len(files))
	}
	entries, _ := os.ReadDir(directory)
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".enc") {
			t.Fatal("temporary plaintext or leftover", entry.Name())
		}
	}
	if leader.lastBackupSuccess.Load() == 0 {
		t.Fatal("backup success not recorded")
	}
	// Followers never write backups.
	for _, n := range g.nodes {
		if n != leader {
			if _, err := n.writeMetadataBackup(passphrase, ""); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(n.config.ContentDir, "metadata-backups")); !os.IsNotExist(err) {
				t.Fatal("follower wrote a backup")
			}
		}
	}
}

func TestBackupFailureDoesNotAffectMetadata(t *testing.T) {
	g := newRecoveryTLSGroup(t, false)
	n := g.nodes[g.leader(t, -1)]
	if err := n.Commit(context.Background(), transaction("sessions/before", 0, `true`)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(n.config.ContentDir, "metadata-backups")); err != nil {
		t.Fatal(err)
	}
	var lastLog time.Time
	n.metadataBackupTick(strings.Repeat("secret", 8), "", &lastLog)
	if n.backupFailures.Load() != 1 {
		t.Fatal("missing failure counter")
	}
	if err := n.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := n.Commit(context.Background(), transaction("sessions/after", 0, `true`)); err != nil {
		t.Fatal("backup failure affected writes", err)
	}
}

func TestBackupRejectsInvalidFSMBeforeUse(t *testing.T) {
	passphrase := strings.Repeat("secret", 8)
	header := backupHeader{Schema: 1, ClusterID: testClusterID, Index: 4, Term: 2, CreatedAt: time.Now().UTC()}
	invalid := snapshot{Schema: 1, ClusterID: testClusterID, Index: 4, Records: map[string]Record{"not-valid": {Version: 1, Value: []byte(`true`)}}}
	plain, _ := json.Marshal(invalid)
	data, err := encryptMetadataBackup(header, plain, passphrase)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decryptMetadataBackup(data, testClusterID, passphrase); err == nil {
		t.Fatal("invalid authenticated FSM accepted")
	}
}

func TestMetadataBackupsRejectPublicAliasRoots(t *testing.T) {
	for _, root := range []string{"/data/projects", "/data/projects/nested", "/var/lib/drop-cluster/storage-cache"} {
		if _, err := metadataBackupDirectory(Config{ContentDir: root}); err == nil {
			t.Fatal("backup directory inside Nginx alias", root)
		}
	}
}

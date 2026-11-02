package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

func TestRestoreSingletonAfterTotalStateLoss(t *testing.T) {
	passphrase := strings.Repeat("restore-private-", 4)
	t.Setenv(PassphraseEnv, passphrase)
	g := newRecoveryTLSGroup(t, true)
	n := g.nodes[g.leader(t, -1)]
	awaitAsync(t, n)
	for _, key := range []string{"sessions/owner", "projects/project", "owner_projects/owner"} {
		if err := n.Commit(context.Background(), transaction(key, 0, `{"retained":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := n.writeMetadataBackup(passphrase, ""); err != nil {
		t.Fatal(err)
	}
	paths, _ := filepath.Glob(filepath.Join(n.config.ContentDir, "metadata-backups", "*.enc"))
	if len(paths) != 1 {
		t.Fatal(paths)
	}
	if err := n.Commit(context.Background(), transaction("projects/lostlater", 0, `true`)); err != nil {
		t.Fatal(err)
	}
	for _, node := range g.nodes {
		node.Close()
	}
	c := n.config
	c.StateDir = t.TempDir()
	os.Chmod(c.StateDir, 0700)
	c.Local.ID = "restored"
	c.InitialVoters = []Member{c.Local}
	for _, member := range n.config.InitialVoters {
		if member.Address != c.Local.Address {
			c.InitialVoters = append(c.InitialVoters, member)
		}
	}
	// A pre-provisioned matching identity is not Raft history. Its presence
	// must not turn an explicit restore into the forbidden implicit rebootstrap.
	binding := c
	binding.InitialVoters = nil
	if err := bindIdentity(binding, false); err != nil {
		t.Fatal(err)
	}
	for _, node := range g.nodes {
		if err := os.RemoveAll(node.config.StateDir); err != nil {
			t.Fatal(err)
		}
	}
	// Only replicated content survives. All three old state roots are unused.
	// An operator-staged, private off-node copy is an equally valid source.
	privateBackup := filepath.Join(c.StateDir, "recovery-backup", "selected.enc")
	if err := os.Mkdir(filepath.Dir(privateBackup), 0700); err != nil {
		t.Fatal(err)
	}
	encrypted, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(privateBackup, encrypted, 0600); err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(restoreSingletonRequest{ClusterID: c.ClusterID, NodeID: c.Local.ID, BackupFile: privateBackup})
	if err := os.WriteFile(filepath.Join(c.StateDir, "restore-singleton.json"), request, 0600); err != nil {
		t.Fatal(err)
	}
	issue := g.issue
	restored, err := StartTLS(c, c.Local.Address, issue(c.Local.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	await(t, func() bool { return restored.raft.State() == raft.Leader && !restored.generationPending.Load() })
	awaitAsync(t, restored)
	records := restored.state.read([]string{"sessions/owner", "projects/project", "owner_projects/owner", "projects/lostlater"})
	if len(records) != 3 || records["projects/lostlater"].Version != 0 {
		t.Fatal("restore lost pre-backup state or retained later state", records)
	}
	before := n.state.read([]string{"projects/project"})["projects/project"].Version
	if restored.Status().ReplicatedRevision < before {
		t.Fatal("revision ceiling went backwards")
	}
	done, _ := filepath.Glob(filepath.Join(c.StateDir, "restore-singleton.done-*"))
	if len(done) != 1 {
		t.Fatal("request not consumed", done)
	}
	restored.Close()
	again, err := StartTLS(c, c.Local.Address, issue(c.Local.ID))
	if err != nil {
		t.Fatal("second restart", err)
	}
	defer again.Close()
	await(t, func() bool { return again.raft.State() == raft.Leader && !again.generationPending.Load() })
}

func TestRestoreRefusalsLeaveStateUnchanged(t *testing.T) {
	passphrase := strings.Repeat("restore-secret-", 4)
	t.Setenv(PassphraseEnv, passphrase)
	g := newRecoveryTLSGroup(t, false)
	n := g.nodes[g.leader(t, -1)]
	if err := n.Commit(context.Background(), transaction("sessions/s", 0, `true`)); err != nil {
		t.Fatal(err)
	}
	if _, err := n.writeMetadataBackup(passphrase, ""); err != nil {
		t.Fatal(err)
	}
	paths, _ := filepath.Glob(filepath.Join(n.config.ContentDir, "metadata-backups", "*.enc"))
	data, _ := os.ReadFile(paths[0])
	for _, node := range g.nodes {
		node.Close()
	}
	for _, name := range []string{"tampered", "wrong-cluster", "wrong-key", "existing", "symlink", "malformed", "both"} {
		t.Run(name, func(t *testing.T) {
			c := n.config
			c.StateDir = t.TempDir()
			os.Chmod(c.StateDir, 0700)
			c.InitialVoters = nil
			path := filepath.Join(c.ContentDir, "metadata-backups", "test-"+name+".enc")
			payload := bytes.Clone(data)
			if name == "tampered" {
				payload[len(payload)-1] ^= 1
			}
			if name == "wrong-cluster" {
				payload = bytes.Replace(payload, []byte(c.ClusterID), []byte(strings.Repeat("d", 32)), 1)
			}
			if err := os.WriteFile(path, payload, 0600); err != nil {
				t.Fatal(err)
			}
			if name == "symlink" {
				os.Remove(path)
				os.Symlink(paths[0], path)
			}
			if name == "wrong-key" {
				t.Setenv(PassphraseEnv, strings.Repeat("bad-secret", 5))
			}
			if name == "existing" {
				c.StateDir = n.config.StateDir
			}
			request, _ := json.Marshal(restoreSingletonRequest{ClusterID: c.ClusterID, NodeID: c.Local.ID, BackupFile: path})
			if name == "malformed" {
				request = []byte(`{"nodeID":`)
			}
			os.WriteFile(filepath.Join(c.StateDir, "restore-singleton.json"), request, 0600)
			if name == "both" {
				raw, _ := json.Marshal(singletonRequest{ClusterID: c.ClusterID, NodeID: c.Local.ID, ExpectedLastIndex: 1})
				os.WriteFile(filepath.Join(c.StateDir, "recover-singleton.json"), raw, 0600)
			}
			dbPath := filepath.Join(c.StateDir, "raft.db")
			before, beforeErr := os.ReadFile(dbPath)
			started, err := StartTLS(c, c.Local.Address, g.issue(c.Local.ID))
			if err == nil {
				started.Close()
				t.Fatal("invalid restore accepted")
			}
			after, afterErr := os.ReadFile(dbPath)
			if !bytes.Equal(before, after) || (os.IsNotExist(beforeErr) != os.IsNotExist(afterErr)) {
				t.Fatal("refusal modified Raft state")
			}
		})
	}
}

func TestAutomaticStateSecurityAcceptsRecoveryArtifacts(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"pre-recovery-20261101T000000Z/snapshots", "recovery-backup"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0777); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"recover-singleton.json", "restore-singleton.json", "recover-singleton.done-20261101", "pre-recovery-20261101T000000Z/raft.db", "recovery-backup/raft.db"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(`{}`), 0666); err != nil {
			t.Fatal(err)
		}
	}
	if err := secureAutomaticStateTree(root); err != nil {
		t.Fatal(err)
	}
	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		want := os.FileMode(0600)
		if info.IsDir() {
			want = 0700
		}
		if info.Mode().Perm() != want {
			t.Errorf("wrong recovery artifact permissions: %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

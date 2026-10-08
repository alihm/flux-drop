package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareAutomaticStateDirectoryOnFluxStyleMount(t *testing.T) {
	mount := t.TempDir()
	if err := os.Chmod(mount, 0777); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(mount, "private")
	if err := prepareAutomaticStateDirectory(mount, state); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(state)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("private state mode = %v, %v", info, err)
	}
	if err := os.WriteFile(filepath.Join(state, "node.key"), []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(state, 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(state, "node.key"), 0777); err != nil {
		t.Fatal(err)
	}
	if repaired, err := MaintainAutomaticState(state); err != nil || !repaired {
		t.Fatalf("live permission repair = %v, %v", repaired, err)
	}
	if repaired, err := MaintainAutomaticState(state); err != nil || repaired {
		t.Fatalf("already private state repaired again = %v, %v", repaired, err)
	}
	if err := prepareAutomaticStateDirectory(mount, state); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]os.FileMode{"": 0700, "node.key": 0600} {
		info, err := os.Stat(filepath.Join(state, name))
		if err != nil || info.Mode().Perm() != want {
			t.Fatalf("%s mode = %v, %v; want %v", name, info, err, want)
		}
	}
}

func TestPrepareAutomaticStateDirectoryRejectsLegacyAndLinks(t *testing.T) {
	mount := t.TempDir()
	state := filepath.Join(mount, "private")
	if err := os.WriteFile(filepath.Join(mount, "raft.db"), []byte("old state"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepareAutomaticStateDirectory(mount, state); err == nil || !strings.Contains(err.Error(), "explicit migration") {
		t.Fatalf("legacy state accepted: %v", err)
	}
	if err := os.Remove(filepath.Join(mount, "raft.db")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), state); err != nil {
		t.Fatal(err)
	}
	if err := prepareAutomaticStateDirectory(mount, state); err == nil {
		t.Fatal("symlinked private state accepted")
	}
}

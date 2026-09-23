package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCoordinatorProvisioning(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "cluster.json")
	if enabled, err := coordinatorConfigured(path); enabled || err != nil {
		t.Fatal("absent manifest must preserve existing runtime", enabled, err)
	}
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if enabled, err := coordinatorConfigured(path); !enabled || err != nil {
		t.Fatal("manifest must enable coordinator", enabled, err)
	}
	if _, err := coordinatorConfigured(root); err == nil {
		t.Fatal("directory accepted as manifest")
	}
}

func TestSingleContainerChildren(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		children := childCommands(enabled)
		want := 2
		if enabled {
			want = 3
		}
		if len(children) != want || children[0].Path != "/usr/local/bin/drop" || children[1].Args[0] != "nginx" {
			t.Fatal("unexpected supervised services", children)
		}
		if enabled && (children[2].Path != "/usr/local/bin/drop-cluster" || children[2].Args[2] != clusterManifest) {
			t.Fatal("missing coordinator manifest argument")
		}
	}
}

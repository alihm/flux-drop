package cluster

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// prepareAutomaticStateDirectory keeps credentials and Raft state below the
// Flux-owned ml: bind root. Flux creates that root for arbitrary container UIDs
// and grants it broad permissions; a non-root image cannot chmod or chown it.
// The child directory and every entry within it must belong to Drop's UID.
func prepareAutomaticStateDirectory(mountRoot, stateDir string) error {
	if filepath.Dir(stateDir) != mountRoot {
		return errors.New("private state must be directly inside the node-local mount")
	}
	root, err := os.Lstat(mountRoot)
	if err != nil {
		return fmt.Errorf("node-local state mount: %w", err)
	}
	if !root.IsDir() || root.Mode()&os.ModeSymlink != 0 {
		return errors.New("node-local state mount must be a real directory")
	}
	// Never discard an identity left by an older layout. The operator must
	// explicitly migrate it before this image can use the new private layout.
	for _, name := range []string{"cluster-node.json", "identity.json", "raft.db", "node.key", "cluster-ca.json", "content-journal.db", "primary-history.json", "bootstrap-intent.json", "snapshots"} {
		if _, err := os.Lstat(filepath.Join(mountRoot, name)); err == nil {
			return fmt.Errorf("legacy node state at mount root (%s); explicit migration required", name)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.Mkdir(stateDir, 0700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("create private node state: %w", err)
	}
	return secureAutomaticStateTree(stateDir)
}

// Flux may run a host-side chmod -R 777 as part of an app redeploy. Restore
// owner-only modes before opening any persisted key or Raft database. Reject
// links and foreign-owned entries rather than following or rewriting them.
func secureAutomaticStateTree(stateDir string) error {
	return filepath.WalkDir(stateDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("unsafe node-local state entry: %s", path)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != uint32(os.Geteuid()) {
			return fmt.Errorf("node-local state entry is not owned by Drop: %s", path)
		}
		want := fs.FileMode(0600)
		if info.IsDir() {
			want = 0700
		}
		if info.Mode().Perm() != want {
			if err := os.Chmod(path, want); err != nil {
				return fmt.Errorf("secure node-local state entry %s: %w", path, err)
			}
		}
		return nil
	})
}

// MaintainAutomaticState is a cheap live guard against Flux's host-side
// permission sweeps. A sweep widens the private root along with its contents;
// when that happens, restore the whole tree immediately. A foreign owner or
// replacement link stops the supervisor instead of being silently trusted.
func MaintainAutomaticState(stateDir string) (bool, error) {
	info, err := os.Lstat(stateDir)
	if err != nil {
		return false, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !ok || stat.Uid != uint32(os.Geteuid()) {
		return false, errors.New("private node-local state directory was replaced or changed owner")
	}
	if info.Mode().Perm() == 0700 {
		return false, nil
	}
	if err := secureAutomaticStateTree(stateDir); err != nil {
		return false, err
	}
	return true, nil
}

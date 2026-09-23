package content

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var versionDigestRE = regexp.MustCompile(`^[a-f0-9]{64}$`)
var projectIDRE = regexp.MustCompile(`^[a-f0-9]{32}$`)
var markerRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}-[a-f0-9]{6}$`)

// VerifyVersion validates actual bytes and rejects symlinks, unexpected files,
// missing files, malformed manifests and noncanonical file ordering. Replicas
// must run this before treating a newly replicated version as ready.
func VerifyVersion(directory, digest string) (Manifest, error) {
	if !versionDigestRE.MatchString(digest) {
		return Manifest{}, ErrInvalid
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return Manifest{}, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return Manifest{}, ErrInvalid
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return Manifest{}, err
	}
	defer root.Close()
	f, err := root.Open("manifest.json")
	if err != nil {
		return Manifest{}, err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, (8<<20)+1))
	_ = f.Close()
	if readErr != nil {
		return Manifest{}, readErr
	}
	if len(data) > 8<<20 {
		return Manifest{}, ErrLimit
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, ErrInvalid
	}
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return Manifest{}, err
	}
	computed := sha256.Sum256(canonical)
	if hex.EncodeToString(computed[:]) != digest || manifest.Schema != 1 || len(manifest.Files) == 0 || len(manifest.Files) > 5000 {
		return Manifest{}, ErrInvalid
	}
	expected := make(map[string]File, len(manifest.Files))
	var total int64
	previous := ""
	hasIndex := false
	for _, item := range manifest.Files {
		if validateFile(item.Path) != nil || item.Path <= previous || item.Size < 0 || item.Size > 200<<20 || !versionDigestRE.MatchString(item.SHA256) {
			return Manifest{}, ErrInvalid
		}
		total += item.Size
		if total > 200<<20 {
			return Manifest{}, ErrLimit
		}
		previous = item.Path
		expected["public/"+item.Path] = item
		if item.Path == "index.html" {
			hasIndex = true
		}
	}
	if !hasIndex {
		return Manifest{}, ErrInvalid
	}
	seen := 0
	err = fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: version contains a symlink", ErrInvalid)
		}
		if entry.IsDir() {
			if name != "." && name != "public" && !strings.HasPrefix(name, "public/") {
				return ErrInvalid
			}
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return ErrInvalid
		}
		if name == "manifest.json" {
			return nil
		}
		item, ok := expected[name]
		if !ok || info.Size() != item.Size {
			return ErrInvalid
		}
		file, err := root.Open(name)
		if err != nil {
			return err
		}
		hash := sha256.New()
		n, copyErr := io.Copy(hash, io.LimitReader(file, item.Size+1))
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if n != item.Size || hex.EncodeToString(hash.Sum(nil)) != item.SHA256 {
			return ErrInvalid
		}
		seen++
		return nil
	})
	if err != nil {
		return Manifest{}, err
	}
	if seen != len(manifest.Files) {
		return Manifest{}, ErrInvalid
	}
	return manifest, nil
}

// Install moves an owned staging tree into its immutable content-addressed
// location. Both roots must be on the same filesystem. It does not activate
// metadata; the caller does that only after this method succeeds.
func (s *Staged) Install(dataRoot, projectID, slug string) error {
	return s.install(dataRoot, projectID, slug, "versions", s.Digest)
}

// InstallGeneration installs immutable content at a storage identity independent
// of its digest. Callers must allocate a stable 64-hex generation per operation;
// retries reuse that identity, while restores receive a new one. A generation
// must never be reused after retirement, even if its bytes have been removed.
// This primitive does not allocate or authorize a generation in metadata.
func (s *Staged) InstallGeneration(dataRoot, projectID, slug, generation string) error {
	if !versionDigestRE.MatchString(generation) {
		return ErrInvalid
	}
	return s.install(dataRoot, projectID, slug, "generations", generation)
}

// GenerationPath returns only a validated path relative to the data root.
// Content digest verification remains separate from selecting this directory.
func GenerationPath(projectID, generation string) (string, error) {
	if !projectIDRE.MatchString(projectID) || !versionDigestRE.MatchString(generation) {
		return "", ErrInvalid
	}
	return filepath.Join("projects", projectID, "generations", generation), nil
}

func (s *Staged) install(dataRoot, projectID, slug, namespace, versionID string) error {
	if s.ownedDirectory == "" || s.Directory != s.ownedDirectory || !projectIDRE.MatchString(projectID) || !markerRE.MatchString(slug) {
		return ErrInvalid
	}
	if _, err := VerifyVersion(s.Directory, s.Digest); err != nil {
		return err
	}
	projectDir := filepath.Join(dataRoot, "projects", projectID)
	versions := filepath.Join(projectDir, namespace)
	if err := os.MkdirAll(versions, 0700); err != nil {
		return err
	}
	// Persist the stable project identifier outside every served public tree.
	marker := filepath.Join(projectDir, "hash")
	m, err := os.CreateTemp(projectDir, ".hash-")
	if err != nil {
		return err
	}
	temporary := m.Name()
	defer os.Remove(temporary)
	_, writeErr := m.WriteString(slug)
	syncErr := m.Sync()
	closeErr := m.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	// Publish a fully written marker atomically; concurrent retries cannot see
	// an empty marker or overwrite a different existing identifier.
	if err := os.Link(temporary, marker); err != nil {
		if !os.IsExist(err) {
			return err
		}
		b, err := os.ReadFile(marker)
		if err != nil {
			return err
		}
		if string(b) != slug {
			return ErrInvalid
		}
	}
	if err := syncTree(s.Directory); err != nil {
		return err
	}
	destination := filepath.Join(versions, versionID)
	if _, err := os.Lstat(destination); err == nil {
		if _, err := VerifyVersion(destination, s.Digest); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	} else if err := os.Rename(s.Directory, destination); err != nil {
		// Another retry may have installed identical content concurrently.
		if _, verifyErr := VerifyVersion(destination, s.Digest); verifyErr != nil {
			return err
		}
	} else {
		s.ownedDirectory = ""
	}
	if err := syncTree(destination); err != nil {
		return err
	}
	for _, directory := range []string{destination, versions, projectDir, filepath.Dir(projectDir), dataRoot} {
		if err := syncPath(directory); err != nil {
			return err
		}
	}
	return nil
}

func syncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
func syncTree(root string) error {
	var directories []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return ErrInvalid
		}
		if entry.IsDir() {
			directories = append(directories, path)
			return nil
		}
		return syncPath(path)
	})
	if err != nil {
		return err
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := syncPath(directories[i]); err != nil {
			return err
		}
	}
	return nil
}

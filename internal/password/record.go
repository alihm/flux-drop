package password

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var idPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type Record struct {
	ProjectID      string `json:"projectId"`
	PolicyRevision int64  `json:"policyRevision"`
	Hash           Hash   `json:"hash"`
}

func (r Record) bytes() ([]byte, error) {
	if !idPattern.MatchString(r.ProjectID) || r.PolicyRevision < 1 {
		return nil, ErrInvalid
	}
	if _, _, err := r.Hash.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}

// Store installs immutable, content-addressed metadata outside every public
// version tree. Firestore must select the winning digest/revision transactionally
// only after this returns. Orphaned records grant no authority.
func Store(dataRoot string, record Record) (string, error) {
	data, err := record.bytes()
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	digest := hex.EncodeToString(h[:])
	root, err := os.OpenRoot(dataRoot)
	if err != nil {
		return "", err
	}
	defer root.Close()
	directory := filepath.Join("projects", record.ProjectID, "metadata", digest)
	current := ""
	for _, part := range strings.Split(directory, string(os.PathSeparator)) {
		current = filepath.Join(current, part)
		if err := root.Mkdir(current, 0700); err != nil && !os.IsExist(err) {
			return "", err
		}
		info, err := root.Lstat(current)
		if err != nil {
			return "", err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", ErrInvalid
		}
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	temp := filepath.Join(directory, ".password-"+hex.EncodeToString(random[:]))
	f, err := root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	defer root.Remove(temp)
	if _, err := f.Write(data); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	target := filepath.Join(directory, "password.json")
	if err := root.Link(temp, target); err != nil {
		if !os.IsExist(err) {
			return "", err
		}
		if _, err := Load(dataRoot, record.ProjectID, record.PolicyRevision, digest); err != nil {
			return "", err
		}
	}
	for p := directory; ; p = filepath.Dir(p) {
		d, err := root.Open(p)
		if err != nil {
			return "", err
		}
		err = d.Sync()
		d.Close()
		if err != nil {
			return "", err
		}
		if p == "." {
			break
		}
	}
	return digest, nil
}

// Load binds actual bytes to the authoritative digest, project and policy.
func Load(dataRoot, id string, revision int64, digest string) (Record, error) {
	if !idPattern.MatchString(id) || !digestPattern.MatchString(digest) || revision < 1 {
		return Record{}, ErrInvalid
	}
	root, err := os.OpenRoot(dataRoot)
	if err != nil {
		return Record{}, err
	}
	defer root.Close()
	path := "projects/" + id + "/metadata/" + digest + "/password.json"
	info, err := root.Lstat(path)
	if err != nil {
		return Record{}, err
	}
	if !info.Mode().IsRegular() {
		return Record{}, ErrInvalid
	}
	f, err := root.Open(path)
	if err != nil {
		return Record{}, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return Record{}, err
	}
	if len(data) > 4096 {
		return Record{}, ErrInvalid
	}
	h := sha256.Sum256(data)
	if hex.EncodeToString(h[:]) != digest {
		return Record{}, ErrInvalid
	}
	var record Record
	if json.Unmarshal(data, &record) != nil || record.ProjectID != id || record.PolicyRevision != revision {
		return Record{}, ErrInvalid
	}
	if _, err := record.bytes(); err != nil {
		return Record{}, err
	}
	return record, nil
}

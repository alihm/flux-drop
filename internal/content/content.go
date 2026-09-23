// Package content validates untrusted static uploads and stages immutable content.
// It does not authorize publication or make staged files publicly accessible.
package content

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var ErrInvalid = errors.New("invalid static upload")
var ErrLimit = errors.New("upload limit exceeded")

type Limits struct {
	UploadBytes   int64 `json:"uploadBytes"`
	ExpandedBytes int64 `json:"expandedBytes"`
	Files         int   `json:"files"`
}

func DefaultLimits() Limits { return Limits{50 << 20, 200 << 20, 5000} }

func (l Limits) Validate() error {
	if l.UploadBytes <= 0 || l.ExpandedBytes <= 0 || l.UploadBytes > 1<<40 || l.ExpandedBytes > 1<<40 || l.Files <= 0 || l.Files > 100000 {
		return fmt.Errorf("limits must be positive; byte limits must not exceed 1 TiB; file count must not exceed 100000")
	}
	return nil
}

type File struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Manifest has no timestamps or upload names: equal trees hash identically.
type Manifest struct {
	Schema int    `json:"schema"`
	Files  []File `json:"files"`
}

type Staged struct {
	Directory      string
	Digest         string
	Manifest       Manifest
	ownedDirectory string
}

// Discard removes only the private directory created by this package, even if
// a caller modified the public metadata. Call after failed publication.
func (s *Staged) Discard() error {
	if s.ownedDirectory == "" {
		return nil
	}
	return os.RemoveAll(s.ownedDirectory)
}

// Source is an upload part, not an existing user-selected filesystem path.
// Open must return a fresh reader. HTTP callers must also bound the entire body.
type Source struct {
	Name string
	Open func() (io.ReadCloser, error)
}

var segmentPattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_. -]*$`)
var allowedExtensions = map[string]bool{
	".html": true, ".htm": true, ".css": true, ".js": true, ".mjs": true,
	".json": true, ".webmanifest": true, ".txt": true, ".xml": true,
	".svg": true, ".png": true, ".jpg": true, ".jpeg": true, ".gif": true,
	".webp": true, ".avif": true, ".ico": true, ".woff": true, ".woff2": true,
	".ttf": true, ".otf": true, ".mp3": true, ".mp4": true, ".ogg": true,
	".webm": true, ".wav": true, ".pdf": true, ".wasm": true,
}

// ValidatePath deliberately rejects ambiguous names rather than repairing them.
// Encoded separators, dotfiles, Windows device paths and case aliases are not
// accepted. This initial ASCII filename policy is documented and can be extended
// only with a tested Unicode normalization policy.
func ValidatePath(name string) error {
	if name == "" || len(name) > 1024 || strings.ContainsAny(name, "\\%:#?\x00") || path.Clean(name) != name || strings.HasPrefix(name, "/") {
		return fmt.Errorf("%w: unsafe path", ErrInvalid)
	}
	parts := strings.Split(name, "/")
	if len(parts) > 20 {
		return fmt.Errorf("%w: excessive path depth", ErrInvalid)
	}
	for _, part := range parts {
		if len(part) > 200 || !segmentPattern.MatchString(part) || strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
			return fmt.Errorf("%w: unsupported filename", ErrInvalid)
		}
		base := strings.ToUpper(strings.SplitN(part, ".", 2)[0])
		if base == "CON" || base == "PRN" || base == "AUX" || base == "NUL" || (len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '0' && base[3] <= '9') {
			return fmt.Errorf("%w: reserved filename", ErrInvalid)
		}
	}
	return nil
}

func validateFile(name string) error {
	if err := ValidatePath(name); err != nil {
		return err
	}
	base := strings.ToLower(path.Base(name))
	if !allowedExtensions[strings.ToLower(path.Ext(name))] {
		return fmt.Errorf("%w: unsupported file type", ErrInvalid)
	}
	// Obvious build/source configuration should never be silently published.
	for _, forbidden := range []string{"package.json", "package-lock.json", "composer.json", "credentials.json", "service-account.json", "firebase-adminsdk.json", "tsconfig.json"} {
		if base == forbidden {
			return fmt.Errorf("%w: source or credential file", ErrInvalid)
		}
	}
	return nil
}

// StageZIP checks the compressed size before reading the central directory.
// Readers passed by HTTP handlers must be bounded/spooled before this call.
func StageZIP(parent string, reader io.ReaderAt, size int64, limits Limits) (*Staged, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if size < 0 || size > limits.UploadBytes {
		return nil, ErrLimit
	}
	z, err := zip.NewReader(reader, size)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed ZIP", ErrInvalid)
	}
	if len(z.File) > limits.Files*2 {
		return nil, ErrLimit
	}
	sources := make([]Source, 0, len(z.File))
	for _, f := range z.File {
		if f.Flags&1 != 0 || (f.Method != zip.Store && f.Method != zip.Deflate) {
			return nil, fmt.Errorf("%w: unsupported ZIP entry", ErrInvalid)
		}
		if f.FileInfo().IsDir() {
			if err := ValidatePath(strings.TrimSuffix(f.Name, "/")); err != nil {
				return nil, err
			}
			continue
		}
		if !f.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: non-regular file", ErrInvalid)
		}
		if f.UncompressedSize64 > uint64(limits.ExpandedBytes) {
			return nil, ErrLimit
		}
		entry := f
		sources = append(sources, Source{Name: f.Name, Open: entry.Open})
	}
	return stage(parent, sources, true, limits.ExpandedBytes, limits)
}

// StageFolder accepts paths supplied by a folder picker or multipart upload.
// A single common enclosing directory is stripped only if root index is absent.
func StageFolder(parent string, sources []Source, limits Limits) (*Staged, error) {
	return stage(parent, sources, true, limits.UploadBytes, limits)
}

// StageHTML names a single uploaded document index.html regardless of its name.
func StageHTML(parent string, reader io.Reader, limits Limits) (*Staged, error) {
	return stage(parent, []Source{{Name: "index.html", Open: func() (io.ReadCloser, error) { return io.NopCloser(reader), nil }}}, false, limits.UploadBytes, limits)
}

func stage(parent string, input []Source, unwrap bool, byteLimit int64, limits Limits) (_ *Staged, err error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if len(input) == 0 {
		return nil, fmt.Errorf("%w: empty project", ErrInvalid)
	}
	if len(input) > limits.Files {
		return nil, ErrLimit
	}
	if byteLimit > limits.ExpandedBytes {
		byteLimit = limits.ExpandedBytes
	}
	sources := append([]Source(nil), input...)
	for _, src := range sources {
		if err := validateFile(src.Name); err != nil {
			return nil, err
		}
		if src.Open == nil {
			return nil, fmt.Errorf("%w: missing file reader", ErrInvalid)
		}
	}
	if unwrap {
		hasIndex := false
		for _, src := range sources {
			if src.Name == "index.html" {
				hasIndex = true
			}
		}
		if !hasIndex {
			prefix, _, ok := strings.Cut(sources[0].Name, "/")
			if ok {
				prefix += "/"
				common := true
				for _, src := range sources {
					if !strings.HasPrefix(src.Name, prefix) {
						common = false
					}
				}
				if common {
					for i := range sources {
						sources[i].Name = strings.TrimPrefix(sources[i].Name, prefix)
					}
				}
			}
		}
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Name < sources[j].Name })
	seen := make(map[string]bool)
	spellings := make(map[string]string)
	index := false
	for _, src := range sources {
		for part := src.Name; part != "."; part = path.Dir(part) {
			folded := strings.ToLower(part)
			if previous, exists := spellings[folded]; exists && previous != part {
				return nil, fmt.Errorf("%w: inconsistent path casing", ErrInvalid)
			}
			spellings[folded] = part
		}
		key := strings.ToLower(src.Name)
		if seen[key] {
			return nil, fmt.Errorf("%w: duplicate filename", ErrInvalid)
		}
		seen[key] = true
		if src.Name == "index.html" {
			index = true
		}
	}
	for name := range seen {
		for ancestor := path.Dir(name); ancestor != "."; ancestor = path.Dir(ancestor) {
			if seen[ancestor] {
				return nil, fmt.Errorf("%w: file/directory conflict", ErrInvalid)
			}
		}
	}
	if !index {
		return nil, fmt.Errorf("%w: root index.html required", ErrInvalid)
	}
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(parent, "upload-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dir)
		}
	}()
	manifest := Manifest{Schema: 1, Files: make([]File, 0, len(sources))}
	var total int64
	for _, src := range sources {
		file, writeErr := writeSource(filepath.Join(dir, "public"), src, byteLimit-total)
		if writeErr != nil {
			return nil, writeErr
		}
		total += file.Size
		manifest.Files = append(manifest.Files, file)
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), encoded, 0600); err != nil {
		return nil, err
	}
	return &Staged{Directory: dir, ownedDirectory: dir, Digest: hex.EncodeToString(digest[:]), Manifest: manifest}, nil
}

func writeSource(root string, src Source, remaining int64) (File, error) {
	dest := filepath.Join(root, filepath.FromSlash(src.Name))
	if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		return File{}, err
	}
	in, err := src.Open()
	if err != nil {
		return File{}, err
	}
	defer in.Close()
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return File{}, err
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, hash), io.LimitReader(in, remaining+1))
	closeErr := out.Close()
	if copyErr != nil {
		return File{}, fmt.Errorf("%w: file read failed: %w", ErrInvalid, copyErr)
	}
	if closeErr != nil {
		return File{}, closeErr
	}
	if n > remaining {
		return File{}, ErrLimit
	}
	return File{Path: src.Name, Size: n, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

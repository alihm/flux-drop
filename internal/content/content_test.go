package content

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func source(name, data string) Source {
	return Source{Name: name, Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(data)), nil }}
}

func archive(t *testing.T, sources []Source) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for _, src := range sources {
		f, err := w.Create(src.Name)
		if err != nil {
			t.Fatal(err)
		}
		r, _ := src.Open()
		_, err = io.Copy(f, r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestEquivalentUploadsHaveSameDigest(t *testing.T) {
	files := []Source{source("index.html", "<h1>Hello</h1>"), source("assets/main.js", "console.log('hello')")}
	folder, err := StageFolder(t.TempDir(), files, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	z := archive(t, []Source{source("wrapper/assets/main.js", "console.log('hello')"), source("wrapper/index.html", "<h1>Hello</h1>")})
	packed, err := StageZIP(t.TempDir(), bytes.NewReader(z), int64(len(z)), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if folder.Digest != packed.Digest || len(folder.Digest) != 64 {
		t.Fatal("equivalent trees must hash identically")
	}
	if _, err := os.Stat(filepath.Join(packed.Directory, "public", "index.html")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(packed.Directory, "public", "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("manifest must not be public")
	}
}

func TestDigestIncludesPathsAndContents(t *testing.T) {
	var digests []string
	for _, files := range [][]Source{
		{source("index.html", "a"), source("a.js", "x")},
		{source("index.html", "b"), source("a.js", "x")},
		{source("index.html", "a"), source("b.js", "x")},
	} {
		s, err := StageFolder(t.TempDir(), files, DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		digests = append(digests, s.Digest)
	}
	if digests[0] == digests[1] || digests[0] == digests[2] {
		t.Fatal("digest ignored path or content")
	}
}

func TestRejectUnsafePaths(t *testing.T) {
	for _, name := range []string{"../index.html", "/index.html", "a/../../index.html", `a\index.html`, "a//index.html", "a/./index.html", "%2e%2e/index.html", ".env", ".git/config", "a/.hidden.js", "C:/index.html", "index.html:stream", "a\x00.js", "con.html", "a./x.js", "a /x.js", "a?x.js", "a#x.js"} {
		t.Run(name, func(t *testing.T) {
			if ValidatePath(name) == nil {
				t.Fatal("accepted unsafe path")
			}
		})
	}
}

func TestRejectInvalidProjects(t *testing.T) {
	for name, files := range map[string][]Source{
		"empty":              {},
		"no index":           {source("hello.html", "x")},
		"case duplicate":     {source("index.html", "a"), source("INDEX.html", "b")},
		"exact duplicate":    {source("index.html", "a"), source("index.html", "b")},
		"directory conflict": {source("index.html", "a"), source("index.html/x.js", "b")},
		"php":                {source("index.html", "a"), source("shell.php", "b")},
		"source config":      {source("index.html", "a"), source("package.json", "{}")},
		"secret":             {source("index.html", "a"), source(".env", "secret")},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := StageFolder(t.TempDir(), files, DefaultLimits())
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected invalid: %v", err)
			}
		})
	}
}

func TestLimitsAndCleanup(t *testing.T) {
	parent := t.TempDir()
	limits := Limits{UploadBytes: 10, ExpandedBytes: 10, Files: 2}
	_, err := StageFolder(parent, []Source{source("index.html", "123456"), source("a.js", "123456")}, limits)
	if !errors.Is(err, ErrLimit) {
		t.Fatalf("expected limit: %v", err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("failed staging left content behind")
	}
	_, err = StageFolder(parent, []Source{source("index.html", ""), source("a.js", ""), source("b.js", "")}, limits)
	if !errors.Is(err, ErrLimit) {
		t.Fatal("file count limit not enforced")
	}
	_, err = StageHTML(parent, strings.NewReader("12345678901"), limits)
	if !errors.Is(err, ErrLimit) {
		t.Fatal("HTML upload limit not enforced")
	}
}

func TestZipBombAndSymlink(t *testing.T) {
	z := archive(t, []Source{source("index.html", strings.Repeat("x", 8192))})
	_, err := StageZIP(t.TempDir(), bytes.NewReader(z), int64(len(z)), Limits{UploadBytes: 4096, ExpandedBytes: 4096, Files: 2})
	if !errors.Is(err, ErrLimit) {
		t.Fatalf("expanded limit not enforced: %v", err)
	}
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	h := &zip.FileHeader{Name: "index.html", Method: zip.Store}
	h.SetMode(os.ModeSymlink | 0777)
	f, err := w.CreateHeader(h)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("/etc/passwd"))
	_ = w.Close()
	_, err = StageZIP(t.TempDir(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), DefaultLimits())
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("symlink accepted: %v", err)
	}
}

func TestZipTraversalAndCompressedLimit(t *testing.T) {
	z := archive(t, []Source{source("../index.html", "bad")})
	_, err := StageZIP(t.TempDir(), bytes.NewReader(z), int64(len(z)), DefaultLimits())
	if !errors.Is(err, ErrInvalid) {
		t.Fatal("ZIP traversal accepted")
	}
	_, err = StageZIP(t.TempDir(), bytes.NewReader(z), int64(len(z)), Limits{UploadBytes: 1, ExpandedBytes: 100, Files: 2})
	if !errors.Is(err, ErrLimit) {
		t.Fatal("compressed limit not enforced")
	}
}

func TestSingleHTMLAndDiscard(t *testing.T) {
	s, err := StageHTML(t.TempDir(), strings.NewReader("<!doctype html><h1>Hi</h1>"), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Manifest.Files) != 1 || s.Manifest.Files[0].Path != "index.html" {
		t.Fatal("wrong HTML root")
	}
	if err := s.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.Directory); !os.IsNotExist(err) {
		t.Fatal("discard failed")
	}
}

func FuzzValidatePath(f *testing.F) {
	for _, seed := range []string{"index.html", "../a", "a%2fb", "a/b.js", "a\\b", "a/./b", ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if ValidatePath(name) == nil {
			if filepath.IsAbs(name) || strings.Contains(name, "\\") || strings.Contains(name, "%") {
				t.Fatalf("unsafe accepted path: %q", name)
			}
			for _, part := range strings.Split(name, "/") {
				if part == "" || part == "." || part == ".." {
					t.Fatalf("unsafe segment: %q", name)
				}
			}
		}
	})
}

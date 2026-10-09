package content

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func assertFileErrors(t *testing.T, err error, want []string) *FileErrors {
	t.Helper()
	var files *FileErrors
	if !errors.Is(err, ErrInvalid) || !errors.As(err, &files) {
		t.Fatalf("expected file diagnostics wrapping ErrInvalid: %v", err)
	}
	if !slices.Equal(files.Paths(), want) {
		t.Fatalf("got paths %q, want %q: %v", files.Paths(), want, err)
	}
	for _, name := range want {
		if !strings.Contains(err.Error(), strconv.Quote(name)) {
			t.Fatalf("message omitted path %q: %v", name, err)
		}
	}
	return files
}

func TestUploadFileDiagnosticsZIPAndFolder(t *testing.T) {
	sources := []Source{source("site/index.html", "hi"), source("site/assets/app.js.map", "{}"), source("site/.env", "secret"), source("site/CNAME", "example.com")}
	want := []string{"site/assets/app.js.map", "site/.env", "site/CNAME"}
	for _, kind := range []string{"zip", "folder"} {
		t.Run(kind, func(t *testing.T) {
			parent := t.TempDir()
			var err error
			if kind == "zip" {
				packed := archive(t, sources)
				_, err = StageZIP(parent, bytes.NewReader(packed), int64(len(packed)), DefaultLimits())
			} else {
				_, err = StageFolder(parent, sources, DefaultLimits())
			}
			assertFileErrors(t, err, want)
			for _, reason := range []string{"unsupported file type (.map)", "unsupported filename", "unsupported file type (no extension)"} {
				if !strings.Contains(err.Error(), reason) {
					t.Fatalf("message omitted reason %q: %v", reason, err)
				}
			}
			entries, readErr := os.ReadDir(parent)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("invalid batch left staging files: %v %v", entries, readErr)
			}
		})
	}
}

func TestFileDiagnosticReasons(t *testing.T) {
	for _, tc := range []struct{ path, reason string }{
		{"/index.html", "unsafe path"},
		{"../index.html", "unsupported filename"},
		{".env", "unsupported filename"},
		{"con.html", "reserved filename"},
		{strings.Repeat("a/", 20) + "index.html", "excessive path depth"},
		{"assets/app.js.map", "unsupported file type (.map)"},
		{"CNAME", "unsupported file type (no extension)"},
		{"package.json", "source or credential file"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			err := validateFile(tc.path)
			assertFileErrors(t, err, []string{tc.path})
			if !strings.Contains(err.Error(), tc.reason) {
				t.Fatal(err)
			}
		})
	}
}

func TestFileDiagnosticsBoundedAndValidatedBeforeOpening(t *testing.T) {
	files := []Source{{Name: "index.html", Open: func() (io.ReadCloser, error) {
		t.Fatal("opened source in invalid batch")
		return nil, nil
	}}}
	var want []string
	for i := 0; i < 30; i++ {
		name := fmt.Sprintf("assets/invalid-%d.map", i)
		files = append(files, source(name, "x"), source(name, "duplicate invalid name"))
		if i < 20 {
			want = append(want, name)
		}
	}
	for _, kind := range []string{"folder", "zip"} {
		t.Run(kind, func(t *testing.T) {
			var err error
			if kind == "folder" {
				_, err = StageFolder(t.TempDir(), files, DefaultLimits())
			} else {
				packed := archive(t, append([]Source{source("index.html", "hi")}, files[1:]...))
				_, err = StageZIP(t.TempDir(), bytes.NewReader(packed), int64(len(packed)), DefaultLimits())
			}
			assertFileErrors(t, err, want)
		})
	}
}

func TestFileDiagnosticsIncludeStructuralConflicts(t *testing.T) {
	_, err := StageFolder(t.TempDir(), []Source{
		source("wrapper/index.html", "hi"), source("wrapper/invalid.map", "x"),
		source("wrapper/a.js", "a"), source("wrapper/a.js", "b"),
		source("wrapper/index.html/nested.js", "x"),
	}, DefaultLimits())
	assertFileErrors(t, err, []string{"wrapper/invalid.map", "wrapper/a.js", "wrapper/index.html/nested.js"})
	for _, reason := range []string{"duplicate filename", "file/directory conflict"} {
		if !strings.Contains(err.Error(), reason) {
			t.Fatal(err)
		}
	}
}

func TestFileDiagnosticsIncludeZIPDirectoriesAndSymlinks(t *testing.T) {
	var packed bytes.Buffer
	z := zip.NewWriter(&packed)
	for _, header := range []*zip.FileHeader{
		{Name: "index.html", Method: zip.Store},
		{Name: ".hidden/", Method: zip.Store},
		{Name: "link.js", Method: zip.Store},
		{Name: "app.js.map", Method: zip.Store},
	} {
		if header.Name == "link.js" {
			header.SetMode(os.ModeSymlink | 0777)
		}
		if _, err := z.CreateHeader(header); err != nil {
			t.Fatal(err)
		}
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := StageZIP(t.TempDir(), bytes.NewReader(packed.Bytes()), int64(packed.Len()), DefaultLimits())
	assertFileErrors(t, err, []string{".hidden/", "link.js", "app.js.map"})
}

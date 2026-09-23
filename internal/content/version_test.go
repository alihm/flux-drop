package content

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallVerifiesAndKeepsMetadataPrivate(t *testing.T) {
	root := t.TempDir()
	staged, err := StageHTML(filepath.Join(root, "staging"), strings.NewReader("<h1>hello</h1>"), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("a", 32)
	slug := "hello-" + staged.Digest[:6]
	if err := staged.Install(root, id, slug); err != nil {
		t.Fatal(err)
	}
	_ = staged.Discard()
	version := filepath.Join(root, "projects", id, "versions", staged.Digest)
	if _, err := VerifyVersion(version, staged.Digest); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(root, "projects", id, "hash"))
	if err != nil || string(marker) != slug {
		t.Fatal("incorrect marker")
	}
	if _, err := os.Stat(filepath.Join(version, "public", "hash")); !os.IsNotExist(err) {
		t.Fatal("marker exposed")
	}
	// Identical retries accept the existing verified version without overwriting.
	retry, _ := StageHTML(filepath.Join(root, "staging"), strings.NewReader("<h1>hello</h1>"), DefaultLimits())
	defer retry.Discard()
	if err := retry.Install(root, id, slug); err != nil {
		t.Fatal(err)
	}
}

func TestVersionRejectsIncompleteCorruptAndLinkedFiles(t *testing.T) {
	for _, attack := range []string{"missing", "corrupt", "extra", "symlink", "manifest"} {
		t.Run(attack, func(t *testing.T) {
			s, err := StageHTML(t.TempDir(), strings.NewReader("safe"), DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Discard()
			file := filepath.Join(s.Directory, "public", "index.html")
			switch attack {
			case "missing":
				err = os.Remove(file)
			case "corrupt":
				err = os.WriteFile(file, []byte("evil"), 0600)
			case "extra":
				err = os.WriteFile(filepath.Join(s.Directory, "public", "secret.txt"), []byte("secret"), 0600)
			case "manifest":
				err = os.WriteFile(filepath.Join(s.Directory, "manifest.json"), []byte(`{}`), 0600)
			case "symlink":
				if err = os.Remove(file); err == nil {
					err = os.Symlink("/etc/passwd", file)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := VerifyVersion(s.Directory, s.Digest); err == nil {
				t.Fatal("invalid replica marked ready")
			}
		})
	}
}

func TestInstallRejectsForgedLocationAndCorruptExistingVersion(t *testing.T) {
	root := t.TempDir()
	id := strings.Repeat("a", 32)
	s, _ := StageHTML(filepath.Join(root, "staging"), strings.NewReader("safe"), DefaultLimits())
	defer s.Discard()
	if err := s.Install(root, "../escape", "site-123456"); err == nil {
		t.Fatal("unsafe project ID accepted")
	}
	slug := "site-" + s.Digest[:6]
	if err := s.Install(root, id, slug); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "projects", id, "versions", s.Digest, "public", "index.html")
	if err := os.WriteFile(file, []byte("evil"), 0600); err != nil {
		t.Fatal(err)
	}
	retry, _ := StageHTML(filepath.Join(root, "staging"), strings.NewReader("safe"), DefaultLimits())
	defer retry.Discard()
	if err := retry.Install(root, id, slug); err == nil {
		t.Fatal("corrupt version accepted or overwritten")
	}
}

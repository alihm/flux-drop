package content

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestGenerationPaths(t *testing.T) {
	id, generation := strings.Repeat("a", 32), strings.Repeat("b", 64)
	got, err := GenerationPath(id, generation)
	if err != nil || got != filepath.Join("projects", id, "generations", generation) {
		t.Fatal(got, err)
	}
	for _, pair := range [][2]string{{"../escape", generation}, {id, "../escape"}, {id, ""}, {id, strings.Repeat("B", 64)}, {id, generation + "/extra"}} {
		if _, err := GenerationPath(pair[0], pair[1]); !errors.Is(err, ErrInvalid) {
			t.Fatal(pair, err)
		}
	}
}

func TestGenerationInstallRestoreIsolation(t *testing.T) {
	root := t.TempDir()
	id := strings.Repeat("a", 32)
	one, two := strings.Repeat("1", 64), strings.Repeat("2", 64)
	stage := func() *Staged {
		t.Helper()
		s, err := StageHTML(filepath.Join(root, "staging"), strings.NewReader("<h1>same bytes</h1>"), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Discard() })
		return s
	}
	first, second := stage(), stage()
	slug := "site-" + first.Digest[:6]
	if err := first.InstallGeneration(root, id, slug, one); err != nil {
		t.Fatal(err)
	}
	if err := second.InstallGeneration(root, id, slug, two); err != nil {
		t.Fatal(err)
	}
	firstPath, _ := GenerationPath(id, one)
	secondPath, _ := GenerationPath(id, two)
	for _, path := range []string{firstPath, secondPath} {
		if _, err := VerifyVersion(filepath.Join(root, path), first.Digest); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate damage to the old copy. A restored generation has independent
	// files and verification; it does not inherit a shared inode or directory.
	if err := os.WriteFile(filepath.Join(root, firstPath, "public", "index.html"), []byte("corrupt old copy"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyVersion(filepath.Join(root, secondPath), second.Digest); err != nil {
		t.Fatal("restored copy changed", err)
	}
	retry := stage()
	if err := retry.InstallGeneration(root, id, slug, two); err != nil {
		t.Fatal("identical retry", err)
	}
	if err := stage().InstallGeneration(root, id, slug, one); err == nil {
		t.Fatal("corrupt generation overwritten")
	}
	if _, err := os.Stat(filepath.Join(root, "projects", id, "versions", first.Digest)); !os.IsNotExist(err) {
		t.Fatal("generation install wrote legacy path", err)
	}
	marker, err := os.ReadFile(filepath.Join(root, "projects", id, "hash"))
	if err != nil || string(marker) != slug {
		t.Fatal("stable marker", string(marker), err)
	}
	if _, err := os.Stat(filepath.Join(root, secondPath, "public", "hash")); !os.IsNotExist(err) {
		t.Fatal("private marker exposed", err)
	}
}

func TestGenerationCannotBeRebound(t *testing.T) {
	root := t.TempDir()
	id := strings.Repeat("a", 32)
	generation := strings.Repeat("c", 64)
	first, err := StageHTML(filepath.Join(root, "staging"), strings.NewReader("original"), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Discard()
	slug := "site-" + first.Digest[:6]
	if err := first.InstallGeneration(root, id, slug, generation); err != nil {
		t.Fatal(err)
	}
	second, err := StageHTML(filepath.Join(root, "staging"), strings.NewReader("replacement"), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Discard()
	if err := second.InstallGeneration(root, id, slug, generation); err == nil {
		t.Fatal("generation rebound to different digest")
	}
	relative, _ := GenerationPath(id, generation)
	if _, err := VerifyVersion(filepath.Join(root, relative), first.Digest); err != nil {
		t.Fatal("original generation damaged", err)
	}
	if err := second.InstallGeneration(root, id, slug, "../bad"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := second.InstallGeneration(root, id, "different-abcdef", strings.Repeat("d", 64)); err == nil {
		t.Fatal("stable marker overwritten")
	}
}

func TestGenerationConcurrentRetries(t *testing.T) {
	root := t.TempDir()
	id := strings.Repeat("a", 32)
	generation := strings.Repeat("e", 64)
	var staged []*Staged
	for i := 0; i < 8; i++ {
		s, err := StageHTML(filepath.Join(root, "staging"), strings.NewReader("concurrent"), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Discard()
		staged = append(staged, s)
	}
	slug := "site-" + staged[0].Digest[:6]
	var wg sync.WaitGroup
	results := make(chan error, len(staged))
	for _, s := range staged {
		wg.Add(1)
		go func(s *Staged) { defer wg.Done(); results <- s.InstallGeneration(root, id, slug, generation) }(s)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	relative, _ := GenerationPath(id, generation)
	if _, err := VerifyVersion(filepath.Join(root, relative), staged[0].Digest); err != nil {
		t.Fatal(err)
	}
}

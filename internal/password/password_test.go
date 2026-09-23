package password

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPasswordAndRecord(t *testing.T) {
	h := NewHasher()
	ctx := context.Background()
	a, err := h.Create(ctx, "a long private password")
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.Create(ctx, "a long private password")
	if err != nil {
		t.Fatal(err)
	}
	if a.Salt == b.Salt || a.Key == b.Key {
		t.Fatal("salt reuse")
	}
	if ok, err := h.Verify(ctx, "a long private password", a); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, err := h.Verify(ctx, "wrong long private password", a); ok || err != nil {
		t.Fatal(ok, err)
	}
	root := t.TempDir()
	id := strings.Repeat("a", 32)
	record := Record{id, 2, a}
	digest, err := Store(root, record)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := Store(root, record); err != nil || again != digest {
		t.Fatal("retry", again, err)
	}
	if loaded, err := Load(root, id, 2, digest); err != nil || loaded != record {
		t.Fatal(loaded, err)
	}
	if _, err := Load(root, id, 3, digest); err == nil {
		t.Fatal("wrong policy accepted")
	}
	path := filepath.Join(root, "projects", id, "metadata", digest, "password.json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("unsafe permissions", err)
	}
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(root, id, 2, digest); err == nil {
		t.Fatal("tampering accepted")
	}
	if _, err := Store(root, record); err == nil {
		t.Fatal("overwrote corrupt record")
	}
	root = t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "projects")); err != nil {
		t.Fatal(err)
	}
	if _, err := Store(root, record); err == nil {
		t.Fatal("symlink write accepted")
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatal("wrote outside root", err)
	}
}

func TestBoundsAndCancellation(t *testing.T) {
	h := NewHasher()
	ctx := context.Background()
	for _, value := range []string{"short", strings.Repeat("x", 1025), string([]byte{255})} {
		if _, err := h.Create(ctx, value); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	h.slots <- struct{}{}
	h.slots <- struct{}{}
	if _, err := h.Create(ctx, "long enough password"); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	<-h.slots
	<-h.slots
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := h.Create(cancelled, "long enough password"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := h.Verify(ctx, "long enough password", Hash{Schema: 999, Algorithm: "argon2id"}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

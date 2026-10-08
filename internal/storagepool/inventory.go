package storagepool

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type InventoryEntry struct {
	ProjectID string `json:"projectId"`
	Digest    string `json:"digest"`
}

// Inventory is an observational, bounded namespace scan. A listed directory is
// not proof of verification, fsync, complete replication, or deletion safety.
func (s *Secondary) inventory(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	for key, values := range query {
		if (key != "cursor" && key != "limit") || len(values) != 1 {
			storageError(w, errConflict)
			return
		}
	}
	limit := 100
	if raw := query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			storageError(w, errConflict)
			return
		}
		limit = n
	}
	cursor := query.Get("cursor")
	if cursor != "" {
		id, digest, ok := strings.Cut(cursor, "/")
		if !ok || !idRE.MatchString(id) || !digestRE.MatchString(digest) {
			storageError(w, errConflict)
			return
		}
	}
	projects, err := boundedDirectories(filepath.Join(s.root, "projects"))
	if os.IsNotExist(err) {
		jsonReply(w, 200, map[string]any{"versions": []InventoryEntry{}, "nextCursor": ""})
		return
	}
	if err != nil {
		storageError(w, err)
		return
	}
	result := []InventoryEntry{}
	next := ""
	for _, p := range projects {
		if r.Context().Err() != nil {
			storageError(w, r.Context().Err())
			return
		}
		if !p.IsDir() || p.Type()&os.ModeSymlink != 0 || !idRE.MatchString(p.Name()) {
			continue
		}
		if cursor != "" && p.Name() < strings.SplitN(cursor, "/", 2)[0] {
			continue
		}
		versions, err := boundedDirectories(filepath.Join(s.root, "projects", p.Name(), "versions"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			storageError(w, err)
			return
		}
		for _, v := range versions {
			if !v.IsDir() || v.Type()&os.ModeSymlink != 0 || !digestRE.MatchString(v.Name()) {
				continue
			}
			key := p.Name() + "/" + v.Name()
			if key <= cursor {
				continue
			}
			if len(result) == limit {
				last := result[len(result)-1]
				next = last.ProjectID + "/" + last.Digest
				jsonReply(w, 200, map[string]any{"versions": result, "nextCursor": next})
				return
			}
			result = append(result, InventoryEntry{p.Name(), v.Name()})
		}
	}
	jsonReply(w, 200, map[string]any{"versions": result, "nextCursor": next})
}
func boundedDirectories(path string) ([]os.DirEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	entries, err := f.ReadDir(100001)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(entries) > 100000 {
		return nil, errFull
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

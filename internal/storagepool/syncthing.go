package storagepool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

// Cleanup deliberately requires a local, authenticated Syncthing adapter. Flux
// location discovery is not replication membership. Without this adapter, old
// content is retained and its allowance is never refunded.
type ReplicationSnapshot struct {
	Device  string   `json:"device"`
	Epoch   string   `json:"epoch"`
	Members []string `json:"members"`
}

type syncFolder struct {
	ID           string `json:"id"`
	Path         string `json:"path"`
	Type         string `json:"type"`
	Paused       bool   `json:"paused"`
	IgnoreDelete bool   `json:"ignoreDelete"`
	Devices      []struct {
		DeviceID string `json:"deviceID"`
	} `json:"devices"`
	Versioning struct {
		Type string `json:"type"`
	} `json:"versioning"`
}

var deviceRE = regexp.MustCompile(`^[A-Z2-7]{7}(?:-[A-Z2-7]{7}){7}$`)

func (s *Secondary) syncRequest(ctx context.Context, method, path string, value, result any) error {
	if s.config.SyncthingURL == "" {
		return errFull
	}
	var body io.Reader
	if value != nil {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.config.SyncthingURL, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-Key", s.config.SyncthingAPIKey)
	if value != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return errConflict }}
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return errors.New("Syncthing coordination unavailable")
	}
	if result == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 2<<20))
		return err
	}
	return readResponse(res, result, 2<<20)
}

func (s *Secondary) replication(ctx context.Context) (ReplicationSnapshot, syncFolder, error) {
	var status struct {
		MyID string `json:"myID"`
	}
	var version struct {
		Version string `json:"version"`
	}
	var config struct {
		Folders []syncFolder `json:"folders"`
		Devices []struct {
			DeviceID   string `json:"deviceID"`
			Introducer bool   `json:"introducer"`
		} `json:"devices"`
	}
	if err := s.syncRequest(ctx, "GET", "/rest/system/version", nil, &version); err != nil {
		return ReplicationSnapshot{}, syncFolder{}, err
	}
	// The pause-and-wait contract has been audited and integration-tested against
	// this release. Do not infer writer quiescence from an idle status or a delay.
	if version.Version != "v1.30.0" {
		return ReplicationSnapshot{}, syncFolder{}, errConflict
	}
	if err := s.syncRequest(ctx, "GET", "/rest/system/status", nil, &status); err != nil {
		return ReplicationSnapshot{}, syncFolder{}, err
	}
	if !deviceRE.MatchString(status.MyID) {
		return ReplicationSnapshot{}, syncFolder{}, errConflict
	}
	if err := s.syncRequest(ctx, "GET", "/rest/config", nil, &config); err != nil {
		return ReplicationSnapshot{}, syncFolder{}, err
	}
	for _, d := range config.Devices {
		if d.Introducer {
			return ReplicationSnapshot{}, syncFolder{}, errConflict
		}
	}
	var folder syncFolder
	for _, f := range config.Folders {
		if f.ID == s.config.SyncthingFolder {
			folder = f
			break
		}
	}
	root, err := filepath.EvalSymlinks(s.root)
	if err != nil || folder.ID == "" || !filepath.IsAbs(folder.Path) || filepath.Clean(folder.Path) != root || folder.Type != "sendreceive" || folder.IgnoreDelete || folder.Versioning.Type != "" {
		return ReplicationSnapshot{}, syncFolder{}, errConflict
	}
	// Nested/overlapping folders can provide an unfenced second replication writer.
	for _, f := range config.Folders {
		if f.ID == folder.ID {
			continue
		}
		if !filepath.IsAbs(f.Path) {
			return ReplicationSnapshot{}, syncFolder{}, errConflict
		}
		clean, err := filepath.EvalSymlinks(f.Path)
		if err != nil {
			return ReplicationSnapshot{}, syncFolder{}, errConflict
		}
		if clean == root || strings.HasPrefix(clean, root+string(os.PathSeparator)) || strings.HasPrefix(root, clean+string(os.PathSeparator)) {
			return ReplicationSnapshot{}, syncFolder{}, errConflict
		}
	}
	members := []string{}
	for _, d := range folder.Devices {
		if !deviceRE.MatchString(d.DeviceID) || slices.Contains(members, d.DeviceID) {
			return ReplicationSnapshot{}, syncFolder{}, errConflict
		}
		members = append(members, d.DeviceID)
	}
	if !slices.Contains(members, status.MyID) || len(members) < 1 || len(members) > 64 {
		return ReplicationSnapshot{}, syncFolder{}, errConflict
	}
	sort.Strings(members)
	raw, _ := json.Marshal(struct {
		Folder  string
		Members []string
	}{folder.ID, members})
	sum := sha256.Sum256(raw)
	return ReplicationSnapshot{Device: status.MyID, Epoch: hex.EncodeToString(sum[:]), Members: members}, folder, nil
}

// Syncthing v1.30.0's config HTTP handler waits for CommitConfiguration, and
// restartFolder waits for StopAndWaitChan before completing a pause. A successful
// PATCH therefore fences pullers/scanners before local deletion. SetIgnores also
// works on paused folders and persists .stignore atomically. Resume starts a new
// runner, which loads those ignores before accepting work.
func (s *Secondary) syncPaused(ctx context.Context, paused bool) error {
	return s.syncRequest(ctx, "PATCH", "/rest/config/folders/"+url.PathEscape(s.config.SyncthingFolder), map[string]bool{"paused": paused}, nil)
}

func (s *Secondary) ignoreVersion(ctx context.Context, relative string) error {
	path := "/rest/db/ignores?folder=" + url.QueryEscape(s.config.SyncthingFolder)
	var existing struct {
		Ignore []string `json:"ignore"`
	}
	if err := s.syncRequest(ctx, "GET", path, nil, &existing); err != nil {
		return err
	}
	pattern := "/" + filepath.ToSlash(relative)
	// First match wins: the permanent retirement fence precedes any negations.
	if len(existing.Ignore) == 0 || existing.Ignore[0] != pattern {
		existing.Ignore = append([]string{pattern}, existing.Ignore...)
	}
	if len(existing.Ignore) > 100000 {
		return errFull
	}
	var confirmed struct {
		Ignore []string `json:"ignore"`
	}
	if err := s.syncRequest(ctx, "POST", path, existing, &confirmed); err != nil {
		return err
	}
	if !slices.Equal(existing.Ignore, confirmed.Ignore) {
		return errConflict
	}
	// Require durable on-disk proof on the exact mounted volume, not just an API
	// response from a different Syncthing process or a different filesystem.
	f, err := os.Open(filepath.Join(s.root, ".stignore"))
	if err != nil {
		return err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 8<<20))
	if err != nil {
		return err
	}
	if !strings.HasPrefix(string(raw), pattern+"\n") {
		return errConflict
	}
	if err = f.Sync(); err != nil {
		return err
	}
	dir, err := os.Open(s.root)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

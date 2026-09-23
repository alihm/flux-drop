// Package agentmcp exposes account-bound static publishing as local MCP tools.
// It never serves an unauthenticated network listener; transport is stdio.
package agentmcp

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/runonflux/flux-drop/internal/content"
)

type Config struct {
	Origin  string
	KeyFile string
	Root    string
	Client  *http.Client
}

type Publisher struct {
	origin  *url.URL
	keyFile string
	root    *os.Root
	client  *http.Client
}

type PublishHTMLInput struct {
	HTML            string `json:"html" jsonschema:"Complete HTML document to publish"`
	Name            string `json:"name,omitempty" jsonschema:"Optional lowercase site name; a unique suffix is added"`
	IdempotencyKey  string `json:"idempotencyKey,omitempty" jsonschema:"Reuse only when retrying the exact same publish attempt"`
	PrivatePassword string `json:"privatePassword,omitempty" jsonschema:"Optional password of at least 12 characters for a private self-contained HTML page"`
}

type PublishPathInput struct {
	Path           string `json:"path" jsonschema:"Relative path inside the configured workspace root"`
	Name           string `json:"name,omitempty" jsonschema:"Optional lowercase site name; a unique suffix is added"`
	IdempotencyKey string `json:"idempotencyKey,omitempty" jsonschema:"Reuse only when retrying the exact same publish attempt"`
}

type PublishResult struct {
	URL              string `json:"url"`
	Path             string `json:"path"`
	ProjectID        string `json:"projectId,omitempty"`
	Slug             string `json:"slug,omitempty"`
	AlreadyPublished bool   `json:"alreadyPublished,omitempty"`
}

var siteName = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,46}[a-z0-9])?$`)
var retryKey = regexp.MustCompile(`^[A-Za-z0-9_-]{8,128}$`)

func New(config Config) (*Publisher, error) {
	u, err := url.Parse(config.Origin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("DROP_MCP_ORIGIN must be a bare HTTPS origin")
	}
	if config.KeyFile == "" || !filepath.IsAbs(config.KeyFile) {
		return nil, errors.New("DROP_MCP_API_KEY_FILE must be an absolute path")
	}
	var root *os.Root
	if config.Root != "" {
		if !filepath.IsAbs(config.Root) {
			return nil, errors.New("DROP_MCP_ROOT must be an absolute path")
		}
		root, err = os.OpenRoot(config.Root)
		if err != nil {
			return nil, fmt.Errorf("workspace root: %w", err)
		}
	}
	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: 7 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	p := &Publisher{origin: u, keyFile: config.KeyFile, root: root, client: client}
	if _, err := p.readKey(); err != nil {
		if root != nil {
			root.Close()
		}
		return nil, err
	}
	return p, nil
}

func (p *Publisher) Close() error {
	if p.root != nil {
		return p.root.Close()
	}
	return nil
}

func (p *Publisher) readKey() (string, error) {
	f, err := os.Open(p.keyFile)
	if err != nil {
		return "", fmt.Errorf("API key file: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 257))
	if err != nil {
		return "", fmt.Errorf("API key file: %w", err)
	}
	if len(b) > 256 {
		return "", errors.New("API key file is too large")
	}
	key := strings.TrimSpace(string(b))
	if len(key) != 48 || !strings.HasPrefix(key, "drop_") {
		return "", errors.New("API key file must contain one Flux Drop publish-only key")
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(key, "drop_"))
	if err != nil || len(raw) != 32 || "drop_"+base64.RawURLEncoding.EncodeToString(raw) != key {
		return "", errors.New("API key file contains an invalid key")
	}
	return key, nil
}

func (p *Publisher) Server() *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "flux-drop", Version: "1.0.0"}, &mcp.ServerOptions{
		Instructions: "These tools publish static sites into the configured Flux Drop account. Publishing creates a public URL unless a private HTML password is supplied. Never include secrets in site content.",
	})
	additive := false
	annotation := &mcp.ToolAnnotations{DestructiveHint: &additive}
	mcp.AddTool(s, &mcp.Tool{Name: "publish_html", Description: "Publish a complete HTML page to a permanent Flux Drop URL in the key owner's account.", Annotations: annotation}, func(ctx context.Context, _ *mcp.CallToolRequest, in PublishHTMLInput) (*mcp.CallToolResult, PublishResult, error) {
		if in.HTML == "" {
			return nil, PublishResult{}, errors.New("html is required")
		}
		result, err := p.publish(ctx, in.Name, in.IdempotencyKey, "text/html", strings.NewReader(in.HTML), int64(len(in.HTML)), in.PrivatePassword)
		return nil, result, err
	})
	mcp.AddTool(s, &mcp.Tool{Name: "publish_file", Description: "Publish an HTML or ZIP file from the configured workspace root.", Annotations: annotation}, func(ctx context.Context, _ *mcp.CallToolRequest, in PublishPathInput) (*mcp.CallToolResult, PublishResult, error) {
		f, kind, size, err := p.openFile(in.Path)
		if err != nil {
			return nil, PublishResult{}, err
		}
		defer f.Close()
		result, err := p.publish(ctx, in.Name, in.IdempotencyKey, kind, f, size, "")
		return nil, result, err
	})
	mcp.AddTool(s, &mcp.Tool{Name: "publish_folder", Description: "Publish a static folder with index.html at its root from the configured workspace root.", Annotations: annotation}, func(ctx context.Context, _ *mcp.CallToolRequest, in PublishPathInput) (*mcp.CallToolResult, PublishResult, error) {
		f, size, err := p.packFolder(in.Path)
		if err != nil {
			return nil, PublishResult{}, err
		}
		defer func() { f.Close(); os.Remove(f.Name()) }()
		result, err := p.publish(ctx, in.Name, in.IdempotencyKey, "application/zip", f, size, "")
		return nil, result, err
	})
	return s
}

func (p *Publisher) publish(ctx context.Context, name, idempotency, kind string, body io.Reader, size int64, password string) (PublishResult, error) {
	if name != "" && !siteName.MatchString(name) {
		return PublishResult{}, errors.New("name must contain 1–48 lowercase letters, numbers, or internal hyphens")
	}
	if size < 1 || size > content.DefaultLimits().UploadBytes {
		return PublishResult{}, errors.New("upload must be between 1 byte and 50 MiB")
	}
	if idempotency == "" {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return PublishResult{}, err
		}
		idempotency = hex.EncodeToString(random)
	}
	if !retryKey.MatchString(idempotency) {
		return PublishResult{}, errors.New("idempotencyKey must contain 8–128 letters, numbers, underscores, or hyphens")
	}
	if password != "" && (utf8.RuneCountInString(password) < 12 || len(password) > 1024) {
		return PublishResult{}, errors.New("privatePassword must contain at least 12 characters and at most 1,024 UTF-8 bytes")
	}
	key, err := p.readKey()
	if err != nil {
		return PublishResult{}, err
	}
	u := *p.origin
	u.Path = "/api/agent/projects"
	query := url.Values{}
	if name != "" {
		query.Set("name", name)
	}
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return PublishResult{}, err
	}
	req.ContentLength = size
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Idempotency-Key", idempotency)
	req.Header.Set("Content-Type", kind)
	if password != "" {
		req.Header.Set("X-Drop-Password", base64.RawURLEncoding.EncodeToString([]byte(password)))
	}
	response, err := p.client.Do(req)
	if err != nil {
		return PublishResult{}, fmt.Errorf("publish result uncertain; retry the same content with idempotencyKey %q: %w", idempotency, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8192))
	if err != nil {
		return PublishResult{}, fmt.Errorf("publish result uncertain; retry the same content with idempotencyKey %q: %w", idempotency, err)
	}
	var value struct {
		Path    string `json:"path"`
		Project struct {
			ID   string `json:"id"`
			Slug string `json:"slug"`
		} `json:"project"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return PublishResult{}, fmt.Errorf("publish result uncertain; retry the same content with idempotencyKey %q", idempotency)
	}
	if response.StatusCode != http.StatusOK && !(response.StatusCode == http.StatusConflict && value.Error == "duplicate_content") {
		return PublishResult{}, fmt.Errorf("publish rejected (HTTP %d: %s)", response.StatusCode, value.Error)
	}
	if !strings.HasPrefix(value.Path, "/") || strings.HasPrefix(value.Path, "//") || strings.ContainsAny(value.Path, "?#\\\r\n") {
		return PublishResult{}, errors.New("publish response contained an invalid path")
	}
	return PublishResult{URL: p.origin.String() + value.Path, Path: value.Path, ProjectID: value.Project.ID, Slug: value.Project.Slug, AlreadyPublished: response.StatusCode == http.StatusConflict}, nil
}

func (p *Publisher) localPath(relative string) (string, error) {
	if p.root == nil {
		return "", errors.New("DROP_MCP_ROOT is required for filesystem publishing")
	}
	if !filepath.IsLocal(relative) || relative == "." {
		return "", errors.New("path must be relative to the workspace root")
	}
	return filepath.ToSlash(relative), nil
}

func (p *Publisher) openFile(relative string) (*os.File, string, int64, error) {
	path, err := p.localPath(relative)
	if err != nil {
		return nil, "", 0, err
	}
	ext := strings.ToLower(filepath.Ext(path))
	kind := ""
	if ext == ".html" || ext == ".htm" {
		kind = "text/html"
	} else if ext == ".zip" {
		kind = "application/zip"
	}
	if kind == "" {
		return nil, "", 0, errors.New("file must be HTML or ZIP")
	}
	linkInfo, err := p.root.Lstat(path)
	if err != nil {
		return nil, "", 0, err
	}
	if linkInfo.Mode()&os.ModeSymlink != 0 {
		return nil, "", 0, errors.New("symlinks are not publishable")
	}
	f, err := p.root.Open(path)
	if err != nil {
		return nil, "", 0, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > content.DefaultLimits().UploadBytes {
		f.Close()
		return nil, "", 0, errors.New("file must be regular and at most 50 MiB")
	}
	return f, kind, info.Size(), nil
}

func (p *Publisher) packFolder(relative string) (*os.File, int64, error) {
	base, err := p.localPath(relative)
	if err != nil {
		return nil, 0, err
	}
	info, err := p.root.Lstat(base)
	if err != nil || !info.IsDir() {
		return nil, 0, errors.New("folder path must be a directory")
	}
	f, err := os.CreateTemp("", "flux-drop-mcp-*.zip")
	if err != nil {
		return nil, 0, err
	}
	clean := func(e error) (*os.File, int64, error) { f.Close(); os.Remove(f.Name()); return nil, 0, e }
	writer := zip.NewWriter(f)
	count, total, hasIndex := 0, int64(0), false
	err = fs.WalkDir(p.root.FS(), base, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("folder contains a symlink")
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return errors.New("folder contains a non-regular file")
		}
		name := strings.TrimPrefix(path, base+"/")
		if err := content.ValidatePath(name); err != nil {
			return err
		}
		count++
		if count > content.DefaultLimits().Files {
			return errors.New("folder contains more than 5,000 files")
		}
		if name == "index.html" {
			hasIndex = true
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		if total > content.DefaultLimits().ExpandedBytes {
			return errors.New("folder exceeds 200 MiB expanded")
		}
		input, err := p.root.Open(path)
		if err != nil {
			return err
		}
		if opened, err := input.Stat(); err != nil || !opened.Mode().IsRegular() {
			input.Close()
			return errors.New("folder file changed during packaging")
		}
		output, err := writer.Create(name)
		if err != nil {
			input.Close()
			return err
		}
		copied, err := io.Copy(output, io.LimitReader(input, info.Size()+1))
		closeErr := input.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil && copied != info.Size() {
			err = errors.New("folder file changed during packaging")
		}
		return err
	})
	if err != nil {
		writer.Close()
		return clean(err)
	}
	if !hasIndex {
		writer.Close()
		return clean(errors.New("folder must contain index.html at its root"))
	}
	if err := writer.Close(); err != nil {
		return clean(err)
	}
	info, err = f.Stat()
	if err != nil || info.Size() > content.DefaultLimits().UploadBytes {
		return clean(errors.New("ZIP exceeds 50 MiB upload limit"))
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return clean(err)
	}
	return f, info.Size(), nil
}

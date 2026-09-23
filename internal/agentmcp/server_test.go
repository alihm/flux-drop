package agentmcp

import (
	"archive/zip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func testKey(t *testing.T) string {
	t.Helper()
	key := "drop_" + base64.RawURLEncoding.EncodeToString([]byte("12345678901234567890123456789012"))
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, []byte(key+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMCPPublishesThroughAuthenticatedAPI(t *testing.T) {
	keyFile := testKey(t)
	keyBytes, _ := os.ReadFile(keyFile)
	key := strings.TrimSpace(string(keyBytes))
	requests := 0
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/agent/projects" || r.URL.Query().Get("name") != "example" || r.Header.Get("Authorization") != "Bearer "+key || r.Header.Get("Origin") != "" || r.Header.Get("Idempotency-Key") != "test-key-123" || r.Header.Get("Content-Type") != "text/html" {
			t.Errorf("incorrect publish request: %s %s %v", r.Method, r.URL, r.Header)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "<h1>hello</h1>" {
			t.Errorf("body: %q", body)
		}
		if got := r.Header.Get("X-Drop-Password"); got != base64.RawURLEncoding.EncodeToString([]byte("a long private password")) {
			t.Errorf("private password encoding: %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"path":"/example-abcdef/","project":{"id":"abc","slug":"example-abcdef"}}`)
	}))
	defer api.Close()
	publisher, err := New(Config{Origin: api.URL, KeyFile: keyFile, Client: api.Client()})
	if err != nil {
		t.Fatal(err)
	}
	server := publisher.Server()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1.0.0"}, nil)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	session, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	listed, err := session.ListTools(context.Background(), nil)
	if err != nil || len(listed.Tools) != 3 {
		t.Fatal(listed, err)
	}
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "publish_html", Arguments: map[string]any{"name": "example", "html": "<h1>hello</h1>", "idempotencyKey": "test-key-123", "privatePassword": "a long private password"}})
	if err != nil || result.IsError {
		t.Fatal(result, err)
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil || !strings.Contains(string(encoded), api.URL+"/example-abcdef/") {
		t.Fatal(string(encoded), err)
	}
	if requests != 1 {
		t.Fatal("request count", requests)
	}
}

func TestWorkspaceConfinementAndFolderPackage(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.html")
	if err := os.WriteFile(outside, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link.html")); err != nil {
		t.Fatal(err)
	}
	keyFile := testKey(t)
	p, err := New(Config{Origin: "https://drop.example.com", KeyFile: keyFile, Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, _, _, err := p.openFile("../secret.html"); err == nil {
		t.Fatal("accepted traversal")
	}
	if _, _, _, err := p.openFile("link.html"); err == nil {
		t.Fatal("accepted symlink escape")
	}
	if err := os.Mkdir(filepath.Join(root, "site"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "site", "index.html"), []byte("<h1>site</h1>"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "site", "assets"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "site", "assets", "app.js"), []byte("console.log(1)"), 0600); err != nil {
		t.Fatal(err)
	}
	f, _, err := p.packFolder("site")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { f.Close(); os.Remove(f.Name()) }()
	stat, _ := f.Stat()
	z, err := zip.NewReader(f, stat.Size())
	if err != nil || len(z.File) != 2 {
		t.Fatal(z, err)
	}
	if z.File[0].Name != "assets/app.js" || z.File[1].Name != "index.html" {
		t.Fatal(z.File[0].Name, z.File[1].Name)
	}
	if err := os.WriteFile(filepath.Join(root, "site", ".env"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.packFolder("site"); err == nil {
		t.Fatal("included dotfile")
	}
	if err := os.Remove(filepath.Join(root, "site", ".env")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "site", "link.html")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.packFolder("site"); err == nil {
		t.Fatal("included symlink")
	}
}

func TestMCPRejectsRedirectAndInvalidCredential(t *testing.T) {
	keyFile := testKey(t)
	if _, err := New(Config{Origin: "http://drop.example.com", KeyFile: keyFile}); err == nil {
		t.Fatal("accepted HTTP origin")
	}
	if _, err := New(Config{Origin: "https://drop.example.com/path", KeyFile: keyFile}); err == nil {
		t.Fatal("accepted origin path")
	}
	if err := os.WriteFile(keyFile, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Origin: "https://drop.example.com", KeyFile: keyFile}); err == nil {
		t.Fatal("accepted malformed key")
	}
	keyFile = testKey(t)
	defaultPublisher, err := New(Config{Origin: "https://drop.example.com", KeyFile: keyFile})
	if err != nil {
		t.Fatal(err)
	}
	if err := defaultPublisher.client.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Fatal("default client can follow redirects", err)
	}
	redirected := false
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected = true }))
	defer target.Close()
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	client := source.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	p, err := New(Config{Origin: source.URL, KeyFile: keyFile, Client: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.publish(context.Background(), "", "test-key-123", "text/html", strings.NewReader("hello"), 5, ""); err == nil {
		t.Fatal("accepted redirect")
	}
	if redirected {
		t.Fatal("bearer was sent to redirect target")
	}
}

func TestMCPFileAndFolderTools(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "page.html"), []byte("<title>Page</title>"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(workspace, "site"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "site", "index.html"), []byte("<title>Site</title>"), 0600); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind := r.Header.Get("Content-Type")
		seen[kind] = true
		body, _ := io.ReadAll(r.Body)
		if kind == "text/html" && string(body) != "<title>Page</title>" {
			t.Errorf("HTML body: %q", body)
		}
		if kind == "application/zip" {
			archive, err := zip.NewReader(strings.NewReader(string(body)), int64(len(body)))
			if err != nil || len(archive.File) != 1 || archive.File[0].Name != "index.html" {
				t.Errorf("ZIP body: %v %v", archive, err)
			}
		}
		io.WriteString(w, `{"path":"/site-abcdef/","project":{"id":"abc","slug":"site-abcdef"}}`)
	}))
	defer api.Close()
	p, err := New(Config{Origin: api.URL, KeyFile: testKey(t), Root: workspace, Client: api.Client()})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s := p.Server()
	ct, st := mcp.NewInMemoryTransports()
	serverSession, err := s.Connect(context.Background(), st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1.0.0"}, nil)
	cs, err := client.Connect(context.Background(), ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	for _, tool := range []struct{ Name, Path string }{{"publish_file", "page.html"}, {"publish_folder", "site"}} {
		result, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool.Name, Arguments: map[string]any{"path": tool.Path}})
		if err != nil || result.IsError {
			t.Fatal(tool.Name, result, err)
		}
	}
	if !seen["text/html"] || !seen["application/zip"] {
		t.Fatal("not all formats published", seen)
	}
}

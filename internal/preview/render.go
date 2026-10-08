package preview

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
)

// Assets serves only verified uploaded files, never application routes. The
// capability stays in the trusted Node controller; page scripts never see it.
type Assets func(http.ResponseWriter, *http.Request, project.Project, string)

func BrowserRenderer(assets Assets) Renderer {
	return func(ctx context.Context, p project.Project) ([]byte, error) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		nonce := make([]byte, 32)
		if _, err := rand.Read(nonce); err != nil {
			return nil, err
		}
		token := hex.EncodeToString(nonce)
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		server := &http.Server{ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 15 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name, ok := strings.CutPrefix(r.URL.Path, "/"+token+"/")
			if !ok || r.Method != "GET" || content.ValidatePath(name) != nil || (p.Private && name != "index.html") {
				http.NotFound(w, r)
				return
			}
			assets(w, r.WithContext(ctx), p, name)
		})}
		defer server.Close()
		go func() { _ = server.Serve(listener) }()
		job, err := os.MkdirTemp("", "drop-browser-")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(job)
		input, _ := json.Marshal(struct{ Source, Slug string }{"http://" + listener.Addr().String() + "/" + token + "/", p.Slug})
		cmd := exec.CommandContext(ctx, "node", "/opt/drop-preview/render.mjs")
		cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=" + job, "TMPDIR=" + job, "DROP_BROWSER_JOB=" + job}
		cmd.Stdin = bytes.NewReader(input)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error {
			if cmd.Process == nil {
				return nil
			}
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		cmd.WaitDelay = time.Second
		var output limitedBuffer
		output.limit = maxImage
		var diagnostic limitedBuffer
		diagnostic.limit = 4096
		cmd.Stdout = &output
		cmd.Stderr = &diagnostic
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("browser preview: %w: %s", err, diagnostic.String())
		}
		// Also reap children if the controller closed before a browser process.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if output.overflow {
			return nil, errors.New("preview exceeded image limit")
		}
		return output.Bytes(), nil
	}
}

type limitedBuffer struct {
	bytes.Buffer
	limit    int
	overflow bool
}

func (b *limitedBuffer) Write(raw []byte) (int, error) {
	n := len(raw)
	if b.Len()+n > b.limit {
		b.overflow = true
		raw = raw[:b.limit-b.Len()]
	}
	_, _ = b.Buffer.Write(raw)
	return n, nil
}

// LocalAssets supports single-app installations while rejecting unlisted paths
// and symlinks. Storage pools supply their existing verified file fallback.
func LocalAssets(dataRoot string) Assets {
	return func(w http.ResponseWriter, r *http.Request, p project.Project, name string) {
		dir := filepath.Join(dataRoot, "projects", p.ID, "versions", p.ActiveDigest)
		manifest, err := content.VerifyVersion(dir, p.ActiveDigest)
		if err != nil {
			w.WriteHeader(503)
			return
		}
		for _, file := range manifest.Files {
			if file.Path != name {
				continue
			}
			root, err := os.OpenRoot(filepath.Join(dir, "public"))
			if err != nil {
				w.WriteHeader(503)
				return
			}
			defer root.Close()
			f, err := root.Open(name)
			if err != nil {
				w.WriteHeader(503)
				return
			}
			defer f.Close()
			http.ServeContent(w, r, name, time.Time{}, f)
			return
		}
		http.NotFound(w, r)
	}
}

var _ io.Writer = (*limitedBuffer)(nil)

package preview_test

import (
	"bytes"
	"context"
	"image/jpeg"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/preview"
	"github.com/runonflux/flux-drop/internal/project"
)

func TestProductionBrowserRenderer(t *testing.T) {
	if os.Getenv("DROP_PREVIEW_IMAGE_TEST") != "true" {
		t.Skip("run compiled test inside the final image")
	}
	var assets atomic.Int32
	renderer := preview.BrowserRenderer(func(w http.ResponseWriter, r *http.Request, p project.Project, name string) {
		assets.Add(1)
		switch name {
		case "index.html":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<link rel="stylesheet" href="main.css"><h1 id="headline">Initial</h1><script>document.getElementById('headline').textContent='Rendered';fetch('/api/config').catch(()=>{});fetch('http://127.0.0.1:8081/api/config').catch(()=>{})</script>`))
		case "main.css":
			w.Header().Set("Content-Type", "text/css")
			_, _ = w.Write([]byte(`body{background:#0b121c;color:#6fe3c8;font:80px sans-serif;padding:80px}h1{margin:0}`))
		default:
			t.Errorf("renderer requested unexpected path %q", name)
			http.NotFound(w, r)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	raw, err := renderer(ctx, project.Project{ID: strings.Repeat("a", 32), Slug: "sample-abcdef", ActiveDigest: strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal(err)
	}
	image, err := jpeg.Decode(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if image.Bounds().Dx() != 320 || image.Bounds().Dy() != 180 || len(raw) > 64<<10 {
		t.Fatal("invalid thumbnail", image.Bounds(), len(raw))
	}
	if assets.Load() < 2 {
		t.Fatal("uploaded stylesheet was not fetched")
	}
	pixel := image.At(300, 150)
	r, g, b, _ := pixel.RGBA()
	if r > 6000 || g > 9000 || b > 13000 {
		t.Fatal("page background did not render", r, g, b)
	}
	output := os.Getenv("DROP_PREVIEW_IMAGE_OUTPUT")
	if output == "" {
		output = "/tmp/production-preview.jpg"
	}
	if err := os.WriteFile(output, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("rendered 320x180 JPEG, %d bytes", len(raw))
}

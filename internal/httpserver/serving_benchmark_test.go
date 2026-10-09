package httpserver

import (
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/project"
)

func BenchmarkSiteAssets(b *testing.B) {
	root := b.TempDir()
	sources := []content.Source{}
	payload := strings.Repeat("x", 256<<10)
	for i := 0; i < 30; i++ {
		name := fmt.Sprintf("asset%d.js", i)
		if i == 0 {
			name = "index.html"
		}
		sources = append(sources, content.Source{Name: name, Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(payload)), nil }})
	}
	staged, err := content.StageFolder(root, sources, content.DefaultLimits())
	if err != nil {
		b.Fatal(err)
	}
	p := project.Project{ID: strings.Repeat("a", 32), Slug: "bench", ActiveDigest: staged.Digest, Status: "active", WatermarkDisabled: true}
	if err = staged.Install(root, p.ID, p.Slug); err != nil {
		b.Fatal(err)
	}
	h := ProjectDelivery(&deliveryRepository{p: p}, root)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/bench/asset1.js", nil))
		if w.Code != 200 {
			b.Fatal(w.Code)
		}
	}
}

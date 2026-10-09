package httpserver

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestWatermarkRepresentation(t *testing.T) {
	for _, kind := range []string{"text/html; charset=utf-8", "image/svg+xml", "text/javascript", "text/css", "image/jpeg"} {
		for _, method := range []string{"GET", "HEAD"} {
			original := "<!doctype html><html><body><h1>Site</h1></body></html>"
			rec := httptest.NewRecorder()
			rec.Header().Set("Content-Type", kind)
			rec.Header().Set("Content-Length", strconv.Itoa(len(original)))
			rec.Header().Set("ETag", `"upload-hash"`)
			w := &watermarkWriter{ResponseWriter: rec, head: method == "HEAD"}
			w.WriteHeader(http.StatusOK)
			if method == "GET" {
				_, _ = io.WriteString(w, original)
			}
			w.finish()
			want := original
			if strings.HasPrefix(kind, "text/html") {
				want += projectWatermark
				if rec.Header().Get("ETag") == "" || rec.Header().Get("ETag") == `"upload-hash"` || rec.Header().Get("Accept-Ranges") != "none" {
					t.Fatal("source validators advertised", rec.Header())
				}
			} else if rec.Header().Get("ETag") != `"upload-hash"` {
				t.Fatal("asset headers changed")
			}
			if rec.Header().Get("Content-Length") != strconv.Itoa(len(want)) {
				t.Fatal("wrong length", method, kind, rec.Header())
			}
			if method == "HEAD" {
				want = ""
			}
			if rec.Body.String() != want {
				t.Fatal(method, kind, rec.Body.String())
			}
		}
	}
	for _, status := range []int{404, 503} {
		rec := httptest.NewRecorder()
		w := &watermarkWriter{ResponseWriter: rec}
		rec.Header().Set("Content-Type", "text/html")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, "error")
		w.finish()
		if rec.Body.String() != "error" {
			t.Fatal("branded error response")
		}
	}
	// Do not append success branding after a truncated transfer.
	rec := httptest.NewRecorder()
	rec.Header().Set("Content-Type", "text/html")
	rec.Header().Set("Content-Length", "100")
	w := &watermarkWriter{ResponseWriter: rec}
	_, _ = io.WriteString(w, "short")
	w.finish()
	if rec.Body.String() != "short" {
		t.Fatal("hid incomplete transfer")
	}
}

package httpserver

import (
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

// Presentation only: no script, external resource, or sandbox permission needed.
// Appending also covers HTML fragments without a closing body tag and preserves
// the original document's doctype and bytes. Browsers place it inside the body.
const projectWatermark = `<a data-drop-watermark="runonflux" href="https://runonflux.com/apps/drop" target="_self" rel="noopener noreferrer" aria-label="Powered by RunOnFlux" style="all:initial!important;position:fixed!important;right:4px!important;bottom:4px!important;z-index:2147483647!important;display:inline-flex!important;align-items:center!important;gap:3px!important;padding:4px 7px!important;border:1px solid rgba(255,255,255,.16)!important;border-radius:5px!important;background:rgba(15,23,42,.9)!important;color:#f8fafc!important;font:10px/1.2 system-ui,sans-serif!important;box-shadow:0 2px 12px #0003!important;pointer-events:auto!important;cursor:pointer!important;outline:revert!important;outline-offset:2px!important;user-select:none!important">Powered by <b style="all:initial!important;color:#fff!important;font:600 10px/1.2 system-ui,sans-serif!important;cursor:inherit!important">RunOnFlux</b></a>`

func htmlFile(name string) bool {
	kind, _, _ := mime.ParseMediaType(mime.TypeByExtension(strings.ToLower(filepath.Ext(name))))
	return kind == "text/html"
}

// Branded HTML is always a complete representation. Ignore source-file ranges
// and validators, which describe the immutable upload rather than this response.
func watermarkRequest(r *http.Request) *http.Request {
	r = r.Clone(r.Context())
	for _, key := range []string{"Range", "If-Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
		r.Header.Del(key)
	}
	return r
}

type watermarkWriter struct {
	http.ResponseWriter
	head, started, branded, failed bool
	written, expected              int64
}

func (w *watermarkWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *watermarkWriter) WriteHeader(status int) {
	if w.started {
		return
	}
	if status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	w.started = true
	kind, _, _ := mime.ParseMediaType(w.Header().Get("Content-Type"))
	w.branded = status == http.StatusOK && kind == "text/html" && w.Header().Get("Content-Encoding") == ""
	if w.branded {
		w.expected = -1
		if n, err := strconv.ParseInt(w.Header().Get("Content-Length"), 10, 64); err == nil && n >= 0 {
			w.expected = n
			w.Header().Set("Content-Length", strconv.FormatInt(n+int64(len(projectWatermark)), 10))
		} else {
			w.Header().Del("Content-Length")
		}
		w.Header().Del("ETag")
		w.Header().Del("Last-Modified")
		w.Header().Set("Accept-Ranges", "none")
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *watermarkWriter) Write(p []byte) (int, error) {
	if !w.started {
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", http.DetectContentType(p))
		}
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	w.written += int64(n)
	if err != nil {
		w.failed = true
	}
	return n, err
}
func (w *watermarkWriter) finish() {
	if w.branded && !w.head && !w.failed && (w.expected < 0 || w.written == w.expected) {
		if _, err := io.WriteString(w.ResponseWriter, projectWatermark); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
}

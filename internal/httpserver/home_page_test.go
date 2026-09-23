package httpserver

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHomePage(t *testing.T) {
	h := HomePage()
	for _, method := range []string{"GET", "HEAD", "POST"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/", nil))
		if method == "POST" {
			if w.Code != 405 {
				t.Fatal(w.Code)
			}
			continue
		}
		if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Security-Policy"), "script-src 'sha256-") || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(w.Code, w.Header())
		}
		if method == "HEAD" && w.Body.Len() != 0 {
			t.Fatal("HEAD body")
		}
		if method == "GET" && (!strings.Contains(w.Body.String(), `id="publish"`) || !strings.Contains(w.Body.String(), `id="claim-result"`) || !strings.Contains(w.Body.String(), "manage-dialog")) {
			t.Fatal("missing publishing UI or release limitation")
		}
	}
}

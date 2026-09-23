package httpserver

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUnlockPage(t *testing.T) {
	h := UnlockPage()
	for _, tc := range []struct {
		method, path string
		code         int
	}{{"GET", "/unlock/site-abcdef", 200}, {"HEAD", "/unlock/site-abcdef", 200}, {"GET", "/unlock/<script>", 404}, {"GET", "/unlock/site-abcdef/extra", 404}, {"POST", "/unlock/site-abcdef", 405}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.code {
			t.Fatal(tc, w.Code)
		}
		if tc.code == 200 {
			if !strings.Contains(w.Header().Get("Content-Security-Policy"), "script-src 'sha256-") || strings.Contains(w.Header().Get("Content-Security-Policy"), "unsafe-inline") {
				t.Fatal(w.Header())
			}
			if tc.method == "HEAD" && w.Body.Len() != 0 {
				t.Fatal("HEAD body")
			}
			if tc.method == "GET" && !strings.Contains(w.Body.String(), "Site password") {
				t.Fatal("missing form")
			}
		}
	}
}

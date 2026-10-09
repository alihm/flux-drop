package httpserver

import (
	"archive/zip"
	"bytes"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func invalidUploadBody(t *testing.T, kind string, count int) (string, string, []string) {
	t.Helper()
	names := []string{"index.html", "assets/app.js.map", ".env", "CNAME"}
	if count > 0 {
		names = []string{"index.html"}
		for i := 0; i < count; i++ {
			names = append(names, fmt.Sprintf("assets/bad-%d.map", i))
		}
	}
	var body bytes.Buffer
	media := "application/zip"
	if kind == "zip" {
		w := zip.NewWriter(&body)
		for _, name := range names {
			part, err := w.Create(name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := part.Write([]byte("data")); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		w := multipart.NewWriter(&body)
		for _, name := range names {
			part, err := w.CreateFormFile("files", name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := part.Write([]byte("data")); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		media = w.FormDataContentType()
	}
	want := names[1:]
	if len(want) > 20 {
		want = want[:20]
	}
	return body.String(), media, want
}

func assertUploadDiagnostics(t *testing.T, w *httptest.ResponseRecorder, want []string) {
	t.Helper()
	projectStatus(t, w, http.StatusBadRequest, "invalid_project")
	out := oauthDecode[struct {
		Message string   `json:"message"`
		Files   []string `json:"files"`
	}](t, w)
	if !slices.Equal(out.Files, want) {
		t.Fatalf("got %q, want %q: %s", out.Files, want, w.Body.String())
	}
	for _, name := range want {
		if !strings.Contains(out.Message, strconv.Quote(name)) {
			t.Fatalf("message omitted %q: %s", name, out.Message)
		}
	}
}

func TestUploadDiagnosticsAcrossBrowserBearerAndTickets(t *testing.T) {
	h := firebaseProjectHarness(t, true)
	token := h.projectToken("alice", "google.com", nil)
	cookie, csrf := bootstrap(t, h.handler)
	for _, kind := range []string{"zip", "multipart"} {
		body, media, want := invalidUploadBody(t, kind, 0)
		t.Run("browser-"+kind, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/api/projects?name=bad-browser", strings.NewReader(body))
			r.Header.Set("Content-Type", media)
			r.Header.Set("Origin", h.auth.Origin)
			r.Header.Set("X-CSRF-Token", csrf)
			r.Header.Set("Idempotency-Key", "browser-errors-"+kind)
			r.AddCookie(cookie)
			w := httptest.NewRecorder()
			h.handler.ServeHTTP(w, r)
			assertUploadDiagnostics(t, w, want)
		})
		t.Run("bearer-"+kind, func(t *testing.T) {
			w := h.projectRequest("POST", "/api/agent/projects?name=bad-bearer", body, media, token, 0, http.Header{"Idempotency-Key": {"bearer-errors-" + kind}})
			assertUploadDiagnostics(t, w, want)
		})
	}
	t.Run("upload-ticket", func(t *testing.T) {
		body, media, want := invalidUploadBody(t, "zip", 0)
		path := h.ticketURL(`{"name":"bad-ticket"}`)
		assertUploadDiagnostics(t, h.request("PUT", path, body, media, "", "", nil), want)
		// Failed validation must leave the ticket available for a corrected upload.
		projectStatus(t, h.request("PUT", path, "<h1>Corrected</h1>", "text/html", "", "", nil), 200, "")
	})
	t.Run("bounded-response", func(t *testing.T) {
		body, media, want := invalidUploadBody(t, "zip", 30)
		assertUploadDiagnostics(t, h.projectRequest("POST", "/api/agent/projects?name=bad-many", body, media, token, 0, http.Header{"Idempotency-Key": {"many-errors"}}), want)
	})
}

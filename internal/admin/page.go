package admin

import (
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
)

//go:embed page.html
var pageHTML string

func inlineHash(tag string) string {
	_, rest, _ := strings.Cut(pageHTML, "<"+tag+">")
	body, _, _ := strings.Cut(rest, "</"+tag+">")
	sum := sha256.Sum256([]byte(body))
	return base64.StdEncoding.EncodeToString(sum[:])
}

var pagePolicy = fmt.Sprintf("default-src 'none'; script-src 'sha256-%s'; style-src 'sha256-%s'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'", inlineHash("script"), inlineHash("style"))

func page(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", pagePolicy)
	_, _ = w.Write([]byte(pageHTML))
}

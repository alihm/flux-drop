package httpserver

import (
	"net/http"
	"strings"
)

func notModified(r *http.Request, etag string) bool {
	for _, value := range strings.Split(r.Header.Get("If-None-Match"), ",") {
		value = strings.TrimSpace(value)
		if value == "*" || strings.TrimPrefix(value, "W/") == etag {
			return true
		}
	}
	return false
}

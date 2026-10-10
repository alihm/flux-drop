package httpserver

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/runonflux/flux-drop/internal/analytics"
	"github.com/runonflux/flux-drop/internal/project"
)

type ProjectAnalytics interface {
	Record(string, time.Time)
	PageViews(context.Context, analytics.Query) (analytics.Result, error)
}

type pageViewWriter struct {
	http.ResponseWriter
	status int
}

func (w *pageViewWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *pageViewWriter) WriteHeader(code int) {
	if code >= 100 && code < 200 {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if w.status != 0 {
		return
	}
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
func (w *pageViewWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(data)
}
func (w *pageViewWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func countPageView(r *http.Request, file string) bool {
	if r.Method != http.MethodGet || !htmlFile(file) || r.Header.Get("Range") != "" {
		return false
	}
	if dest := r.Header.Get("Sec-Fetch-Dest"); dest != "" && dest != "document" && dest != "iframe" {
		return false
	}
	purpose := strings.ToLower(r.Header.Get("Purpose") + " " + r.Header.Get("Sec-Purpose"))
	return !strings.Contains(purpose, "prefetch") && !strings.Contains(purpose, "prerender")
}

func registerAnalytics(mux *http.ServeMux, deps Dependencies, browser, bearer func(*http.Request, bool) (project.Actor, error)) {
	handler := func(actor func(*http.Request, bool) (project.Actor, error)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
			defer cancel()
			r = r.WithContext(ctx)
			a, e := actor(r, false)
			if e != nil {
				projectError(w, e)
				return
			}
			// Always use management ownership, never eventual serving metadata or a cache.
			p, e := deps.Projects.Repository.GetOwned(ctx, a, r.PathValue("id"))
			if e != nil {
				projectError(w, e)
				return
			}
			if deps.Analytics == nil {
				analyticsError(w, analytics.ErrUnavailable)
				return
			}
			now := time.Now()
			q := analytics.Query{ProjectID: p.ID, From: analytics.Day(now).AddDate(0, 0, -6), To: analytics.Day(now).AddDate(0, 0, 1), Interval: "day"}
			for name, values := range r.URL.Query() {
				if len(values) != 1 || (name != "from" && name != "to" && name != "interval") {
					analyticsError(w, analytics.ErrInvalid)
					return
				}
			}
			if value := r.URL.Query().Get("interval"); value != "" {
				q.Interval = value
			}
			if value := r.URL.Query().Get("from"); value != "" {
				q.From, e = time.Parse(time.RFC3339, value)
				if e != nil {
					analyticsError(w, analytics.ErrInvalid)
					return
				}
			}
			if value := r.URL.Query().Get("to"); value != "" {
				q.To, e = time.Parse(time.RFC3339, value)
				if e != nil {
					analyticsError(w, analytics.ErrInvalid)
					return
				}
			}
			if e = q.Validate(now); e != nil {
				analyticsError(w, e)
				return
			}
			result, e := deps.Analytics.PageViews(ctx, q)
			if e != nil {
				analyticsError(w, e)
				return
			}
			w.Header().Set("Cache-Control", "no-store")
			respond(w, 200, result)
		}
	}
	mux.HandleFunc("GET /api/projects/{id}/analytics", handler(browser))
	mux.HandleFunc("GET /api/agent/projects/{id}/analytics", handler(bearer))
}
func analyticsError(w http.ResponseWriter, e error) {
	if errors.Is(e, analytics.ErrInvalid) {
		respond(w, 400, map[string]string{"error": "invalid_request", "message": "Use an hourly aligned RFC3339 range within the last 180 UTC days and interval hour or day. Daily ranges must start at midnight UTC."})
		return
	}
	w.Header().Set("Retry-After", "5")
	respond(w, 503, map[string]string{"error": "analytics_unavailable", "message": "Page-view analytics are temporarily unavailable. Site delivery is unaffected."})
}

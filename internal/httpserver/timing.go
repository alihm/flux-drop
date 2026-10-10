package httpserver

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type timingKey struct{}
type timingPhase struct {
	name  string
	start time.Time
}
type requestTiming struct {
	mu     sync.Mutex
	phases map[string]time.Duration
	active []timingPhase
}

// Nested phases pause their parent, so metadata and file durations never overlap.
func measure[T any](r *http.Request, phase string, fn func() (T, error)) (T, error) {
	if t, ok := r.Context().Value(timingKey{}).(*requestTiming); ok {
		now := time.Now()
		t.mu.Lock()
		if n := len(t.active); n > 0 {
			p := t.active[n-1]
			t.phases[p.name] += now.Sub(p.start)
		}
		t.active = append(t.active, timingPhase{phase, now})
		t.mu.Unlock()
		defer func() {
			now := time.Now()
			t.mu.Lock()
			n := len(t.active)
			p := t.active[n-1]
			t.phases[p.name] += now.Sub(p.start)
			t.active = t.active[:n-1]
			if n > 1 {
				t.active[n-2].start = now
			}
			t.mu.Unlock()
		}()
	}
	return fn()
}
func (t *requestTiming) header(elapsed time.Duration) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	values := map[string]time.Duration{}
	for k, v := range t.phases {
		values[k] = v
	}
	if n := len(t.active); n > 0 {
		p := t.active[n-1]
		values[p.name] += time.Since(p.start)
	}
	return fmt.Sprintf("metadata;dur=%.3f, verify;dur=%.3f, file;dur=%.3f, app;dur=%.3f", float64(values["metadata"])/float64(time.Millisecond), float64(values["verify"])/float64(time.Millisecond), float64(values["file"])/float64(time.Millisecond), float64(elapsed)/float64(time.Millisecond))
}

type timingWriter struct {
	http.ResponseWriter
	timing *requestTiming
	start  time.Time
	status int
}

func (w *timingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *timingWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	if status >= 200 {
		w.status = status
		w.Header().Set("Server-Timing", w.timing.header(time.Since(w.start)))
		if status == 503 && w.Header().Get("Retry-After") == "" {
			w.Header().Set("Retry-After", "2")
		}
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *timingWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(p)
}
func (w *timingWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

var slowLogLimit = rate.NewLimiter(rate.Every(100*time.Millisecond), 20)

func observeRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		timing := &requestTiming{phases: map[string]time.Duration{}}
		tw := &timingWriter{ResponseWriter: w, timing: timing, start: start}
		defer func() {
			if elapsed := time.Since(start); elapsed > time.Second && slowLogLimit.Allow() {
				slog.Warn("slow HTTP request", "method", r.Method, "path", safeTimingPath(r), "status", tw.status, "duration", elapsed, "server_timing", timing.header(elapsed))
			}
		}()
		next.ServeHTTP(tw, r.WithContext(context.WithValue(r.Context(), timingKey{}, timing)))
	})
}

func safeTimingPath(r *http.Request) string {
	if strings.HasPrefix(r.URL.Path, "/api/agent/uploads/") {
		return "/api/agent/uploads/{ticket}"
	}
	return r.URL.Path
}

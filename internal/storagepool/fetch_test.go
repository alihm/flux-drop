package storagepool

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
)

func TestColdBurstSharesVerifiedFilesAndManifest(t *testing.T) {
	var manifests, downloads atomic.Int64
	_, _, p := fixture(t, t.TempDir(), func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/manifest") {
				manifests.Add(1)
			}
			if strings.Contains(r.URL.Path, "/files/") {
				downloads.Add(1)
			}
			next.ServeHTTP(w, r)
		})
	})
	var raw bytes.Buffer
	z := zip.NewWriter(&raw)
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("asset%d.txt", i)
		if i == 0 {
			name = "index.html"
		}
		f, _ := z.Create(name)
		fmt.Fprintf(f, "asset %d", i)
	}
	z.Close()
	staged, err := content.StageZIP(t.TempDir(), bytes.NewReader(raw.Bytes()), int64(raw.Len()), content.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Discard()
	pr := prepared(staged)
	pr.Operation.Bytes = 0
	for _, f := range staged.Manifest.Files {
		pr.Operation.Bytes += f.Size
	}
	pr.Project.WatermarkDisabled = true
	if err := p.Install(context.Background(), pr, staged); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			name := fmt.Sprintf("asset%d.txt", i%50)
			if i%50 == 0 {
				name = "index.html"
			}
			r := httptest.NewRequest("GET", "/", nil)
			rec := httptest.NewRecorder()
			p.ServeProject(rec, r, pr.Project, name)
			if rec.Code != 200 || rec.Body.String() != fmt.Sprintf("asset %d", i%50) {
				t.Errorf("response %d: %d %q", i, rec.Code, rec.Body.String())
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if manifests.Load() != 1 || downloads.Load() != 50 {
		t.Fatal("unshared work", manifests.Load(), downloads.Load())
	}
	p.fetch.mu.Lock()
	defer p.fetch.mu.Unlock()
	if len(p.fetch.flights) != 0 || p.fetch.spoolBytes != 0 {
		t.Fatal("leaked flight/reservation")
	}
}
func TestSharedFetchInitiatorCancellationAndIndependentOffsets(t *testing.T) {
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	_, _, p := fixture(t, t.TempDir(), func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/files/") {
				once.Do(func() { close(entered) })
				<-proceed
			}
			next.ServeHTTP(w, r)
		})
	})
	staged := stagedHTML(t, strings.Repeat("a", 32768))
	pr := prepared(staged)
	if err := p.Install(context.Background(), pr, staged); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, _, e := p.fetchFile(ctx, pr.Project, "index.html"); done <- e }()
	<-entered
	result := make(chan error, 1)
	go func() {
		f, _, release, e := p.fetchFile(context.Background(), pr.Project, "index.html")
		if e == nil {
			defer release()
			b, err := io.ReadAll(f)
			e = err
			if len(b) != 32768 {
				e = fmt.Errorf("wrong length %d", len(b))
			}
		}
		result <- e
	}()
	cancel()
	if <-done == nil {
		t.Fatal("canceled caller succeeded")
	}
	close(proceed)
	if e := <-result; e != nil {
		t.Fatal(e)
	}
	f, _, release, e := p.fetchFile(context.Background(), pr.Project, "index.html")
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	b, _ := io.ReadAll(f)
	if len(b) != 32768 {
		t.Fatal("shared offset")
	}
}
func TestFetchAdmissionAndDiskBounds(t *testing.T) {
	_, _, p := fixture(t, t.TempDir(), nil)
	p.fetch.wait = time.Millisecond
	for i := 0; i < cap(p.downloads); i++ {
		p.downloads <- struct{}{}
	}
	if p.acquireFetch(context.Background()) == nil || p.fetch.metrics.WaitTimeout.Load() != 1 {
		t.Fatal("wait timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p.acquireFetch(ctx) == nil {
		t.Fatal("cancellation ignored")
	}
	for len(p.downloads) > 0 {
		<-p.downloads
	}
	p.config.FetchBytes = 8192
	release, err := p.reserveSpool(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.reserveSpool(1); err == nil {
		t.Fatal("oversubscribed spools")
	}
	release()
	release()
	p.fetch.mu.Lock()
	if p.fetch.spoolBytes != 0 {
		t.Fatal("reservation leak")
	}
	p.fetch.mu.Unlock()
}
func TestFetchConfiguration(t *testing.T) {
	values := map[string]string{"DROP_ROLE": "primary", "FLUX_APP_NAME": "primarya", "DROP_STORAGE_APPS_JSON": `[{"appName":"storagea","keyId":"v1","apiKey":"` + testKey + `"}]`}
	for _, key := range []string{"DROP_STORAGE_FETCH_CONCURRENCY", "DROP_STORAGE_FETCH_QUEUE", "DROP_STORAGE_FETCH_BYTES", "DROP_CACHE_ENTRIES"} {
		for _, v := range []string{"-1", "bad", "99999999999999999999999"} {
			values[key] = v
			if _, e := FromEnv(func(k string) string { return values[k] }); e == nil {
				t.Fatal("accepted", key, v)
			}
		}
		delete(values, key)
	}
	values["DROP_STORAGE_FETCH_QUEUE"] = "0"
	c, e := FromEnv(func(k string) string { return values[k] })
	if e != nil || c.FetchQueue != 0 || c.FetchConcurrency != 32 {
		t.Fatal(c, e)
	}
}

func TestWarmHitDoesNotRepeatAuthorization(t *testing.T) {
	_, _, p := fixture(t, t.TempDir(), nil)
	staged := stagedHTML(t, "<h1>hit</h1>")
	pr := prepared(staged)
	if e := p.Install(context.Background(), pr, staged); e != nil {
		t.Fatal(e)
	}
	calls := 0
	check := func(context.Context) error { calls++; return nil }
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		p.ServeAuthorizedProject(rec, httptest.NewRequest("GET", "/", nil), pr.Project, "index.html", check)
		if rec.Code != 200 {
			t.Fatal(rec.Code)
		}
	}
	if calls != 1 {
		t.Fatal("warm hit repeated authorization", calls)
	}
}

package admin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/testmetadata"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testAddress = "1BgGZ9tcN4rm9KBzDn7KprQz87SZ26SAMH"

func signOther(message string) string {
	raw := make([]byte, 32)
	raw[31] = 2
	return base64.StdEncoding.EncodeToString(ecdsa.SignCompact(secp256k1.PrivKeyFromBytes(raw), messageHash(message), true))
}

func sign(message string) string {
	raw := make([]byte, 32)
	raw[31] = 1
	return base64.StdEncoding.EncodeToString(ecdsa.SignCompact(secp256k1.PrivKeyFromBytes(raw), messageHash(message), true))
}
func setup(t *testing.T) *Service {
	t.Helper()
	s, err := New(&metadata.Store{Backend: &testmetadata.Backend{}}, "https://drop.example", testAddress)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestWalletBitcoinJSInteroperability(t *testing.T) {
	// Independently generated with bitcoinjs-message, using the public test key 1.
	vectors := []struct{ message, address, signature string }{
		{"Flux Drop wallet interoperability test", testAddress, "HyEXJ1N3h8+gDmDrmvuf5Et2fyowhzeOgUgYW8BzsOqMV4PdLEnLkGEMaCIqedUBv/Omj97GSl6WS440f+ZbnkI="},
		{"Flux Drop wallet interoperability test", "1EHNa6Q4Jz2uvNExL497mE43ikXhwF6kZm", "GyEXJ1N3h8+gDmDrmvuf5Et2fyowhzeOgUgYW8BzsOqMV4PdLEnLkGEMaCIqedUBv/Omj97GSl6WS440f+ZbnkI="},
		{strings.Repeat("x", 300), testAddress, "IBLBkr8WtWpmpgY+qwfNyTxB102lcO26sUVgxtPS53SKIt8hRC/lGzLkj3i9SmtX2jeuQ8YURTSMK9Qil6KJgO4="},
	}
	for _, v := range vectors {
		if !VerifyWallet(v.message, v.address, v.signature) {
			t.Fatal("valid wallet rejected")
		}
		if VerifyWallet(v.message+"!", v.address, v.signature) || VerifyWallet(v.message, DefaultAddress, v.signature) {
			t.Fatal("invalid signature accepted")
		}
	}
	if addressPayload(DefaultAddress) == nil {
		t.Fatal("default ZelID is invalid")
	}
	if VerifyWallet("test", testAddress, strings.Repeat("A", 88)) || addressPayload(testAddress+"1") != nil {
		t.Fatal("invalid input accepted")
	}
}
func TestReplicaSharedLoginSingleRedemptionAndRevocation(t *testing.T) {
	s := setup(t)
	other, _ := New(s.Store, s.Origin, s.Address)
	ctx := context.Background()
	c, b, err := s.Issue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ready, err := other.Redeem(ctx, c.ID, c.PollToken, b, ""); err != nil || ready {
		t.Fatal("unapproved login", err)
	}
	if _, _, _, err := other.Redeem(ctx, c.ID, c.PollToken, strings.Repeat("0", 64), ""); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("browser binding missing", err)
	}
	if err := other.Approve(ctx, c.Message, sign(c.Message)); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var token string
	var session Session
	success := 0
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, v, ready, err := s.Redeem(ctx, c.ID, c.PollToken, b, "")
			if err == nil && ready {
				mu.Lock()
				success++
				token, session = tok, v
				mu.Unlock()
			} else if !errors.Is(err, ErrUnauthorized) {
				t.Errorf("redemption error %v", err)
			}
		}()
	}
	wg.Wait()
	if success != 1 {
		t.Fatal("challenge replay", success)
	}
	if _, err := other.Read(ctx, token); err != nil {
		t.Fatal("session not shared", err)
	}
	if err := other.Logout(ctx, token, strings.Repeat("x", 43)); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("bad csrf", err)
	}
	if err := other.Logout(ctx, token, session.CSRF); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, token); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revocation not shared", err)
	}
}
func TestChallengeExpiryRateLimitAndWrongWallet(t *testing.T) {
	s := setup(t)
	now := time.Now()
	s.now = func() time.Time { return now }
	ctx := context.Background()
	c, b, _ := s.Issue(ctx)
	if _, _, _, err := s.Redeem(ctx, c.ID, c.PollToken, b, signOther(c.Message)); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("wrong wallet", err)
	}
	for i := 1; i < 20; i++ {
		if _, _, err := s.Issue(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := s.Issue(ctx); !errors.Is(err, ErrLimited) {
		t.Fatal("rate limit", err)
	}
	now = now.Add(5 * time.Minute)
	if err := s.Approve(ctx, c.Message, sign(c.Message)); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("expired challenge", err)
	}
	if _, _, err := s.Issue(ctx); err != nil {
		t.Fatal("budget reset", err)
	}
}
func TestHTTPWalletCallbackOriginCookiesAndCSRF(t *testing.T) {
	s := setup(t)
	h := s.Handler(Source{Snapshot: func(context.Context) (any, error) { return map[string]any{"apps": []string{}}, nil }, Change: func(*metadata.Tx, string, string) error { return nil }})
	request := func(method, path, body, origin string, cookies []*http.Cookie, csrf string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "https://drop.example"+path, strings.NewReader(body))
		if body != "" {
			r.Header.Set("Content-Type", "application/json")
		}
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		r.Header.Set("X-CSRF-Token", csrf)
		for _, c := range cookies {
			r.AddCookie(c)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := request("GET", "/admin/api/apps", "", "", nil, ""); w.Code != 401 {
		t.Fatal("anonymous admin", w.Code)
	}
	if w := request("POST", "/admin/api/challenge", "{}", "https://evil.example", nil, ""); w.Code != 403 {
		t.Fatal("origin accepted", w.Code)
	}
	w := request("POST", "/admin/api/challenge", "{}", s.Origin, nil, "")
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
	var c Challenge
	_ = json.Unmarshal(w.Body.Bytes(), &c)
	if strings.Contains(w.Body.String(), s.Address) || strings.Contains(w.Body.String(), "address") || strings.Contains(c.Message, "Wallet:") {
		t.Fatal("login response reveals configured ZelID")
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatal("unsafe cookie")
	}
	body, _ := json.Marshal(input{Message: c.Message, Signature: sign(c.Message)})
	if w = request("POST", "/admin/api/wallet-callback", string(body), "", nil, ""); w.Code != 200 {
		t.Fatal("callback failed", w.Body)
	}
	body, _ = json.Marshal(input{ID: c.ID, PollToken: c.PollToken})
	w = request("POST", "/admin/api/wallet-status", string(body), s.Origin, cookies, "")
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
	var response struct{ Session Session }
	_ = json.Unmarshal(w.Body.Bytes(), &response)
	if strings.Contains(w.Body.String(), s.Address) || strings.Contains(w.Body.String(), "address") {
		t.Fatal("session response reveals configured ZelID")
	}
	var auth []*http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookie {
			auth = append(auth, c)
		}
	}
	if w = request("GET", "/admin/api/apps", "", "", auth, ""); w.Code != 200 {
		t.Fatal(w.Body)
	}
	if w = request("GET", "/admin/api/apps", "", "", append(auth, auth...), ""); w.Code != 401 {
		t.Fatal("duplicate cookie accepted")
	}
	if w = request("POST", "/admin/api/apps/storagea", `{"action":"remove"}`, s.Origin, auth, ""); w.Code != 401 {
		t.Fatal("csrf missing accepted", w.Code)
	}
	if w = request("POST", "/admin/api/apps/storagea", `{"action":"remove"}`, s.Origin, auth, response.Session.CSRF); w.Code != 200 {
		t.Fatal(w.Body)
	}
	if w = request("POST", "/admin/api/logout", "{}", s.Origin, auth, response.Session.CSRF); w.Code != 200 {
		t.Fatal(w.Body)
	}
	if w = request("GET", "/admin/api/apps", "", "", auth, ""); w.Code != 401 {
		t.Fatal("logged out access")
	}
	w = request("GET", "/admin/", "", "", nil, "")
	if strings.Contains(w.Body.String(), s.Address) || strings.Contains(w.Body.String(), DefaultAddress) || strings.Contains(w.Body.String(), "authorized-wallet") {
		t.Fatal("login page reveals configured ZelID")
	}
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Security-Policy"), "sha256-") || !strings.Contains(w.Body.String(), "Storage overview") {
		t.Fatal("admin page/CSP missing")
	}
}

func TestHTTPManualLoginVerifiesServerWalletWithoutClientAddress(t *testing.T) {
	s := setup(t)
	h := s.Handler(Source{})
	r := httptest.NewRequest("POST", "https://drop.example/admin/api/challenge", nil)
	r.Header.Set("Origin", s.Origin)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
	var c Challenge
	if err := json.Unmarshal(w.Body.Bytes(), &c); err != nil {
		t.Fatal(err)
	}
	browser := w.Result().Cookies()[0]
	for i, sig := range []string{signOther(c.Message), sign(c.Message)} {
		body, _ := json.Marshal(map[string]string{"id": c.ID, "pollToken": c.PollToken, "signature": sig})
		r = httptest.NewRequest("POST", "https://drop.example/admin/api/login", strings.NewReader(string(body)))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Origin", s.Origin)
		r.AddCookie(browser)
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		want := 401
		if i == 1 {
			want = 200
		}
		if w.Code != want {
			t.Fatal("backend wallet check", i, w.Code, w.Body)
		}
		if strings.Contains(w.Body.String(), s.Address) || strings.Contains(w.Body.String(), "address") {
			t.Fatal("wallet disclosed", w.Body)
		}
	}
}

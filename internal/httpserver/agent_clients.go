package httpserver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/session"
)

func agentLoopback(u *url.URL) bool {
	h := strings.ToLower(u.Hostname())
	return u.Scheme == "http" && (h == "127.0.0.1" || h == "::1" || h == "localhost")
}
func agentRedirect(s string) bool {
	if len(s) == 0 || len(s) > 2048 || strings.ContainsAny(s, "\r\n\\") {
		return false
	}
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return false
	}
	if _, e := url.ParseQuery(u.RawQuery); e != nil {
		return false
	}
	if u.Port() != "" {
		p, e := strconv.Atoi(u.Port())
		if e != nil || p < 1 || p > 65535 {
			return false
		}
	}
	switch u.Scheme {
	case "https":
		return u.Hostname() != ""
	case "http":
		return agentLoopback(u)
	case "javascript", "data", "file", "ftp", "ws", "wss", "mailto", "tel", "about", "blob", "vbscript":
		return false
	default:
		return (u.Scheme == "cursor" || u.Scheme == "vscode" || u.Scheme == "vscode-insiders" || strings.Contains(u.Scheme, ".")) && (u.Host != "" || strings.HasPrefix(u.Path, "/"))
	}
}
func agentRedirectMatches(registered, candidate string) bool {
	if !agentRedirect(candidate) {
		return false
	}
	if registered == candidate {
		return true
	}
	a, e1 := url.Parse(registered)
	b, e2 := url.Parse(candidate)
	if e1 != nil || e2 != nil || !agentLoopback(a) || !agentLoopback(b) || a.Hostname() != b.Hostname() {
		return false
	}
	a.Host = a.Hostname()
	b.Host = b.Hostname()
	return a.String() == b.String()
}
func agentClientValid(c project.AgentClient) bool {
	if len(c.RedirectURIs) == 0 || len(c.RedirectURIs) > 20 || len(c.Name) > 200 || !utf8.ValidString(c.Name) || len(c.URI) > 2048 || len(c.Logo) > 2048 {
		return false
	}
	for _, ch := range c.Name {
		if unicode.IsControl(ch) {
			return false
		}
	}
	for _, s := range c.RedirectURIs {
		if !agentRedirect(s) {
			return false
		}
	}
	if c.URI != "" && !agentHTTPSURL(c.URI) {
		return false
	}
	if c.Logo != "" && !agentHTTPSURL(c.Logo) {
		return false
	}
	if c.AuthMethod != "" && c.AuthMethod != "none" {
		return false
	}
	for _, s := range c.ResponseTypes {
		if s != "code" {
			return false
		}
	}
	for _, s := range c.GrantTypes {
		if s != "authorization_code" && s != "refresh_token" {
			return false
		}
	}
	return true
}
func (a *AgentAuth) rate(r *http.Request, kind string, limit int) error {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	// Nginx overwrites this header. Only the loopback Go listener trusts it.
	if ip, e := netip.ParseAddr(host); e == nil && ip.IsLoopback() {
		if peer, e := netip.ParseAddr(r.Header.Get("X-Drop-Client-IP")); e == nil {
			host = peer.String()
		}
	}
	var denied bool
	err = a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error {
		denied = false
		key := "agent_rate/" + agentHash(kind+host+"/"+strconv.FormatInt(a.now().Unix()/60, 10))
		var bucket project.AgentRate
		err := tx.Get(key, &bucket)
		if err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		if bucket.Count >= limit {
			denied = true
			return nil
		}
		bucket.Count++
		bucket.ExpiresAt = a.now().Add(2 * time.Minute)
		return tx.Set(key, bucket)
	})
	if denied {
		return session.ErrRateLimited
	}
	return err
}
func (a *AgentAuth) registerClient(w http.ResponseWriter, r *http.Request) {
	if err := a.rate(r, "register", 20); err != nil {
		if errors.Is(err, session.ErrRateLimited) {
			w.Header().Set("Retry-After", "60")
			agentError(w, 429, "temporarily_unavailable", "Registration rate exceeded")
		} else {
			agentError(w, 503, "temporarily_unavailable", "Registration unavailable")
		}
		return
	}
	kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || kind != "application/json" {
		agentError(w, 400, "invalid_client_metadata", "JSON required")
		return
	}
	// RFC 7591 allows extension metadata. Ignore unrecognized fields, but never
	// reflect them into server records or claim support for confidential clients.
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	var c project.AgentClient
	d := json.NewDecoder(r.Body)
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF || !agentClientValid(c) {
		agentError(w, 400, "invalid_client_metadata", "Public client with valid redirect_uris required")
		return
	}
	// A client cannot self-assign its identifier, domain, or registration lifetime.
	c.ID = "flux_" + agentRandom()
	c.Domain = ""
	c.AuthMethod = "none"
	c.IssuedAt = a.now().Unix()
	c.ExpiresAt = a.now().Add(90 * 24 * time.Hour)
	c.ResponseTypes = []string{"code"}
	c.GrantTypes = []string{"authorization_code", "refresh_token"}
	if c.Name == "" {
		c.Name = "MCP client"
	}
	err = a.Repository.AgentTransaction(r.Context(), func(tx *metadata.Tx) error { return tx.Create("agent_clients/"+agentHash(c.ID), c) })
	if err != nil {
		agentError(w, 503, "temporarily_unavailable", "Registration unavailable")
		return
	}
	respond(w, 201, c)
}
func (a *AgentAuth) clientFor(ctx context.Context, id string) (project.AgentClient, error) {
	if len(id) > 2048 {
		return project.AgentClient{}, project.ErrInvalid
	}
	if strings.HasPrefix(id, "https://") {
		return a.metadataClient(ctx, id)
	}
	var c project.AgentClient
	err := a.Repository.AgentTransaction(ctx, func(tx *metadata.Tx) error { return tx.Get("agent_clients/"+agentHash(id), &c) })
	if err != nil {
		return c, err
	}
	if !a.now().Before(c.ExpiresAt) {
		return c, project.ErrInvalid
	}
	return c, nil
}
func (a *AgentAuth) metadataClient(ctx context.Context, id string) (project.AgentClient, error) {
	u, e := url.Parse(id)
	if e != nil || !agentHTTPSURL(id) || u.Path == "" || u.Path == "/" {
		return project.AgentClient{}, project.ErrInvalid
	}
	a.cimdMu.Lock()
	defer a.cimdMu.Unlock()
	if c, ok := a.cimd[id]; ok && a.now().Before(c.ExpiresAt) {
		return c, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, "GET", id, nil)
	if e != nil {
		return project.AgentClient{}, e
	}
	req.Header.Set("Accept", "application/json")
	resp, e := a.cimdClient.Do(req)
	if e != nil {
		return project.AgentClient{}, project.ErrInvalid
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return project.AgentClient{}, project.ErrInvalid
	}
	data, e := io.ReadAll(io.LimitReader(resp.Body, 32<<10+1))
	var c project.AgentClient
	if e != nil || len(data) > 32<<10 || json.Unmarshal(data, &c) != nil || c.ID != id || !agentClientValid(c) {
		return c, project.ErrInvalid
	}
	c.Domain = u.Hostname()
	c.AuthMethod = "none"
	c.ExpiresAt = a.now().Add(5 * time.Minute)
	if c.Name == "" {
		c.Name = c.Domain
	}
	if len(a.cimd) >= 256 {
		for k, v := range a.cimd {
			if !a.now().Before(v.ExpiresAt) {
				delete(a.cimd, k)
			}
		}
		if len(a.cimd) >= 256 {
			a.cimd = map[string]project.AgentClient{}
		}
	}
	a.cimd[id] = c
	return c, nil
}

var agentDeniedNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2001::/32"), netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("64:ff9b::/96"),
}

func agentPublicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
		return false
	}
	for _, p := range agentDeniedNetworks {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}
func newAgentMetadataClient() *http.Client {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	t := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 3 * time.Second, DisableKeepAlives: true}
	t.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, e
		}
		ips, e := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if e != nil || len(ips) == 0 {
			return nil, project.ErrInvalid
		}
		for _, ip := range ips {
			if !agentPublicIP(ip) {
				return nil, project.ErrInvalid
			}
		}
		// Dial the validated literal, keeping TLS ServerName derived from the URL.
		// There is no second DNS lookup and no proxy/redirect SSRF escape hatch.
		var last error
		for _, ip := range ips {
			conn, e := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if e == nil {
				return conn, nil
			}
			last = e
		}
		return nil, last
	}
	return &http.Client{Timeout: 5 * time.Second, Transport: t, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

package replica

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var instanceName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// PeerTLS requires an application-scoped CA and a unique certificate/key per
// replica. Certificates need serverAuth and clientAuth usage, the shared DNS SAN
// <app>.peer.flux-drop, and URI spiffe://flux-drop/<app>/<instance>.
// No secrets are read from replicated content storage by this constructor.
func PeerTLS(app, instance string, certificate tls.Certificate, roots *x509.CertPool) (client, server *tls.Config, err error) {
	if !appName.MatchString(app) || !instanceName.MatchString(instance) || roots == nil || len(roots.Subjects()) == 0 || len(certificate.Certificate) == 0 || certificate.PrivateKey == nil {
		return nil, nil, errors.New("invalid peer TLS configuration")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, nil, err
	}
	if identity(leaf, app) != instance {
		return nil, nil, errors.New("local peer certificate identity mismatch")
	}
	name := strings.ToLower(app) + ".peer.flux-drop"
	if err := leaf.VerifyHostname(name); err != nil {
		return nil, nil, err
	}
	verify := func(state tls.ConnectionState) error {
		// Standard chain, expiry, EKU and server hostname checks run first.
		if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 || identity(state.PeerCertificates[0], app) == "" {
			return errors.New("untrusted peer application identity")
		}
		return nil
	}
	client = &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots.Clone(), Certificates: []tls.Certificate{certificate}, ServerName: name, VerifyConnection: verify}
	server = &tls.Config{MinVersion: tls.VersionTLS13, ClientCAs: roots.Clone(), Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert, VerifyConnection: verify}
	return client, server, nil
}

func identity(cert *x509.Certificate, app string) string {
	if len(cert.URIs) != 1 {
		return ""
	}
	u := cert.URIs[0]
	if u.Scheme != "spiffe" || u.Host != "flux-drop" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
		return ""
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 || parts[0] != app || !instanceName.MatchString(parts[1]) {
		return ""
	}
	return parts[1]
}

// PeerIdentity exposes the already-validated application-scoped certificate
// identity to the cluster transport. A caller must also verify the TLS chain.
func PeerIdentity(cert *x509.Certificate, app string) string { return identity(cert, app) }

// NewPeerClient never uses environment proxies, follows redirects, or adds
// browser credentials. Callers must select destinations from fresh discovery.
func NewPeerClient(config *tls.Config) (*http.Client, error) {
	if config == nil || config.InsecureSkipVerify || config.RootCAs == nil || config.ServerName == "" || config.VerifyConnection == nil || len(config.Certificates) == 0 || config.MinVersion < tls.VersionTLS13 {
		return nil, errors.New("verified peer TLS required")
	}
	transport := &http.Transport{
		TLSClientConfig:     config.Clone(),
		DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 5 * time.Second,
		MaxResponseHeaderBytes: 16 << 10, MaxIdleConns: 16, MaxIdleConnsPerHost: 2,
		MaxConnsPerHost: 4, IdleConnTimeout: 30 * time.Second, DisableCompression: true,
	}
	return &http.Client{Transport: transport, Timeout: 5 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("peer redirects forbidden") }}, nil
}

// PeerBoundary must wrap a local-only handler on the separate TLS listener.
// It is not a public Nginx route and cannot authorize private project content.
// local must check authoritative metadata and must never invoke peer fallback.
func PeerBoundary(app string, local http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 || identity(r.TLS.PeerCertificates[0], app) == "" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if len(r.Header.Values("X-Drop-Peer-Hop")) != 1 || r.Header.Get("X-Drop-Peer-Hop") != "1" || r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			http.Error(w, "invalid peer request", http.StatusBadRequest)
			return
		}
		if err := validatePeerPath(r.URL); err != nil {
			http.NotFound(w, r)
			return
		}
		local.ServeHTTP(w, r)
	})
}

var peerSlug = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,46}[a-z0-9])?-[a-f0-9]{6}$`)

func validatePeerPath(u *url.URL) error {
	const prefix = "/_drop_peer/content/"
	if u.RawQuery != "" || u.ForceQuery || u.EscapedPath() != (&url.URL{Path: u.Path}).EscapedPath() || strings.ContainsAny(u.Path, "%\\") || len(u.Path) > 1200 {
		return fmt.Errorf("invalid peer path")
	}
	if slug, ok := strings.CutPrefix(u.Path, "/_drop_peer/manifest/"); ok {
		if peerSlug.MatchString(slug) {
			return nil
		}
		return fmt.Errorf("invalid peer path")
	}
	if !strings.HasPrefix(u.Path, prefix) {
		return fmt.Errorf("invalid peer path")
	}
	slug, file, ok := strings.Cut(strings.TrimPrefix(u.Path, prefix), "/")
	if !ok || !peerSlug.MatchString(slug) || file == "" {
		return fmt.Errorf("invalid peer path")
	}
	for _, part := range strings.Split(file, "/") {
		if part == "" || strings.HasPrefix(part, ".") {
			return fmt.Errorf("invalid peer path")
		}
	}
	return nil
}

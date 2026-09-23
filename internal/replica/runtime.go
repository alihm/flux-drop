package replica

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type RuntimeConfig struct {
	App, Instance, DataRoot, Certificate, Key, CA string
	Port                                          uint16
	ListenPort                                    uint16
	Self                                          []netip.Addr
}

// RuntimeConfigFromEnv is all-or-nothing: partial peer configuration cannot
// silently enable an unauthenticated listener or a default application.
func RuntimeConfigFromEnv(get func(string) string) (*RuntimeConfig, error) {
	enabled := get("DROP_PEERS_ENABLED")
	if enabled == "" || enabled == "false" {
		return nil, nil
	}
	if enabled != "true" {
		return nil, errors.New("DROP_PEERS_ENABLED must be true or false")
	}
	port, err := strconv.ParseUint(get("REPLICA_PORT"), 10, 16)
	if err != nil || port < 1024 || port == 8080 || port == 8081 {
		return nil, errors.New("REPLICA_PORT must be an unprivileged dedicated peer port")
	}
	c := &RuntimeConfig{App: get("FLUX_APP_NAME"), Instance: get("DROP_INSTANCE_ID"), DataRoot: get("DROP_DATA_DIR"), Certificate: get("DROP_PEER_CERT_FILE"), Key: get("DROP_PEER_KEY_FILE"), CA: get("DROP_PEER_CA_FILE"), Port: uint16(port)}
	c.ListenPort = c.Port
	if raw := get("DROP_PEER_LISTEN_PORT"); raw != "" {
		p, err := strconv.ParseUint(raw, 10, 16)
		if err != nil || p < 1024 || p == 8080 || p == 8081 {
			return nil, errors.New("invalid DROP_PEER_LISTEN_PORT")
		}
		c.ListenPort = uint16(p)
	}
	if !appName.MatchString(c.App) || !instanceName.MatchString(c.Instance) {
		return nil, errors.New("invalid peer app or instance identity")
	}
	self := get("DROP_REPLICA_SELF_IPS")
	if self == "" {
		self = get("FLUX_NODE_HOST_IP")
	}
	for _, raw := range strings.Split(self, ",") {
		ip, err := netip.ParseAddr(strings.TrimSpace(raw))
		if err != nil || !publicIP(ip.Unmap()) {
			return nil, errors.New("DROP_REPLICA_SELF_IPS requires public IP literals")
		}
		c.Self = append(c.Self, ip.Unmap())
	}
	for _, path := range []string{c.DataRoot, c.Certificate, c.Key, c.CA} {
		if !filepath.IsAbs(path) || filepath.Clean(path) == "/" {
			return nil, errors.New("peer data and credential paths must be explicit absolute paths")
		}
	}
	return c, nil
}

type Runtime struct {
	Discovery  *Discovery
	Fallback   *Fallback
	server     *http.Server
	cancel     context.CancelFunc
	workerDone chan struct{}
	serveDone  chan error
}

func StartRuntime(parent context.Context, c RuntimeConfig, repository Resolver) (*Runtime, error) {
	return startRuntime(parent, c, repository, runtimeHooks{listen: net.Listen})
}

// Private hooks let lifecycle tests use loopback sockets and a recorded
// discovery response without adding insecure production environment switches.
type runtimeHooks struct {
	listen    func(string, string) (net.Listener, error)
	configure func(*Discovery, *Fallback)
}

func startRuntime(parent context.Context, c RuntimeConfig, repository Resolver, hooks runtimeHooks) (*Runtime, error) {
	if c.ListenPort == 0 {
		c.ListenPort = c.Port
	}
	if c.ListenPort < 1024 || c.ListenPort == 8080 || c.ListenPort == 8081 {
		return nil, errors.New("invalid peer listen port")
	}
	if repository == nil || c.Port < 1024 || c.Port == 8080 || c.Port == 8081 || len(c.Self) == 0 {
		return nil, errors.New("invalid peer runtime configuration")
	}
	dataRoot, err := filepath.EvalSymlinks(c.DataRoot)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(dataRoot)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("peer data root must be a directory")
	}
	// Resolve symlinks before checking secret placement: mounted secrets may be
	// symlinks, but must never resolve into the replicated project volume.
	for _, path := range []string{c.Certificate, c.Key, c.CA} {
		real, err := filepath.EvalSymlinks(path)
		if err != nil {
			return nil, err
		}
		rel, err := filepath.Rel(dataRoot, real)
		if err != nil || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))) {
			return nil, errors.New("peer credentials must be outside replicated data")
		}
	}
	certificate, err := tls.LoadX509KeyPair(c.Certificate, c.Key)
	if err != nil {
		return nil, err
	}
	caBytes, err := os.ReadFile(c.CA)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caBytes) {
		return nil, errors.New("invalid peer CA bundle")
	}
	clientTLS, serverTLS, err := PeerTLS(c.App, c.Instance, certificate, roots)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, err
	}
	intermediates := x509.NewCertPool()
	for _, der := range certificate.Certificate[1:] {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, err
		}
		intermediates.AddCert(cert)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth} {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return nil, fmt.Errorf("invalid local peer certificate: %w", err)
		}
	}
	// The coordinator renews the shared node-local leaf file. Reload it for
	// new content-peer handshakes, while pinning the original key and CA.
	publicKey := bytes.Clone(leaf.RawSubjectPublicKeyInfo)
	reload := func() (*tls.Certificate, error) {
		pair, err := tls.LoadX509KeyPair(c.Certificate, c.Key)
		if err != nil {
			return nil, err
		}
		cert, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			return nil, err
		}
		if PeerIdentity(cert, c.App) != c.Instance || !bytes.Equal(cert.RawSubjectPublicKeyInfo, publicKey) {
			return nil, errors.New("content peer identity changed")
		}
		chain := x509.NewCertPool()
		for _, der := range pair.Certificate[1:] {
			parent, err := x509.ParseCertificate(der)
			if err != nil {
				return nil, err
			}
			chain.AddCert(parent)
		}
		for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth} {
			if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, Intermediates: chain, DNSName: clientTLS.ServerName, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
				return nil, err
			}
		}
		return &pair, nil
	}
	clientTLS.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return reload() }
	serverTLS.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return reload() }
	discovery, err := NewDiscovery(c.App, c.Port, c.Self)
	if err != nil {
		return nil, err
	}
	fallback, err := NewFallback(discovery, clientTLS)
	if err != nil {
		return nil, err
	}
	if hooks.configure != nil {
		hooks.configure(discovery, fallback)
	}
	listener, err := hooks.listen("tcp", net.JoinHostPort("", strconv.Itoa(int(c.ListenPort))))
	if err != nil {
		fallback.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	r := &Runtime{Discovery: discovery, Fallback: fallback, cancel: cancel, workerDone: make(chan struct{}), serveDone: make(chan error, 1)}
	r.server = &http.Server{Handler: LocalDelivery(c.App, repository, dataRoot), TLSConfig: serverTLS, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 310 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() { defer close(r.workerDone); discovery.Run(ctx) }()
	go func() { r.serveDone <- r.server.ServeTLS(listener, "", "") }()
	return r, nil
}

func (r *Runtime) Errors() <-chan error { return r.serveDone }
func (r *Runtime) Close(ctx context.Context) error {
	r.cancel()
	err := r.server.Shutdown(ctx)
	if err != nil {
		_ = r.server.Close()
	}
	r.Fallback.Close()
	select {
	case <-r.workerDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	return err
}

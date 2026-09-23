package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/runonflux/flux-drop/internal/replica"
)

// RuntimeConfig is an operator-supplied, node-specific manifest. It is not
// synthesized from a possibly partial discovery response or request headers.
type RuntimeConfig struct {
	App                  string       `json:"app"`
	ClusterID            string       `json:"clusterID"`
	Local                Member       `json:"local"`
	StateDir             string       `json:"stateDir"`
	ContentDir           string       `json:"contentDir"`
	Listen               string       `json:"listen"`
	StatusListen         string       `json:"statusListen"`
	StatusPort           uint16       `json:"statusPort"`
	SelfIPs              []netip.Addr `json:"selfIPs"`
	Certificate          string       `json:"certificate"`
	Key                  string       `json:"key"`
	CA                   string       `json:"ca"`
	CABundleFile         string       `json:"caBundleFile,omitempty"`
	EnrollmentCredential string       `json:"enrollmentCredential,omitempty"`
	InitialVoters        []Member     `json:"initialVoters"`
	AsyncContent         bool         `json:"asyncContent,omitempty"`
	Automatic            bool         `json:"automatic,omitempty"`
}

func DecodeRuntimeConfig(data []byte) (RuntimeConfig, error) {
	var c RuntimeConfig
	if len(data) > 64<<10 || decodeStrict(data, &c) != nil {
		return c, errors.New("invalid cluster runtime manifest")
	}
	if c.ContentDir == "" {
		c.ContentDir = "/data"
	}
	if c.StateDir == "" {
		c.StateDir = "/var/lib/drop-cluster"
	}
	if c.Listen == "" {
		c.Listen = "0.0.0.0:8445"
	}
	if c.StatusListen == "" {
		c.StatusListen = "0.0.0.0:8446"
	}
	if c.StatusPort == 0 {
		c.StatusPort = 8446
	}
	if c.managedCertificates() {
		if c.Certificate == "" {
			c.Certificate = filepath.Join(c.StateDir, "node.crt")
		}
		if c.Key == "" {
			c.Key = filepath.Join(c.StateDir, "node.key")
		}
		if c.CA == "" {
			c.CA = filepath.Join(c.StateDir, "ca.crt")
		}
	}
	return c, c.validate()
}

func LoadRuntimeConfig(path string) (RuntimeConfig, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return RuntimeConfig{}, err
	}
	file, err := os.Open(abs)
	if err != nil {
		return RuntimeConfig{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return RuntimeConfig{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 {
		return RuntimeConfig{}, errors.New("cluster manifest must be a regular file not writable by group/others")
	}
	data, err := io.ReadAll(io.LimitReader(file, 64<<10+1))
	if err != nil {
		return RuntimeConfig{}, err
	}
	c, err := DecodeRuntimeConfig(data)
	if err != nil {
		return c, err
	}
	manifest, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return c, err
	}
	content, err := filepath.EvalSymlinks(c.ContentDir)
	if err != nil {
		return c, err
	}
	if overlap(content, manifest) {
		return c, errors.New("cluster manifest must be outside Flux-synchronized content")
	}
	return c, nil
}

func (c RuntimeConfig) nodeConfig() Config {
	return Config{ClusterID: c.ClusterID, Local: c.Local, StateDir: c.StateDir, ContentDir: c.ContentDir, InitialVoters: c.InitialVoters, AsyncContent: c.AsyncContent, Automatic: c.Automatic}
}

func (c RuntimeConfig) validate() error {
	if c.Automatic && (!c.AsyncContent || c.CABundleFile == "" || len(c.InitialVoters) != 0) {
		return errors.New("automatic mode requires managed local TLS, asynchronous content and discovery bootstrap")
	}
	if err := c.nodeConfig().validate(); err != nil {
		return err
	}
	raftListen, err := netip.ParseAddrPort(c.Listen)
	if err != nil {
		return errors.New("invalid Raft listen address")
	}
	statusListen, err := netip.ParseAddrPort(c.StatusListen)
	if err != nil {
		return errors.New("invalid status listen address")
	}
	for _, p := range []uint16{raftListen.Port(), statusListen.Port(), c.StatusPort} {
		if p < 1024 || p == 8080 || p == 8081 {
			return errors.New("cluster listeners need dedicated unprivileged ports")
		}
	}
	if raftListen.Port() == statusListen.Port() {
		return errors.New("Raft and status listeners must use separate ports")
	}
	for _, a := range []netip.Addr{raftListen.Addr(), statusListen.Addr()} {
		if a.IsMulticast() || a.Zone() != "" {
			return errors.New("invalid cluster listener address")
		}
	}
	if len(c.SelfIPs) == 0 {
		return errors.New("explicit local node IPs are required for discovery self-exclusion")
	}
	for _, ip := range c.SelfIPs {
		if !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() || ip.Zone() != "" {
			return errors.New("invalid discovery self address")
		}
	}
	for _, path := range []string{c.Certificate, c.Key, c.CA} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("explicit absolute cluster certificate paths are required")
		}
	}
	if c.EnrollmentCredential != "" && (!filepath.IsAbs(c.EnrollmentCredential) || filepath.Clean(c.EnrollmentCredential) != c.EnrollmentCredential) {
		return errors.New("enrollment credential path must be absolute and clean")
	}
	if c.CABundleFile != "" && (!filepath.IsAbs(c.CABundleFile) || filepath.Clean(c.CABundleFile) != c.CABundleFile) {
		return errors.New("CA bundle path must be absolute and clean")
	}
	_, err = replica.NewDiscovery(c.App, c.StatusPort, c.SelfIPs)
	return err
}

func (c RuntimeConfig) tlsMaterial() (TLSMaterial, error) {
	root, err := filepath.EvalSymlinks(c.ContentDir)
	if err != nil {
		return TLSMaterial{}, err
	}
	for _, path := range []string{c.Certificate, c.Key, c.CA} {
		real, err := filepath.EvalSymlinks(path)
		if err != nil {
			return TLSMaterial{}, err
		}
		if overlap(root, real) {
			return TLSMaterial{}, errors.New("cluster certificates must be outside synchronized content")
		}
	}
	certificate, err := tls.LoadX509KeyPair(c.Certificate, c.Key)
	if err != nil {
		return TLSMaterial{}, err
	}
	ca, err := os.ReadFile(c.CA)
	if err != nil {
		return TLSMaterial{}, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return TLSMaterial{}, errors.New("invalid cluster trust bundle")
	}
	client, server, err := replica.PeerTLS(c.App, c.Local.ID, certificate, roots)
	if err != nil {
		return TLSMaterial{}, err
	}
	leaf, err := parseLeaf(certificate)
	if err != nil {
		return TLSMaterial{}, err
	}
	intermediates := x509.NewCertPool()
	for _, der := range certificate.Certificate[1:] {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return TLSMaterial{}, err
		}
		intermediates.AddCert(cert)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return TLSMaterial{}, fmt.Errorf("invalid local cluster certificate: %w", err)
		}
	}
	publicKey := bytes.Clone(leaf.RawSubjectPublicKeyInfo)
	reload := func() (*tls.Certificate, error) {
		pair, err := tls.LoadX509KeyPair(c.Certificate, c.Key)
		if err != nil {
			return nil, err
		}
		leaf, err := parseLeaf(pair)
		if err != nil {
			return nil, err
		}
		if replica.PeerIdentity(leaf, c.App) != c.Local.ID || !bytes.Equal(publicKey, leaf.RawSubjectPublicKeyInfo) {
			return nil, errors.New("TLS renewal changed node identity")
		}
		chain := x509.NewCertPool()
		for _, der := range pair.Certificate[1:] {
			cert, err := x509.ParseCertificate(der)
			if err != nil {
				return nil, err
			}
			chain.AddCert(cert)
		}
		for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: chain, DNSName: client.ServerName, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
				return nil, err
			}
		}
		return &pair, nil
	}
	client.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return reload() }
	server.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return reload() }
	return TLSMaterial{App: c.App, Client: client, Server: server}, nil
}

type Runtime struct {
	Node      *Node
	Observer  *Observer
	Discovery *replica.Discovery
	server    *http.Server
	cancel    context.CancelFunc
	workers   sync.WaitGroup
	errors    chan error
}

func StartRuntime(parent context.Context, c RuntimeConfig) (*Runtime, error) {
	return startRuntime(parent, c, func(ctx context.Context, d *replica.Discovery) { d.Run(ctx) })
}

func startRuntime(parent context.Context, c RuntimeConfig, runDiscovery func(context.Context, *replica.Discovery)) (*Runtime, error) {
	return startRuntimeWithSetup(parent, c, runDiscovery, nil)
}

// setup is an internal test seam, never exposed through deployment settings.
func startRuntimeWithSetup(parent context.Context, c RuntimeConfig, runDiscovery func(context.Context, *replica.Discovery), setup func(*Runtime)) (*Runtime, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if err := EnsureCertificate(c); err != nil {
		return nil, err
	}
	material, err := c.tlsMaterial()
	if err != nil {
		return nil, err
	}
	discovery, err := replica.NewDiscovery(c.App, c.StatusPort, c.SelfIPs)
	if err != nil {
		return nil, err
	}
	observer, err := NewObserver(discovery, c.ClusterID, c.App, material.Client)
	if err != nil {
		return nil, err
	}
	node, err := StartTLS(c.nodeConfig(), c.Listen, material)
	if err != nil {
		return nil, err
	}
	if c.Automatic {
		source, err := replica.NewDiscovery(c.App, c.StatusPort, nil)
		if err != nil {
			_ = node.Close()
			return nil, err
		}
		node.auto = &automatic{node: node, observer: observer, source: source, refresh: source.Refresh}
	}
	if c.EnrollmentCredential != "" || os.Getenv("DROP_CLUSTER_ENROLLMENT_KEY") != "" || c.managedCertificates() {
		node.enrollment, err = newEnrollment(node, observer, c, material)
		if err != nil {
			_ = node.Close()
			return nil, err
		}
	}
	listener, err := net.Listen("tcp", c.StatusListen)
	if err != nil {
		_ = node.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	r := &Runtime{Node: node, Observer: observer, Discovery: discovery, cancel: cancel, errors: make(chan error, 1)}
	r.server = &http.Server{Handler: node.statusHandler(c.App, observer), TLSConfig: material.Server.Clone(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 7 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 8 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	if setup != nil {
		setup(r)
	}
	r.workers.Add(2)
	go func() { defer r.workers.Done(); runDiscovery(ctx, discovery) }()
	go func() { defer r.workers.Done(); observer.Run(ctx) }()
	if node.auto != nil {
		r.workers.Add(1)
		go func() { defer r.workers.Done(); node.auto.run(ctx) }()
	}
	if c.managedCertificates() {
		r.workers.Add(1)
		go func() { defer r.workers.Done(); c.renewCertificates(ctx) }()
	}
	if node.enrollment != nil {
		r.workers.Add(1)
		go func() { defer r.workers.Done(); node.enrollment.Run(ctx) }()
	}
	go func() { r.errors <- r.server.ServeTLS(listener, "", "") }()
	return r, nil
}

func (r *Runtime) Errors() <-chan error { return r.errors }
func (r *Runtime) Close(ctx context.Context) error {
	r.cancel()
	err := r.server.Shutdown(ctx)
	if err != nil {
		_ = r.server.Close()
	}
	closed := make(chan struct{})
	go func() { r.workers.Wait(); close(closed) }()
	select {
	case <-closed:
	case <-ctx.Done():
		if err == nil {
			err = ctx.Err()
		}
	}
	if closeErr := r.Node.Close(); err == nil {
		err = closeErr
	}
	return err
}

package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/replica"
)

// TLSMaterial must originate from the dedicated cluster's trust bundle, never
// from public discovery or a certificate/CA stored in synchronized content.
type TLSMaterial struct {
	App    string
	Client *tls.Config
	Server *tls.Config
}

func (m TLSMaterial) validate() error {
	c, s := m.Client, m.Server
	if m.App == "" || c == nil || s == nil || c.InsecureSkipVerify || c.MinVersion < tls.VersionTLS13 || s.MinVersion < tls.VersionTLS13 || c.RootCAs == nil || s.ClientCAs == nil || c.ServerName == "" || c.VerifyConnection == nil || s.VerifyConnection == nil || s.ClientAuth != tls.RequireAndVerifyClientCert || len(c.Certificates) != 1 || len(s.Certificates) != 1 {
		return errors.New("verified, mutually authenticated cluster TLS is required")
	}
	return nil
}

type streamAddress string

func (a streamAddress) Network() string { return "tcp" }
func (a streamAddress) String() string  { return string(a) }

type tlsStream struct {
	listener  net.Listener
	material  TLSMaterial
	local     Member
	protocol  string
	accepted  chan net.Conn
	done      chan struct{}
	slots     chan struct{}
	workers   sync.WaitGroup
	closeOnce sync.Once
}

func newStream(listener net.Listener, c Config, m TLSMaterial) (*tlsStream, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	// Own these configurations: net/http mutates its server TLS config while
	// enabling HTTP/2, which must never race with Raft's handshake workers.
	m.Client = m.Client.Clone()
	m.Server = m.Server.Clone()
	for _, cfg := range []*tls.Config{m.Client, m.Server} {
		leaf, err := parseLeaf(cfg.Certificates[0])
		if err != nil || replica.PeerIdentity(leaf, m.App) != c.Local.ID {
			return nil, errors.New("TLS certificate does not match local Raft identity")
		}
	}
	s := &tlsStream{listener: listener, material: m, local: c.Local, protocol: "flux-drop-raft/1/" + c.ClusterID, accepted: make(chan net.Conn), done: make(chan struct{}), slots: make(chan struct{}, 32)}
	s.workers.Add(1)
	go s.acceptLoop()
	return s, nil
}

func (s *tlsStream) acceptLoop() {
	defer s.workers.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		select {
		case s.slots <- struct{}{}:
		default:
			_ = conn.Close()
			continue
		}
		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			defer func() { <-s.slots }()
			cfg := s.material.Server.Clone()
			cfg.NextProtos = []string{s.protocol}
			secure := tls.Server(conn, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := secure.HandshakeContext(ctx); err != nil || secure.ConnectionState().NegotiatedProtocol != s.protocol {
				_ = secure.Close()
				return
			}
			select {
			case s.accepted <- secure:
			case <-s.done:
				_ = secure.Close()
			}
		}()
	}
}

func (s *tlsStream) Accept() (net.Conn, error) {
	select {
	case c := <-s.accepted:
		return c, nil
	case <-s.done:
		return nil, net.ErrClosed
	}
}
func (s *tlsStream) Addr() net.Addr { return streamAddress(s.local.raftAddress()) }
func (s *tlsStream) Close() error {
	var err error
	s.closeOnce.Do(func() { close(s.done); err = s.listener.Close(); s.workers.Wait() })
	return err
}
func (s *tlsStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	member, err := parseAddress(string(address))
	if err != nil {
		return nil, err
	}
	if timeout <= 0 || timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cfg := s.material.Client.Clone()
	cfg.NextProtos = []string{s.protocol}
	verify := cfg.VerifyConnection
	cfg.VerifyConnection = func(state tls.ConnectionState) error {
		if err := verify(state); err != nil {
			return err
		}
		if state.NegotiatedProtocol != s.protocol || len(state.PeerCertificates) == 0 || replica.PeerIdentity(state.PeerCertificates[0], s.material.App) != member.ID {
			return errors.New("Raft destination identity or cluster mismatch")
		}
		return nil
	}
	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: timeout}, Config: cfg}
	return dialer.DialContext(ctx, "tcp", member.Address)
}

// StartTLS is the only production node constructor. Both raw Raft traffic and
// future status/forwarding listeners are separate from public Nginx.
func StartTLS(c Config, listen string, material TLSMaterial) (*Node, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	stream, err := newStream(listener, c, material)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	transport := raft.NewNetworkTransport(stream, 3, 5*time.Second, io.Discard)
	node, err := start(c, transport, nil)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	return node, nil
}

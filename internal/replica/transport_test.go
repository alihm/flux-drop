package replica

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func peerCertificates(t *testing.T) (*x509.CertPool, func(string, string) tls.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	serial := int64(1)
	return roots, func(app, instance string) tls.Certificate {
		serial++
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		u, _ := url.Parse("spiffe://flux-drop/" + app + "/" + instance)
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{strings.ToLower(app) + ".peer.flux-drop"}, URIs: []*url.URL{u}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &k.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der, ca.Raw}, PrivateKey: k}
	}
}

func TestPeerMutualTLSAndBoundary(t *testing.T) {
	roots, issue := peerCertificates(t)
	clientTLS, _, err := PeerTLS("drop", "first", issue("drop", "first"), roots)
	if err != nil {
		t.Fatal(err)
	}
	_, serverTLS, err := PeerTLS("drop", "second", issue("drop", "second"), roots)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(PeerBoundary("drop", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("local only")) })))
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()
	client, err := NewPeerClient(clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	for _, tc := range []struct {
		method, path, hop, cookie string
		status                    int
	}{
		{"GET", "/_drop_peer/content/site-abcdef/index.html", "1", "", 200},
		{"HEAD", "/_drop_peer/content/site-abcdef/index.html", "1", "", 200},
		{"GET", "/_drop_peer/content/site-abcdef/index.html", "2", "", 400},
		{"GET", "/_drop_peer/content/site-abcdef/index.html", "", "", 400},
		{"GET", "/_drop_peer/content/site-abcdef/index.html", "1", "session=secret", 400},
		{"POST", "/_drop_peer/content/site-abcdef/index.html", "1", "", 405},
		{"GET", "/api/session", "1", "", 404},
		{"GET", "/_drop_peer/content/site-abcdef/../manifest.json", "1", "", 404},
		{"GET", "/_drop_peer/content/site-abcdef/%69ndex.html", "1", "", 404},
		{"GET", "/_drop_peer/content/site-abcdef/index.html?token=secret", "1", "", 404},
	} {
		req, _ := http.NewRequest(tc.method, server.URL+tc.path, nil)
		if tc.hop != "" {
			req.Header.Set("X-Drop-Peer-Hop", tc.hop)
		}
		if tc.cookie != "" {
			req.Header.Set("Cookie", tc.cookie)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != tc.status {
			t.Errorf("%s %s: %d", tc.method, tc.path, res.StatusCode)
		}
	}
	// A valid certificate under the same CA is insufficient for another app.
	for _, cert := range []tls.Certificate{{}, issue("other", "third")} {
		cfg := clientTLS.Clone()
		cfg.Certificates = nil
		if len(cert.Certificate) > 0 {
			cfg.Certificates = []tls.Certificate{cert}
		}
		bad := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}, Timeout: time.Second}
		res, err := bad.Get(server.URL + "/_drop_peer/content/site-abcdef/index.html")
		if err == nil {
			res.Body.Close()
			t.Error("untrusted client completed handshake")
		}
		bad.CloseIdleConnections()
	}
	wrongRoots, _ := peerCertificates(t)
	cfg := clientTLS.Clone()
	cfg.RootCAs = wrongRoots
	bad, _ := NewPeerClient(cfg)
	if res, err := bad.Get(server.URL); err == nil {
		res.Body.Close()
		t.Error("untrusted server accepted")
	}
	bad.CloseIdleConnections()
	plain := httptest.NewRecorder()
	PeerBoundary("drop", http.NotFoundHandler()).ServeHTTP(plain, httptest.NewRequest("GET", "/_drop_peer/content/site-abcdef/index.html", nil))
	if plain.Code != 403 {
		t.Fatal("plaintext accepted")
	}
}

func TestPeerTLSConfiguration(t *testing.T) {
	roots, issue := peerCertificates(t)
	cert := issue("drop", "first")
	if _, _, err := PeerTLS("drop", "second", cert, roots); err == nil {
		t.Fatal("wrong local identity accepted")
	}
	if _, _, err := PeerTLS("drop", "first", cert, nil); err == nil {
		t.Fatal("missing CA accepted")
	}
	if _, err := NewPeerClient(&tls.Config{InsecureSkipVerify: true}); err == nil {
		t.Fatal("insecure client accepted")
	}
}

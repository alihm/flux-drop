package main

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"

	"github.com/runonflux/flux-drop/internal/replica"
)

func TestOfflineCertificates(t *testing.T) {
	parent := t.TempDir()
	ca := filepath.Join(parent, "ca")
	leaf := filepath.Join(parent, "leaf")
	if err := generate("ca", ca, "", "", ""); err != nil {
		t.Fatal(err)
	}
	if err := generate("issue", leaf, ca, "DropApp", "node-one"); err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.LoadX509KeyPair(filepath.Join(leaf, "peer.crt"), filepath.Join(leaf, "peer.key"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	data, _ := os.ReadFile(filepath.Join(leaf, "ca.crt"))
	roots.AppendCertsFromPEM(data)
	if _, _, err := replica.PeerTLS("DropApp", "node-one", certificate, roots); err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth} {
		if _, err := parsed.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := generate("issue", leaf, ca, "DropApp", "node-one"); err == nil {
		t.Fatal("overwrote identity")
	}
	if _, err := os.Stat(filepath.Join(leaf, "ca.key")); !os.IsNotExist(err) {
		t.Fatal("CA key copied to runtime")
	}
	info, _ := os.Stat(filepath.Join(leaf, "peer.key"))
	if info.Mode().Perm() != 0600 {
		t.Fatal("private key mode")
	}
}

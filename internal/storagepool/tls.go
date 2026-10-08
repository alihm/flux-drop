package storagepool

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"
	"time"
)

// High-entropy app keys establish TLS trust without insecure bootstrap, public
// PKI, or transferring cluster CA keys across apps. Domain separation prevents
// using this certificate as a same-app coordinator credential. Secrets MUST be
// randomly generated, private Flux settings. SNI selects an overlapping key ID.
func keyCertificate(app, primary, id, secret string) (tls.Certificate, error) {
	if !appRE.MatchString(app) || !appRE.MatchString(primary) || !keyRE.MatchString(id) || !validSecret(secret) {
		return tls.Certificate{}, errors.New("invalid storage TLS identity")
	}
	appLabel := sha256.Sum256([]byte(app))
	name := strings.ToLower(id + "." + hex.EncodeToString(appLabel[:16]) + ".storage.flux-drop")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("flux-drop/storage-tls/v1/" + app + "/" + primary + "/" + id))
	seed := mac.Sum(nil)
	key := ed25519.NewKeyFromSeed(seed)
	clear(seed)
	serial := sha256.Sum256(key.Public().(ed25519.PublicKey))
	template := &x509.Certificate{SerialNumber: new(big.Int).SetBytes(serial[:16]), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), NotAfter: time.Date(2045, 1, 1, 0, 0, 0, 0, time.UTC), BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		return tls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, err
}
func clientTLS(a App, primary string) (*tls.Config, error) {
	cert, err := keyCertificate(a.AppName, primary, a.KeyID, a.APIKey)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert.Leaf)
	return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: cert.Leaf.DNSNames[0]}, nil
}
func serverTLS(c Config) (*tls.Config, error) {
	certs := map[string]*tls.Certificate{}
	for id, secret := range c.Keys {
		cert, err := keyCertificate(c.AppName, c.PrimaryApp, id, secret)
		if err != nil {
			return nil, err
		}
		certs[cert.Leaf.DNSNames[0]] = &cert
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, GetCertificate: func(h *tls.ClientHelloInfo) (*tls.Certificate, error) {
		cert := certs[h.ServerName]
		if cert == nil {
			return nil, errors.New("unknown storage TLS identity")
		}
		return cert, nil
	}}, nil
}

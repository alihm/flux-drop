// Offline operator utility. CA private keys must never enter an app image,
// app environment, or replicated volume.
package main

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func main() {
	mode := flag.String("mode", "", "ca or issue")
	out := flag.String("out", "", "new output directory (never overwritten)")
	caDir := flag.String("ca", "", "offline CA directory, for issue only")
	app := flag.String("app", "", "Flux app name")
	instance := flag.String("instance", "", "unique replica ID")
	flag.Parse()
	if err := generate(*mode, *out, *caDir, *app, *instance); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func generate(mode, out, caDir, app, instance string) error {
	if out == "" || (mode != "ca" && mode != "issue") {
		return errors.New("require -mode ca|issue and -out new-directory")
	}
	if mode == "issue" && (!regexp.MustCompile(`^[A-Za-z0-9]{1,64}$`).MatchString(app) || !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(instance)) {
		return errors.New("invalid app or instance identity")
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Flux Drop peer"}, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(7 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	parent := template
	var signer crypto.Signer = key
	var caPEM []byte
	if mode == "ca" {
		template.Subject.CommonName = "Flux Drop dedicated peer CA"
		template.IsCA = true
		template.BasicConstraintsValid = true
		template.MaxPathLenZero = true
		template.KeyUsage |= x509.KeyUsageCertSign
		template.NotAfter = now.Add(365 * 24 * time.Hour)
	} else {
		caPEM, err = os.ReadFile(filepath.Join(caDir, "ca.crt"))
		if err != nil {
			return err
		}
		block, _ := pem.Decode(caPEM)
		if block == nil {
			return errors.New("invalid CA certificate")
		}
		parent, err = x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}
		if !parent.IsCA || now.Before(parent.NotBefore) || !now.Before(parent.NotAfter) {
			return errors.New("CA is not currently valid")
		}
		secret, err := os.ReadFile(filepath.Join(caDir, "ca.key"))
		if err != nil {
			return err
		}
		block, _ = pem.Decode(secret)
		if block == nil {
			return errors.New("invalid CA key")
		}
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return err
		}
		var ok bool
		signer, ok = parsed.(crypto.Signer)
		if !ok {
			return errors.New("invalid signing key")
		}
		expected, _ := x509.MarshalPKIXPublicKey(parent.PublicKey)
		actual, _ := x509.MarshalPKIXPublicKey(signer.Public())
		if !bytes.Equal(expected, actual) {
			return errors.New("CA key does not match certificate")
		}
		if parent.NotAfter.Before(template.NotAfter) {
			template.NotAfter = parent.NotAfter
		}
		template.DNSNames = []string{strings.ToLower(app) + ".peer.flux-drop"}
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
		identity, _ := url.Parse("spiffe://flux-drop/" + app + "/" + instance)
		template.URIs = []*url.URL{identity}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, pub, signer)
	if err != nil {
		return err
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	// Require a fresh directory, including on retry: never replace operator keys.
	if err := os.Mkdir(out, 0700); err != nil {
		return err
	}
	name := "peer"
	if mode == "ca" {
		name = "ca"
	}
	write := func(name string, data []byte, perm os.FileMode) error {
		f, err := os.OpenFile(filepath.Join(out, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err = f.Write(data); err != nil {
			return err
		}
		return f.Sync()
	}
	if err := write(name+".key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600); err != nil {
		return err
	}
	if err := write(name+".crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644); err != nil {
		return err
	}
	if mode == "issue" {
		return write("ca.crt", caPEM, 0644)
	}
	return nil
}

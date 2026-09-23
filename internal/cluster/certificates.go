package cluster

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/runonflux/flux-drop/internal/replica"
)

const caBundleEnv = "DROP_CLUSTER_CA_BUNDLE_B64"

// CABundle is a PRIVATE, application/cluster-scoped bootstrap credential. All
// holders are trusted cluster administrators. Never log or serve this object.
type CABundle struct {
	Schema      int    `json:"schema"`
	App         string `json:"app"`
	ClusterID   string `json:"clusterID"`
	Certificate string `json:"certificate"`
	PrivateKey  string `json:"privateKey"`
}

type certificateAuthority struct {
	certificate *x509.Certificate
	key         crypto.Signer
	keyDER      []byte
	certPEM     []byte
}

func (c RuntimeConfig) managedCertificates() bool {
	return c.CABundleFile != "" || os.Getenv(caBundleEnv) != ""
}

// CreateCABundle is an explicit, offline bootstrap action. It does not create
// consensus membership, publish a secret, or replace an existing credential.
func CreateCABundle(app, clusterID string) (CABundle, error) {
	if !regexp.MustCompile(`^[A-Za-z0-9]{1,64}$`).MatchString(app) || !clusterIDPattern.MatchString(clusterID) {
		return CABundle{}, ErrInvalid
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return CABundle{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return CABundle{}, err
	}
	now := time.Now().UTC()
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Flux Drop cluster CA", Organization: []string{app}, SerialNumber: clusterID}, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(5 * 365 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, MaxPathLenZero: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		return CABundle{}, err
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return CABundle{}, err
	}
	return CABundle{Schema: 1, App: app, ClusterID: clusterID, Certificate: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), PrivateKey: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}))}, nil
}

func privateFile(path, contentDir string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("private credential path must be absolute and clean")
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	content, err := filepath.EvalSymlinks(contentDir)
	if err != nil {
		return nil, err
	}
	if overlap(content, real) {
		return nil, errors.New("private credentials cannot be stored in synchronized content")
	}
	f, err := os.Open(real)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("private credential must be a regular mode-0600 file")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("private credential exceeds size limit")
	}
	return data, nil
}

func (c RuntimeConfig) authority(now time.Time) (*certificateAuthority, error) {
	var data []byte
	var err error
	if raw := os.Getenv(caBundleEnv); raw != "" {
		if c.CABundleFile != "" || len(raw) > 96<<10 {
			return nil, errors.New("configure one bounded private CA bundle source")
		}
		data, err = base64.StdEncoding.DecodeString(raw)
	} else {
		data, err = privateFile(c.CABundleFile, c.ContentDir, 64<<10)
	}
	if err != nil {
		return nil, errors.New("cannot load private cluster CA bundle")
	}
	var bundle CABundle
	if len(data) > 64<<10 || decodeStrict(data, &bundle) != nil || bundle.Schema != 1 || bundle.App != c.App || bundle.ClusterID != c.ClusterID {
		return nil, errors.New("private CA bundle does not match this app and cluster")
	}
	certBlock, rest := pem.Decode([]byte(bundle.Certificate))
	if certBlock == nil || certBlock.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("invalid cluster CA certificate")
	}
	ca, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil || !ca.IsCA || !ca.BasicConstraintsValid || ca.KeyUsage&x509.KeyUsageCertSign == 0 || now.Before(ca.NotBefore) || !now.Add(48*time.Hour).Before(ca.NotAfter) || ca.CheckSignatureFrom(ca) != nil {
		return nil, errors.New("cluster CA is invalid or needs rotation")
	}
	keyBlock, rest := pem.Decode([]byte(bundle.PrivateKey))
	if keyBlock == nil || keyBlock.Type != "PRIVATE KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("invalid cluster CA private key")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, errors.New("invalid cluster CA private key")
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("managed cluster CA requires an Ed25519 signing key")
	}
	pub, err := x509.MarshalPKIXPublicKey(key.Public())
	if err != nil || !bytes.Equal(pub, ca.RawSubjectPublicKeyInfo) {
		return nil, errors.New("CA certificate/key mismatch")
	}
	return &certificateAuthority{certificate: ca, key: key, keyDER: keyBlock.Bytes, certPEM: []byte(bundle.Certificate)}, nil
}

func (a *certificateAuthority) admissionKey(c RuntimeConfig) [32]byte {
	h := hmac.New(sha256.New, a.keyDER)
	_, _ = h.Write([]byte("flux-drop/admission/v1/" + c.App + "/" + c.ClusterID))
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func managedPaths(c RuntimeConfig) error {
	for _, entry := range []struct{ path, name string }{{c.Certificate, "node.crt"}, {c.Key, "node.key"}, {c.CA, "ca.crt"}} {
		path, name := entry.path, entry.name
		if path != filepath.Join(c.StateDir, name) {
			return errors.New("managed TLS files must use node.crt, node.key and ca.crt in the node-local state directory")
		}
		if info, err := os.Lstat(path); err == nil {
			if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
				return errors.New("managed TLS files must be private regular files")
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// replaceCertificate uses a synced temporary file and atomic rename. Private
// keys are never replaced by renewal; concurrent workers serialize via flock.
func replaceCertificate(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".certificate-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// EnsureCertificate prepares or renews a leaf using the PRIVATE shared CA.
// It never creates the state mount, chooses a node ID, changes membership, or
// repairs a missing identity key on an established node.
func EnsureCertificate(c RuntimeConfig) error { return ensureCertificate(c, time.Now().UTC()) }
func ensureCertificate(c RuntimeConfig, now time.Time) error {
	if !c.managedCertificates() {
		return nil
	}
	if err := c.validate(); err != nil {
		return err
	}
	if err := prepareDirectory(c.nodeConfig()); err != nil {
		return err
	}
	if err := managedPaths(c); err != nil {
		return err
	}
	a, err := c.authority(now)
	if err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(c.StateDir, "certificates.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("certificate provisioning already in progress")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if data, err := os.ReadFile(filepath.Join(c.StateDir, "identity.json")); err == nil {
		var identity struct {
			ClusterID string `json:"clusterID"`
			NodeID    string `json:"nodeID"`
		}
		if decodeStrict(data, &identity) != nil || identity.ClusterID != c.ClusterID || identity.NodeID != c.Local.ID {
			return errors.New("node identity mismatch during certificate provisioning")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if existing, err := os.ReadFile(c.CA); err == nil {
		block, rest := pem.Decode(existing)
		if block == nil || len(bytes.TrimSpace(rest)) != 0 || !bytes.Equal(block.Bytes, a.certificate.Raw) {
			return errors.New("refusing implicit cluster CA replacement")
		}
	} else if os.IsNotExist(err) {
		if err := replaceCertificate(c.CA, a.certPEM); err != nil {
			return err
		}
	} else {
		return err
	}
	var key ed25519.PrivateKey
	data, err := privateFile(c.Key, c.ContentDir, 16<<10)
	if os.IsNotExist(err) {
		for _, path := range []string{c.Certificate, filepath.Join(c.StateDir, "identity.json"), filepath.Join(c.StateDir, "raft.db")} {
			if _, err := os.Lstat(path); err == nil {
				return errors.New("existing node lost its TLS key; restore it or provision a new identity")
			} else if !os.IsNotExist(err) {
				return err
			}
		}
		_, key, err = ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return err
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return err
		}
		if err = replaceCertificate(c.Key, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "PRIVATE KEY" || len(bytes.TrimSpace(rest)) != 0 {
			return errors.New("invalid node identity key")
		}
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return errors.New("invalid node identity key")
		}
		var ok bool
		key, ok = parsed.(ed25519.PrivateKey)
		if !ok {
			return errors.New("managed node key must be Ed25519")
		}
	}
	if existing, err := tls.LoadX509KeyPair(c.Certificate, c.Key); err == nil {
		leaf, err := parseLeaf(existing)
		if err != nil {
			return err
		}
		if replica.PeerIdentity(leaf, c.App) != c.Local.ID || leaf.CheckSignatureFrom(a.certificate) != nil {
			return errors.New("existing certificate has the wrong identity or issuer")
		}
		if now.Before(leaf.NotBefore) {
			return errors.New("existing node certificate is not yet valid")
		}
		if leaf.NotAfter.After(now.Add(48 * time.Hour)) {
			return nil
		}
	} else if !os.IsNotExist(err) {
		return errors.New("existing certificate/key pair is invalid")
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}
	uri, _ := url.Parse("spiffe://flux-drop/" + c.App + "/" + c.Local.ID)
	expires := now.Add(7 * 24 * time.Hour)
	if a.certificate.NotAfter.Before(expires) {
		expires = a.certificate.NotAfter
	}
	leaf := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: c.Local.ID}, NotBefore: now.Add(-5 * time.Minute), NotAfter: expires, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, DNSNames: []string{strings.ToLower(c.App) + ".peer.flux-drop"}, URIs: []*url.URL{uri}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, a.certificate, key.Public(), a.key)
	if err != nil {
		return err
	}
	return replaceCertificate(c.Certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func (c RuntimeConfig) renewCertificates(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := EnsureCertificate(c); err != nil {
				slog.Warn("cluster certificate renewal deferred; verify private CA configuration")
			}
		}
	}
}

// WriteCABundle creates a new private file, never overwrites operator material.
func WriteCABundle(path string, bundle CABundle) error {
	data, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

package cluster

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/argon2"
)

const StateDirectory = "/var/lib/drop-cluster"
const ManifestPath = StateDirectory + "/cluster-node.json"
const PassphraseEnv = "DROP_CLUSTER_PASSPHRASE"

// These dedicated defaults can be mapped identically in the Flux component;
// they require no additional environment configuration or node-specific ports.
const AutoRaftPort = 34445
const AutoStatusPort = 34446
const AutoContentPort = 34444

type HostIdentity struct {
	App string
	IP  netip.Addr
}

func discoverHost(ctx context.Context, get func(string) string) (HostIdentity, error) {
	h := HostIdentity{App: get("FLUX_APP_NAME")}
	if h.App == "" {
		h.App = get("APP_NAME")
	}
	h.IP, _ = netip.ParseAddr(get("FLUX_NODE_HOST_IP"))
	if h.App != "" && h.IP.IsValid() {
		return h, nil
	}
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("hostinfo redirect refused") }}
	defer client.CloseIdleConnections()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://fluxnode.service:16101/hostinfo", nil)
	res, err := client.Do(req)
	if err != nil {
		return h, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 64<<10+1))
	if err != nil || res.StatusCode != 200 || len(data) > 64<<10 {
		return h, errors.New("Flux hostinfo unavailable")
	}
	var info struct {
		Status string
		Data   struct {
			AppName string
			IP      string
		}
	}
	if json.Unmarshal(data, &info) != nil || info.Status != "success" {
		return h, errors.New("invalid Flux hostinfo")
	}
	if h.App == "" {
		h.App = info.Data.AppName
	}
	if !h.IP.IsValid() {
		h.IP, _ = netip.ParseAddr(info.Data.IP)
	}
	return h, nil
}

// Deterministic app-scoped CA material allows replicas to authenticate without
// distributing manually generated bundles. Node leaf keys remain random/local.
// Fixed validity dates are essential: wall-clock-dependent roots would differ
// across staggered boots. This v1 root expires in 2045; rotation is explicit.
func passphraseBundle(app, passphrase string) (CABundle, error) {
	if len(passphrase) < 32 || len(passphrase) > 1024 {
		return CABundle{}, errors.New("DROP_CLUSTER_PASSPHRASE must contain 32–1024 bytes; use a strong random secret")
	}
	salt := sha256.Sum256([]byte("flux-drop/private-cluster-ca/v1/" + app))
	seed := argon2.IDKey([]byte(passphrase), salt[:], 3, 64*1024, 2, 32)
	key := ed25519.NewKeyFromSeed(seed)
	clear(seed)
	public := key.Public().(ed25519.PublicKey)
	digest := sha256.Sum256(append([]byte("flux-drop/cluster-id/v1/"+app+"/"), public...))
	id := hex.EncodeToString(digest[:16])
	cert := &x509.Certificate{SerialNumber: new(big.Int).SetBytes(digest[:16]), Subject: pkix.Name{CommonName: "Flux Drop managed cluster", Organization: []string{app}, SerialNumber: id}, NotBefore: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), NotAfter: time.Date(2045, 1, 1, 0, 0, 0, 0, time.UTC), IsCA: true, BasicConstraintsValid: true, MaxPathLenZero: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, public, key)
	if err != nil {
		return CABundle{}, err
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return CABundle{}, err
	}
	return CABundle{Schema: 1, App: app, ClusterID: id, Certificate: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), PrivateKey: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}))}, nil
}

// ProvisionAutomatic is called by the supervisor before starting its children.
// No public-network failure is interpreted as an empty/singleton deployment.
func ProvisionAutomatic(ctx context.Context, get func(string) string) error {
	for {
		h, err := discoverHost(ctx, get)
		if err == nil && h.App != "" && h.IP.IsValid() {
			_, err = provisionAutomatic(h, get(PassphraseEnv), StateDirectory, "/data")
			return err
		}
		select {
		case <-ctx.Done():
			return errors.New("waiting for trusted Flux app/IP discovery")
		case <-time.After(time.Second):
		}
	}
}

func provisionAutomatic(h HostIdentity, passphrase, stateDir, contentDir string) (RuntimeConfig, error) {
	var empty RuntimeConfig
	h.IP = h.IP.Unmap()
	if !nodeIDPattern.MatchString(h.App) || len(h.App) > 64 || !h.IP.IsValid() || h.IP.IsUnspecified() || h.IP.IsMulticast() || h.IP.Zone() != "" {
		return empty, ErrInvalid
	}
	if os.Getenv(caBundleEnv) != "" || os.Getenv("DROP_CLUSTER_ENROLLMENT_KEY") != "" {
		return empty, errors.New("passphrase mode cannot be combined with separately supplied cluster credentials")
	}
	bundle, err := passphraseBundle(h.App, passphrase)
	if err != nil {
		return empty, err
	}
	// Replicated deployment evidence is public identity, never a credential.
	// Do not start an unrelated metadata authority over an existing content root.
	markerPath := filepath.Join(contentDir, "cluster-genesis.json")
	if info, err := os.Lstat(markerPath); err == nil {
		if !info.Mode().IsRegular() {
			return empty, ErrInvalid
		}
		f, err := os.Open(markerPath)
		if err != nil {
			return empty, err
		}
		raw, readErr := io.ReadAll(io.LimitReader(f, 1025))
		f.Close()
		var marker struct {
			ClusterID string `json:"clusterID"`
		}
		if readErr != nil || len(raw) > 1024 || decodeStrict(raw, &marker) != nil || marker.ClusterID != bundle.ClusterID {
			return empty, errors.New("content root belongs to another or damaged cluster; explicit recovery required")
		}
	} else if !os.IsNotExist(err) {
		return empty, err
	}
	manifest := filepath.Join(stateDir, "cluster-node.json")
	var c RuntimeConfig
	if _, err := os.Lstat(manifest); err == nil {
		c, err = LoadRuntimeConfig(manifest)
		if err != nil {
			return empty, err
		}
		if !c.Automatic || !c.AsyncContent || c.App != h.App || c.ClusterID != bundle.ClusterID || c.StateDir != stateDir || c.ContentDir != contentDir || c.CABundleFile != filepath.Join(stateDir, "cluster-ca.json") {
			return empty, errors.New("existing cluster identity does not match passphrase/app; refusing reset")
		}
		// Address changes are persisted here and reconciled by the old quorum;
		// node identity/key and durable log are never regenerated.
		c.Local.Address = netip.AddrPortFrom(h.IP, AutoRaftPort).String()
		c.SelfIPs = []netip.Addr{h.IP}
	} else if !os.IsNotExist(err) {
		return empty, err
	} else {
		for _, name := range []string{"raft.db", "identity.json", "node.key", "content-journal.db", "primary-history.json", "bootstrap-intent.json"} {
			if _, err := os.Lstat(filepath.Join(stateDir, name)); !os.IsNotExist(err) {
				return empty, errors.New("existing node state has no automatic manifest; explicit migration required")
			}
		}
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return empty, err
		}
		c = RuntimeConfig{App: h.App, ClusterID: bundle.ClusterID, Local: Member{ID: hex.EncodeToString(id), Address: netip.AddrPortFrom(h.IP, AutoRaftPort).String()}, StateDir: stateDir, ContentDir: contentDir, Listen: "0.0.0.0:34445", StatusListen: "0.0.0.0:34446", StatusPort: AutoStatusPort, SelfIPs: []netip.Addr{h.IP}, CABundleFile: filepath.Join(stateDir, "cluster-ca.json"), Certificate: filepath.Join(stateDir, "node.crt"), Key: filepath.Join(stateDir, "node.key"), CA: filepath.Join(stateDir, "ca.crt"), Automatic: true, AsyncContent: true}
	}
	if h.IP.Is6() {
		c.Listen = "[::]:34445"
		c.StatusListen = "[::]:34446"
	} else {
		c.Listen = "0.0.0.0:34445"
		c.StatusListen = "0.0.0.0:34446"
	}
	if err := c.validate(); err != nil {
		return empty, err
	}
	if err := prepareDirectory(c.nodeConfig()); err != nil {
		return empty, err
	}
	if _, err := os.Lstat(c.CABundleFile); os.IsNotExist(err) {
		if err := WriteCABundle(c.CABundleFile, bundle); err != nil {
			return empty, err
		}
	} else if err != nil {
		return empty, err
	}
	// Verify stored authority against the supplied secret, not just JSON labels.
	a, err := c.authority(time.Now())
	if err != nil {
		return empty, err
	}
	block, _ := pem.Decode([]byte(bundle.Certificate))
	if block == nil || !bytes.Equal(a.certificate.Raw, block.Bytes) {
		return empty, errors.New("stored CA differs from supplied passphrase")
	}
	data, _ := json.Marshal(c)
	if err := replaceCertificate(manifest, data); err != nil {
		return empty, err
	}
	return c, EnsureCertificate(c)
}

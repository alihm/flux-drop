package cluster

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func managedConfig(t *testing.T) RuntimeConfig {
	t.Helper()
	t.Setenv(caBundleEnv, "")
	state, content := t.TempDir(), t.TempDir()
	if err := os.Chmod(state, 0700); err != nil {
		t.Fatal(err)
	}
	bundle, err := CreateCABundle("testapp", testClusterID)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.json")
	if err := WriteCABundle(path, bundle); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"app": "testapp", "clusterID": testClusterID, "local": Member{ID: "one", Address: "127.0.0.1:8445"}, "selfIPs": []string{"127.0.0.1"}, "stateDir": state, "contentDir": content, "caBundleFile": path})
	c, err := DecodeRuntimeConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestManagedCertificateRenewal(t *testing.T) {
	c := managedConfig(t)
	if err := EnsureCertificate(c); err != nil {
		t.Fatal(err)
	}
	key, _ := os.ReadFile(c.Key)
	first, _ := os.ReadFile(c.Certificate)
	if err := EnsureCertificate(c); err != nil {
		t.Fatal(err)
	}
	again, _ := os.ReadFile(c.Certificate)
	if !bytes.Equal(first, again) {
		t.Fatal("fresh certificate changed")
	}
	material, err := c.tlsMaterial()
	if err != nil {
		t.Fatal(err)
	}
	authority, err := c.authority(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.LoadX509KeyPair(c.Certificate, c.Key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := parseLeaf(pair)
	leaf.NotAfter = time.Now().Add(time.Hour)
	der, err := x509.CreateCertificate(rand.Reader, leaf, authority.certificate, leaf.PublicKey, authority.key)
	if err != nil {
		t.Fatal(err)
	}
	if err := replaceCertificate(c.Certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})); err != nil {
		t.Fatal(err)
	}
	if err := EnsureCertificate(c); err != nil {
		t.Fatal(err)
	}
	newKey, _ := os.ReadFile(c.Key)
	if !bytes.Equal(key, newKey) {
		t.Fatal("renewal replaced identity key")
	}
	for _, get := range []func() (*tls.Certificate, error){func() (*tls.Certificate, error) { return material.Client.GetClientCertificate(nil) }, func() (*tls.Certificate, error) { return material.Server.GetCertificate(nil) }} {
		cert, err := get()
		if err != nil {
			t.Fatal(err)
		}
		renewed, _ := parseLeaf(*cert)
		if renewed.SerialNumber.Cmp(leaf.SerialNumber) == 0 || time.Until(renewed.NotAfter) < 6*24*time.Hour {
			t.Fatal("callback did not load renewed certificate")
		}
	}
	second := c
	second.Local.ID = "two"
	second.StateDir = t.TempDir()
	_ = os.Chmod(second.StateDir, 0700)
	second.Certificate = filepath.Join(second.StateDir, "node.crt")
	second.Key = filepath.Join(second.StateDir, "node.key")
	second.CA = filepath.Join(second.StateDir, "ca.crt")
	if err := EnsureCertificate(second); err != nil {
		t.Fatal(err)
	}
	otherKey, _ := os.ReadFile(second.Key)
	if bytes.Equal(key, otherKey) {
		t.Fatal("nodes share a leaf key")
	}
	otherCert, _ := os.ReadFile(second.Certificate)
	if err := replaceCertificate(c.Key, otherKey); err != nil {
		t.Fatal(err)
	}
	if err := replaceCertificate(c.Certificate, otherCert); err != nil {
		t.Fatal(err)
	}
	if _, err := material.Client.GetClientCertificate(nil); err == nil {
		t.Fatal("accepted replaced node identity")
	}
	if _, err := material.Server.GetCertificate(nil); err == nil {
		t.Fatal("served replaced node identity")
	}
	if err := os.Remove(c.Key); err != nil {
		t.Fatal(err)
	}
	if err := EnsureCertificate(c); err == nil {
		t.Fatal("silently repaired lost identity key")
	}
}

func TestManagedCARejectsUnsafeConfiguration(t *testing.T) {
	for _, kind := range []string{"permissions", "content", "cluster", "collision", "rotation", "two-sources"} {
		t.Run(kind, func(t *testing.T) {
			c := managedConfig(t)
			switch kind {
			case "permissions":
				_ = os.Chmod(c.CABundleFile, 0644)
			case "content":
				data, _ := os.ReadFile(c.CABundleFile)
				c.CABundleFile = filepath.Join(c.ContentDir, "ca.json")
				_ = os.WriteFile(c.CABundleFile, data, 0600)
			case "cluster":
				c.ClusterID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			case "collision":
				c.Certificate = c.Key
			case "rotation":
				if err := EnsureCertificate(c); err != nil {
					t.Fatal(err)
				}
				bundle, _ := CreateCABundle(c.App, c.ClusterID)
				data, _ := json.Marshal(bundle)
				_ = os.WriteFile(c.CABundleFile, data, 0600)
			case "two-sources":
				data, _ := os.ReadFile(c.CABundleFile)
				t.Setenv(caBundleEnv, base64.StdEncoding.EncodeToString(data))
			}
			if err := EnsureCertificate(c); err == nil {
				t.Fatal("accepted unsafe configuration")
			}
		})
	}
}

func TestManagedCAEnvironmentAndAdmission(t *testing.T) {
	c := managedConfig(t)
	data, _ := os.ReadFile(c.CABundleFile)
	a, err := c.authority(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteCABundle(c.CABundleFile, CABundle{}); err == nil {
		t.Fatal("overwrote bundle")
	}
	c.CABundleFile = ""
	t.Setenv(caBundleEnv, base64.StdEncoding.EncodeToString(data))
	if err := EnsureCertificate(c); err != nil {
		t.Fatal(err)
	}
	b, err := c.authority(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if a.admissionKey(c) != b.admissionKey(c) {
		t.Fatal("admission differs across sources")
	}
	other := c
	other.ClusterID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if a.admissionKey(c) == a.admissionKey(other) {
		t.Fatal("admission not cluster scoped")
	}
}

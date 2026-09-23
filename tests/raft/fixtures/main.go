// Generates disposable private shared-CA bundles. Each application container
// generates its own leaf key on its unsynchronized state volume.
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func directory(path string) {
	must(os.MkdirAll(path, 0700))
	must(os.Chmod(path, 0700))
	must(os.Chown(path, 65534, 65534))
}
func write(path string, data []byte) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	must(err)
	_, err = f.Write(data)
	must(err)
	must(f.Sync())
	must(f.Close())
	must(os.Chown(path, 65534, 65534))
}
func main() {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	must(err)
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Disposable Drop Raft test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(30 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	must(err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	must(err)
	bundle, err := json.Marshal(map[string]any{"schema": 1, "app": "drop", "clusterID": "1234567890abcdef1234567890abcdef", "certificate": string(caPEM), "privateKey": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))})
	must(err)
	type member struct {
		ID      string `json:"id"`
		Address string `json:"address"`
	}
	ids := []string{"one", "two", "three"}
	members := []member{}
	for i, id := range ids {
		members = append(members, member{id, fmt.Sprintf("172.29.188.%d:8445", 11+i)})
	}
	for i, id := range ids {
		root := "/state-" + id
		directory(root)
		directory("/state-" + id)
		write(root+"/cluster-ca.json", bundle)
		manifest := map[string]any{"app": "drop", "clusterID": "1234567890abcdef1234567890abcdef", "local": members[i], "stateDir": "/var/lib/drop-cluster", "contentDir": "/data", "listen": "0.0.0.0:8445", "statusListen": "0.0.0.0:8446", "statusPort": 8446, "selfIPs": []string{fmt.Sprintf("172.29.188.%d", 11+i)}, "caBundleFile": "/var/lib/drop-cluster/cluster-ca.json", "initialVoters": members, "asyncContent": true}
		data, err := json.Marshal(manifest)
		must(err)
		write(root+"/cluster-node.json", data)
	}
}

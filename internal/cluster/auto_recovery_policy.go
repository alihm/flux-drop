package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/hkdf"
)

type autoRecoveryPolicy struct {
	ClusterID string    `json:"clusterID"`
	Mode      string    `json:"mode"`
	UpdatedAt time.Time `json:"updatedAt"`
	MAC       string    `json:"mac"`
}
type AutoRecoveryPolicyStatus struct {
	Mode  string `json:"mode"`
	Valid bool   `json:"valid"`
	File  string `json:"file"`
}

func autoRecoveryPolicyPath(c Config) (string, error) {
	if !filepath.IsAbs(c.ContentDir) || filepath.Clean(c.ContentDir) != c.ContentDir {
		return "", errors.New("automatic recovery policy needs a clean absolute content directory")
	}
	root, err := filepath.EvalSymlinks(c.ContentDir)
	if err != nil {
		return "", err
	}
	path := filepath.Join(root, "auto-recovery.json")
	for _, alias := range []string{"/data/projects", "/var/lib/drop-cluster/storage-cache"} {
		if overlap(alias, path) {
			return "", errors.New("automatic recovery policy must be outside Nginx aliases")
		}
	}
	return path, nil
}
func autoRecoveryMAC(c Config, p autoRecoveryPolicy) ([]byte, error) {
	secret := os.Getenv(PassphraseEnv)
	if len(secret) < 32 || len(secret) > 1024 {
		return nil, errors.New("automatic recovery policy requires the cluster passphrase")
	}
	key := make([]byte, 32)
	defer clear(key)
	if _, err := io.ReadFull(hkdf.New(sha256.New, []byte(secret), []byte(c.ClusterID), []byte("flux-drop/automatic-recovery-policy/v1")), key); err != nil {
		return nil, err
	}
	p.MAC = ""
	raw, _ := json.Marshal(p)
	hash := hmac.New(sha256.New, key)
	hash.Write(raw)
	return hash.Sum(nil), nil
}
func loadAutoRecoveryPolicy(c Config) (AutoRecoveryPolicyStatus, string) {
	path, err := autoRecoveryPolicyPath(c)
	status := AutoRecoveryPolicyStatus{Mode: "observe", File: path}
	if err != nil {
		return status, "invalid-path"
	}
	var policy autoRecoveryPolicy
	_, err = readOperatorRequest(path, &policy, "clusterID", "mode", "updatedAt", "mac")
	if err != nil {
		return status, "missing-or-invalid"
	}
	if policy.ClusterID != c.ClusterID || policy.UpdatedAt.IsZero() || (policy.Mode != "off" && policy.Mode != "observe" && policy.Mode != "on") {
		return status, "invalid-policy"
	}
	expected, err := autoRecoveryMAC(c, policy)
	provided, decodeErr := hex.DecodeString(policy.MAC)
	if err != nil || decodeErr != nil || !hmac.Equal(expected, provided) {
		return status, "invalid-signature"
	}
	status.Mode = policy.Mode
	status.Valid = true
	return status, policy.MAC
}

// ConfigureAutoRecovery never opens Raft or creates a recovery request. The
// signed replicated-volume policy is evaluated afresh by every primary.
func ConfigureAutoRecovery(c RuntimeConfig, mode string) (AutoRecoveryPolicyStatus, error) {
	if mode == "status" {
		status, _ := loadAutoRecoveryPolicy(c.nodeConfig())
		return status, nil
	}
	if mode != "off" && mode != "observe" && mode != "on" {
		return AutoRecoveryPolicyStatus{}, errors.New("auto-recovery must be off, observe, on or status")
	}
	path, err := autoRecoveryPolicyPath(c.nodeConfig())
	if err != nil {
		return AutoRecoveryPolicyStatus{}, err
	}
	policy := autoRecoveryPolicy{ClusterID: c.ClusterID, Mode: mode, UpdatedAt: time.Now().UTC()}
	mac, err := autoRecoveryMAC(c.nodeConfig(), policy)
	if err != nil {
		return AutoRecoveryPolicyStatus{}, err
	}
	policy.MAC = hex.EncodeToString(mac)
	raw, _ := json.Marshal(policy)
	if err := replaceCertificate(path, raw); err != nil {
		return AutoRecoveryPolicyStatus{}, err
	}
	status, _ := loadAutoRecoveryPolicy(c.nodeConfig())
	return status, nil
}

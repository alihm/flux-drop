package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/replica"
)

const enrollmentPath = "/_drop_cluster/enroll"
const membershipPolicyKey = "cluster_policy/membership"

type admission struct {
	Member  Member `json:"member"`
	KeyHash string `json:"keyHash"`
	Retired bool   `json:"retired"`
}
type membershipPolicy struct {
	Voters int `json:"voters"`
}

// Enrollment requires BOTH a unique application-scoped TLS identity and a
// private shared admission credential. The credential never replaces TLS trust
// or bootstraps a voting configuration from discovery.
type enrollment struct {
	node       *Node
	observer   *Observer
	credential [32]byte
	port       uint16
	client     *http.Client
	missing    map[string]time.Time
	now        func() time.Time
	grace      time.Duration
}

func loadEnrollmentCredential(path, contentDir string) ([32]byte, error) {
	var key [32]byte
	if !filepath.IsAbs(path) {
		return key, errors.New("enrollment credential path must be absolute")
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return key, err
	}
	content, err := filepath.EvalSymlinks(contentDir)
	if err != nil {
		return key, err
	}
	if overlap(content, real) {
		return key, errors.New("enrollment credentials cannot be replicated")
	}
	f, err := os.Open(real)
	if err != nil {
		return key, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return key, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return key, errors.New("enrollment credential must be a private regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, 128))
	if err != nil {
		return key, err
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(data)))
	if err != nil || len(decoded) != 32 || bytes.Equal(decoded, make([]byte, 32)) {
		return key, errors.New("enrollment credential must be base64-encoded 32 random bytes")
	}
	copy(key[:], decoded)
	return key, nil
}

func newEnrollment(n *Node, o *Observer, c RuntimeConfig, material TLSMaterial) (*enrollment, error) {
	var key [32]byte
	var err error
	if raw := os.Getenv("DROP_CLUSTER_ENROLLMENT_KEY"); raw != "" {
		if c.EnrollmentCredential != "" {
			return nil, errors.New("configure one enrollment credential source, not both")
		}
		decoded, decodeErr := base64.StdEncoding.DecodeString(raw)
		if decodeErr != nil || len(decoded) != 32 || base64.StdEncoding.EncodeToString(decoded) != raw || bytes.Equal(decoded, make([]byte, 32)) {
			return nil, errors.New("DROP_CLUSTER_ENROLLMENT_KEY must encode 32 random bytes")
		}
		copy(key[:], decoded)
	} else if c.EnrollmentCredential == "" && c.managedCertificates() {
		var authority *certificateAuthority
		authority, err = c.authority(time.Now().UTC())
		if err == nil {
			key = authority.admissionKey(c)
		}
	} else {
		key, err = loadEnrollmentCredential(c.EnrollmentCredential, c.ContentDir)
	}
	if err != nil {
		return nil, err
	}
	client, err := replica.NewPeerClient(material.Client)
	if err != nil {
		return nil, err
	}
	client.Timeout = 8 * time.Second
	grace := 5 * time.Minute
	if c.Automatic {
		grace = 2 * time.Minute
	}
	return &enrollment{node: n, observer: o, credential: key, port: c.StatusPort, client: client, missing: make(map[string]time.Time), now: time.Now, grace: grace}, nil
}

func (e *enrollment) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.RawPath != "" || r.URL.RawQuery != "" || r.URL.ForceQuery || r.Header.Get("Cookie") != "" || r.Header.Get("Content-Type") != "application/json" || len(r.Header.Values("X-Drop-Cluster")) != 1 || r.Header.Get("X-Drop-Cluster") != e.node.config.ClusterID || len(r.Header.Values("Authorization")) != 1 {
		http.Error(w, "invalid enrollment", 400)
		return
	}
	token, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if err != nil || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare(token, e.credential[:]) != 1 {
		http.Error(w, "forbidden", 403)
		return
	}
	var m Member
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2048))
	if err != nil || decodeStrict(data, &m) != nil || m.validate() != nil || m.ID != replica.PeerIdentity(r.TLS.PeerCertificates[0], e.observer.app) {
		http.Error(w, "invalid enrollment", 400)
		return
	}
	// Only a fresh, independently authenticated Flux candidate can be admitted.
	address, _ := netip.ParseAddrPort(m.Address)
	localAddress, _ := netip.ParseAddrPort(e.node.config.Local.Address)
	peers, fresh := e.observer.Snapshot()
	found := false
	for _, p := range peers {
		a, err := netip.ParseAddrPort(p.Address)
		if err == nil && a.Addr() == address.Addr() && p.Status.ID == m.ID {
			found = true
		}
	}
	if !fresh || !found || address.Port() != localAddress.Port() {
		http.Error(w, "candidate not verified", 503)
		return
	}
	h := sha256.Sum256(r.TLS.PeerCertificates[0].RawSubjectPublicKeyInfo)
	if err := e.admit(r.Context(), admission{Member: m, KeyHash: hex.EncodeToString(h[:])}); err != nil {
		http.Error(w, "admission unavailable", 503)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (e *enrollment) admit(ctx context.Context, a admission) error {
	configuration, _, err := e.node.configuration(ctx)
	if err != nil {
		return err
	}
	addressChange := false
	for _, s := range configuration.Servers {
		if string(s.ID) == a.Member.ID && (s.Suffrage == raft.Voter || string(s.Address) != a.Member.raftAddress()) {
			if !e.node.config.Automatic || string(s.Address) == a.Member.raftAddress() {
				return errors.New("existing voter identity cannot be reenrolled after losing local state")
			}
			ip, _ := netip.ParseAddrPort(a.Member.Address)
			observed, err := e.observer.probe(ctx, netip.AddrPortFrom(ip.Addr(), e.port))
			if err != nil || observed.Status.ID != a.Member.ID || observed.Status.LastIndex == 0 {
				return errors.New("address replacement requires existing durable history")
			}
			addressChange = true
		}
	}
	key := "cluster_admissions/" + a.Member.ID
	records, err := e.node.Read(ctx, []string{key})
	if err != nil {
		return err
	}
	if r := records[key]; r.Version != 0 {
		var existing admission
		if decodeStrict(r.Value, &existing) != nil || existing.Retired || existing.KeyHash != a.KeyHash || existing.Member.ID != a.Member.ID || (!addressChange && existing != a) {
			return ErrInvalid
		}
		if addressChange {
			data, _ := json.Marshal(a)
			if err := e.node.Commit(ctx, Transaction{Schema: 1, Checks: []Check{{Key: key, Version: r.Version}}, Writes: []Write{{Key: key, Value: data}}}); err != nil {
				return err
			}
		}
	} else {
		data, _ := json.Marshal(a)
		if err := e.node.Commit(ctx, Transaction{Schema: 1, Checks: []Check{{Key: key}}, Writes: []Write{{Key: key, Value: data}}}); err != nil {
			return err
		}
	}
	if addressChange {
		return e.node.updateAddress(ctx, a.Member)
	}
	configuration, _, err = e.node.configuration(ctx)
	if err != nil {
		return err
	}
	for _, s := range configuration.Servers {
		if string(s.ID) == a.Member.ID {
			if string(s.Address) != a.Member.raftAddress() {
				return ErrInvalid
			}
			return nil
		}
	}
	return e.node.AddLearner(ctx, a.Member)
}

func (e *enrollment) retire(ctx context.Context, m Member) error {
	key := "cluster_admissions/" + m.ID
	records, err := e.node.Read(ctx, []string{key})
	if err != nil {
		return err
	}
	r := records[key]
	a := admission{Member: m}
	if r.Version != 0 && decodeStrict(r.Value, &a) != nil {
		return ErrInvalid
	}
	a.Retired = true
	data, _ := json.Marshal(a)
	if err := e.node.Commit(ctx, Transaction{Schema: 1, Checks: []Check{{Key: key, Version: r.Version}}, Writes: []Write{{Key: key, Value: data}}}); err != nil {
		return err
	}
	return e.node.RemoveMember(ctx, m.ID)
}

func (e *enrollment) target(ctx context.Context) (int, error) {
	records, err := e.node.Read(ctx, []string{membershipPolicyKey})
	if err != nil {
		return 0, err
	}
	if r := records[membershipPolicyKey]; r.Version != 0 {
		var p membershipPolicy
		if decodeStrict(r.Value, &p) != nil || (p.Voters != 3 && p.Voters != 5) {
			return 0, ErrInvalid
		}
		return p.Voters, nil
	}
	// Only an explicit bootstrap manifest may set the immutable target. A
	// temporarily enlarged membership must never become the new target.
	n := len(e.node.config.InitialVoters)
	if e.node.config.Automatic {
		n = 3
	}
	if n != 3 && n != 5 {
		return 0, ErrInvalid
	}
	data, _ := json.Marshal(membershipPolicy{Voters: n})
	err = e.node.Commit(ctx, Transaction{Schema: 1, Checks: []Check{{Key: membershipPolicyKey}}, Writes: []Write{{Key: membershipPolicyKey, Value: data}}})
	return n, err
}

func (e *enrollment) reconcile(ctx context.Context) error {
	configuration, _, err := e.node.configuration(ctx)
	if err != nil {
		return err
	}
	target, err := e.target(ctx)
	if err != nil {
		return err
	}
	peers, fresh := e.observer.Snapshot()
	if !fresh {
		e.missing = make(map[string]time.Time)
		return ErrNotLeader
	}
	healthy := map[string]bool{e.node.config.Local.ID: true}
	for _, p := range peers {
		if p.Status.Role == "Follower" || p.Status.Role == "Leader" {
			for _, server := range configuration.Servers {
				m, err := parseAddress(string(server.Address))
				if err != nil {
					return err
				}
				a, _ := netip.ParseAddrPort(m.Address)
				if m.ID == p.Status.ID && p.Address == netip.AddrPortFrom(a.Addr(), e.port).String() {
					healthy[p.Status.ID] = true
				}
			}
		}
	}
	var voters, learners []Member
	for _, s := range configuration.Servers {
		m, err := parseAddress(string(s.Address))
		if err != nil {
			return err
		}
		if !healthy[m.ID] {
			// Discovery omission is not evidence of failure. Probe the committed
			// address directly before starting or extending a replacement grace.
			a, _ := netip.ParseAddrPort(m.Address)
			p, probeErr := e.observer.probe(ctx, netip.AddrPortFrom(a.Addr(), e.port))
			if probeErr == nil && p.Status.ID == m.ID && (p.Status.Role == "Follower" || p.Status.Role == "Leader") {
				healthy[m.ID] = true
			}
		}
		if healthy[m.ID] {
			delete(e.missing, m.ID)
		} else if e.missing[m.ID].IsZero() {
			e.missing[m.ID] = e.now()
		}
		if s.Suffrage == raft.Voter {
			voters = append(voters, m)
		} else {
			learners = append(learners, m)
		}
	}
	// Deterministic preference avoids churn; a newly elected controller starts
	// its grace period afresh. Absence alone never shrinks the voter target.
	sort.Slice(voters, func(i, j int) bool { return voters[i].ID < voters[j].ID })
	var failed *Member
	for i := range voters {
		if since := e.missing[voters[i].ID]; !since.IsZero() && e.now().Sub(since) >= e.grace {
			failed = &voters[i]
			break
		}
	}
	if len(voters) > target && failed != nil {
		return e.retire(ctx, *failed)
	}
	for _, m := range learners {
		if since := e.missing[m.ID]; !since.IsZero() && e.now().Sub(since) >= e.grace {
			return e.retire(ctx, m)
		}
		if !healthy[m.ID] || (len(voters) >= target && failed == nil) || len(voters) > target {
			continue
		}
		key := "cluster_admissions/" + m.ID
		records, err := e.node.Read(ctx, []string{key})
		if err != nil {
			return err
		}
		var a admission
		if decodeStrict(records[key].Value, &a) != nil || a.Retired || a.Member != m {
			continue
		}
		return e.node.Promote(ctx, m, e.port, e.observer)
	}
	return nil
}

func (e *enrollment) request(ctx context.Context) error {
	peers, fresh := e.observer.Snapshot()
	if !fresh {
		return ErrNotLeader
	}
	data, _ := json.Marshal(e.node.config.Local)
	for _, p := range peers {
		if p.Status.Role != "Leader" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+p.Address+enrollmentPath, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Drop-Cluster", e.node.config.ClusterID)
		req.Header.Set("Authorization", "Bearer "+base64.StdEncoding.EncodeToString(e.credential[:]))
		transport := e.client.Transport.(*http.Transport).Clone()
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		verify := transport.TLSClientConfig.VerifyConnection
		expectedID := p.Status.ID
		transport.TLSClientConfig.VerifyConnection = func(state tls.ConnectionState) error {
			if err := verify(state); err != nil {
				return err
			}
			if replica.PeerIdentity(state.PeerCertificates[0], e.observer.app) != expectedID {
				return errors.New("enrollment destination identity mismatch")
			}
			return nil
		}
		client := *e.client
		client.Transport = transport
		response, err := client.Do(req)
		if err != nil {
			transport.CloseIdleConnections()
			return err
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		response.Body.Close()
		transport.CloseIdleConnections()
		if response.StatusCode != http.StatusNoContent {
			return ErrNotLeader
		}
		return nil
	}
	return ErrNotLeader
}

func (e *enrollment) Run(ctx context.Context) {
	defer e.client.CloseIdleConnections()
	for ctx.Err() == nil {
		bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
		var err error
		if e.node.raft.State() == raft.Leader {
			err = e.reconcile(bounded)
		} else {
			e.missing = make(map[string]time.Time)
			// Established members need not repeatedly transmit admission secrets.
			var configuration raft.ConfigurationFuture
			if e.node.wait(bounded, func() raft.Future { configuration = e.node.raft.GetConfiguration(); return configuration }) == nil {
				found := false
				for _, s := range configuration.Configuration().Servers {
					if string(s.ID) == e.node.config.Local.ID && string(s.Address) == e.node.config.Local.raftAddress() {
						found = true
					}
				}
				if !found {
					err = e.request(bounded)
				}
			}
		}
		cancel()
		if err != nil && !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNotCaughtUp) && ctx.Err() == nil {
			slog.Warn("cluster membership reconciliation deferred")
		}
		timer := time.NewTimer(15 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

package cluster

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var nodeIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
var clusterIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

type Member struct {
	ID      string `json:"id"`
	Address string `json:"address"` // Raft IP:port, NOT an HTTP origin.
}

func (m Member) validate() error {
	address, err := netip.ParseAddrPort(m.Address)
	if !nodeIDPattern.MatchString(m.ID) || err != nil || address.Addr().IsUnspecified() || address.Addr().IsMulticast() || address.Addr().Zone() != "" || address.Port() < 1024 {
		return errors.New("invalid cluster member identity or address")
	}
	return nil
}

// The member identity is encoded in the Raft address so the TLS transport can
// pin the destination's certificate ID, rather than just trusting any app peer.
func (m Member) raftAddress() string { return m.ID + "@" + m.Address }

func parseAddress(raw string) (Member, error) {
	id, address, ok := strings.Cut(raw, "@")
	m := Member{ID: id, Address: address}
	if !ok || m.validate() != nil {
		return Member{}, errors.New("invalid authenticated Raft address")
	}
	return m, nil
}

type Config struct {
	ClusterID  string
	Local      Member
	StateDir   string
	ContentDir string
	// InitialVoters is an explicit identical manifest supplied to initial nodes.
	// Empty means join/restart, never automatically form a singleton cluster.
	InitialVoters []Member
	AsyncContent  bool
	Automatic     bool
}

func (c Config) validate() error {
	if !clusterIDPattern.MatchString(c.ClusterID) || c.Local.validate() != nil {
		return errors.New("invalid cluster configuration")
	}
	if c.StateDir == "" || c.ContentDir == "" || !filepath.IsAbs(c.StateDir) || !filepath.IsAbs(c.ContentDir) || filepath.Clean(c.StateDir) != c.StateDir || filepath.Clean(c.ContentDir) != c.ContentDir || c.StateDir == "/" {
		return errors.New("explicit clean node-local state and content directories are required")
	}
	if len(c.InitialVoters) != 0 && len(c.InitialVoters) != 3 && len(c.InitialVoters) != 5 {
		return errors.New("bootstrap requires exactly three or five explicit voters")
	}
	ids, addresses := map[string]bool{}, map[string]bool{}
	found := false
	for _, m := range c.InitialVoters {
		if m.validate() != nil || ids[m.ID] || addresses[m.Address] {
			return errors.New("invalid or duplicate bootstrap member")
		}
		ids[m.ID], addresses[m.Address] = true, true
		if m.ID == c.Local.ID {
			if m != c.Local {
				return errors.New("bootstrap local address mismatch")
			}
			found = true
		}
	}
	if len(c.InitialVoters) > 0 && !found {
		return errors.New("bootstrap manifest omits local voter")
	}
	return nil
}

func prepareDirectory(c Config) error {
	// The operator must provision the mount. Never silently use an ephemeral
	// fallback if a required state mount is missing.
	info, err := os.Lstat(c.StateDir)
	if err != nil {
		return fmt.Errorf("node-local state mount: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return errors.New("node-local state directory must be private (0700), real and not a symlink")
	}
	state, err := filepath.EvalSymlinks(c.StateDir)
	if err != nil {
		return err
	}
	content, err := filepath.EvalSymlinks(c.ContentDir)
	if err != nil {
		return err
	}
	if overlap(state, content) || overlap(content, state) {
		return errors.New("Raft state and Flux-replicated content directories must not overlap")
	}
	for _, name := range []string{"raft.db", "identity.json", "snapshots", "content-journal.db", "cluster-node.json", "cluster-ca.json", "bootstrap-intent.json"} {
		if entry, err := os.Lstat(filepath.Join(state, name)); err == nil {
			if entry.Mode()&os.ModeSymlink != 0 {
				return errors.New("symlink in Raft state")
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func overlap(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

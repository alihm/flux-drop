package cluster

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/runonflux/flux-drop/internal/replica"
)

type evolvingPeers struct {
	mu        sync.Mutex
	addresses []netip.AddrPort
}

func (s *evolvingPeers) Snapshot() ([]netip.AddrPort, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]netip.AddrPort(nil), s.addresses...), true
}
func (s *evolvingPeers) add(ip netip.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addresses = append(s.addresses, netip.AddrPortFrom(ip, AutoStatusPort))
}

type otherPeers struct {
	source *evolvingPeers
	self   netip.Addr
}

func (s otherPeers) Snapshot() ([]netip.AddrPort, bool) {
	all, fresh := s.source.Snapshot()
	result := []netip.AddrPort{}
	for _, a := range all {
		if a.Addr() != s.self {
			result = append(result, a)
		}
	}
	return result, fresh
}

func TestAutomaticRuntimeStaggeredJoin(t *testing.T) {
	t.Setenv(caBundleEnv, "")
	t.Setenv("DROP_CLUSTER_ENROLLMENT_KEY", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &evolvingPeers{}
	startNode := func(ip string) *Runtime {
		t.Helper()
		address := netip.MustParseAddr(ip)
		source.add(address)
		state := t.TempDir()
		_ = os.Chmod(state, 0700)
		c, err := provisionAutomatic(HostIdentity{App: "testapp", IP: address}, strings.Repeat("staggered-private-test-", 3), state, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		c.Listen = c.Local.Address
		c.StatusListen = netip.AddrPortFrom(address, AutoStatusPort).String()
		r, err := startRuntimeWithSetup(ctx, c, func(ctx context.Context, _ *replica.Discovery) { <-ctx.Done() }, func(r *Runtime) {
			r.Observer.source = otherPeers{source, address}
			r.Node.auto.source = source
			// Drive genesis explicitly with synthetic time; all membership,
			// TLS, replication, admission and replacement workers are real.
			r.Node.auto.refresh = func(context.Context) error { return errors.New("test clock owns bootstrap") }
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := r.Close(ctx); err != nil {
				t.Error(err)
			}
		})
		return r
	}
	one := startNode("127.0.0.31")
	now := time.Now()
	if err := one.Node.auto.tick(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := one.Node.auto.tick(ctx, now.Add(31*time.Second)); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return one.Node.raft.State() == raft.Leader })
	awaitAsync(t, one.Node)
	if err := one.Node.Commit(ctx, transaction("sessions/early", 0, `true`)); err == nil {
		t.Fatal("singleton reported security replication")
	}
	two := startNode("127.0.0.32")
	three := startNode("127.0.0.33")
	deadline := time.Now().Add(80 * time.Second)
	for {
		configuration := one.Node.raft.GetConfiguration()
		if err := configuration.Error(); err != nil {
			t.Fatal(err)
		}
		voters := 0
		for _, s := range configuration.Configuration().Servers {
			if s.Suffrage == raft.Voter {
				voters++
			}
		}
		if voters == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("staggered enrollment failed: %+v", configuration.Configuration())
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := one.Node.Commit(ctx, transaction("sessions/ready", 0, `{"revoked":true}`)); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		return two.Node.state.read([]string{"sessions/ready"})["sessions/ready"].Version != 0 && three.Node.state.read([]string{"sessions/ready"})["sessions/ready"].Version != 0
	})
}

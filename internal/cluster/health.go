package cluster

import (
	"context"
	"errors"
	"net/netip"
	"time"
)

// CheckHealth probes this node's authenticated status listener. Followers and
// nodes without quorum are alive, not authoritative: container restarts cannot
// repair quorum loss. This function never opens the Raft database.
func CheckHealth(ctx context.Context, c RuntimeConfig) error {
	_, err := LocalStatus(ctx, c)
	return err
}

// LocalStatus authenticates the local listener without opening Raft state.
func LocalStatus(ctx context.Context, c RuntimeConfig) (Status, error) {
	if err := c.validate(); err != nil {
		return Status{}, err
	}
	material, err := c.tlsMaterial()
	if err != nil {
		return Status{}, err
	}
	address, err := netip.ParseAddrPort(c.StatusListen)
	if err != nil {
		return Status{}, err
	}
	if address.Addr().IsUnspecified() {
		loopback := netip.MustParseAddr("127.0.0.1")
		if address.Addr().Is6() {
			loopback = netip.IPv6Loopback()
		}
		address = netip.AddrPortFrom(loopback, address.Port())
	}
	// Use the same bounded, certificate-bound status validation as discovery,
	// without consulting discovery or accepting a different local node ID.
	observer, err := NewObserver(healthSource{}, c.ClusterID, c.App, material.Client)
	if err != nil {
		return Status{}, err
	}
	defer observer.client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	observation, err := observer.probe(ctx, address)
	if err != nil {
		return Status{}, err
	}
	if observation.Status.ID != c.Local.ID {
		return Status{}, errors.New("local coordinator identity mismatch")
	}
	return observation.Status, nil
}

type healthSource struct{}

func (healthSource) Snapshot() ([]netip.AddrPort, bool) { return nil, false }

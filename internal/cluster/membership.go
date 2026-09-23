package cluster

import (
	"context"
	"errors"
	"net/netip"

	"github.com/hashicorp/raft"
)

var ErrNotCaughtUp = errors.New("learner has not caught up to committed state")

// A returning node with its original identity/key/log may have a new host IP.
// Only the current quorum can change the address; membership size is unchanged.
func (n *Node) updateAddress(ctx context.Context, member Member) error {
	if member.validate() != nil {
		return ErrInvalid
	}
	n.changeMu.Lock()
	defer n.changeMu.Unlock()
	configuration, index, err := n.configuration(ctx)
	if err != nil {
		return err
	}
	for _, s := range configuration.Servers {
		other, err := parseAddress(string(s.Address))
		if err != nil {
			return err
		}
		if other.Address == member.Address && other.ID != member.ID {
			return ErrInvalid
		}
	}
	for _, s := range configuration.Servers {
		if string(s.ID) == member.ID {
			if s.Suffrage == raft.Voter {
				return n.wait(ctx, func() raft.Future {
					return n.raft.AddVoter(raft.ServerID(member.ID), raft.ServerAddress(member.raftAddress()), index, operationTimeout)
				})
			}
			return n.wait(ctx, func() raft.Future {
				return n.raft.AddNonvoter(raft.ServerID(member.ID), raft.ServerAddress(member.raftAddress()), index, operationTimeout)
			})
		}
	}
	return ErrInvalid
}

// Membership operations are deliberately NOT exposed on a public or peer HTTP
// API. A trusted enrollment/controller layer must explicitly approve candidates.
// Flux discovery and failed health probes must never call RemoveMember directly.

func (n *Node) configuration(ctx context.Context) (raft.Configuration, uint64, error) {
	if err := n.fence(ctx); err != nil {
		return raft.Configuration{}, 0, err
	}
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return raft.Configuration{}, 0, err
	}
	return future.Configuration(), future.Index(), nil
}

func (n *Node) AddLearner(ctx context.Context, member Member) error {
	if err := member.validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	n.changeMu.Lock()
	defer n.changeMu.Unlock()
	configuration, index, err := n.configuration(ctx)
	if err != nil {
		return err
	}
	if len(configuration.Servers) >= 9 {
		return errors.New("cluster member limit reached")
	}
	for _, server := range configuration.Servers {
		if string(server.ID) == member.ID || string(server.Address) == member.raftAddress() {
			return errors.New("member already exists; address replacement requires explicit handling")
		}
		other, err := parseAddress(string(server.Address))
		if err != nil {
			return err
		}
		if other.Address == member.Address {
			return errors.New("member address already in use")
		}
	}
	return n.wait(ctx, func() raft.Future {
		return n.raft.AddNonvoter(raft.ServerID(member.ID), raft.ServerAddress(member.raftAddress()), index, operationTimeout)
	})
}

// Promote probes the candidate over verified TLS before granting it a vote. The
// proof must match the committed member address/ID and this exact cluster. Uptime
// is deliberately not consulted. Replication progress is evidence of catch-up,
// not an alternative to Raft's own membership/majority checks.
func (n *Node) Promote(ctx context.Context, member Member, statusPort uint16, observer *Observer) error {
	if member.validate() != nil || statusPort < 1024 || observer == nil || observer.clusterID != n.config.ClusterID {
		return ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	n.changeMu.Lock()
	defer n.changeMu.Unlock()
	configuration, index, err := n.configuration(ctx)
	if err != nil {
		return err
	}
	voters := 0
	found := false
	for _, s := range configuration.Servers {
		if s.Suffrage == raft.Voter {
			voters++
		}
		if string(s.ID) == member.ID {
			if string(s.Address) != member.raftAddress() || s.Suffrage != raft.Nonvoter {
				return ErrInvalid
			}
			found = true
		}
	}
	if !found || voters >= 6 {
		return ErrInvalid
	}
	address, _ := netip.ParseAddrPort(member.Address)
	targetState := n.state.appliedIndex()
	// configuration() adds a barrier. On a singleton, that new barrier can
	// commit before learners receive it, causing perpetual false lag on every
	// poll. Require the stable metadata + membership prefix instead; AddVoter
	// still uses Raft's ordered configuration-change/replication protocol.
	target := max(targetState, index)
	observation, err := observer.probe(ctx, netip.AddrPortFrom(address.Addr(), statusPort))
	if err != nil {
		return err
	}
	if observation.Status.ID != member.ID || observation.Status.Role != "Follower" || observation.Status.LastIndex < target || observation.Status.StateIndex < targetState {
		return ErrNotCaughtUp
	}
	return n.wait(ctx, func() raft.Future {
		return n.raft.AddVoter(raft.ServerID(member.ID), raft.ServerAddress(member.raftAddress()), index, operationTimeout)
	})
}

func (n *Node) RemoveMember(ctx context.Context, id string) error {
	if !nodeIDPattern.MatchString(id) || id == n.config.Local.ID {
		return ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	n.changeMu.Lock()
	defer n.changeMu.Unlock()
	configuration, index, err := n.configuration(ctx)
	if err != nil {
		return err
	}
	voters := 0
	found := false
	isVoter := false
	for _, s := range configuration.Servers {
		if s.Suffrage == raft.Voter {
			voters++
		}
		if string(s.ID) == id {
			found = true
			isVoter = s.Suffrage == raft.Voter
		}
	}
	if !found || (isVoter && voters <= 3) {
		return errors.New("removal would violate minimum three-voter membership")
	}
	return n.wait(ctx, func() raft.Future { return n.raft.RemoveServer(raft.ServerID(id), index, operationTimeout) })
}

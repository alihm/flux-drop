package cluster

import (
	"context"
	"log/slog"
	"net/netip"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type diagnosticState struct {
	noLeaderSince    time.Time
	lastLeaderLog    time.Time
	lastAdmissionLog time.Time
}

// Observational only: neither these timers, probes nor status fields may alter
// membership, bootstrap, recover, or change the Docker liveness semantics.
func (r *Runtime) diagnosticTick(ctx context.Context, state *diagnosticState, now time.Time) {
	n := r.Node
	_, leader := n.raft.LeaderWithID()
	if leader != "" {
		state.noLeaderSince = time.Time{}
	} else if state.noLeaderSince.IsZero() {
		state.noLeaderSince = now
	}
	if !state.noLeaderSince.IsZero() && now.Sub(state.noLeaderSince) >= 2*time.Minute && (state.lastLeaderLog.IsZero() || now.Sub(state.lastLeaderLog) >= 2*time.Minute) {
		state.lastLeaderLog = now
		configuration := n.raft.GetConfiguration()
		var unreachable []string
		if configuration.Error() == nil {
			// There are at most nine members. Bounded concurrent authenticated probes
			// prevent two dead hosts from serially delaying this diagnostic worker.
			var workers sync.WaitGroup
			var mu sync.Mutex
			for _, server := range configuration.Configuration().Servers {
				if server.Suffrage != raft.Voter || string(server.ID) == n.config.Local.ID {
					continue
				}
				workers.Add(1)
				go func(server raft.Server) {
					defer workers.Done()
					member, err := parseAddress(string(server.Address))
					if err == nil {
						address, parseErr := netip.ParseAddrPort(member.Address)
						err = parseErr
						if err == nil {
							var observation Observation
							observation, err = r.Observer.probe(ctx, netip.AddrPortFrom(address.Addr(), r.statusPort))
							if err == nil && observation.Status.ID != member.ID {
								err = ErrInvalid
							}
						}
					}
					if err != nil {
						mu.Lock()
						unreachable = append(unreachable, string(server.ID))
						mu.Unlock()
					}
				}(server)
			}
			workers.Wait()
		}
		slog.Error("Raft has had no leader for more than two minutes; alert on readiness and investigate majority loss", "node", n.config.Local.ID, "leaderless_seconds", int64(now.Sub(state.noLeaderSince)/time.Second), "unreachable_voters", unreachable)
	}
	if n.raft.LastIndex() == 0 && now.Sub(n.started) >= 5*time.Minute && (state.lastAdmissionLog.IsZero() || now.Sub(state.lastAdmissionLog) >= 5*time.Minute) {
		state.lastAdmissionLog = now
		slog.Warn("empty Raft node has waited more than five minutes for admission; a surviving leader is required", "node", n.config.Local.ID, "waiting_seconds", int64(now.Sub(n.started)/time.Second))
	}
}

func (r *Runtime) runDiagnostics(ctx context.Context) {
	timer := time.NewTicker(15 * time.Second)
	defer timer.Stop()
	state := diagnosticState{}
	for ctx.Err() == nil {
		bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
		r.diagnosticTick(bounded, &state, time.Now())
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

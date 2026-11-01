package cluster

import (
	"context"
	"encoding/json"
	"github.com/hashicorp/raft"
	"time"
)

func (e *enrollment) shouldRequestRejoin(found bool, now time.Time) bool {
	if !found {
		return true
	}
	if e.node.raft.State() == raft.Leader {
		return false
	}
	_, own := e.node.raft.LeaderWithID()
	if own != "" {
		return false
	} // Raft's recent contact is stronger than observation.
	peers, fresh := e.observer.Snapshot()
	if !fresh {
		return false
	}
	leaders := 0
	for _, peer := range peers {
		if peer.Status.Role == "Leader" && peer.Status.ID != e.node.config.Local.ID {
			leaders++
		}
	}
	if leaders != 1 {
		return false
	}
	contact := e.node.raft.LastContact()
	if contact.IsZero() {
		contact = e.node.started
	}
	return now.Sub(contact) >= 2*time.Minute
}

// Explicit bootstrap voters did not pass enrollment. Pin their verified keys
// while healthy, so subsequent health retirement can be reversed with the same
// identity/key checks as every admitted learner. Never re-pin an existing row.
func (e *enrollment) rememberMemberKeys(ctx context.Context, configuration raft.Configuration) error {
	for _, server := range configuration.Servers {
		if server.Suffrage != raft.Voter {
			continue
		}
		hash := e.memberKeys[string(server.ID)]
		if hash == "" {
			continue
		}
		key := "cluster_admissions/" + string(server.ID)
		records, err := e.node.Read(ctx, []string{key})
		if err != nil {
			return err
		}
		if records[key].Version != 0 {
			continue
		}
		member, err := parseAddress(string(server.Address))
		if err != nil {
			return err
		}
		data, _ := json.Marshal(admission{Member: member, KeyHash: hash})
		if err := e.node.Commit(ctx, Transaction{Schema: 1, Checks: []Check{{Key: key}}, Writes: []Write{{Key: key, Value: data}}}); err != nil {
			return err
		}
	}
	return nil
}

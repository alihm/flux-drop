package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestSelfFencedLeaderTransfersToHealthyMajority(t *testing.T) {
	for _, async := range []bool{true, false} {
		name := "synchronous"
		if async {
			name = "asynchronous"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv(PassphraseEnv, strings.Repeat("fenced-transfer-", 3))
			h := newChurnClusterWithAsync(t, async)
			leader := h.leader()
			var healthy []*churnNode
			for _, n := range h.nodes {
				if n != leader {
					healthy = append(healthy, n)
				}
			}
			onlyListed(h, healthy...)
			signedRecoveryMode(t, leader, "on")
			m := recoveryTestManager(h, leader)
			for i := 0; i <= int(autoRecoveryWindow/autoRecoveryInterval); i++ {
				_ = m.tick(context.Background())
				h.now = h.now.Add(autoRecoveryInterval)
			}
			if !leader.node.selfFenced.Load() {
				t.Fatal("missing leader was not fenced")
			}
			deadline := time.Now().Add(5 * time.Second)
			var next *churnNode
			for time.Now().Before(deadline) {
				for _, n := range healthy {
					if n.node.raft.State() == raft.Leader && n.node.Ready(context.Background()) == nil {
						next = n
						break
					}
				}
				if next != nil {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if next == nil {
				t.Fatal("fenced leader did not transfer within five seconds")
			}
			if err := next.node.Commit(context.Background(), transaction("sessions/healthy-majority", 0, `true`)); err != nil {
				t.Fatal("healthy majority could not write", err)
			}
			if !leader.node.selfFenced.Load() {
				t.Fatal("leadership transfer cleared authority fence")
			}
			if err := leader.node.Ready(context.Background()); !errors.Is(err, ErrNotLeader) {
				t.Fatal("fenced node grants authority", err)
			}
			if err := leader.node.Commit(context.Background(), transaction("sessions/still-fenced", 0, `true`)); !errors.Is(err, ErrNotLeader) {
				t.Fatal("fenced node accepted write", err)
			}
			onlyListed(h, h.nodes...)
			_ = m.tick(context.Background())
			if leader.node.selfFenced.Load() {
				t.Fatal("relisting did not clear fence")
			}
			if next.node.raft.State() != raft.Leader {
				t.Fatal("relisting disturbed healthy leader")
			}
		})
	}
}

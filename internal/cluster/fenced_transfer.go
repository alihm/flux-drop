package cluster

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
)

// Async nodes already have a 100ms authority heartbeat. Explicit/manual
// manifests can use synchronous writes too, and need the same handoff without
// waiting for the 15-second discovery evaluator. Neither path clears the fence.
func (n *Node) startFencedTransfers() {
	if n.async != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.fencedTransferCancel = cancel
	n.fencedTransferDone = make(chan struct{})
	go func() {
		defer close(n.fencedTransferDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var lastTransfer time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if !n.selfFenced.Load() || n.raft.State() != raft.Leader || !lastTransfer.IsZero() && now.Sub(lastTransfer) < time.Second {
					continue
				}
				lastTransfer = now
				bounded, stop := context.WithTimeout(ctx, time.Second)
				_ = n.wait(bounded, func() raft.Future { return n.raft.LeadershipTransfer() })
				stop()
			}
		}
	}()
}

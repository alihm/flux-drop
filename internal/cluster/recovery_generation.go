package cluster

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
)

const recoveryGenerationKey = "cluster_recovery/generation"
const generationPendingFile = "recovery-generation.json"

type recoveryGeneration struct {
	Generation uint64 `json:"generation"`
	Lineage    string `json:"lineage"`
}
type generationIntent struct {
	ClusterID string             `json:"clusterID"`
	NodeID    string             `json:"nodeID"`
	Recovery  recoveryGeneration `json:"recovery"`
}

func (n *Node) recoveryGeneration() (recoveryGeneration, error) {
	row := n.state.read([]string{recoveryGenerationKey})[recoveryGenerationKey]
	if row.Version == 0 {
		return recoveryGeneration{}, nil
	}
	var value recoveryGeneration
	if decodeStrict(row.Value, &value) != nil || value.Generation == 0 || len(value.Lineage) != 32 {
		return value, ErrInvalid
	}
	return value, nil
}

// Write this intent before either forced recovery mutates stores. A restart
// retries the same generation rather than incrementing an already committed one.
func prepareGenerationIntent(c Config) error {
	path := filepath.Join(c.StateDir, generationPendingFile)
	var existing generationIntent
	_, err := readOperatorRequest(path, &existing, "clusterID", "nodeID", "recovery")
	if err == nil {
		if existing.ClusterID != c.ClusterID || existing.NodeID != c.Local.ID {
			return ErrInvalid
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	intent := generationIntent{ClusterID: c.ClusterID, NodeID: c.Local.ID}
	raw, _ := json.Marshal(intent)
	return replaceCertificate(path, raw)
}

func (n *Node) startRecoveryGeneration() error {
	path := filepath.Join(n.config.StateDir, generationPendingFile)
	var intent generationIntent
	info, err := readOperatorRequest(path, &intent, "clusterID", "nodeID", "recovery")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if intent.ClusterID != n.config.ClusterID || intent.NodeID != n.config.Local.ID {
		return ErrInvalid
	}
	n.generationPending.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	n.generationCancel = cancel
	n.generationDone = make(chan struct{})
	go func() {
		defer close(n.generationDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for ctx.Err() == nil {
			if n.raft.State() == raft.Leader {
				bounded, stop := context.WithTimeout(ctx, operationTimeout)
				err := n.finishRecoveryGeneration(bounded, &intent, &info)
				stop()
				if err == nil {
					n.generationPending.Store(false)
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

func (n *Node) finishRecoveryGeneration(ctx context.Context, intent *generationIntent, info *os.FileInfo) error {
	if err := n.fence(ctx); err != nil {
		return err
	}
	current, err := n.recoveryGeneration()
	if err != nil {
		return err
	}
	if intent.Recovery.Generation == 0 {
		// A replicated-volume high-water mark prevents restoring an older backup
		// from reusing a known generation. Time is an additional floor, not proof of
		// history; random lineage makes independently recovered histories unequal.
		highest := current.Generation
		if n.raftGuard != nil {
			known, err := n.raftGuard.effective()
			if err != nil {
				return err
			}
			highest = max(highest, known.Generation)
		}
		var watermark generationIntent
		_, err := readOperatorRequest(filepath.Join(n.config.ContentDir, "recovery-generation.json"), &watermark, "clusterID", "nodeID", "recovery")
		if err == nil {
			if watermark.ClusterID != n.config.ClusterID {
				return ErrInvalid
			}
			highest = max(highest, watermark.Recovery.Generation)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		intent.Recovery = recoveryGeneration{Generation: max(highest+1, uint64(time.Now().UnixMilli())), Lineage: fmt.Sprintf("%x", randomLineage())}
		raw, _ := json.Marshal(intent)
		if err := replaceCertificate(filepath.Join(n.config.StateDir, generationPendingFile), raw); err != nil {
			return err
		}
		*info, err = os.Lstat(filepath.Join(n.config.StateDir, generationPendingFile))
		if err != nil {
			return err
		}
	}
	if current != intent.Recovery {
		if current.Generation >= intent.Recovery.Generation {
			return ErrInvalid
		}
		row := n.state.read([]string{recoveryGenerationKey})[recoveryGenerationKey]
		raw, _ := json.Marshal(intent.Recovery)
		if err := n.Commit(ctx, Transaction{Schema: 1, Checks: []Check{{Key: recoveryGenerationKey, Version: row.Version}}, Writes: []Write{{Key: recoveryGenerationKey, Value: raw}}}); err != nil {
			return err
		}
	}
	if n.raftGuard != nil {
		if _, err := n.raftGuard.effective(); err != nil {
			return err
		}
	}
	raw, _ := json.Marshal(intent)
	if err := replaceCertificate(filepath.Join(n.config.ContentDir, "recovery-generation.json"), raw); err != nil {
		return err
	}
	// Intent is deliberately not a public request and cannot trigger recovery.
	return consumeRecoveryRequest(n.config, "recovery-generation", recoveryTimestamp(), *info)
}

func randomLineage() []byte {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		panic(err)
	}
	return value
}

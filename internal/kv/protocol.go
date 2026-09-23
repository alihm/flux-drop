// Package kv defines the internal consensus storage protocol without application
// dependencies. It is not a public HTTP API.
package kv

import (
	"encoding/json"
	"errors"
)

var (
	ErrConflict  = errors.New("metadata revision conflict")
	ErrInvalid   = errors.New("invalid metadata transaction")
	ErrCapacity  = errors.New("metadata capacity exceeded")
	ErrNotLeader = errors.New("authoritative leader unavailable")
)

type Record struct {
	Version uint64          `json:"version"`
	Value   json.RawMessage `json:"value"`
}
type Check struct {
	Key     string `json:"key"`
	Version uint64 `json:"version"`
}
type Write struct {
	Key    string          `json:"key"`
	Value  json.RawMessage `json:"value,omitempty"`
	Delete bool            `json:"delete,omitempty"`
}

// Durability is the minimum acknowledgement guarantee, not a request to bypass
// primary fencing or authorization. An omitted value always means Replicated.
// Backends may provide a stronger guarantee (the Raft backend always does).
type Durability string

const (
	Replicated Durability = "replicated"
	Local      Durability = "local"
)

func (d Durability) Valid() bool { return d == "" || d == Replicated || d == Local }

func (d Durability) Effective() Durability {
	if d == "" {
		return Replicated
	}
	return d
}

type Transaction struct {
	Schema     int        `json:"schema"`
	Checks     []Check    `json:"checks"`
	Writes     []Write    `json:"writes"`
	Durability Durability `json:"durability,omitempty"`
	// Revision is assigned by the coordinator, never by application callers.
	Revision uint64 `json:"revision,omitempty"`
}

// Page is an observational scan. Mutations must re-read/check each candidate.
type Page struct {
	Records map[string]Record `json:"records"`
	Next    string            `json:"next,omitempty"`
}

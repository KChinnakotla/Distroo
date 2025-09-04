package raft

import "time"

// One log entry in Raft's replicated log.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte // opaque command (e.g., JSON "PUT/DEL" for the KV layer)
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool

	// Optional fast-backoff hint (see §5.3 optimization). Keep simple for now.
	ConflictIndex uint64
	ConflictTerm  uint64
}

type ApplyMsg struct {
	Index uint64
	Term  uint64
	Data  []byte // command to apply to the state machine
}

// Helpers
const (
	HeartbeatInterval   = 100 * time.Millisecond
	ElectionTimeoutMin  = 500 * time.Millisecond
	ElectionTimeoutMax  = 900 * time.Millisecond
	ReplicationTimeout  = 4 * time.Second
	ClientCommitTimeout = 5 * time.Second
)

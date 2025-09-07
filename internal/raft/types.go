package raft

import "time"

// One log entry in Raft's replicated log.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte // opaque command (e.g., JSON "PUT/DEL" for the KV layer)
}

type AppendEntriesRequest struct {
    Term         uint64  `json:"term"`
    LeaderID     string  `json:"leader_id"`
    PrevLogIndex uint64  `json:"prev_log_index"`
    PrevLogTerm  uint64  `json:"prev_log_term"`
    Entries      []Entry `json:"entries"`
    LeaderCommit uint64  `json:"leader_commit"`
}

type AppendEntriesResponse struct {
    Term          uint64 `json:"term"`
    Success       bool   `json:"success"`
    ConflictIndex uint64 `json:"conflict_index,omitempty"`
    ConflictTerm  uint64 `json:"conflict_term,omitempty"`
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

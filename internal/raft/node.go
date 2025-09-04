package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type role int

const (
	follower role = iota
	candidate
	leader
)

type peer struct {
	ID   string
	Addr string // base URL like http://kv2:8080
}

type RaftNode struct {
	mu sync.Mutex

	id      string
	addr    string
	peers   []peer
	role    role
	term    uint64
	votedFor string

	log         *Log
	commitIndex uint64
	lastApplied uint64

	// Leader-only replication state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Apply channel for committed entries -> KV layer
	applyCh chan ApplyMsg

	// Waiters: entry index -> chans to signal once committed
	waiters map[uint64][]chan struct{}

	// timers
	heartbeatTicker *time.Ticker
	electionTimer   *time.Timer

	httpClient *http.Client
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewRaftNode(id, addr string, peers []peer) *RaftNode {
	ctx, cancel := context.WithCancel(context.Background())
	rn := &RaftNode{
		id:       id,
		addr:     addr,
		peers:    peers,
		role:     follower,
		term:     0,
		log:      NewLog(),
		applyCh:  make(chan ApplyMsg, 1024),
		waiters:  make(map[uint64][]chan struct{}),
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
		ctx:    ctx,
		cancel: cancel,
	}
	rn.resetElectionTimer()
	return rn
}

func (rn *RaftNode) ApplyCh() <-chan ApplyMsg { return rn.applyCh }

// ---------- Public API called by KV layer ----------

// StartCommand is called by the leader to replicate a client command.
// It returns (index, term, okLeader).
func (rn *RaftNode) StartCommand(data []byte) (uint64, uint64, bool) {
	rn.mu.Lock()
	if rn.role != leader {
		rn.mu.Unlock()
		return 0, rn.term, false
	}
	// Append to leader log
	idx := rn.log.LastIndex() + 1
	ent := Entry{Index: idx, Term: rn.term, Data: append([]byte(nil), data...)}
	rn.log.Append(ent)

	// Register a waiter to let HTTP handler block until commit
	ch := make(chan struct{}, 1)
	rn.waiters[idx] = append(rn.waiters[idx], ch)

	// Kick off replication (async)
	term := rn.term
	peers := append([]peer(nil), rn.peers...)
	if rn.nextIndex == nil {
		rn.nextIndex = make(map[string]uint64)
		rn.matchIndex = make(map[string]uint64)
		for _, p := range peers {
			if p.ID == rn.id {
				continue
			}
			rn.nextIndex[p.ID] = rn.log.LastIndex() // next entry to send
			rn.matchIndex[p.ID] = 0
		}
	}
	rn.mu.Unlock()

	go rn.replicateToAll(term)

	return idx, term, true
}

// WaitForCommit blocks (up to timeout) until index is committed.
func (rn *RaftNode) WaitForCommit(index uint64, timeout time.Duration) bool {
	rn.mu.Lock()
	// fast path: already committed
	if rn.commitIndex >= index {
		rn.mu.Unlock()
		return true
	}
	ch := make(chan struct{}, 1)
	rn.waiters[index] = append(rn.waiters[index], ch)
	rn.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-timer.C:
		return false
	}
}

// ---------- Leader replication loop ----------

func (rn *RaftNode) replicateToAll(term uint64) {
	var wg sync.WaitGroup
	for _, p := range rn.peers {
		if p.ID == rn.id {
			continue
		}
		wg.Add(1)
		go func(pp peer) {
			defer wg.Done()
			rn.replicateToPeer(term, pp)
		}(p)
	}
	wg.Wait()
}

func (rn *RaftNode) replicateToPeer(term uint64, p peer) {
	for {
		rn.mu.Lock()
		// step down guard
		if rn.role != leader || rn.term != term {
			rn.mu.Unlock()
			return
		}
		next := rn.nextIndex[p.ID]
		if next == 0 {
			next = rn.log.LastIndex()
		}
		prevIdx := next - 1
		prevTerm := rn.log.Term(prevIdx)
		entries := rn.log.Slice(next)
		req := AppendEntriesRequest{
			Term:         rn.term,
			LeaderID:     rn.id,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rn.commitIndex,
		}
		rn.mu.Unlock()

		ok, respTerm, conflictIdx, conflictTerm := rn.sendAppendEntries(p, req)
		rn.mu.Lock()
		// term changed?
		if respTerm > rn.term {
			rn.term = respTerm
			rn.role = follower
			rn.votedFor = ""
			rn.mu.Unlock()
			return
		}
		if ok {
			// follower stored entries; advance next/match
			newMatch := req.PrevLogIndex + uint64(len(entries))
			rn.matchIndex[p.ID] = newMatch
			rn.nextIndex[p.ID] = newMatch + 1
			rn.maybeAdvanceCommit()
			rn.mu.Unlock()
			return // sent all outstanding entries
		}
		// back off nextIndex based on conflict hint
		if conflictIdx > 0 {
			// naive backoff: jump to conflict index
			rn.nextIndex[p.ID] = conflictIdx
		} else if rn.nextIndex[p.ID] > 1 {
			rn.nextIndex[p.ID]--
		}
		_ = conflictTerm
		rn.mu.Unlock()

		// small pause to prevent tight spinning
		time.Sleep(10 * time.Millisecond)
	}
}

func (rn *RaftNode) maybeAdvanceCommit() {
	// Only leader calls this under lock.
	// Find the highest N > commitIndex such that a majority have matchIndex >= N AND log[N].Term == currentTerm.
	N := rn.commitIndex + 1
	last := rn.log.LastIndex()
	for n := last; n > rn.commitIndex; n-- {
		if rn.log.Term(n) != rn.term {
			continue
		}
		count := 1 // leader itself
		for _, p := range rn.peers {
			if p.ID == rn.id {
				continue
			}
			if rn.matchIndex[p.ID] >= n {
				count++
			}
		}
		if count*2 > len(rn.peers) { // majority
			rn.commitIndex = n
			go rn.signalWaitersUpTo(n)
			go rn.applyLoopKick()
			break
		}
	}
}

func (rn *RaftNode) signalWaitersUpTo(n uint64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	for idx := range rn.waiters {
		if idx <= n {
			for _, ch := range rn.waiters[idx] {
				select { case ch <- struct{}{}: default: }
			}
			delete(rn.waiters, idx)
		}
	}
}

// ---------- Apply loop ----------

// applyLoopKick nudges the applier; lightweight and safe to call often.
func (rn *RaftNode) applyLoopKick() {
	go rn.applyCommitted()
}

func (rn *RaftNode) applyCommitted() {
	for {
		rn.mu.Lock()
		if rn.lastApplied >= rn.commitIndex {
			rn.mu.Unlock()
			return
		}
		rn.lastApplied++
		ent, _ := rn.log.At(rn.lastApplied)
		msg := ApplyMsg{Index: ent.Index, Term: ent.Term, Data: ent.Data}
		rn.mu.Unlock()

		select {
		case rn.applyCh <- msg:
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// ---------- RPC plumbing (AppendEntries handler & client) ----------

func (rn *RaftNode) HandleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := AppendEntriesResponse{Term: rn.term, Success: false}

	if req.Term < rn.term {
		writeJSON(w, resp); return
	}

	// If term is newer, become follower
	if req.Term > rn.term {
		rn.term = req.Term
		rn.role = follower
		rn.votedFor = ""
	}
	// reset election timer because we heard from leader
	rn.resetElectionTimerLocked()

	// Log consistency check
	if req.PrevLogIndex > rn.log.LastIndex() || rn.log.Term(req.PrevLogIndex) != req.PrevLogTerm {
		// Simple conflict hint
		resp.ConflictIndex = minU64(req.PrevLogIndex, rn.log.LastIndex())
		resp.ConflictTerm = rn.log.Term(resp.ConflictIndex)
		writeJSON(w, resp); return
	}

	// Append new entries, deleting conflicts
	for i, e := range req.Entries {
		pos := req.PrevLogIndex + 1 + uint64(i)
		if pos <= rn.log.LastIndex() {
			if rn.log.Term(pos) != e.Term {
				rn.log.TruncateFrom(pos)
				rn.log.Append(e)
			} else {
				// already present; skip
			}
		} else {
			rn.log.Append(e)
		}
	}

	// Advance commit index
	if req.LeaderCommit > rn.commitIndex {
		rn.commitIndex = minU64(req.LeaderCommit, rn.log.LastIndex())
		go rn.applyLoopKick()
	}

	resp.Success = true
	resp.Term = rn.term
	writeJSON(w, resp)
}

func (rn *RaftNode) sendAppendEntries(p peer, req AppendEntriesRequest) (ok bool, term uint64, cidx uint64, cterm uint64) {
	b, _ := json.Marshal(&req)
	ctx, cancel := context.WithTimeout(rn.ctx, 2*time.Second)
	defer cancel()
	httpReq, _ := http.NewRequestWithContext(ctx, "POST", p.Addr+"/raft/append", bytes.NewReader(b))
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := rn.httpClient.Do(httpReq)
	if err != nil {
		return false, rn.term, 0, 0
	}
	defer resp.Body.Close()
	var ar AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return false, rn.term, 0, 0
	}
	return ar.Success, ar.Term, ar.ConflictIndex, ar.ConflictTerm
}

// ---------- Timer helpers (unchanged structure, now nudging apply/heartbeats) ----------

func (rn *RaftNode) resetElectionTimer() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.resetElectionTimerLocked()
}

func (rn *RaftNode) resetElectionTimerLocked() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	d := randomElectionTimeout()
	rn.electionTimer = time.NewTimer(d)
}

// (Assume you already have code that: followers -> candidate on timeout,
// RequestVote RPCs, leader sends empty AppendEntries as heartbeats, etc.)

// Utility

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func minU64(a, b uint64) uint64 {
	if a < b { return a }
	return b
}

package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// Keep the same Role constants you had earlier
type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Config struct {
	ID        string
	Advertise string   // e.g., http://kv1:8080
	DataDir   string
	Peers     []string // peer base URLs
	Logger    *log.Logger
}

type Node struct {mu sync.Mutex

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]intmu sync.Mutex

	// identity
	id        string
	advertise string
	peers     []string
	logg      *log.Logger

	// role/term
	role   Role
	term   uint64
	votedFor string

	// leader info
	leaderID   string
	leaderAddr string

	// Raft core state
	log *Log

	// commit / apply
	commitIndex uint64
	lastApplied uint64
	applyCh     chan ApplyMsg

	// leader replication state (only meaningful if role==Leader)
	nextIndex  map[string]uint64 // per-peer next index to send
	matchIndex map[string]uint64 // per-peer highest match

	// waiters: index -> list of chans to notify when index committed
	waiters map[uint64][]chan struct{}

	// timers & http client
	electionReset time.Time
	httpClient    *http.Client
	stopCh        chan struct{}
	logger        *log.Logger
}

// NewNode constructs node and initializes log.
func NewNode(cfg Config) *Node {
	n := &Node{
		id:         cfg.ID,
		advertise:  cfg.Advertise,
		peers:      append([]string(nil), cfg.Peers...),
		logg:       cfg.Logger,
		role:       Follower,
		term:       0,
		votedFor:   "",
		leaderID:   "",
		leaderAddr: "",
		log:        NewLog(),
		commitIndex: 0,
		lastApplied: 0,
		applyCh:    make(chan ApplyMsg, 1024),
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
		waiters:    make(map[uint64][]chan struct{}),
		electionReset: time.Now(),
		httpClient: &http.Client{Timeout: 800 * time.Millisecond},
		stopCh:     make(chan struct{}),
		logger:     cfg.Logger,
	}
	return n
}

// IsLeader returns whether this node is leader
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// LeaderInfo returns a small leader view for clients
type LeaderView struct {
	LeaderID   string `json:"leader_id"`
	LeaderAddr string `json:"leader_addr"`
	IsLeader   bool   `json:"is_leader"`
	SelfID     string `json:"self_id"`
}

func (n *Node) LeaderInfo() LeaderView {
	n.mu.Lock()
	defer n.mu.Unlock()
	return LeaderView{
		LeaderID:   n.leaderID,
		LeaderAddr: n.leaderAddr,
		IsLeader:   n.role == Leader,
		SelfID:     n.id,
	}
}

// RegisterHTTP registers HTTP endpoints for raft control
func (n *Node) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/raft/leader", n.handleLeader)
	mux.HandleFunc("/raft/state", n.handleState)
	mux.HandleFunc("/raft/requestvote", n.handleRequestVote)
	mux.HandleFunc("/raft/append", n.handleAppendEntries)
	// debug: inspect log
	mux.HandleFunc("/raft/log", n.handleDebugLog)
}

// -------------------- HTTP handlers --------------------

func (n *Node) handleLeader(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, n.LeaderInfo())
}

func (n *Node) handleState(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"id":          n.id,
		"role":        n.role,
		"term":        n.term,
		"voted_for":   n.votedFor,
		"leader_id":   n.leaderID,
		"leader_addr": n.leaderAddr,
		"last_index":  n.log.LastIndex(),
		"commitIndex": n.commitIndex,
		"lastApplied": n.lastApplied,
	})
}

// RequestVote RPC (kept simple to match earlier behavior; no lastLog checks)
type requestVoteReq struct {
	Term        uint64 `json:"term"`
	CandidateID string `json:"candidate_id"`
}

type requestVoteResp struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

func (n *Node) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var req requestVoteReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad json"})
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term > n.term {
		n.becomeFollowerLocked(req.Term, "")
	}

	resp := requestVoteResp{Term: n.term, VoteGranted: false}
	if req.Term < n.term {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	if n.votedFor == "" || n.votedFor == req.CandidateID {
		n.votedFor = req.CandidateID
		resp.VoteGranted = true
		n.electionReset = time.Now()
	}
	writeJSON(w, http.StatusOK, resp)
}

// AppendEntries RPC types (structure used for replication and heartbeat)
type appendEntriesReq struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries"`
	LeaderCommit uint64  `json:"leader_commit"`
}

type appendEntriesResp struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
}

// handleAppendEntries handles both empty heartbeats and actual replication requests.
func (n *Node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req appendEntriesReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad json"})
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	resp := appendEntriesResp{Term: n.term, Success: false}

	// If leader's term is older, reject
	if req.Term < n.term {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	// If request term is newer, step down to follower
	if req.Term > n.term {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	// reset election timer
	n.electionReset = time.Now()
	n.leaderID = req.LeaderID
	n.leaderAddr = req.LeaderID // in our scaffold advertise == id/addr; keep leaderAddr filled if possible

	// Log consistency check
	if req.PrevLogIndex > n.log.LastIndex() || n.log.Term(req.PrevLogIndex) != req.PrevLogTerm {
		// conflict: tell leader a hint
		resp.ConflictIndex = minU64(req.PrevLogIndex, n.log.LastIndex()+1)
		resp.ConflictTerm = n.log.Term(resp.ConflictIndex)
		writeJSON(w, http.StatusOK, resp)
		return
	}

	// Append new entries, deleting conflicts if necessary
	for i, e := range req.Entries {
		pos := req.PrevLogIndex + 1 + uint64(i)
		if pos <= n.log.LastIndex() {
			if n.log.Term(pos) != e.Term {
				// conflict - truncate and append remainder
				n.log.TruncateFrom(pos)
				n.log.Append(e)
			} else {
				// entry already exists and matches; skip
			}
		} else {
			// append new entry(s)
			n.log.Append(e)
		}
	}

	// Advance commit index
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = minU64(req.LeaderCommit, n.log.LastIndex())
		// kick apply loop outside lock
		go n.applyCommitted()
	}

	resp.Success = true
	resp.Term = n.term
	writeJSON(w, http.StatusOK, resp)
}

// debug endpoint to inspect log
func (n *Node) handleDebugLog(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()
	type L struct {
		Index uint64 `json:"index"`
		Term  uint64 `json:"term"`
		Data  string `json:"data"`
	}
	out := make([]L, 0, int(n.log.LastIndex()+1))
	for i := uint64(0); i <= n.log.LastIndex(); i++ {
		if e, ok := n.log.At(i); ok {
			out = append(out, L{Index: e.Index, Term: e.Term, Data: string(e.Data)})
		}
	}
	_ = json.NewEncoder(w).Encode(out)
}

// -------------------- Election / Run loop --------------------

// Run starts the node's background loops (election + leader heartbeat)
func (n *Node) Run() {
	go func() {
		rand.Seed(time.Now().UnixNano())
		for {
			select {
			case <-n.stopCh:
				return
			default:
			}

			n.mu.Lock()
			role := n.role
			n.mu.Unlock()

			switch role {
			case Leader:
				// send heartbeats (empty AppendEntries) often
				n.sendHeartbeats()
				time.Sleep(HeartbeatInterval)
			default:
				// follower/candidate: check election timeout
				if time.Since(n.getElectionReset()) > randomElectionTimeout() {
					n.startElection()
				}
				time.Sleep(25 * time.Millisecond)
			}
		}
	}()
}

// getElectionReset safely returns electionReset
func (n *Node) getElectionReset() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.electionReset
}

func randomElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin))))
}

// startElection launches a candidate election (kept similar to your previous implementation)
func (n *Node) startElection() {
	n.mu.Lock()
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	term := n.term
	n.electionReset = time.Now()
	peers := append([]string(nil), n.peers...)
	self := n.advertise
	n.mu.Unlock()

	if n.logg != nil {
		n.logg.Printf("[%s] election started term=%d", n.id, term)
	}

	votes := 1
	var wg sync.WaitGroup
	var muVotes sync.Mutex

	for _, p := range peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			req := requestVoteReq{Term: term, CandidateID: n.id}
			b, _ := json.Marshal(req)
			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			defer cancel()
			httpReq, _ := http.NewRequestWithContext(ctx, "POST", peerAddr+"/raft/requestvote", bytes.NewReader(b))
			httpReq.Header.Set("Content-Type", "application/json")
			resp, err := n.httpClient.Do(httpReq)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var rvr requestVoteResp
			_ = json.NewDecoder(resp.Body).Decode(&rvr)
			n.mu.Lock()
			if rvr.Term > n.term {
				n.becomeFollowerLocked(rvr.Term, "")
			}
			n.mu.Unlock()
			if rvr.VoteGranted {
				muVotes.Lock()
				votes++
				muVotes.Unlock()
			}
		}(p)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Candidate || n.term != term {
		return
	}
	total := len(peers) + 1
	if votes*2 > total {
		// become leader
		n.role = Leader
		n.leaderID = n.id
		n.leaderAddr = n.advertise
		// initialize leader state
		lastIdx := n.log.LastIndex()
		for _, p := range n.peers {
			if p == n.advertise {
				continue
			}
			n.nextIndex[p] = lastIdx + 1
			n.matchIndex[p] = 0
		}
		if n.logg != nil {
			n.logg.Printf("[%s] became leader (term=%d) votes=%d/%d", n.id, n.term, votes, total)
		}
	} else {
		n.role = Follower
		n.votedFor = ""
		n.electionReset = time.Now()
		if n.logg != nil {
			n.logg.Printf("[%s] election lost (votes=%d/%d)", n.id, votes, total)
		}
	}
}

// becomeFollowerLocked assumes lock held
func (n *Node) becomeFollowerLocked(term uint64, leaderID string) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
	}
	n.role = Follower
	n.leaderID = leaderID
	// leaderAddr left as-is; leader discovery endpoint can fill it if needed
}

// -------------------- Replication: leader side --------------------

// StartCommand is the leader-facing API for proposing commands.
// Returns (index, term, isLeader). If not leader, returns isLeader=false.
func (n *Node) StartCommand(data []byte) (uint64, uint64, bool) {
	n.mu.Lock()
	if n.role != Leader {
		term := n.term
		n.mu.Unlock()
		return 0, term, false
	}
	newIdx := n.log.LastIndex() + 1
	ent := Entry{Index: newIdx, Term: n.term, Data: append([]byte(nil), data...)}
	n.log.Append(ent)

	// create waiter for this index
	ch := make(chan struct{}, 1)
	n.waiters[newIdx] = append(n.waiters[newIdx], ch)

	term := n.term
	// ensure nextIndex/matchIndex maps include peers
	for _, p := range n.peers {
		if p == n.advertise {
			continue
		}
		if _, ok := n.nextIndex[p]; !ok {
			n.nextIndex[p] = n.log.LastIndex() + 1
			n.matchIndex[p] = 0
		}
	}
	n.mu.Unlock()

	// asynchronously replicate to followers
	go n.replicateToAll(term)

	return newIdx, term, true
}

// WaitForCommit blocks until the given index is committed or timeout fires.
func (n *Node) WaitForCommit(index uint64, timeout time.Duration) bool {
	n.mu.Lock()
	if n.commitIndex >= index {
		n.mu.Unlock()
		return true
	}
	ch := make(chan struct{}, 1)
	n.waiters[index] = append(n.waiters[index], ch)
	n.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-timer.C:
		return false
	}
}

// replicateToAll tries to replicate outstanding entries to all peers (concurrent).
func (n *Node) replicateToAll(term uint64) {
	var wg sync.WaitGroup
	for _, p := range n.peers {
		if p == n.advertise {
			continue
		}
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			n.replicateToPeer(term, peerAddr)
		}(p)
	}
	wg.Wait()
}

// replicateToPeer will keep sending AppendEntries to a peer until caught up or leadership lost.
func (n *Node) replicateToPeer(term uint64, peerAddr string) {
	for {
		n.mu.Lock()
		// leadership / term guard
		if n.role != Leader || n.term != term {
			n.mu.Unlock()
			return
		}
		next := n.nextIndex[peerAddr]
		if next == 0 {
			next = 1
		}
		prevIdx := next - 1
		prevTerm := n.log.Term(prevIdx)
		entries := n.log.Slice(next)
		req := appendEntriesReq{
			Term:         n.term,
			LeaderID:     n.advertise,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}
		n.mu.Unlock()

		ok, respTerm, confIdx, confTerm := n.sendAppendEntries(peerAddr, req)

		n.mu.Lock()
		// if follower reports higher term, step down
		if respTerm > n.term {
			n.term = respTerm
			n.role = Follower
			n.votedFor = ""
			n.mu.Unlock()
			return
		}
		if ok {
			// success: update matchIndex/nextIndex
			newMatch := req.PrevLogIndex + uint64(len(entries))
			n.matchIndex[peerAddr] = newMatch
			n.nextIndex[peerAddr] = newMatch + 1
			// maybe advance commit index
			n.maybeAdvanceCommit()
			n.mu.Unlock()
			return
		}
		// failure: back off nextIndex
		if confIdx > 0 {
			n.nextIndex[peerAddr] = confIdx
		} else if n.nextIndex[peerAddr] > 1 {
			n.nextIndex[peerAddr]--
		}
		_ = confTerm
		n.mu.Unlock()
		// small sleep to avoid busy loop
		time.Sleep(10 * time.Millisecond)
	}
}

// maybeAdvanceCommit advances leader commitIndex if a majority have replicated an index
func (n *Node) maybeAdvanceCommit() {
	// caller must hold lock
	N := n.log.LastIndex()
	for idx := n.commitIndex + 1; idx <= N; idx++ {
		// only consider entries from current term to preserve Raft safety (simplification)
		if n.log.Term(idx) != n.term {
			continue
		}
		count := 1 // include leader
		for _, p := range n.peers {
			if p == n.advertise {
				continue
			}
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count*2 > len(n.peers) {
			n.commitIndex = idx
			// signal waiters & kick apply loop
			go n.signalWaitersUpTo(idx)
			go n.applyCommitted()
			// continue to attempt to advance further in same pass
		}
	}
}

// signalWaitersUpTo notifies waiters for indices <= n
func (n *Node) signalWaitersUpTo(idx uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, chans := range n.waiters {
		if i <= idx {
			for _, ch := range chans {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
			delete(n.waiters, i)
		}
	}
}

// applyCommitted sends committed, unapplied entries to applyCh
func (n *Node) applyCommitted() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		n.lastApplied++
		ent, ok := n.log.At(n.lastApplied)
		n.mu.Unlock()
		if !ok {
			return
		}
		msg := ApplyMsg{Index: ent.Index, Term: ent.Term, Data: ent.Data}
		// best-effort: don't block forever
		select {
		case n.applyCh <- msg:
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// sendAppendEntries sends AppendEntries RPC to peerAddr and decodes response
func (n *Node) sendAppendEntries(peerAddr string, req appendEntriesReq) (ok bool, term uint64, conflictIdx uint64, conflictTerm uint64) {
	b, _ := json.Marshal(req)
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	httpReq, _ := http.NewRequestWithContext(ctx, "POST", peerAddr+"/raft/append", bytes.NewReader(b))
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := n.httpClient.Do(httpReq)
	if err != nil {
		return false, n.term, 0, 0
	}
	defer resp.Body.Close()
	var ar appendEntriesResp
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return false, n.term, 0, 0
	}
	return ar.Success, ar.Term, ar.ConflictIndex, ar.ConflictTerm
}

// -------------------- Helpers --------------------

func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

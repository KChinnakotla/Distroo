package raft

import (
	"bytes"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

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
	Peers     []string
	Logger    *log.Logger
}

type Node struct {
	mu sync.Mutex

	id        string
	advertise string
	peers     []string
	log       *log.Logger

	role Role

	term     int
	votedFor string

	leaderID   string
	leaderAddr string

	// timers
	electionReset time.Time
	httpClient    *http.Client

	// stop
	stopCh chan struct{}
}

func NewNode(cfg Config) *Node {
	n := &Node{
		id:            cfg.ID,
		advertise:     cfg.Advertise,
		peers:         append([]string(nil), cfg.Peers...),
		log:           cfg.Logger,
		role:          Follower,
		term:          0,
		votedFor:      "",
		electionReset: time.Now(),
		httpClient:    &http.Client{Timeout: 800 * time.Millisecond},
		stopCh:        make(chan struct{}),
	}
	return n
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

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

func (n *Node) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/raft/leader", n.handleLeader)
	mux.HandleFunc("/raft/state", n.handleState)
	mux.HandleFunc("/raft/requestvote", n.handleRequestVote)
	mux.HandleFunc("/raft/append", n.handleAppendEntries)
}

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
	})
}

type requestVoteReq struct {
	Term        int    `json:"term"`
	CandidateID string `json:"candidate_id"`
	// For real Raft you'd include lastLogIndex/lastLogTerm
}

type requestVoteResp struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
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

	// grant if not voted this term or already voted for candidate
	if n.votedFor == "" || n.votedFor == req.CandidateID {
		n.votedFor = req.CandidateID
		resp.VoteGranted = true
		n.electionReset = time.Now()
	}
	writeJSON(w, http.StatusOK, resp)
}

type appendEntriesReq struct {
	Term     int    `json:"term"`
	LeaderID string `json:"leader_id"`
	// entries omitted for scaffold heartbeat
}

type appendEntriesResp struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

func (n *Node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req appendEntriesReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad json"})
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term >= n.term {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
		n.electionReset = time.Now()
	}
	writeJSON(w, http.StatusOK, appendEntriesResp{Term: n.term, Success: true})
}

func (n *Node) Run() {
	// election loop
	go func() {
		for {
			select {
			case <-n.stopCh:
				return
			default:
			}

			role := n.getRole()
			switch role {
			case Leader:
				n.sendHeartbeats()
				time.Sleep(100 * time.Millisecond)
			default:
				// follower/candidate
				timeout := electionTimeout()
				if time.Since(n.getElectionReset()) > timeout {
					n.startElection()
				}
				time.Sleep(25 * time.Millisecond)
			}
		}
	}()
}

func (n *Node) getRole() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) getElectionReset() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.electionReset
}

func electionTimeout() time.Duration {
	// 500-900ms
	return time.Duration(500+rand.Intn(400)) * time.Millisecond
}

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

	if n.log != nil {
		n.log.Printf("election started: term=%d", term)
	}

	votes := 1 // self vote
	var wg sync.WaitGroup
	var muVotes sync.Mutex

	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			req := requestVoteReq{Term: term, CandidateID: n.id}
			b, _ := json.Marshal(req)
			resp, err := n.httpClient.Post(p+"/raft/requestvote", "application/json", bytes.NewReader(b))
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
		}(peer)
	}
	wg.Wait()

	// Determine majority
	n.mu.Lock()
	defer n.mu.Unlock()

	// If we stepped down, abort
	if n.role != Candidate || n.term != term {
		return
	}

	total := len(peers) + 1
	if votes*2 > total {
		// become leader
		n.role = Leader
		n.leaderID = n.id
		n.leaderAddr = self
		if n.log != nil {
			n.log.Printf("became leader (term=%d) with %d/%d votes", term, votes, total)
		}
	} else {
		// stay follower (give up this round)
		n.role = Follower
		n.votedFor = ""
		n.electionReset = time.Now()
		if n.log != nil {
			n.log.Printf("election lost (votes=%d/%d), back to follower", votes, total)
		}
	}
}

func (n *Node) becomeFollowerLocked(term int, leaderID string) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
	}
	n.role = Follower
	n.leaderID = leaderID
	// if leaderID known, set leaderAddr if this is in peers/self
	if leaderID == n.id {
		n.leaderAddr = n.advertise
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	term := n.term
	peers := append([]string(nil), n.peers...)
	leaderID := n.id
	n.mu.Unlock()

	req := appendEntriesReq{Term: term, LeaderID: leaderID}
	b, _ := json.Marshal(req)

	for _, p := range peers {
		go func(url string) {
			resp, err := n.httpClient.Post(url+"/raft/append", "application/json", bytes.NewReader(b))
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var ar appendEntriesResp
			_ = json.NewDecoder(resp.Body).Decode(&ar)
			n.mu.Lock()
			if ar.Term > n.term {
				n.becomeFollowerLocked(ar.Term, "")
			}
			n.mu.Unlock()
		}(p)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

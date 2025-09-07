package server

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"kvstore/internal/raft"
)

// ----------------- Data structures -----------------

type ClientSession struct {
	LastReqID  int64
	LastResult []byte // cached result
}

type KVStore struct {
	mu       sync.Mutex
	store    map[string]string
	sessions map[string]*ClientSession
	raft     *raft.RaftNode
}

func NewKVStore(r *raft.RaftNode) *KVStore {
	return &KVStore{
		store:    make(map[string]string),
		sessions: make(map[string]*ClientSession),
		raft:     r,
	}
}

type Command struct {
	ClientID string
	ReqID    int64
	Op       string // "PUT", "DELETE", "GET"
	Key      string
	Value    string
}

type Result struct {
	Value string
	Ok    bool
}

// Deduplication-aware apply
func (kv *KVStore) Apply(cmd Command) Result {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if sess, ok := kv.sessions[cmd.ClientID]; ok {
		if cmd.ReqID <= sess.LastReqID {
			return Result{Value: string(sess.LastResult), Ok: true}
		}
	}

	var res Result
	switch cmd.Op {
	case "PUT":
		kv.store[cmd.Key] = cmd.Value
		res = Result{Ok: true}
	case "DELETE":
		delete(kv.store, cmd.Key)
		res = Result{Ok: true}
	case "GET":
		v, ok := kv.store[cmd.Key]
		res = Result{Value: v, Ok: ok}
	}

	kv.sessions[cmd.ClientID] = &ClientSession{
		LastReqID:  cmd.ReqID,
		LastResult: []byte(res.Value),
	}

	return res
}

// ----------------- HTTP Handlers -----------------

func (kv *KVStore) PutHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ClientID string `json:"clientId"`
		ReqID    int64  `json:"reqId"`
		Key      string `json:"key"`
		Value    string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad json"})
		return
	}
	if body.Key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing key"})
		return
	}

	cmd := Command{
		ClientID: body.ClientID,
		ReqID:    body.ReqID,
		Op:       "PUT",
		Key:      body.Key,
		Value:    body.Value,
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "marshal error"})
		return
	}

	idx, _, isLeader := kv.raft.StartCommand(b)
	if !isLeader {
		writeJSON(w, http.StatusTemporaryRedirect, map[string]any{"error": "not leader"})
		return
	}

	ok := kv.raft.WaitForCommit(idx, 5*time.Second)
	if !ok {
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "commit timeout"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "index": idx})
}

func (kv *KVStore) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ClientID string `json:"clientId"`
		ReqID    int64  `json:"reqId"`
		Key      string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad json"})
		return
	}
	if body.Key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing key"})
		return
	}

	cmd := Command{
		ClientID: body.ClientID,
		ReqID:    body.ReqID,
		Op:       "DELETE",
		Key:      body.Key,
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "marshal error"})
		return
	}

	idx, _, isLeader := kv.raft.StartCommand(b)
	if !isLeader {
		writeJSON(w, http.StatusTemporaryRedirect, map[string]any{"error": "not leader"})
		return
	}

	ok := kv.raft.WaitForCommit(idx, 5*time.Second)
	if !ok {
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "commit timeout"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "index": idx})
}

func (kv *KVStore) GetHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ClientID string `json:"clientId"`
		ReqID    int64  `json:"reqId"`
		Key      string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad json"})
		return
	}

	cmd := Command{
		ClientID: body.ClientID,
		ReqID:    body.ReqID,
		Op:       "GET",
		Key:      body.Key,
	}

	// GETs can be served immediately from state machine
	res := kv.Apply(cmd)
	writeJSON(w, http.StatusOK, res)
}

// ----------------- Utilities -----------------

func (kv *KVStore) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/kv/get", kv.GetHandler)
	mux.HandleFunc("/kv/put", kv.PutHandler)
	mux.HandleFunc("/kv/delete", kv.DeleteHandler)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
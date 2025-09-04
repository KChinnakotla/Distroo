package server

import (
	"encoding/json"
	"net/http"
	"time"

	"kvstore/internal/raft"
)

type KVStore struct {
	// ... existing fields ...
	// mu   sync.RWMutex
	// data map[string]string
	// raft *raft.RaftNode
}

// ----------------- handlers continued -----------------

func (kv *KVStore) PutHandler(w http.ResponseWriter, r *http.Request) {
	// Only leader should call StartCommand — StartCommand returns isLeader=false if this node isn't leader.
	var body struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad json"})
		return
	}
	if body.Key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing key"})
		return
	}

	op := KVOp{
		Op:    OpPut,
		Key:   body.Key,
		Value: body.Value,
	}
	b, err := json.Marshal(op)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "marshal error"})
		return
	}

	// Propose to Raft
	idx, _, isLeader := kv.raft.StartCommand(b)
	if !isLeader {
		// Not the leader — tell client to retry via seeds/leader discovery.
		writeJSON(w, http.StatusTemporaryRedirect, map[string]any{"error": "not leader"})
		return
	}

	// Wait until this entry is committed (bounded wait)
	ok := kv.raft.WaitForCommit(idx, 5*time.Second)
	if !ok {
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "commit timeout"})
		return
	}

	// At this point the apply loop will have applied the change to kv.data,
	// so we can reply success.
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "index": idx})
}

func (kv *KVStore) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad json"})
		return
	}
	if body.Key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing key"})
		return
	}

	op := KVOp{
		Op:  OpDel,
		Key: body.Key,
	}
	b, err := json.Marshal(op)
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

// ----------------- registration & helper -----------------

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

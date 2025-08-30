package server

import (
	"encoding/json"
	"net/http"
	"sync"

	"kvstore/internal/raft"
)

type KV struct {
	mu   sync.RWMutex
	data map[string]string

	raft *raft.Node
}

func NewKV(r *raft.Node) *KV {
	return &KV{
		data: make(map[string]string),
		raft: r,
	}
}

func (k *KV) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/kv/get", k.handleGet)
	mux.HandleFunc("/kv/put", k.handlePut)
	mux.HandleFunc("/kv/delete", k.handleDelete)
}

func (k *KV) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	k.mu.RLock()
	val, ok := k.data[key]
	k.mu.RUnlock()

	resp := map[string]any{"found": ok}
	if ok {
		resp["value"] = val
	}
	writeJSON(w, http.StatusOK, resp)
}

func (k *KV) handlePut(w http.ResponseWriter, r *http.Request) {
	// Only leader accepts writes in this scaffold.
	if !k.raft.IsLeader() {
		li := k.raft.LeaderInfo()
		writeJSON(w, http.StatusTemporaryRedirect, map[string]any{
			"error":       "not leader",
			"leader_addr": li.LeaderAddr,
			"leader_id":   li.LeaderID,
		})
		return
	}

	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad json"})
		return
	}
	k.mu.Lock()
	k.data[req.Key] = req.Value
	k.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (k *KV) handleDelete(w http.ResponseWriter, r *http.Request) {
	if !k.raft.IsLeader() {
		li := k.raft.LeaderInfo()
		writeJSON(w, http.StatusTemporaryRedirect, map[string]any{
			"error":       "not leader",
			"leader_addr": li.LeaderAddr,
			"leader_id":   li.LeaderID,
		})
		return
	}
	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad json"})
		return
	}
	k.mu.Lock()
	delete(k.data, req.Key)
	k.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

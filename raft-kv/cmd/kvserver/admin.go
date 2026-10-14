package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/siddhaantxsingh/raft-kv/kv"
	"github.com/siddhaantxsingh/raft-kv/raft"
)

// membersHandler serves membership administration (single-server changes,
// see raft/membership.go). Changes must be sent to the leader; other
// replicas answer 421 with X-Raft-Leader-ID.
//
//	GET    /admin/members              -> {"members":[...],"index":N,"leader":L}
//	POST   /admin/members  {"id":3,"addr":"http://h:7003"}
//	                                   -> adds as learner, waits for catch-up, promotes
//	DELETE /admin/members/{id}         -> removes (the leader may remove itself)
func membersHandler(s *kv.Server) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/members", func(w http.ResponseWriter, r *http.Request) {
		conf, idx := s.Raft().Configuration()
		writeJSON(w, http.StatusOK, map[string]any{"members": conf.Members, "index": idx, "leader": s.Raft().Leader()})
	})
	mux.HandleFunc("POST /admin/members", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID   int    `json:"id"`
			Addr string `json:"addr"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil || req.ID < 0 || req.Addr == "" {
			http.Error(w, "body must be {\"id\": <non-negative int>, \"addr\": \"<peer URL>\"}", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		changeResult(w, s, s.Raft().AddServer(ctx, req.ID, req.Addr))
	})
	mux.HandleFunc("DELETE /admin/members/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		changeResult(w, s, s.Raft().RemoveServer(ctx, id))
	})
	return mux
}

func changeResult(w http.ResponseWriter, s *kv.Server, err error) {
	switch {
	case err == nil, errors.Is(err, raft.ErrConfigNoop):
		conf, idx := s.Raft().Configuration()
		writeJSON(w, http.StatusOK, map[string]any{"members": conf.Members, "index": idx})
	case errors.Is(err, raft.ErrNotLeader):
		w.Header().Set("X-Raft-Leader-ID", strconv.Itoa(s.Raft().Leader()))
		if u := s.Raft().PeerAddr(s.Raft().Leader()); u != "" {
			w.Header().Set("X-Raft-Leader", u)
		}
		http.Error(w, "not leader", http.StatusMisdirectedRequest)
	case errors.Is(err, raft.ErrConfigInProgress), errors.Is(err, raft.ErrNoCommitInTerm),
		errors.Is(err, raft.ErrLeadershipLost):
		http.Error(w, err.Error(), http.StatusServiceUnavailable) // retry
	case errors.Is(err, raft.ErrNotMember):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, raft.ErrCatchUpTimeout), errors.Is(err, context.DeadlineExceeded):
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
	default:
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

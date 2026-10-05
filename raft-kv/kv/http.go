package kv

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/siddhaantxsingh/raft-kv/secure"
)

// ---------------------------------------------------------------- server side

// API exposes the KV store over a small REST interface:
//
//	GET    /kv/{key}                 -> 200 {"value": "...", "found": true}
//	PUT    /kv/{key}   body=value    -> 204
//	POST   /kv/{key}?op=append       -> 200 {"value": "<new value>"}
//	POST   /kv/{key}?op=cas          body={"expected":"..","value":".."}
//	DELETE /kv/{key}                 -> 200 {"found": bool}
//
// Requests to a follower get 421 Misdirected Request with an X-Raft-Leader
// header naming the leader's client URL. Clients pass X-Client-ID and
// X-Seq headers for exactly-once retries (Seq starts at 1 and increases per
// write); without them a request is anonymous (at-least-once, no server
// state kept). 409 Conflict means the session expired (see
// ErrSessionExpired).
type API struct {
	Server     *Server
	ClientURLs []string // index = peer id
	Timeout    time.Duration
	// MaxKeyBytes / MaxValueBytes bound request sizes (defaults 4 KiB and
	// 1 MiB). Larger requests are rejected (414 / 413), never truncated.
	MaxKeyBytes   int
	MaxValueBytes int64
}

const (
	DefaultMaxKeyBytes   = 4 << 10
	DefaultMaxValueBytes = 1 << 20
)

type apiResponse struct {
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
	OK    bool   `json:"ok"`
}

func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("/kv/{key...}", a.handleKV)
}

func (a *API) handleKV(w http.ResponseWriter, r *http.Request) {
	maxKey, maxVal := a.MaxKeyBytes, a.MaxValueBytes
	if maxKey <= 0 {
		maxKey = DefaultMaxKeyBytes
	}
	if maxVal <= 0 {
		maxVal = DefaultMaxValueBytes
	}
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if len(key) > maxKey {
		http.Error(w, fmt.Sprintf("key longer than %d bytes", maxKey), http.StatusRequestURITooLong)
		return
	}
	// A body over the limit is an error (413), never silently truncated.
	r.Body = http.MaxBytesReader(w, r.Body, maxVal+1024)
	readBody := func() (string, bool) {
		b, err := io.ReadAll(r.Body)
		if err == nil && int64(len(b)) > maxVal {
			err = &http.MaxBytesError{Limit: maxVal}
		}
		if err != nil {
			code := http.StatusBadRequest
			if errors.As(err, new(*http.MaxBytesError)) {
				code = http.StatusRequestEntityTooLarge
			}
			http.Error(w, err.Error(), code)
			return "", false
		}
		return string(b), true
	}
	op := Op{Key: key}
	switch r.Method {
	case http.MethodGet:
		op.Type = OpGet
	case http.MethodPut:
		body, ok := readBody()
		if !ok {
			return
		}
		op.Type, op.Value = OpPut, body
	case http.MethodDelete:
		op.Type = OpDelete
	case http.MethodPost:
		switch r.URL.Query().Get("op") {
		case "append":
			body, ok := readBody()
			if !ok {
				return
			}
			op.Type, op.Value = OpAppend, body
		case "cas":
			body, ok := readBody()
			if !ok {
				return
			}
			var req struct{ Expected, Value string }
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			op.Type, op.Expected, op.Value = OpCAS, req.Expected, req.Value
		default:
			http.Error(w, "unknown op", http.StatusBadRequest)
			return
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h := r.Header.Get("X-Client-ID"); h != "" {
		id, err1 := strconv.ParseInt(h, 10, 64)
		seq, err2 := strconv.ParseInt(r.Header.Get("X-Seq"), 10, 64)
		if err1 != nil || err2 != nil || id <= 0 || seq < 0 {
			http.Error(w, "X-Client-ID must be a positive integer and X-Seq a non-negative integer", http.StatusBadRequest)
			return
		}
		op.ClientID, op.Seq = id, seq
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
	defer cancel()
	res, err := a.Server.Submit(ctx, op)
	switch {
	case errors.Is(err, ErrWrongLeader):
		if l := a.Server.Raft().Leader(); l >= 0 && l < len(a.ClientURLs) {
			w.Header().Set("X-Raft-Leader", a.ClientURLs[l])
			w.Header().Set("X-Raft-Leader-ID", strconv.Itoa(l))
		}
		http.Error(w, "not leader", http.StatusMisdirectedRequest)
		return
	case errors.Is(err, ErrTimeout):
		http.Error(w, "timeout", http.StatusGatewayTimeout)
		return
	case errors.Is(err, ErrSessionExpired):
		http.Error(w, "session expired", http.StatusConflict)
		return
	case err != nil:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if op.Type == OpPut {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if op.Type == OpGet && !res.Found {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(apiResponse{Found: false})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(apiResponse{Value: res.Value, Found: res.Found, OK: res.OK})
}

// ---------------------------------------------------------------- client side

// HTTPCaller implements Caller against the REST API.
type HTTPCaller struct {
	URLs   []string
	Client *http.Client
}

func NewHTTPCaller(urls []string) *HTTPCaller {
	return NewSecureHTTPCaller(urls, nil, "")
}

// NewSecureHTTPCaller uses TLS when tlsCfg is non-nil and sends a bearer
// token when token is set.
func NewSecureHTTPCaller(urls []string, tlsCfg *tls.Config, token string) *HTTPCaller {
	return &HTTPCaller{URLs: urls, Client: &http.Client{
		Transport: &secure.TokenTransport{Token: token, Base: &http.Transport{
			TLSClientConfig: tlsCfg, MaxIdleConnsPerHost: 64,
		}},
	}}
}

func (h *HTTPCaller) NumServers() int { return len(h.URLs) }

type leaderErr struct{ hint int }

func (e leaderErr) Error() string   { return fmt.Sprintf("not leader (hint %d)", e.hint) }
func (e leaderErr) LeaderHint() int { return e.hint }

func (h *HTTPCaller) Call(ctx context.Context, server int, op Op) (Result, error) {
	u := h.URLs[server] + "/kv/" + op.Key
	var method string
	var body io.Reader
	switch op.Type {
	case OpGet:
		method = http.MethodGet
	case OpPut:
		method, body = http.MethodPut, strings.NewReader(op.Value)
	case OpAppend:
		method, body, u = http.MethodPost, strings.NewReader(op.Value), u+"?op=append"
	case OpDelete:
		method = http.MethodDelete
	case OpCAS:
		b, _ := json.Marshal(map[string]string{"expected": op.Expected, "value": op.Value})
		method, body, u = http.MethodPost, strings.NewReader(string(b)), u+"?op=cas"
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("X-Client-ID", strconv.FormatInt(op.ClientID, 10))
	req.Header.Set("X-Seq", strconv.FormatInt(op.Seq, 10))
	resp, err := h.Client.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		return Result{Value: op.Value, Found: true, OK: true}, nil
	case http.StatusOK, http.StatusNotFound:
		var ar apiResponse
		if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
			return Result{}, err
		}
		return Result{Value: ar.Value, Found: ar.Found, OK: ar.OK}, nil
	case http.StatusMisdirectedRequest:
		hint := -1
		if s := resp.Header.Get("X-Raft-Leader-ID"); s != "" {
			hint, _ = strconv.Atoi(s)
		}
		return Result{}, leaderErr{hint}
	case http.StatusConflict:
		return Result{}, ErrSessionExpired
	default:
		b, _ := io.ReadAll(resp.Body)
		return Result{}, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
}

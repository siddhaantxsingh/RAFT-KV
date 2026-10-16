package raft

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/siddhaantxsingh/raft-kv/secure"
)

// HTTPTransport sends Raft RPCs as gob-encoded HTTP POSTs. Peers are
// addressed by index into Addrs (e.g. "http://10.0.0.2:7001").
//
// Addresses of servers added by membership changes arrive through
// SetPeerAddrs (raft.AddressBook) and override Addrs.
type HTTPTransport struct {
	Addrs  []string
	Client *http.Client
	// From, if >= 0, is sent as X-Raft-From so fault-injecting proxies
	// (cmd/kvchaos) can tell which link a request travels on.
	From int

	mu    sync.RWMutex
	extra map[int]string
}

// PeerAddr returns the URL for peer id ("" if unknown).
func (t *HTTPTransport) PeerAddr(id int) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if a, ok := t.extra[id]; ok {
		return a
	}
	if id >= 0 && id < len(t.Addrs) {
		return t.Addrs[id]
	}
	return ""
}

// SetPeerAddrs records the addresses from the latest configuration.
func (t *HTTPTransport) SetPeerAddrs(addrs map[int]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.extra = make(map[int]string, len(addrs))
	for id, a := range addrs {
		t.extra[id] = a
	}
}

func NewHTTPTransport(addrs []string) *HTTPTransport {
	return NewSecureHTTPTransport(addrs, nil, "")
}

// NewSecureHTTPTransport sends RPCs over TLS when tlsCfg is non-nil (peer
// URLs must then be https://) and adds a bearer token when token is set.
func NewSecureHTTPTransport(addrs []string, tlsCfg *tls.Config, token string) *HTTPTransport {
	return &HTTPTransport{
		From:  -1,
		Addrs: addrs,
		Client: &http.Client{
			Transport: &secure.TokenTransport{Token: token, Base: &http.Transport{
				TLSClientConfig:     tlsCfg,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     90 * time.Second,
			}},
		},
	}
}

func (t *HTTPTransport) call(ctx context.Context, peer int, path string, args, reply any) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(args); err != nil {
		return err
	}
	addr := t.PeerAddr(peer)
	if addr == "" {
		return fmt.Errorf("raft rpc %s: no address for peer %d", path, peer)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+path, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-gob")
	if t.From >= 0 {
		req.Header.Set("X-Raft-From", strconv.Itoa(t.From))
	}
	resp, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("raft rpc %s to %d: status %d", path, peer, resp.StatusCode)
	}
	return gob.NewDecoder(resp.Body).Decode(reply)
}

func (t *HTTPTransport) RequestVote(ctx context.Context, peer int, a *RequestVoteArgs) (*RequestVoteReply, error) {
	var r RequestVoteReply
	return &r, t.call(ctx, peer, "/raft/vote", a, &r)
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, peer int, a *AppendEntriesArgs) (*AppendEntriesReply, error) {
	var r AppendEntriesReply
	return &r, t.call(ctx, peer, "/raft/append", a, &r)
}

func (t *HTTPTransport) InstallSnapshot(ctx context.Context, peer int, a *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	var r InstallSnapshotReply
	return &r, t.call(ctx, peer, "/raft/snapshot", a, &r)
}

// HandlerOptions secure the Raft RPC endpoints.
type HandlerOptions struct {
	// Token, if set, must be presented as "Authorization: Bearer <token>".
	Token string
	// MaxBodyBytes caps an RPC body (default 64 MiB). It must exceed the
	// largest AppendEntries batch and Config.SnapshotChunkSize.
	MaxBodyBytes int64
}

// DefaultMaxRPCBody is the default RPC body cap.
const DefaultMaxRPCBody = 64 << 20

// RegisterHandlers mounts the Raft RPC endpoints on mux with default
// options (no authentication). getRaft is a function so the server can be
// wired before Raft is constructed.
func RegisterHandlers(mux *http.ServeMux, getRaft func() *Raft) {
	RegisterHandlersWithOptions(mux, getRaft, HandlerOptions{})
}

// RegisterHandlersWithOptions mounts the Raft RPC endpoints with
// authentication and body limits.
func RegisterHandlersWithOptions(mux *http.ServeMux, getRaft func() *Raft, o HandlerOptions) {
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = DefaultMaxRPCBody
	}
	handle := func(path string, fn func(rf *Raft, dec *gob.Decoder) (any, error)) {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rf := getRaft()
			if rf == nil || rf.killed() {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
			reply, err := fn(rf, gob.NewDecoder(r.Body))
			if err != nil {
				if secure.IsTooLarge(err) {
					http.Error(w, "rpc body too large", http.StatusRequestEntityTooLarge)
					return
				}
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/x-gob")
			gob.NewEncoder(w).Encode(reply)
		})
		mux.Handle("POST "+path, secure.RequireToken(o.Token, secure.LimitBody(o.MaxBodyBytes, h)))
	}
	handle("/raft/vote", func(rf *Raft, d *gob.Decoder) (any, error) {
		var a RequestVoteArgs
		if err := d.Decode(&a); err != nil {
			return nil, err
		}
		return rf.HandleRequestVote(&a), nil
	})
	handle("/raft/append", func(rf *Raft, d *gob.Decoder) (any, error) {
		var a AppendEntriesArgs
		if err := d.Decode(&a); err != nil {
			return nil, err
		}
		return rf.HandleAppendEntries(&a), nil
	})
	handle("/raft/snapshot", func(rf *Raft, d *gob.Decoder) (any, error) {
		var a InstallSnapshotArgs
		if err := d.Decode(&a); err != nil {
			return nil, err
		}
		return rf.HandleInstallSnapshot(&a), nil
	})
}

package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Network is an in-memory transport fabric for tests. It can disconnect
// nodes (partitions), drop messages, and add random delay/reordering, which
// is how the test suite exercises Raft under unreliable conditions.
type Network struct {
	mu         sync.Mutex
	nodes      map[int]*Raft
	connected  map[int]bool
	unreliable bool
	longDelays bool
	rpcCount   int
}

var ErrUnreachable = errors.New("raft: peer unreachable")

func NewNetwork() *Network {
	return &Network{nodes: map[int]*Raft{}, connected: map[int]bool{}}
}

func (n *Network) Register(id int, rf *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = rf
}

func (n *Network) SetConnected(id int, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = on
}

func (n *Network) SetUnreliable(on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.unreliable = on
}

func (n *Network) RPCCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rpcCount
}

// Endpoint returns a Transport bound to the sending node id.
func (n *Network) Endpoint(from int) Transport { return &endpoint{net: n, from: from} }

type endpoint struct {
	net  *Network
	from int
}

func (n *Network) route(ctx context.Context, from, to int) (*Raft, error) {
	n.mu.Lock()
	n.rpcCount++
	unreliable := n.unreliable
	ok := n.connected[from] && n.connected[to]
	target := n.nodes[to]
	n.mu.Unlock()

	if !ok || target == nil || target.killed() {
		// Simulate a timeout rather than an instant failure.
		select {
		case <-time.After(time.Duration(rand.Intn(50)) * time.Millisecond):
		case <-ctx.Done():
		}
		return nil, ErrUnreachable
	}
	if unreliable {
		time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
		if rand.Intn(10) == 0 {
			return nil, ErrUnreachable // drop request
		}
	}
	return target, nil
}

// deliver checks after the handler ran whether the reply can make it back.
func (n *Network) replyOK(from, to int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !(n.connected[from] && n.connected[to]) {
		return false
	}
	if n.unreliable && rand.Intn(10) == 0 {
		return false // drop reply
	}
	return true
}

func (e *endpoint) RequestVote(ctx context.Context, peer int, args *RequestVoteArgs) (*RequestVoteReply, error) {
	rf, err := e.net.route(ctx, e.from, peer)
	if err != nil {
		return nil, err
	}
	r := rf.HandleRequestVote(args)
	if !e.net.replyOK(e.from, peer) {
		return nil, ErrUnreachable
	}
	return r, nil
}

func (e *endpoint) AppendEntries(ctx context.Context, peer int, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	rf, err := e.net.route(ctx, e.from, peer)
	if err != nil {
		return nil, err
	}
	// Deep-copy entries so the receiver never aliases the leader's slice.
	cp := *args
	cp.Entries = append([]LogEntry(nil), args.Entries...)
	r := rf.HandleAppendEntries(&cp)
	if !e.net.replyOK(e.from, peer) {
		return nil, ErrUnreachable
	}
	return r, nil
}

func (e *endpoint) InstallSnapshot(ctx context.Context, peer int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	rf, err := e.net.route(ctx, e.from, peer)
	if err != nil {
		return nil, err
	}
	cp := *args
	cp.Data = clone(args.Data)
	r := rf.HandleInstallSnapshot(&cp)
	if !e.net.replyOK(e.from, peer) {
		return nil, ErrUnreachable
	}
	return r, nil
}

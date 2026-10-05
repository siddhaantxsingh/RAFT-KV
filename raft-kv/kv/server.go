// Package kv is a linearizable key/value state machine replicated with Raft.
//
// Writes go through the Raft log, so all replicas apply the same sequence
// of operations. Clients tag writes with a session (ClientID, Seq): Seq
// starts at 1 and increases by one per write. The server remembers the last
// Seq applied per session so a retried write is applied at most once.
//
// Session table bounds (all deterministic, so replicas stay identical):
//   - ClientID 0 is anonymous: no dedup entry is kept (at-least-once).
//   - At most MaxSessions sessions are kept; when exceeded, the least
//     recently used tenth (by Raft index of their last write) is evicted.
//   - A write from an unknown session with Seq > 1 is rejected with
//     ErrSessionExpired instead of being applied, since it may be a retry
//     of a write applied before the session was evicted. (A retry of a
//     session's *first* write after eviction cannot be detected; eviction
//     needs MaxSessions newer sessions in between.)
package kv

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/siddhaantxsingh/raft-kv/raft"
)

type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpAppend
	OpDelete
	OpCAS // compare-and-swap: set Value if current == Expected
)

type Op struct {
	Type     OpType
	Key      string
	Value    string
	Expected string
	ClientID int64
	Seq      int64
}

type Result struct {
	Value string
	Found bool
	OK    bool // for CAS: whether the swap happened
	// SessionExpired: rejected, see ErrSessionExpired. Deterministic, so
	// every replica computes the same result.
	SessionExpired bool
}

var (
	ErrWrongLeader = errors.New("kv: not leader")
	ErrTimeout     = errors.New("kv: timeout")
	ErrShutdown    = errors.New("kv: shutting down")
	// ErrSessionExpired: the client's session was evicted from the dedup
	// table; the write was NOT applied by this request, but an earlier
	// attempt may have been. The client must start a new session.
	ErrSessionExpired = errors.New("kv: session expired")
)

// DefaultMaxSessions bounds the dedup table. It is part of the replicated
// state machine's behaviour: every replica must use the same value.
const DefaultMaxSessions = 100_000

// ServerOption configures a Server.
type ServerOption func(*Server)

// WithStore sets the state store (default: NewMemStore()). The server
// owns it and closes it on Kill.
func WithStore(st StateStore) ServerOption {
	return func(s *Server) { s.store = st }
}

// WithMaxSessions sets the dedup table bound (must match on all replicas).
func WithMaxSessions(n int) ServerOption {
	return func(s *Server) {
		if n > 0 {
			s.maxSessions = n
		}
	}
}

// SessionEntry is a client session in the dedup table: the last applied
// write's Seq and Result, and the Raft index of that write (LRU order).
type SessionEntry struct {
	Seq    int64
	Result Result
	Index  int // Raft index of the session's last write (LRU order)
}

type waiter struct {
	term int
	ch   chan Result
}

type Server struct {
	mu           sync.Mutex
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	storage      raft.Storage
	maxRaftState int // snapshot when persisted Raft state exceeds this (bytes); -1 disables

	store       StateStore
	lastApplied map[int64]SessionEntry // session table (mirrors the store)
	maxSessions int
	appliedIdx  int
	waiters     map[int]waiter

	stats      Stats
	done       chan struct{}
	appliedSig chan struct{} // closed and replaced whenever appliedIdx advances

	// LogReads forces Gets through the Raft log instead of ReadIndex
	// (useful for comparing the two in benchmarks).
	LogReads bool

	// unsafeLocalReads serves Gets from local state with no leadership
	// check. It is WRONG and exists only so tests can prove the
	// linearizability checker catches stale reads.
	unsafeLocalReads bool
}

type Stats struct {
	Applied   int64
	Snapshots int64
	Restored  int64
	FastReads int64 // Gets served via ReadIndex
}

// NewServer starts a KV replica. transport and storage are passed through
// to Raft; the server restores state from Raft's latest snapshot.
func NewServer(me, peers int, transport raft.Transport, storage raft.Storage, maxRaftState int, cfg raft.Config, opts ...ServerOption) *Server {
	gob.Register(Op{})
	s := &Server{
		applyCh:      make(chan raft.ApplyMsg, 256),
		storage:      storage,
		maxRaftState: maxRaftState,
		lastApplied:  map[int64]SessionEntry{},
		waiters:      map[int]waiter{},
		done:         make(chan struct{}),
		appliedSig:   make(chan struct{}),
		maxSessions:  DefaultMaxSessions,
	}
	for _, o := range opts {
		o(s)
	}
	if s.store == nil {
		s.store = NewMemStore()
	}
	// raft.New loads storage (including the snapshot). Applied entries queue
	// in applyCh until applyLoop starts, so restoring here is race-free.
	s.rf = raft.New(me, peers, transport, storage, s.applyCh, cfg)
	s.recover(storage)
	go s.applyLoop()
	return s
}

func (s *Server) Raft() *raft.Raft { return s.rf }

func (s *Server) Kill() {
	s.rf.Kill()
	close(s.done)
	s.mu.Lock()
	s.store.Close()
	s.mu.Unlock()
}

// recover brings the state machine to the newer of (a) what the store
// recovered on its own and (b) Raft's latest snapshot. Raft then re-applies
// every later committed entry; handleApply skips those <= appliedIdx.
func (s *Server) recover(storage raft.Storage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	applied, err := s.store.AppliedIndex()
	if err != nil {
		panic("kv: state store: " + err.Error())
	}
	snapIndex, _, snap := storage.Snapshot()
	if len(snap) > 0 && snapIndex > applied {
		s.restoreLocked(snap)
		return
	}
	sessions, err := s.store.LoadSessions()
	if err != nil {
		panic("kv: state store: " + err.Error())
	}
	s.lastApplied, s.appliedIdx = sessions, applied
}

// get reads the state store; a read error means the store is unusable.
func (s *Server) get(key string) (string, bool) {
	v, ok, err := s.store.Get(key)
	if err != nil {
		panic("kv: state store read: " + err.Error())
	}
	return v, ok
}

func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func encodeOp(op Op) []byte {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(op); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func decodeOp(b []byte) (Op, error) {
	var op Op
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&op)
	return op, err
}

// Submit proposes op and blocks until it is applied, leadership is lost,
// or ctx expires.
func (s *Server) Submit(ctx context.Context, op Op) (Result, error) {
	if op.Type == OpGet && s.unsafeLocalReads {
		s.mu.Lock()
		v, ok := s.get(op.Key)
		s.mu.Unlock()
		return Result{Value: v, Found: ok, OK: true}, nil
	}
	if op.Type == OpGet && !s.LogReads {
		res, err := s.fastRead(ctx, op.Key)
		if err != raft.ErrNoCommitInTerm {
			return res, err
		}
		// New leader hasn't committed its no-op yet: fall back to the log.
	}
	// Fast path: duplicate of an already-applied write.
	s.mu.Lock()
	if e, ok := s.lastApplied[op.ClientID]; ok && op.ClientID != 0 && op.Type != OpGet && e.Seq >= op.Seq {
		s.mu.Unlock()
		if e.Seq == op.Seq {
			return e.Result, nil
		}
		return Result{}, nil
	}
	s.mu.Unlock()

	// Hold s.mu across Start so the entry cannot be applied before its
	// waiter is registered (otherwise the result is lost and the client
	// waits for a timeout). Lock order is always s.mu -> rf.mu; Raft never
	// acquires s.mu, so this cannot deadlock.
	ch := make(chan Result, 1)
	s.mu.Lock()
	index, term, isLeader := s.rf.Start(encodeOp(op))
	if !isLeader {
		s.mu.Unlock()
		return Result{}, ErrWrongLeader
	}
	s.waiters[index] = waiter{term: term, ch: ch}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if w, ok := s.waiters[index]; ok && w.ch == ch {
			delete(s.waiters, index)
		}
		s.mu.Unlock()
	}()

	// Periodically confirm we're still leader in the same term; otherwise a
	// different entry may land at our index and we'd wait forever.
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				return Result{}, ErrWrongLeader
			}
			if r.SessionExpired {
				return Result{}, ErrSessionExpired
			}
			return r, nil
		case <-tick.C:
			if t, lead := s.rf.GetState(); !lead || t != term {
				return Result{}, ErrWrongLeader
			}
		case <-ctx.Done():
			return Result{}, ErrTimeout
		case <-s.done:
			return Result{}, ErrShutdown
		}
	}
}

func (s *Server) applyLoop() {
	for {
		select {
		case <-s.done:
			return
		case m := <-s.applyCh:
			s.handleApply(m)
		}
	}
}

func (s *Server) handleApply(m raft.ApplyMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if m.SnapshotValid {
		if m.SnapshotIndex > s.appliedIdx {
			s.restoreLocked(m.Snapshot)
			s.appliedIdx = m.SnapshotIndex
			s.notifyApplied()
			s.stats.Restored++
			// Any waiters at or below the snapshot index lost their slot.
			for idx, w := range s.waiters {
				if idx <= m.SnapshotIndex {
					close(w.ch)
					delete(s.waiters, idx)
				}
			}
		}
		return
	}
	if !m.CommandValid || m.CommandIndex <= s.appliedIdx {
		return
	}
	s.appliedIdx = m.CommandIndex
	s.notifyApplied()

	var res Result
	if m.Command != nil { // nil = leader's no-op
		op, err := decodeOp(m.Command)
		if err != nil {
			panic("kv: corrupt log entry: " + err.Error())
		}
		res = s.applyOp(op, m.CommandIndex)
	}
	s.stats.Applied++

	if w, ok := s.waiters[m.CommandIndex]; ok {
		delete(s.waiters, m.CommandIndex)
		if w.term == m.CommandTerm {
			w.ch <- res
		} else {
			close(w.ch) // a different leader's entry took this index
		}
	}

	if s.maxRaftState > 0 && s.storage.StateSize() >= s.maxRaftState {
		snap := s.snapshotLocked()
		s.stats.Snapshots++
		// Safe to call while holding s.mu: Raft never blocks on our lock
		// while holding its own (its applier drops rf.mu before sending).
		s.rf.Snapshot(m.CommandIndex, snap)
	}
}

func (s *Server) applyOp(op Op, index int) Result {
	if op.Type == OpGet {
		v, ok := s.get(op.Key)
		return Result{Value: v, Found: ok, OK: true}
	}
	if op.ClientID != 0 {
		e, known := s.lastApplied[op.ClientID]
		if known && e.Seq >= op.Seq {
			return e.Result // duplicate: don't apply twice
		}
		if !known && op.Seq > 1 {
			return Result{SessionExpired: true}
		}
	}
	var res Result
	var writes []Write
	cur, existed := s.get(op.Key)
	switch op.Type {
	case OpPut:
		writes = []Write{{Key: op.Key, Value: op.Value, Existed: existed}}
		res = Result{Value: op.Value, Found: true, OK: true}
	case OpAppend:
		writes = []Write{{Key: op.Key, Value: cur + op.Value, Existed: existed}}
		res = Result{Value: cur + op.Value, Found: true, OK: true}
	case OpDelete:
		if existed {
			writes = []Write{{Key: op.Key, Delete: true, Existed: true}}
		}
		res = Result{Found: existed, OK: true}
	case OpCAS:
		if (existed && cur == op.Expected) || (!existed && op.Expected == "") {
			writes = []Write{{Key: op.Key, Value: op.Value, Existed: existed}}
			res = Result{Value: op.Value, Found: true, OK: true}
		} else {
			res = Result{Value: cur, Found: existed, OK: false}
		}
	}
	var sessions []SessionUpdate
	if op.ClientID != 0 {
		e := SessionEntry{Seq: op.Seq, Result: res, Index: index}
		s.lastApplied[op.ClientID] = e
		sessions = append(sessions, SessionUpdate{ClientID: op.ClientID, Entry: &e})
		sessions = append(sessions, s.evictSessionsLocked()...)
	}
	// Data, sessions and the applied index change atomically.
	if err := s.store.Apply(index, writes, sessions); err != nil {
		panic("kv: state store apply: " + err.Error())
	}
	return res
}

// evictSessionsLocked drops the least recently written tenth of sessions
// once the table exceeds maxSessions. Order is (Index, ClientID), which
// every replica computes identically.
func (s *Server) evictSessionsLocked() []SessionUpdate {
	if len(s.lastApplied) <= s.maxSessions {
		return nil
	}
	type sess struct {
		id    int64
		index int
	}
	all := make([]sess, 0, len(s.lastApplied))
	for id, e := range s.lastApplied {
		all = append(all, sess{id, e.Index})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].index != all[j].index {
			return all[i].index < all[j].index
		}
		return all[i].id < all[j].id
	})
	n := len(all) - s.maxSessions*9/10
	ev := make([]SessionUpdate, 0, n)
	for _, x := range all[:n] {
		delete(s.lastApplied, x.id)
		ev = append(ev, SessionUpdate{ClientID: x.id})
	}
	return ev
}

// Sessions returns the number of sessions in the dedup table.
func (s *Server) Sessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.lastApplied)
}

type snapshot struct {
	Data        map[string]string
	LastApplied map[int64]SessionEntry
	AppliedIdx  int
}

// snapshotLocked serialises the full state (same format for every store,
// so replicas with different stores can exchange snapshots).
func (s *Server) snapshotLocked() []byte {
	data, err := s.store.Dump()
	if err != nil {
		panic("kv: state store dump: " + err.Error())
	}
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(snapshot{data, s.lastApplied, s.appliedIdx}); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func (s *Server) restoreLocked(b []byte) {
	if len(b) == 0 {
		return
	}
	var snap snapshot
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&snap); err != nil {
		panic("kv: corrupt snapshot: " + err.Error())
	}
	if snap.LastApplied == nil {
		snap.LastApplied = map[int64]SessionEntry{}
	}
	if err := s.store.Restore(snap.Data, snap.LastApplied, snap.AppliedIdx); err != nil {
		panic("kv: state store restore: " + err.Error())
	}
	s.lastApplied, s.appliedIdx = snap.LastApplied, snap.AppliedIdx
}

// Len returns the number of keys (for metrics).
func (s *Server) Len() int {
	return s.store.Len()
}

func (s *Server) notifyApplied() {
	close(s.appliedSig)
	s.appliedSig = make(chan struct{})
}

// fastRead serves a linearizable Get using Raft's ReadIndex: confirm
// leadership with a quorum, wait until the state machine has applied at
// least the read index, then read locally. No log entry, no fsync.
func (s *Server) fastRead(ctx context.Context, key string) (Result, error) {
	idx, err := s.rf.ReadIndex(ctx)
	switch {
	case err == raft.ErrNoCommitInTerm:
		return Result{}, err
	case err != nil:
		return Result{}, ErrWrongLeader
	}
	s.mu.Lock()
	for s.appliedIdx < idx {
		sig := s.appliedSig
		s.mu.Unlock()
		select {
		case <-sig:
		case <-ctx.Done():
			return Result{}, ErrTimeout
		case <-s.done:
			return Result{}, ErrShutdown
		}
		s.mu.Lock()
	}
	v, ok := s.get(key)
	s.stats.FastReads++
	s.mu.Unlock()
	return Result{Value: v, Found: ok, OK: true}, nil
}

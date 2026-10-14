// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", extended version),
// including leader election with pre-vote, log replication with fast
// conflict backup, persistence, and log compaction via snapshots.
package raft

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds timing parameters. Election timeouts are randomised in
// [ElectionTimeoutMin, ElectionTimeoutMax) to make split votes unlikely.
type Config struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RPCTimeout         time.Duration
	MaxEntriesPerRPC   int
	// SnapshotChunkSize is the payload size of one InstallSnapshot RPC.
	SnapshotChunkSize int
	// MaxSnapshotSize bounds the snapshot a follower will buffer.
	MaxSnapshotSize int64
	// Join starts a server that is not part of the bootstrap
	// configuration: it has no voters until the leader sends it the log
	// (or a snapshot) containing a configuration that includes it. Use it
	// for servers added with AddServer. Ignored once the log or snapshot
	// holds a configuration.
	Join   bool
	Logger *log.Logger
}

func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
		HeartbeatInterval:  100 * time.Millisecond,
		RPCTimeout:         150 * time.Millisecond,
		MaxEntriesPerRPC:   512,
		SnapshotChunkSize:  1 << 20,
		MaxSnapshotSize:    4 << 30,
	}
}

// Transport sends RPCs to peers. Implementations: in-memory network for
// tests (network.go) and HTTP for real deployments (http_transport.go).
type Transport interface {
	RequestVote(ctx context.Context, peer int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peer int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, peer int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

type Raft struct {
	mu        sync.Mutex
	me        int
	transport Transport
	storage   Storage
	applyCh   chan<- ApplyMsg
	cfg       Config
	dead      int32

	// Persistent state (Figure 2). log[0] is a sentinel holding the
	// index/term of the last entry covered by the snapshot.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state.
	role        Role
	commitIndex int
	lastApplied int
	leaderID    int

	// Membership (membership.go). conf is the latest configuration in the
	// log (committed or not), set by the entry at confIndex; snapConf is
	// the configuration covered by the snapshot (log[0]).
	conf      Configuration
	confIndex int
	snapConf  Configuration

	// Leader-only volatile state, keyed by server ID. matchIndex[me] is
	// our own durable index.
	nextIndex  map[int]int
	matchIndex map[int]int
	peerState  map[int]*peer // one replicator per other member

	electionDeadline time.Time
	lastHeartbeat    time.Time // last time we heard from a valid leader
	applyCond        *sync.Cond

	// A snapshot waiting to be handed to the service by the applier.
	pendingSnapshot *ApplyMsg

	// Group commit: the leader appends without fsync and a syncer goroutine
	// fsyncs in the background; durableIndex is the highest index known to
	// be on our own disk, and is what counts toward our matchIndex.
	syncCond     *sync.Cond
	syncPending  bool
	durableIndex int

	readQueue []chan readResult
	readCond  *sync.Cond

	// Leader: per-peer resume offset for the snapshot being sent, keyed by
	// the snapshot index it belongs to.
	snapSendIndex  map[int]int
	snapSendOffset map[int]int64
	snapCRCIndex   int // snapshot index snapCRC was computed for
	snapCRC        uint32

	// Follower: the snapshot being received in chunks.
	snapRecv *snapshotRecv

	stats Metrics
}

// peer is the replicator state for one other member.
type peer struct {
	cond    *sync.Cond
	stopped bool // removed from the configuration; the replicator exits
}

func newCond(mu *sync.Mutex) *sync.Cond { return sync.NewCond(mu) }

type snapshotRecv struct {
	term, index, lastTerm int
	total                 int64
	checksum              uint32
	conf                  *Configuration
	buf                   []byte
}

// Metrics are monotonically increasing counters (read with Metrics()).
type Metrics struct {
	SnapshotsSent         int64 // complete snapshots sent to followers
	SnapshotChunksSent    int64
	SnapshotsInstalled    int64 // snapshots received and installed
	SnapshotChecksumFails int64
	ElectionsStarted      int64
	LeaderTerms           int64 // times this node became leader
}

// New creates a Raft peer. peers is the size of the bootstrap
// configuration (voters 0..peers-1); it is used only until the log or
// snapshot holds a configuration entry, and ignored with cfg.Join. New
// restores persisted state, then starts the ticker, applier and per-peer
// replicator goroutines.
func New(me, peers int, transport Transport, storage Storage, applyCh chan<- ApplyMsg, cfg Config) *Raft {
	rf := &Raft{
		me:        me,
		transport: transport,
		storage:   storage,
		applyCh:   applyCh,
		cfg:       cfg,
		votedFor:  -1,
		leaderID:  -1,
		log:       []LogEntry{{Term: 0, Index: 0}},
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.syncCond = sync.NewCond(&rf.mu)
	rf.readCond = sync.NewCond(&rf.mu)
	rf.nextIndex = map[int]int{}
	rf.matchIndex = map[int]int{}
	rf.peerState = map[int]*peer{}
	rf.snapSendIndex = map[int]int{}
	rf.snapSendOffset = map[int]int64{}
	if rf.cfg.SnapshotChunkSize <= 0 {
		rf.cfg.SnapshotChunkSize = 1 << 20
	}
	if rf.cfg.MaxSnapshotSize <= 0 {
		rf.cfg.MaxSnapshotSize = 4 << 30
	}

	snapConf := rf.load()
	switch {
	case snapConf != nil:
		rf.snapConf = *snapConf
	case !cfg.Join:
		rf.snapConf = rf.bootstrapConfig(peers)
	}
	rf.commitIndex = rf.firstIndex()
	rf.lastApplied = rf.firstIndex()
	rf.resetElectionTimer()

	rf.mu.Lock()
	rf.recomputeConfig() // starts a replicator per member
	rf.mu.Unlock()
	go rf.ticker()
	go rf.applier()
	go rf.syncer()
	go rf.readLoop()
	return rf
}

func (rf *Raft) logf(format string, args ...any) {
	if rf.cfg.Logger != nil {
		rf.cfg.Logger.Printf("[peer %d] "+format, append([]any{rf.me}, args...)...)
	}
}

// ---------------------------------------------------------------- log helpers

func (rf *Raft) firstIndex() int { return rf.log[0].Index }
func (rf *Raft) lastIndex() int  { return rf.log[len(rf.log)-1].Index }
func (rf *Raft) lastTerm() int   { return rf.log[len(rf.log)-1].Term }

func (rf *Raft) entry(index int) LogEntry { return rf.log[index-rf.firstIndex()] }

// ---------------------------------------------------------------- persistence

// load restores persisted state and returns the snapshot's configuration
// (nil if there is no snapshot or it predates membership changes).
func (rf *Raft) load() *Configuration {
	hs, snapIndex, snapTerm, entries, err := rf.storage.Load()
	if err != nil {
		panic("raft: load: " + err.Error())
	}
	rf.currentTerm, rf.votedFor = hs.Term, hs.VotedFor
	rf.log = append([]LogEntry{{Term: snapTerm, Index: snapIndex}}, entries...)
	rf.durableIndex = rf.lastIndex()
	_, _, _, conf := rf.storage.SnapshotWithConfig()
	return conf
}

// must panics on storage errors: continuing after a failed write could
// violate safety (e.g. acknowledging an entry that isn't durable).
func must(err error) {
	if err != nil {
		panic("raft: storage: " + err.Error())
	}
}

// saveHardState must be called with rf.mu held whenever currentTerm or
// votedFor change, before replying to any RPC.
func (rf *Raft) saveHardState() {
	must(rf.storage.SetHardState(HardState{Term: rf.currentTerm, VotedFor: rf.votedFor}))
}

// leaderAppend appends to our own log without waiting for fsync and wakes
// the syncer; replication to followers starts immediately in parallel.
func (rf *Raft) leaderAppend(e LogEntry) {
	rf.log = append(rf.log, e)
	must(rf.storage.Append([]LogEntry{e}, false))
	rf.syncPending = true
	rf.syncCond.Signal()
	rf.broadcastAppend()
}

// syncer fsyncs batched leader appends. Many Start() calls between two
// fsyncs share one disk flush (group commit).
func (rf *Raft) syncer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if !rf.syncPending {
			rf.syncCond.Wait()
			continue
		}
		rf.syncPending = false
		target, term := rf.lastIndex(), rf.currentTerm
		rf.mu.Unlock()
		err := rf.storage.Sync()
		rf.mu.Lock()
		must(err)
		if target > rf.durableIndex {
			rf.durableIndex = target
		}
		if rf.role == Leader && rf.currentTerm == term {
			rf.matchIndex[rf.me] = min(rf.durableIndex, rf.lastIndex())
			rf.advanceCommit()
		}
	}
}

// ---------------------------------------------------------------- public API

// GetState returns the current term and whether this peer believes it is leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Leader returns the id of the last known leader, or -1.
func (rf *Raft) Leader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderID
}

// Status is a read-only view for metrics/debug endpoints.
type Status struct {
	ID, Term, Leader, CommitIndex, LastApplied, FirstIndex, LastIndex int
	Role                                                              string
	// SinceLeaderContact is the time since a follower last heard from a
	// valid leader (0 on the leader; -1 if it never has).
	SinceLeaderContact time.Duration
}

func (rf *Raft) Status() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	contact := time.Duration(-1)
	switch {
	case rf.role == Leader:
		contact = 0
	case !rf.lastHeartbeat.IsZero():
		contact = time.Since(rf.lastHeartbeat)
	}
	return Status{rf.me, rf.currentTerm, rf.leaderID, rf.commitIndex, rf.lastApplied,
		rf.firstIndex(), rf.lastIndex(), rf.role.String(), contact}
}

// Start proposes a command. It returns immediately; there is no guarantee
// the command will commit (the leader may lose leadership). Callers watch
// applyCh for the returned (index, term).
func (rf *Raft) Start(command []byte) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}
	e := LogEntry{Term: rf.currentTerm, Index: rf.lastIndex() + 1, Command: command}
	rf.leaderAppend(e)
	return e.Index, e.Term, true
}

// Snapshot tells Raft the service has a snapshot covering all entries up to
// and including index; Raft discards those log entries.
//
// The snapshot file is written without rf.mu held (it can be large), so
// heartbeats, votes and replication continue meanwhile. The stored log is
// only compacted after the snapshot is durable.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	// The service only snapshots entries it has applied, which are committed.
	// (rf.lastApplied lags while the applier is mid-batch, so check commitIndex.)
	if index <= rf.firstIndex() || index > rf.commitIndex {
		rf.mu.Unlock()
		return
	}
	term := rf.entry(index).Term
	conf := rf.configAt(index)
	rf.mu.Unlock()

	must(rf.storage.SaveSnapshot(index, term, snapshot, &conf))

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.firstIndex() {
		return // superseded meanwhile (e.g. a newer snapshot was installed)
	}
	// Committed entries are never truncated, so index is still in the log
	// with the same term.
	rf.snapConf = conf
	rf.trimLogTo(index, term)
	must(rf.storage.CompactLog(index, rf.log[1:]))
	rf.logf("snapshot through index %d", index)
}

// trimLogTo drops entries <= index, keeping index as the sentinel. Must
// hold rf.mu; index must be in the log.
func (rf *Raft) trimLogTo(index, term int) {
	newLog := make([]LogEntry, 0, rf.lastIndex()-index+1)
	newLog = append(newLog, LogEntry{Term: term, Index: index})
	newLog = append(newLog, rf.log[index-rf.firstIndex()+1:]...)
	rf.log = newLog
}

// Metrics returns a copy of this node's counters.
func (rf *Raft) Metrics() Metrics {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.stats
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.syncCond.Broadcast()
	rf.readCond.Broadcast()
	for _, p := range rf.peerState {
		p.cond.Broadcast()
	}
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

// ---------------------------------------------------------------- state transitions

func (rf *Raft) resetElectionTimer() {
	span := rf.cfg.ElectionTimeoutMax - rf.cfg.ElectionTimeoutMin
	rf.electionDeadline = time.Now().Add(rf.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(span))))
}

// stepDown must hold rf.mu. Moves to a higher term as follower.
func (rf *Raft) stepDown(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	if rf.role != Follower {
		rf.logf("step down to follower in term %d", rf.currentTerm)
	}
	rf.role = Follower
	rf.saveHardState()
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	rf.stats.LeaderTerms++
	rf.leaderID = rf.me
	for id := range rf.peerState {
		rf.nextIndex[id] = rf.lastIndex() + 1
		rf.matchIndex[id] = 0
	}
	rf.matchIndex[rf.me] = rf.durableIndex
	rf.logf("became leader for term %d (log %d..%d)", rf.currentTerm, rf.firstIndex(), rf.lastIndex())
	// A leader may only commit entries from its own term by counting
	// replicas (§5.4.2). Appending a no-op lets earlier entries commit
	// promptly instead of waiting for the next client write.
	rf.leaderAppend(LogEntry{Term: rf.currentTerm, Index: rf.lastIndex() + 1, Command: nil})
}

// ---------------------------------------------------------------- election

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}
		if time.Now().After(rf.electionDeadline) {
			rf.resetElectionTimer()
			// Learners and removed servers never campaign.
			if rf.conf.IsVoter(rf.me) {
				go rf.runElection()
			}
		}
		rf.mu.Unlock()
	}
}

// heartbeatLoop wakes every replicator periodically while we are leader.
func (rf *Raft) heartbeatLoop(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.broadcastAppend()
		rf.mu.Unlock()
		time.Sleep(rf.cfg.HeartbeatInterval)
	}
}

// runElection performs a pre-vote round and, if it would win, a real
// election. Pre-vote (Raft thesis §9.6) stops a partitioned node from
// inflating its term and disrupting a healthy cluster when it rejoins.
func (rf *Raft) runElection() {
	if !rf.collectVotes(true) {
		return
	}
	rf.mu.Lock()
	if rf.role == Leader || !rf.conf.IsVoter(rf.me) {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.stats.ElectionsStarted++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.leaderID = -1
	rf.saveHardState()
	rf.resetElectionTimer()
	rf.logf("starting election for term %d", rf.currentTerm)
	rf.mu.Unlock()
	rf.collectVotes(false)
}

func (rf *Raft) collectVotes(preVote bool) bool {
	rf.mu.Lock()
	term := rf.currentTerm
	if preVote {
		term++ // the term we would campaign in
	}
	args := &RequestVoteArgs{
		Term: term, CandidateID: rf.me,
		LastLogIndex: rf.lastIndex(), LastLogTerm: rf.lastTerm(), PreVote: preVote,
	}
	startTerm := rf.currentTerm
	// Votes are counted only from voters of our latest configuration.
	conf := rf.conf.clone()
	rf.mu.Unlock()

	if !conf.IsVoter(rf.me) {
		return false
	}
	var others []int
	for _, v := range conf.Voters() {
		if v != rf.me {
			others = append(others, v)
		}
	}
	votes := 1
	needed := conf.Quorum()
	if votes >= needed { // single-voter cluster
		if !preVote {
			rf.mu.Lock()
			if rf.role == Candidate && rf.currentTerm == startTerm {
				rf.becomeLeader()
				go rf.heartbeatLoop(rf.currentTerm)
			}
			rf.mu.Unlock()
		}
		return true
	}

	type result struct {
		reply *RequestVoteReply
		err   error
	}
	results := make(chan result, len(others))
	for _, p := range others {
		go func(p int) {
			ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout)
			defer cancel()
			r, err := rf.transport.RequestVote(ctx, p, args)
			results <- result{r, err}
		}(p)
	}

	for range others {
		res := <-results
		if res.err != nil {
			continue
		}
		rf.mu.Lock()
		if rf.currentTerm != startTerm || (!preVote && rf.role != Candidate) {
			rf.mu.Unlock()
			return false // state moved on underneath us
		}
		if res.reply.Term > rf.currentTerm {
			rf.stepDown(res.reply.Term)
			rf.resetElectionTimer()
			rf.mu.Unlock()
			return false
		}
		if res.reply.VoteGranted {
			votes++
			if votes >= needed {
				if !preVote {
					rf.becomeLeader()
					go rf.heartbeatLoop(rf.currentTerm)
				}
				rf.mu.Unlock()
				return true
			}
		}
		rf.mu.Unlock()
	}
	return false
}

func (rf *Raft) logUpToDate(lastIndex, lastTerm int) bool {
	return lastTerm > rf.lastTerm() || (lastTerm == rf.lastTerm() && lastIndex >= rf.lastIndex())
}

// HandleRequestVote is the RequestVote RPC handler.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &RequestVoteReply{Term: rf.currentTerm}

	if args.PreVote {
		// Grant a pre-vote only if we haven't heard from a leader recently
		// (our own election timer has expired or we're not following anyone)
		// and the candidate's log is at least as up to date as ours.
		heardFromLeader := rf.role == Leader ||
			(rf.leaderID != -1 && time.Since(rf.lastHeartbeat) < rf.cfg.ElectionTimeoutMin)
		reply.VoteGranted = args.Term > rf.currentTerm && !heardFromLeader &&
			rf.logUpToDate(args.LastLogIndex, args.LastLogTerm)
		return reply
	}

	if args.Term < rf.currentTerm {
		return reply
	}
	// Thesis §4.2.3: ignore a vote request that arrives within the minimum
	// election timeout of hearing from a current leader. Without this, a
	// removed server that never learned of its removal could keep forcing
	// the cluster into new terms. (Its pre-votes already fail for the same
	// reason; this also covers servers that skip pre-vote.)
	if rf.role == Leader || (rf.leaderID != -1 && time.Since(rf.lastHeartbeat) < rf.cfg.ElectionTimeoutMin) {
		return reply
	}
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
		rf.leaderID = -1
	}
	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.logUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateID
		rf.saveHardState()
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply
}

// ---------------------------------------------------------------- replication

func (rf *Raft) broadcastAppend() {
	for _, p := range rf.peerState {
		p.cond.Signal()
	}
}

// replicator is a long-lived goroutine per follower. It sleeps until
// signalled (new entry or heartbeat tick), then sends one AppendEntries or
// InstallSnapshot. Batching falls out naturally: entries appended while an
// RPC is in flight are all sent in the next one.
func (rf *Raft) replicator(peer int, p *peer) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() && !p.stopped {
		p.cond.Wait()
		if rf.killed() || p.stopped {
			return
		}
		if rf.role != Leader {
			continue
		}
		rf.replicateOnce(peer) // releases and re-acquires rf.mu
	}
}

// replicateOnce must be called with rf.mu held; it drops the lock during the RPC.
func (rf *Raft) replicateOnce(peer int) {
	term := rf.currentTerm
	next := rf.nextIndex[peer]

	if next <= rf.firstIndex() {
		rf.sendSnapshotChunk(peer, term)
		return
	}

	prev := rf.entry(next - 1)
	end := min(rf.lastIndex(), next-1+rf.cfg.MaxEntriesPerRPC)
	entries := make([]LogEntry, end-next+1)
	copy(entries, rf.log[next-rf.firstIndex():end-rf.firstIndex()+1])
	args := &AppendEntriesArgs{
		Term: term, LeaderID: rf.me,
		PrevLogIndex: prev.Index, PrevLogTerm: prev.Term,
		Entries: entries, LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout)
	reply, err := rf.transport.AppendEntries(ctx, peer, args)
	cancel()
	rf.mu.Lock()

	if err != nil || rf.currentTerm != term || rf.role != Leader || rf.peerState[peer] == nil {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
		rf.resetElectionTimer()
		return
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.advanceCommit()
		if rf.nextIndex[peer] <= rf.lastIndex() {
			rf.signalPeer(peer) // more to send; don't wait for heartbeat
		}
		return
	}

	// Fast backup (extended paper §5.3 / 6.824 notes).
	switch {
	case reply.XTerm == -1:
		rf.nextIndex[peer] = reply.XLen
	default:
		last := -1
		for i := rf.lastIndex(); i > rf.firstIndex(); i-- {
			if t := rf.entry(i).Term; t == reply.XTerm {
				last = i
				break
			} else if t < reply.XTerm {
				break
			}
		}
		if last != -1 {
			rf.nextIndex[peer] = last + 1
		} else {
			rf.nextIndex[peer] = reply.XIndex
		}
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
	rf.signalPeer(peer)
}

// sendSnapshotChunk sends the next chunk of the latest snapshot to peer.
// Must hold rf.mu; drops it during the RPC. The snapshot's own index/term
// are used (not rf.firstIndex()): the stored snapshot may be newer than
// the in-memory log trim, and data must never be paired with a different
// index. Progress is kept per peer, so a failed or slow chunk is retried
// from where it stopped instead of from the beginning.
func (rf *Raft) sendSnapshotChunk(peer, term int) {
	sIndex, sTerm, data, sConf := rf.storage.SnapshotWithConfig()
	if sIndex < rf.firstIndex() || len(data) == 0 && sIndex > 0 {
		// Cannot happen: the log is only trimmed after its snapshot is saved.
		panic(fmt.Sprintf("raft %d: snapshot %d older than log start %d", rf.me, sIndex, rf.firstIndex()))
	}
	if rf.snapCRCIndex != sIndex {
		rf.snapCRCIndex, rf.snapCRC = sIndex, crc32.Checksum(data, crcTable)
	}
	if rf.snapSendIndex[peer] != sIndex {
		rf.snapSendIndex[peer], rf.snapSendOffset[peer] = sIndex, 0
	}
	off := rf.snapSendOffset[peer]
	end := min(off+int64(rf.cfg.SnapshotChunkSize), int64(len(data)))
	args := &InstallSnapshotArgs{
		Term: term, LeaderID: rf.me,
		LastIncludedIndex: sIndex, LastIncludedTerm: sTerm,
		Offset: off, Data: data[off:end], Done: end == int64(len(data)),
		Total: int64(len(data)), Checksum: rf.snapCRC, Config: sConf,
	}
	rf.stats.SnapshotChunksSent++
	rf.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout*4)
	reply, err := rf.transport.InstallSnapshot(ctx, peer, args)
	cancel()
	rf.mu.Lock()
	if err != nil || rf.currentTerm != term || rf.role != Leader || rf.peerState[peer] == nil {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
		rf.resetElectionTimer()
		return
	}
	if reply.Installed {
		rf.stats.SnapshotsSent++
		rf.snapSendIndex[peer] = 0
		rf.matchIndex[peer] = max(rf.matchIndex[peer], sIndex)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.advanceCommit()
		if rf.nextIndex[peer] <= rf.lastIndex() {
			rf.signalPeer(peer)
		}
		return
	}
	if rf.snapSendIndex[peer] == sIndex {
		rf.snapSendOffset[peer] = min(max(reply.NextOffset, 0), int64(len(data)))
	}
	rf.signalPeer(peer) // next chunk without waiting for a heartbeat
}

// advanceCommit finds the highest N replicated on a majority of the
// latest configuration's voters with log[N].term == currentTerm (Figure 2,
// last bullet for leaders). Learners, and the leader itself once it has
// removed itself, are not counted.
func (rf *Raft) advanceCommit() {
	voters := rf.conf.Voters()
	quorum := len(voters)/2 + 1
	for n := rf.lastIndex(); n > rf.commitIndex && n > rf.firstIndex(); n-- {
		if rf.entry(n).Term != rf.currentTerm {
			break // earlier entries have older terms too
		}
		count := 0
		for _, v := range voters {
			if rf.matchIndex[v] >= n {
				count++
			}
		}
		if count >= quorum {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			break
		}
	}
	rf.maybeStepDownRemoved()
}

func (rf *Raft) signalPeer(id int) {
	if p := rf.peerState[id]; p != nil {
		p.cond.Signal()
	}
}

// HandleAppendEntries is the AppendEntries RPC handler.
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &AppendEntriesReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply
	}
	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.stepDown(args.Term)
	}
	rf.leaderID = args.LeaderID
	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex == -1 {
		// Leadership probe from ReadIndex: acknowledge the term only.
		return reply
	}
	if args.PrevLogIndex < rf.firstIndex() {
		// Prefix already covered by our snapshot; tell the leader where we are.
		reply.XTerm, reply.XLen = -1, rf.firstIndex()+1
		return reply
	}
	if args.PrevLogIndex > rf.lastIndex() {
		reply.XTerm, reply.XLen = -1, rf.lastIndex()+1
		return reply
	}
	if t := rf.entry(args.PrevLogIndex).Term; t != args.PrevLogTerm {
		reply.XTerm = t
		i := args.PrevLogIndex
		for i > rf.firstIndex()+1 && rf.entry(i-1).Term == t {
			i--
		}
		reply.XIndex = i
		reply.XLen = rf.lastIndex() + 1
		return reply
	}

	// Append new entries, truncating only on an actual conflict so that a
	// stale/reordered RPC never removes entries a newer RPC already added.
	confChanged := false
	for i, e := range args.Entries {
		if e.Index <= rf.lastIndex() {
			if rf.entry(e.Index).Term == e.Term {
				continue
			}
			if e.Index <= rf.commitIndex {
				// Log Matching + Leader Completeness guarantee a committed
				// entry is never contradicted; if it is, continuing would
				// silently rewrite applied history.
				panic(fmt.Sprintf("raft %d: safety violation: leader %d term %d conflicts at committed index %d (commitIndex %d)",
					rf.me, args.LeaderID, args.Term, e.Index, rf.commitIndex))
			}
			rf.log = rf.log[:e.Index-rf.firstIndex()]
			if e.Index <= rf.confIndex {
				confChanged = true // the entry that set our config is gone
			}
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		for _, ne := range args.Entries[i:] {
			if ne.Type == EntryConfig {
				confChanged = true
			}
		}
		// Followers must have entries on disk before acknowledging them.
		must(rf.storage.Append(args.Entries[i:], true))
		rf.durableIndex = rf.lastIndex()
		break
	}
	if rf.durableIndex > rf.lastIndex() {
		rf.durableIndex = rf.lastIndex()
	}
	if confChanged {
		rf.recomputeConfig() // configurations take effect on append
	}

	// Only entries this RPC verified (through lastNew) may be marked
	// committed, and commitIndex never moves backwards: a delayed RPC that
	// covers a shorter prefix must not lower it.
	lastNew := args.PrevLogIndex + len(args.Entries)
	if c := min(args.LeaderCommit, lastNew); c > rf.commitIndex {
		rf.commitIndex = c
		rf.applyCond.Broadcast()
	}
	reply.Success = true
	return reply
}

// HandleInstallSnapshot is the InstallSnapshot RPC handler. Chunks are
// buffered until the last one arrives; the reassembled snapshot is checked
// against the leader's size and CRC, written to storage without rf.mu held,
// and only then installed.
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	rf.mu.Lock()
	reply := &InstallSnapshotReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return reply
	}
	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.stepDown(args.Term)
	}
	rf.leaderID = args.LeaderID
	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		// We already have (and will apply) everything it covers.
		rf.snapRecv = nil
		reply.Installed = true
		rf.mu.Unlock()
		return reply
	}

	r := rf.snapRecv
	same := r != nil && r.term == args.Term && r.index == args.LastIncludedIndex &&
		r.lastTerm == args.LastIncludedTerm && r.total == args.Total && r.checksum == args.Checksum
	switch {
	case args.Offset == 0:
		if args.Total < 0 || args.Total > rf.cfg.MaxSnapshotSize {
			rf.mu.Unlock()
			return reply // refuse; leader keeps retrying, operator sees it
		}
		r = &snapshotRecv{term: args.Term, index: args.LastIncludedIndex, lastTerm: args.LastIncludedTerm,
			total: args.Total, checksum: args.Checksum, conf: args.Config, buf: make([]byte, 0, min(args.Total, 64<<20))}
		rf.snapRecv = r
	case !same:
		reply.NextOffset = 0 // unknown transfer: start over
		rf.mu.Unlock()
		return reply
	case args.Offset != int64(len(r.buf)):
		reply.NextOffset = int64(len(r.buf)) // out of order: resume where we are
		rf.mu.Unlock()
		return reply
	}
	if int64(len(r.buf))+int64(len(args.Data)) > r.total {
		rf.snapRecv = nil
		rf.mu.Unlock()
		return reply
	}
	r.buf = append(r.buf, args.Data...)
	reply.NextOffset = int64(len(r.buf))
	if !args.Done {
		rf.mu.Unlock()
		return reply
	}
	rf.snapRecv = nil
	if int64(len(r.buf)) != r.total || crc32.Checksum(r.buf, crcTable) != r.checksum {
		rf.stats.SnapshotChecksumFails++
		rf.logf("snapshot %d failed verification (%d/%d bytes); restarting transfer", r.index, len(r.buf), r.total)
		reply.NextOffset = 0
		rf.mu.Unlock()
		return reply
	}
	rf.mu.Unlock()

	// The snapshot is committed state, so saving it is safe even if our
	// term or log changes meanwhile; the stored log is untouched.
	must(rf.storage.SaveSnapshot(r.index, r.lastTerm, r.buf, r.conf))

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Installed = true
	if r.index <= rf.commitIndex {
		return reply // caught up through the log meanwhile
	}
	if r.index < rf.lastIndex() && r.index > rf.firstIndex() && rf.entry(r.index).Term == r.lastTerm {
		// Keep the suffix that follows the snapshot.
		rf.trimLogTo(r.index, r.lastTerm)
	} else {
		rf.log = []LogEntry{{Term: r.lastTerm, Index: r.index}}
	}
	must(rf.storage.CompactLog(r.index, rf.log[1:]))
	if r.conf != nil {
		rf.snapConf = r.conf.clone()
	}
	rf.recomputeConfig()
	// Entries through r.index are covered by the durable snapshot.
	rf.durableIndex = min(max(rf.durableIndex, r.index), rf.lastIndex())
	rf.commitIndex = r.index
	rf.stats.SnapshotsInstalled++
	rf.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true, Snapshot: r.buf,
		SnapshotTerm: r.lastTerm, SnapshotIndex: r.index,
	}
	rf.applyCond.Broadcast()
	return reply
}

// ---------------------------------------------------------------- applier

// applier delivers committed entries (and installed snapshots) to the
// service in log order. It never holds rf.mu while sending on applyCh, so
// a slow service cannot block RPC handling.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if snap := rf.pendingSnapshot; snap != nil {
			rf.pendingSnapshot = nil
			if snap.SnapshotIndex > rf.lastApplied {
				rf.lastApplied = snap.SnapshotIndex
				rf.mu.Unlock()
				rf.applyCh <- *snap
				rf.mu.Lock()
			}
			continue
		}
		if rf.lastApplied < rf.firstIndex() {
			// Log was compacted past lastApplied (snapshot raced ahead).
			rf.lastApplied = rf.firstIndex()
		}
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			continue
		}
		start, end := rf.lastApplied+1, rf.commitIndex
		batch := make([]LogEntry, end-start+1)
		copy(batch, rf.log[start-rf.firstIndex():end-rf.firstIndex()+1])
		rf.mu.Unlock()
		for _, e := range batch {
			m := ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: e.Index, CommandTerm: e.Term}
			if e.Type == EntryConfig {
				c := decodeConfig(e.Command)
				m.Command, m.Config = nil, &c
			}
			rf.applyCh <- m
		}
		rf.mu.Lock()
		if end > rf.lastApplied {
			rf.lastApplied = end
		}
	}
}

// ---------------------------------------------------------------- ReadIndex

var (
	ErrNotLeader      = errors.New("raft: not leader")
	ErrLeadershipLost = errors.New("raft: leadership not confirmed")
	ErrNoCommitInTerm = errors.New("raft: leader has not committed an entry in its term yet")
)

type readResult struct {
	index int
	err   error
}

// ReadIndex enqueues a read and waits for the next leadership-confirmation
// round. All reads that arrive while a round is in flight are served by the
// following round, so N concurrent reads cost one round of heartbeats, not N.
func (rf *Raft) ReadIndex(ctx context.Context) (int, error) {
	ch := make(chan readResult, 1)
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return 0, ErrNotLeader
	}
	rf.readQueue = append(rf.readQueue, ch)
	rf.readCond.Signal()
	rf.mu.Unlock()
	select {
	case r := <-ch:
		return r.index, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (rf *Raft) readLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if len(rf.readQueue) == 0 {
			rf.readCond.Wait()
			continue
		}
		batch := rf.readQueue
		rf.readQueue = nil
		rf.mu.Unlock()
		// Every request in batch arrived before this round starts, so the
		// commitIndex captured inside confirmRound is a valid read index.
		ctx, cancel := context.WithTimeout(context.Background(), 2*rf.cfg.RPCTimeout)
		idx, err := rf.confirmRound(ctx)
		cancel()
		for _, ch := range batch {
			ch <- readResult{idx, err}
		}
		rf.mu.Lock()
	}
}

// confirmRound implements linearizable reads without writing to the log
// (Raft thesis §6.4). It returns an index such that once the state machine
// has applied it, a local read reflects every write that completed before
// ReadIndex was called. Steps:
//  1. The leader must have committed an entry in its current term (the
//     no-op appended on election), so its commitIndex is up to date.
//  2. Record readIndex = commitIndex.
//  3. Confirm we are still leader by getting heartbeat acks from a majority
//     in the current term; a newer leader would have rejected us.
func (rf *Raft) confirmRound(ctx context.Context) (int, error) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return 0, ErrNotLeader
	}
	if rf.commitIndex < rf.firstIndex() || rf.entry(max(rf.commitIndex, rf.firstIndex())).Term != rf.currentTerm {
		rf.mu.Unlock()
		return 0, ErrNoCommitInTerm
	}
	readIndex := rf.commitIndex
	term := rf.currentTerm
	args := &AppendEntriesArgs{Term: term, LeaderID: rf.me, PrevLogIndex: -1, LeaderCommit: rf.commitIndex}
	// Leadership is confirmed by a majority of the latest configuration's
	// voters (the same set that commits entries).
	conf := rf.conf.clone()
	rf.mu.Unlock()

	got, needed := 0, conf.Quorum()
	var others []int
	for _, v := range conf.Voters() {
		if v == rf.me {
			got = 1
		} else {
			others = append(others, v)
		}
	}
	if got >= needed {
		return readIndex, nil
	}
	acks := make(chan bool, len(others))
	for _, p := range others {
		go func(p int) {
			c, cancel := context.WithTimeout(ctx, rf.cfg.RPCTimeout)
			defer cancel()
			r, err := rf.transport.AppendEntries(c, p, args)
			if err != nil {
				acks <- false
				return
			}
			if r.Term > term {
				rf.mu.Lock()
				if r.Term > rf.currentTerm {
					rf.stepDown(r.Term)
				}
				rf.mu.Unlock()
				acks <- false
				return
			}
			acks <- true
		}(p)
	}
	for range others {
		if <-acks {
			got++
			if got >= needed {
				rf.mu.Lock()
				ok := rf.role == Leader && rf.currentTerm == term
				rf.mu.Unlock()
				if !ok {
					return 0, ErrLeadershipLost
				}
				return readIndex, nil
			}
		}
	}
	return 0, ErrLeadershipLost
}

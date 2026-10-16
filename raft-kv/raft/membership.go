package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Membership changes use single-server changes (Raft thesis §4.1–§4.3):
//
//   - A configuration is the full member list, stored as a log entry of
//     type EntryConfig. Every server uses the latest configuration in its
//     log, committed or not ("takes effect on append"). If that entry is
//     later truncated away, the server falls back to the previous one.
//   - Each change adds or removes at most one voter, so any majority of
//     the old voter set overlaps any majority of the new one. The leader
//     only proposes a change when (a) it has committed an entry in its
//     current term (the fix to the single-server algorithm posted to
//     raft-dev in 2015) and (b) no earlier change is still uncommitted.
//   - New servers join as non-voting learners. The leader replicates to
//     them but does not count them for commit or elections. AddServer
//     waits until the learner has caught up (§4.2.1) before promoting it.
//   - A leader that removes itself keeps leading without counting its own
//     vote until the change commits, then steps down (§4.2.2). It is not a
//     voter in its own configuration, so it never campaigns again.
//   - The configuration covering a snapshot is stored with the snapshot
//     and sent in InstallSnapshot, so compaction never loses it.

// EntryType distinguishes normal commands from configuration entries.
type EntryType uint8

const (
	EntryNormal EntryType = iota // Command is opaque service data (nil = no-op)
	EntryConfig                  // Command is a gob-encoded Configuration
)

// Member is one server in a configuration.
type Member struct {
	ID      int
	Addr    string // peer URL; "" if unknown (bootstrap members of an in-memory test cluster)
	Learner bool   // replicated to, but does not vote or count toward commit
}

// Configuration is the full membership, sorted by ID.
type Configuration struct {
	Members []Member
}

func (c Configuration) clone() Configuration {
	return Configuration{Members: append([]Member(nil), c.Members...)}
}

func (c Configuration) find(id int) (Member, bool) {
	for _, m := range c.Members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

// Contains reports whether id is a member (voter or learner).
func (c Configuration) Contains(id int) bool { _, ok := c.find(id); return ok }

// IsVoter reports whether id is a voting member.
func (c Configuration) IsVoter(id int) bool { m, ok := c.find(id); return ok && !m.Learner }

// Voters returns the voting members' IDs in ascending order.
func (c Configuration) Voters() []int {
	var out []int
	for _, m := range c.Members {
		if !m.Learner {
			out = append(out, m.ID)
		}
	}
	return out
}

// Quorum is the number of voters that form a majority.
func (c Configuration) Quorum() int { return len(c.Voters())/2 + 1 }

func (c Configuration) String() string {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, m := range c.Members {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%d", m.ID)
		if m.Learner {
			b.WriteString("(learner)")
		}
	}
	b.WriteByte('}')
	return b.String()
}

func (c *Configuration) set(m Member) {
	for i := range c.Members {
		if c.Members[i].ID == m.ID {
			c.Members[i] = m
			return
		}
	}
	c.Members = append(c.Members, m)
	sort.Slice(c.Members, func(i, j int) bool { return c.Members[i].ID < c.Members[j].ID })
}

func (c *Configuration) remove(id int) {
	out := c.Members[:0]
	for _, m := range c.Members {
		if m.ID != id {
			out = append(out, m)
		}
	}
	c.Members = out
}

// votersDiffer counts voters in exactly one of a and b.
func votersDiffer(a, b Configuration) int {
	n := 0
	for _, v := range a.Voters() {
		if !b.IsVoter(v) {
			n++
		}
	}
	for _, v := range b.Voters() {
		if !a.IsVoter(v) {
			n++
		}
	}
	return n
}

func encodeConfig(c Configuration) []byte {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(c); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func decodeConfig(b []byte) Configuration {
	var c Configuration
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&c); err != nil {
		panic("raft: corrupt configuration entry: " + err.Error())
	}
	return c
}

// AddressBook is implemented by transports that address peers by URL. Raft
// reads bootstrap addresses from it and pushes every new configuration to
// it, so added servers become reachable.
type AddressBook interface {
	PeerAddr(id int) string
	SetPeerAddrs(addrs map[int]string)
}

var (
	ErrConfigInProgress = errors.New("raft: another membership change is not committed yet")
	ErrConfigNoop       = errors.New("raft: membership change has no effect")
	ErrNotMember        = errors.New("raft: server is not a member")
	ErrCatchUpTimeout   = errors.New("raft: new server did not catch up; it stays a learner")
)

// ---------------------------------------------------------------- state (rf.mu held)

// bootstrapConfig is the implicit initial configuration: voters 0..n-1.
func (rf *Raft) bootstrapConfig(n int) Configuration {
	var c Configuration
	ab, _ := rf.transport.(AddressBook)
	for i := 0; i < n; i++ {
		m := Member{ID: i}
		if ab != nil {
			m.Addr = ab.PeerAddr(i)
		}
		c.Members = append(c.Members, m)
	}
	return c
}

// configAt returns the configuration in effect at index (the latest config
// entry at or below it, else the snapshot's). index must be >= firstIndex.
func (rf *Raft) configAt(index int) Configuration {
	for i := min(index, rf.lastIndex()); i > rf.firstIndex(); i-- {
		if e := rf.entry(i); e.Type == EntryConfig {
			return decodeConfig(e.Command)
		}
	}
	return rf.snapConf.clone()
}

// recomputeConfig sets rf.conf to the latest configuration in the log and
// starts/stops replicators to match. Called after any log change that can
// add or remove a config entry.
func (rf *Raft) recomputeConfig() {
	idx := rf.firstIndex()
	conf := rf.snapConf.clone()
	for i := rf.lastIndex(); i > rf.firstIndex(); i-- {
		if e := rf.entry(i); e.Type == EntryConfig {
			idx, conf = i, decodeConfig(e.Command)
			break
		}
	}
	rf.setConfig(conf, idx)
}

func (rf *Raft) setConfig(conf Configuration, index int) {
	changed := index != rf.confIndex || conf.String() != rf.conf.String()
	rf.conf, rf.confIndex = conf, index
	if changed {
		rf.logf("configuration %v at index %d", conf, index)
	}
	if ab, ok := rf.transport.(AddressBook); ok {
		addrs := map[int]string{}
		for _, m := range conf.Members {
			if m.Addr != "" {
				addrs[m.ID] = m.Addr
			}
		}
		ab.SetPeerAddrs(addrs)
	}
	// Start replicators for new members, stop them for removed ones.
	for _, m := range conf.Members {
		if m.ID == rf.me {
			continue
		}
		if _, ok := rf.peerState[m.ID]; !ok {
			p := &peer{cond: newCond(&rf.mu)}
			rf.peerState[m.ID] = p
			rf.nextIndex[m.ID] = rf.lastIndex() + 1
			rf.matchIndex[m.ID] = 0
			go rf.replicator(m.ID, p)
			if rf.role == Leader {
				p.cond.Signal()
			}
		}
	}
	for id, p := range rf.peerState {
		if !conf.Contains(id) {
			p.stopped = true
			p.cond.Broadcast()
			delete(rf.peerState, id)
			delete(rf.nextIndex, id)
			delete(rf.matchIndex, id)
			delete(rf.snapSendIndex, id)
			delete(rf.snapSendOffset, id)
		}
	}
}

// maybeStepDownRemoved: a leader that is no longer a voter steps down once
// the configuration that removed it is committed (thesis §4.2.2).
func (rf *Raft) maybeStepDownRemoved() {
	if rf.role == Leader && !rf.conf.IsVoter(rf.me) && rf.commitIndex >= rf.confIndex {
		rf.logf("removed from configuration %v (committed at %d); stepping down", rf.conf, rf.confIndex)
		rf.role = Follower
		rf.leaderID = -1
		rf.resetElectionTimer()
	}
}

// ---------------------------------------------------------------- public API

// Configuration returns the latest configuration and the index of the
// entry that set it (the snapshot index or 0 if it is the bootstrap one).
func (rf *Raft) Configuration() (Configuration, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.conf.clone(), rf.confIndex
}

// PeerAddr returns the address of member id from the latest configuration.
func (rf *Raft) PeerAddr(id int) string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	m, _ := rf.conf.find(id)
	return m.Addr
}

// proposeConfig appends a configuration entry built by change from the
// current one. It returns the entry's index and term.
func (rf *Raft) proposeConfig(change func(c *Configuration) error) (int, int, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.killed() {
		return 0, 0, ErrNotLeader
	}
	if rf.commitIndex < rf.firstIndex() || rf.entry(max(rf.commitIndex, rf.firstIndex())).Term != rf.currentTerm {
		return 0, 0, ErrNoCommitInTerm
	}
	if rf.confIndex > rf.commitIndex {
		return 0, 0, ErrConfigInProgress
	}
	next := rf.conf.clone()
	if err := change(&next); err != nil {
		return 0, 0, err
	}
	if votersDiffer(rf.conf, next) > 1 {
		return 0, 0, fmt.Errorf("raft: change %v -> %v alters more than one voter", rf.conf, next)
	}
	if next.String() == rf.conf.String() && sameAddrs(next, rf.conf) {
		return 0, 0, ErrConfigNoop
	}
	if len(next.Voters()) == 0 {
		return 0, 0, errors.New("raft: cannot remove the last voter")
	}
	e := LogEntry{Term: rf.currentTerm, Index: rf.lastIndex() + 1, Type: EntryConfig, Command: encodeConfig(next)}
	rf.leaderAppend(e)
	rf.setConfig(next, e.Index)
	if !next.IsVoter(rf.me) {
		// Our own vote no longer counts; re-evaluate commit without it.
		rf.advanceCommit()
	}
	return e.Index, e.Term, nil
}

func sameAddrs(a, b Configuration) bool {
	if len(a.Members) != len(b.Members) {
		return false
	}
	for i := range a.Members {
		if a.Members[i] != b.Members[i] {
			return false
		}
	}
	return true
}

// waitCommitted blocks until index commits in term, leadership is lost or
// ctx ends.
func (rf *Raft) waitCommitted(ctx context.Context, index, term int) error {
	for {
		rf.mu.Lock()
		committed := rf.commitIndex >= index
		var ok bool
		if index > rf.firstIndex() {
			ok = committed && rf.entry(index).Term == term
		} else {
			// Compacted: the entry is ours if no other leader has existed.
			ok = committed && rf.currentTerm == term
		}
		lost := rf.currentTerm != term || (rf.role != Leader && !committed)
		rf.mu.Unlock()
		if ok {
			return nil
		}
		if lost {
			return ErrLeadershipLost
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// AddLearner adds a non-voting member (or updates a learner's address) and
// waits for the change to commit.
func (rf *Raft) AddLearner(ctx context.Context, id int, addr string) error {
	idx, term, err := rf.proposeConfig(func(c *Configuration) error {
		if m, ok := c.find(id); ok && !m.Learner {
			return fmt.Errorf("raft: server %d is already a voter", id)
		}
		c.set(Member{ID: id, Addr: addr, Learner: true})
		return nil
	})
	if err != nil {
		return err
	}
	return rf.waitCommitted(ctx, idx, term)
}

// PromoteLearner makes learner id a voter and waits for the change to commit.
func (rf *Raft) PromoteLearner(ctx context.Context, id int) error {
	idx, term, err := rf.proposeConfig(func(c *Configuration) error {
		m, ok := c.find(id)
		if !ok {
			return ErrNotMember
		}
		if !m.Learner {
			return ErrConfigNoop
		}
		m.Learner = false
		c.set(m)
		return nil
	})
	if err != nil {
		return err
	}
	return rf.waitCommitted(ctx, idx, term)
}

// RemoveServer removes member id (voter or learner) and waits for the
// change to commit. Removing the leader itself is allowed: it steps down
// once the change commits.
func (rf *Raft) RemoveServer(ctx context.Context, id int) error {
	idx, term, err := rf.proposeConfig(func(c *Configuration) error {
		if !c.Contains(id) {
			return ErrNotMember
		}
		c.remove(id)
		return nil
	})
	if err != nil {
		return err
	}
	err = rf.waitCommitted(ctx, idx, term)
	if errors.Is(err, ErrLeadershipLost) && id == rf.me {
		// We stepped down because the change committed (or lost leadership
		// for another reason): report success only if it committed.
		rf.mu.Lock()
		done := rf.commitIndex >= idx
		rf.mu.Unlock()
		if done {
			return nil
		}
	}
	return err
}

// AddServer adds id as a learner, waits until it has caught up with the
// leader's log, then promotes it to voter (thesis §4.2.1). Catch-up runs
// in rounds: each round waits until the learner has every entry the
// leader had when the round started. When a round finishes within one
// minimum election timeout, the learner is close enough to promote: the
// cluster will not be unavailable for long while it finishes. After
// maxRounds slow rounds (or ctx expiry) the learner is left in place and
// ErrCatchUpTimeout is returned; the operator can retry or remove it.
func (rf *Raft) AddServer(ctx context.Context, id int, addr string) error {
	rf.mu.Lock()
	m, known := rf.conf.find(id)
	rf.mu.Unlock()
	if !known || m.Addr != addr && addr != "" {
		if err := rf.AddLearner(ctx, id, addr); err != nil {
			return err
		}
	} else if !m.Learner {
		return ErrConfigNoop
	}
	const maxRounds = 20
	for round := 0; round < maxRounds; round++ {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return ErrNotLeader
		}
		target := rf.lastIndex()
		rf.mu.Unlock()
		start := time.Now()
		for {
			rf.mu.Lock()
			match, isLeader := rf.matchIndex[id], rf.role == Leader
			rf.mu.Unlock()
			if !isLeader {
				return ErrNotLeader
			}
			if match >= target {
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("%w: %v", ErrCatchUpTimeout, ctx.Err())
			case <-time.After(5 * time.Millisecond):
			}
		}
		if time.Since(start) < rf.cfg.ElectionTimeoutMin {
			return rf.PromoteLearner(ctx, id)
		}
	}
	return ErrCatchUpTimeout
}

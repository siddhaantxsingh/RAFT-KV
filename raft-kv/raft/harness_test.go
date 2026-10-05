package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
	"time"
)

// cluster is the test harness: n Raft peers on an in-memory Network, with a
// checker that verifies the core safety property — no two peers ever apply
// different commands at the same log index.
type cluster struct {
	t          *testing.T
	mu         sync.Mutex
	n          int
	net        *Network
	rafts      []*Raft
	persisters []*MemoryStorage
	applyChs   []chan ApplyMsg
	logs       []map[int]int // per-peer: index -> command value
	maxIndex   int
	snapEvery  int // if >0, peers snapshot every snapEvery applied entries
	errMsg     string
	start      time.Time
	stopMon    chan struct{}
}

func encInt(v int) []byte {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(v)
	return b.Bytes()
}

func decInt(b []byte) int {
	var v int
	gob.NewDecoder(bytes.NewReader(b)).Decode(&v)
	return v
}

func testConfig() Config {
	c := DefaultConfig()
	c.ElectionTimeoutMin = 250 * time.Millisecond
	c.ElectionTimeoutMax = 500 * time.Millisecond
	c.HeartbeatInterval = 60 * time.Millisecond
	c.RPCTimeout = 100 * time.Millisecond
	// Tiny chunks so every snapshot transfer in the tests is multi-chunk.
	c.SnapshotChunkSize = 64
	return c
}

func newCluster(t *testing.T, n int, unreliable bool, snapEvery int) *cluster {
	c := &cluster{
		t: t, n: n, net: NewNetwork(),
		rafts: make([]*Raft, n), persisters: make([]*MemoryStorage, n),
		applyChs: make([]chan ApplyMsg, n), logs: make([]map[int]int, n),
		snapEvery: snapEvery, start: time.Now(),
	}
	c.net.SetUnreliable(unreliable)
	for i := 0; i < n; i++ {
		c.persisters[i] = NewMemoryStorage()
		c.startPeer(i)
		c.connect(i)
	}
	c.stopMon = make(chan struct{})
	go c.monitorInvariants(c.stopMon)
	return c
}

// monitorInvariants samples every live peer throughout the test and
// records a failure if a Raft safety invariant is observed broken:
//   - Election Safety: at most one leader per term (across restarts too).
//   - commitIndex never decreases while a peer instance is alive, and
//     lastApplied never exceeds commitIndex.
//
// Sampling can miss short-lived states; the apply checker (record) covers
// State Machine Safety exhaustively.
func (c *cluster) monitorInvariants(stop <-chan struct{}) {
	leaders := map[int]int{} // term -> leader id
	lastCommit := map[*Raft]int{}
	t := time.NewTicker(2 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		c.mu.Lock()
		rafts := append([]*Raft(nil), c.rafts...)
		c.mu.Unlock()
		for id, rf := range rafts {
			if rf == nil || rf.killed() {
				continue
			}
			st := rf.Status()
			if st.Role == "leader" {
				if other, ok := leaders[st.Term]; ok && other != id {
					c.fail(fmt.Sprintf("election safety violated: peers %d and %d both leader in term %d", other, id, st.Term))
				}
				leaders[st.Term] = id
			}
			if prev, ok := lastCommit[rf]; ok && st.CommitIndex < prev {
				c.fail(fmt.Sprintf("peer %d commitIndex decreased %d -> %d", id, prev, st.CommitIndex))
			}
			lastCommit[rf] = st.CommitIndex
			if st.LastApplied > st.CommitIndex {
				c.fail(fmt.Sprintf("peer %d lastApplied %d > commitIndex %d", id, st.LastApplied, st.CommitIndex))
			}
		}
	}
}

func (c *cluster) startPeer(i int) {
	c.crash(i)
	if c.persisters[i] != nil {
		c.persisters[i] = c.persisters[i].Copy()
	}
	ch := make(chan ApplyMsg, 64)
	c.mu.Lock()
	c.logs[i] = map[int]int{}
	c.applyChs[i] = ch
	c.mu.Unlock()

	// Restore the applier's view from the snapshot, as a real service would.
	if _, _, snap := c.persisters[i].Snapshot(); len(snap) > 0 {
		c.restoreSnapshot(i, snap)
	}
	rf := New(i, c.n, c.net.Endpoint(i), c.persisters[i], ch, testConfig())
	c.mu.Lock()
	c.rafts[i] = rf
	c.mu.Unlock()
	c.net.Register(i, rf)
	go c.applier(i, rf, ch)
}

// snapshot format: lastIndex + map of index->value (the applied log).
type snapState struct {
	LastIndex int
	Log       map[int]int
}

func (c *cluster) restoreSnapshot(i int, snap []byte) int {
	var s snapState
	if err := gob.NewDecoder(bytes.NewReader(snap)).Decode(&s); err != nil {
		c.t.Fatalf("decode snapshot: %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logs[i] = map[int]int{}
	for k, v := range s.Log {
		c.logs[i][k] = v
	}
	return s.LastIndex
}

func (c *cluster) applier(i int, rf *Raft, ch chan ApplyMsg) {
	lastApplied := 0
	if _, _, snap := c.persisters[i].Snapshot(); len(snap) > 0 {
		var s snapState
		gob.NewDecoder(bytes.NewReader(snap)).Decode(&s)
		lastApplied = s.LastIndex
	}
	for m := range ch {
		if m.SnapshotValid {
			lastApplied = c.restoreSnapshot(i, m.Snapshot)
			continue
		}
		if !m.CommandValid {
			continue
		}
		if m.CommandIndex <= lastApplied {
			continue
		}
		if m.CommandIndex != lastApplied+1 {
			c.fail(fmt.Sprintf("peer %d applied index %d out of order (expected %d)", i, m.CommandIndex, lastApplied+1))
		}
		lastApplied = m.CommandIndex
		if m.Command == nil {
			c.record(i, m.CommandIndex, -1) // leader no-op
		} else {
			c.record(i, m.CommandIndex, decInt(m.Command))
		}
		if c.snapEvery > 0 && m.CommandIndex%c.snapEvery == 0 {
			c.mu.Lock()
			s := snapState{LastIndex: m.CommandIndex, Log: map[int]int{}}
			for k, v := range c.logs[i] {
				s.Log[k] = v
			}
			c.mu.Unlock()
			var b bytes.Buffer
			gob.NewEncoder(&b).Encode(s)
			rf.Snapshot(m.CommandIndex, b.Bytes())
		}
	}
}

func (c *cluster) record(i, index, v int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for j := 0; j < c.n; j++ {
		if old, ok := c.logs[j][index]; ok && old != v {
			c.errMsg = fmt.Sprintf("commit index=%d peer=%d value %d != peer=%d value %d", index, i, v, j, old)
		}
	}
	c.logs[i][index] = v
	if index > c.maxIndex {
		c.maxIndex = index
	}
}

func (c *cluster) fail(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.errMsg == "" {
		c.errMsg = msg
	}
}

func (c *cluster) checkErr() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.errMsg != "" {
		c.t.Fatal(c.errMsg)
	}
}

func (c *cluster) crash(i int) {
	c.net.SetConnected(i, false)
	c.mu.Lock()
	rf := c.rafts[i]
	c.rafts[i] = nil
	c.mu.Unlock()
	if rf != nil {
		rf.Kill()
	}
}

func (c *cluster) connect(i int)    { c.net.SetConnected(i, true) }
func (c *cluster) disconnect(i int) { c.net.SetConnected(i, false) }

func (c *cluster) cleanup() {
	if c.stopMon != nil {
		close(c.stopMon)
		c.stopMon = nil
	}
	c.checkErr()
	for i := 0; i < c.n; i++ {
		c.crash(i)
	}
}

// checkOneLeader waits for exactly one leader among connected peers.
func (c *cluster) checkOneLeader() int {
	for iter := 0; iter < 20; iter++ {
		time.Sleep(time.Duration(300+iter*20) * time.Millisecond)
		leaders := map[int][]int{}
		for i := 0; i < c.n; i++ {
			if !c.isConnected(i) {
				continue
			}
			c.mu.Lock()
			rf := c.rafts[i]
			c.mu.Unlock()
			if rf == nil {
				continue
			}
			if term, lead := rf.GetState(); lead {
				leaders[term] = append(leaders[term], i)
			}
		}
		last := -1
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders", term, len(ls))
			}
			if term > last {
				last = term
			}
		}
		if last != -1 {
			return leaders[last][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

func (c *cluster) isConnected(i int) bool {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.net.connected[i]
}

func (c *cluster) checkNoLeader() {
	for i := 0; i < c.n; i++ {
		if !c.isConnected(i) {
			continue
		}
		c.mu.Lock()
		rf := c.rafts[i]
		c.mu.Unlock()
		if rf == nil {
			continue
		}
		if _, lead := rf.GetState(); lead {
			c.t.Fatalf("expected no leader among connected peers, but %d claims leadership", i)
		}
	}
}

func (c *cluster) checkTerms() int {
	term := -1
	for i := 0; i < c.n; i++ {
		if !c.isConnected(i) {
			continue
		}
		c.mu.Lock()
		rf := c.rafts[i]
		c.mu.Unlock()
		t, _ := rf.GetState()
		if term == -1 {
			term = t
		} else if term != t {
			c.t.Fatalf("peers disagree on term")
		}
	}
	return term
}

// nCommitted returns how many peers have applied index, and the value.
func (c *cluster) nCommitted(index int) (int, int) {
	c.checkErr()
	c.mu.Lock()
	defer c.mu.Unlock()
	count, val := 0, 0
	for i := 0; i < c.n; i++ {
		if v, ok := c.logs[i][index]; ok {
			count++
			val = v
		}
	}
	return count, val
}

// one submits cmd to a leader and waits for it to be applied by at least
// expected peers. With retry, it resubmits if the leader fails.
func (c *cluster) one(cmd, expected int, retry bool) int {
	deadline := time.Now().Add(10 * time.Second)
	starts := 0
	for time.Now().Before(deadline) {
		index := -1
		for k := 0; k < c.n; k++ {
			starts = (starts + 1) % c.n
			c.mu.Lock()
			rf := c.rafts[starts]
			c.mu.Unlock()
			if rf == nil || !c.isConnected(starts) {
				continue
			}
			if idx, _, ok := rf.Start(encInt(cmd)); ok {
				index = idx
				break
			}
		}
		if index != -1 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				if nd, v := c.nCommitted(index); nd > 0 && nd >= expected && v == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%d) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%d) failed to reach agreement", cmd)
	return -1
}

func (c *cluster) wait(index, n, startTerm int) int {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if nd, _ := c.nCommitted(index); nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, rf := range c.rafts {
				if rf == nil {
					continue
				}
				if t, _ := rf.GetState(); t > startTerm {
					return -1
				}
			}
		}
	}
	nd, v := c.nCommitted(index)
	if nd < n {
		c.t.Fatalf("only %d decided for index %d; wanted %d", nd, index, n)
	}
	return v
}

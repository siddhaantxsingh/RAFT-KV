package raft

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, false, 0)
	defer c.cleanup()
	c.checkOneLeader()
	time.Sleep(50 * time.Millisecond)
	t1 := c.checkTerms()
	time.Sleep(1 * time.Second)
	if t2 := c.checkTerms(); t1 != t2 {
		t.Logf("warning: term changed with no failures (%d -> %d)", t1, t2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, false, 0)
	defer c.cleanup()
	l1 := c.checkOneLeader()
	c.disconnect(l1)
	c.checkOneLeader()

	c.connect(l1) // old leader rejoins; must not disrupt
	l2 := c.checkOneLeader()

	c.disconnect(l2)
	c.disconnect((l2 + 1) % 3)
	time.Sleep(1 * time.Second)
	c.checkNoLeader() // no quorum

	c.connect((l2 + 1) % 3)
	c.checkOneLeader()
	c.connect(l2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	c := newCluster(t, 7, false, 0)
	defer c.cleanup()
	c.checkOneLeader()
	for i := 0; i < 8; i++ {
		a, b, d := rand.Intn(7), rand.Intn(7), rand.Intn(7)
		c.disconnect(a)
		c.disconnect(b)
		c.disconnect(d)
		c.checkOneLeader() // at least 4 remain
		c.connect(a)
		c.connect(b)
		c.connect(d)
	}
	c.checkOneLeader()
}

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3, false, 0)
	defer c.cleanup()
	for i := 1; i <= 5; i++ {
		c.one(i*100, 3, false)
	}
}

func TestFailAgree(t *testing.T) {
	c := newCluster(t, 3, false, 0)
	defer c.cleanup()
	c.one(101, 3, false)
	l := c.checkOneLeader()
	c.disconnect((l + 1) % 3)
	c.one(102, 2, false)
	c.one(103, 2, false)
	time.Sleep(500 * time.Millisecond)
	c.one(104, 2, false)
	c.connect((l + 1) % 3)
	c.one(106, 3, true)
}

func TestFailNoAgree(t *testing.T) {
	c := newCluster(t, 5, false, 0)
	defer c.cleanup()
	c.one(10, 5, false)
	l := c.checkOneLeader()
	c.disconnect((l + 1) % 5)
	c.disconnect((l + 2) % 5)
	c.disconnect((l + 3) % 5)

	c.mu.Lock()
	rf := c.rafts[l]
	c.mu.Unlock()
	index, _, ok := rf.Start(encInt(20))
	if !ok {
		t.Fatal("leader rejected Start")
	}
	time.Sleep(1 * time.Second)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("%d committed without majority", n)
	}
	c.connect((l + 1) % 5)
	c.connect((l + 2) % 5)
	c.connect((l + 3) % 5)
	c.checkOneLeader()
	c.one(1000, 5, true)
}

func TestConcurrentStarts(t *testing.T) {
	c := newCluster(t, 3, false, 0)
	defer c.cleanup()
	for attempt := 0; attempt < 5; attempt++ {
		l := c.checkOneLeader()
		c.mu.Lock()
		rf := c.rafts[l]
		c.mu.Unlock()
		_, term, ok := rf.Start(encInt(1))
		if !ok {
			continue
		}
		var wg sync.WaitGroup
		idx := make(chan int, 5)
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if index, t2, ok := rf.Start(encInt(100 + i)); ok && t2 == term {
					idx <- index
				}
			}(i)
		}
		wg.Wait()
		close(idx)
		ok2 := true
		for index := range idx {
			if c.wait(index, 3, term) == -1 {
				ok2 = false
			}
		}
		if ok2 {
			return
		}
	}
	t.Fatal("term changed too often")
}

func TestRejoin(t *testing.T) {
	c := newCluster(t, 3, false, 0)
	defer c.cleanup()
	c.one(101, 3, true)
	l1 := c.checkOneLeader()
	c.disconnect(l1)
	// Old leader accepts entries that can never commit.
	c.mu.Lock()
	rf := c.rafts[l1]
	c.mu.Unlock()
	rf.Start(encInt(102))
	rf.Start(encInt(103))
	rf.Start(encInt(104))

	c.one(103, 2, true)
	l2 := c.checkOneLeader()
	c.disconnect(l2)
	c.connect(l1)
	c.one(104, 2, true)
	c.connect(l2)
	c.one(105, 3, true)
}

// TestBackup checks the fast-backup path: a leader must overwrite a long
// run of conflicting, uncommitted entries quickly.
func TestBackup(t *testing.T) {
	c := newCluster(t, 5, false, 0)
	defer c.cleanup()
	c.one(rand.Int(), 5, true)

	l1 := c.checkOneLeader()
	c.disconnect((l1 + 2) % 5)
	c.disconnect((l1 + 3) % 5)
	c.disconnect((l1 + 4) % 5)
	c.mu.Lock()
	rf := c.rafts[l1]
	c.mu.Unlock()
	for i := 0; i < 50; i++ {
		rf.Start(encInt(rand.Int()))
	}
	time.Sleep(300 * time.Millisecond)
	c.disconnect(l1)
	c.disconnect((l1 + 1) % 5)

	c.connect((l1 + 2) % 5)
	c.connect((l1 + 3) % 5)
	c.connect((l1 + 4) % 5)
	for i := 0; i < 50; i++ {
		c.one(rand.Int(), 3, true)
	}

	l2 := c.checkOneLeader()
	other := (l1 + 2) % 5
	if l2 == other {
		other = (l2 + 1) % 5
	}
	c.disconnect(other)
	c.mu.Lock()
	rf = c.rafts[l2]
	c.mu.Unlock()
	for i := 0; i < 50; i++ {
		rf.Start(encInt(rand.Int()))
	}
	time.Sleep(300 * time.Millisecond)

	for i := 0; i < 5; i++ {
		c.disconnect(i)
	}
	c.connect(l1)
	c.connect((l1 + 1) % 5)
	c.connect(other)
	for i := 0; i < 50; i++ {
		c.one(rand.Int(), 3, true)
	}
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.one(rand.Int(), 5, true)
}

func TestPersistCrashRestart(t *testing.T) {
	c := newCluster(t, 3, false, 0)
	defer c.cleanup()
	c.one(11, 3, true)
	for i := 0; i < 3; i++ {
		c.startPeer(i)
	}
	for i := 0; i < 3; i++ {
		c.connect(i)
	}
	c.one(12, 3, true)

	l := c.checkOneLeader()
	c.startPeer(l)
	c.connect(l)
	c.one(13, 3, true)

	l = c.checkOneLeader()
	c.crash(l)
	c.one(14, 2, true)
	c.startPeer(l)
	c.connect(l)
	c.wait(4, 3, -1)
}

// TestFigure8Unreliable is the classic stress test: random crashes,
// partitions, drops and delays while clients keep submitting. Safety is
// checked continuously by the harness (no conflicting applies).
func TestFigure8Unreliable(t *testing.T) {
	if testing.Short() {
		t.Skip("long test")
	}
	c := newCluster(t, 5, true, 0)
	defer c.cleanup()
	c.one(rand.Intn(10000), 1, true)
	nup := 5
	for iters := 0; iters < 300; iters++ {
		if iters == 200 {
			c.net.SetUnreliable(false)
		}
		for i := 0; i < 5; i++ {
			c.mu.Lock()
			rf := c.rafts[i]
			c.mu.Unlock()
			if rf != nil && c.isConnected(i) {
				rf.Start(encInt(rand.Intn(10000)))
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Int63()%130) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Int63()%13) * time.Millisecond)
		}
		if l := rand.Intn(5); c.isConnected(l) && rand.Intn(1000) < 500 {
			c.disconnect(l)
			nup--
		}
		if nup < 3 {
			s := rand.Intn(5)
			if !c.isConnected(s) {
				c.connect(s)
				nup++
			}
		}
	}
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.net.SetUnreliable(false)
	c.one(rand.Intn(10000), 5, true)
}

func TestSnapshotBasic(t *testing.T) {
	c := newCluster(t, 3, false, 10)
	defer c.cleanup()
	for i := 0; i < 60; i++ {
		c.one(i, 3, true)
	}
	for i := 0; i < 3; i++ {
		c.mu.Lock()
		rf := c.rafts[i]
		c.mu.Unlock()
		st := rf.Status()
		if st.FirstIndex == 0 {
			t.Fatalf("peer %d never compacted its log", i)
		}
	}
}

// A follower that misses many entries must catch up via InstallSnapshot.
func TestSnapshotInstall(t *testing.T) {
	c := newCluster(t, 3, false, 10)
	defer c.cleanup()
	c.one(1, 3, true)
	l := c.checkOneLeader()
	victim := (l + 1) % 3
	c.disconnect(victim)
	for i := 0; i < 80; i++ {
		c.one(100+i, 2, true)
	}
	c.connect(victim)
	idx := c.one(9999, 3, true)
	c.wait(idx, 3, -1)
	c.mu.Lock()
	vm := c.rafts[victim].Metrics()
	lm := c.rafts[l].Metrics()
	c.mu.Unlock()
	if vm.SnapshotsInstalled == 0 {
		t.Fatalf("victim caught up without installing a snapshot")
	}
	if lm.SnapshotChunksSent <= lm.SnapshotsSent {
		t.Fatalf("expected multi-chunk transfers: %d chunks for %d snapshots", lm.SnapshotChunksSent, lm.SnapshotsSent)
	}
}

func TestSnapshotCrashRestartUnreliable(t *testing.T) {
	if testing.Short() {
		t.Skip("long test")
	}
	c := newCluster(t, 3, true, 10)
	defer c.cleanup()
	c.one(rand.Int()%1000, 1, true)
	for round := 0; round < 12; round++ {
		victim := rand.Intn(3)
		if round%3 == 0 {
			c.crash(victim)
		} else {
			c.disconnect(victim)
		}
		for i := 0; i < 11; i++ {
			c.one(rand.Int()%1000, 2, true)
		}
		if round%3 == 0 {
			c.startPeer(victim)
		}
		c.connect(victim)
		c.one(rand.Int()%1000, 3, true)
	}
}

// Audit R-M1: a delayed AppendEntries carrying a shorter prefix but a
// higher LeaderCommit lowered commitIndex (10 -> 3 in the probe).
func TestCommitIndexNeverDecreases(t *testing.T) {
	net := NewNetwork()
	ch := make(chan ApplyMsg, 1000)
	cfg := DefaultConfig()
	cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax = time.Hour, 2*time.Hour // stay follower
	rf := New(0, 3, net.Endpoint(0), NewMemoryStorage(), ch, cfg)
	defer rf.Kill()
	var es []LogEntry
	for i := 1; i <= 10; i++ {
		es = append(es, LogEntry{Term: 1, Index: i, Command: []byte{byte(i)}})
	}
	rf.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 0, Entries: es, LeaderCommit: 10})
	if c := rf.Status().CommitIndex; c != 10 {
		t.Fatalf("commitIndex %d, want 10", c)
	}
	// Reordered RPC from the same leader: prefix through 3, commit 12.
	r := rf.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 12})
	if !r.Success {
		t.Fatalf("stale-but-consistent RPC rejected")
	}
	if c := rf.Status().CommitIndex; c != 10 {
		t.Fatalf("commitIndex moved to %d after a stale RPC, want 10", c)
	}
}

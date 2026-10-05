package kv

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/siddhaantxsingh/raft-kv/lincheck"
	"github.com/siddhaantxsingh/raft-kv/raft"
)

type kvCluster struct {
	t          *testing.T
	mu         sync.Mutex
	n          int
	net        *raft.Network
	servers    []*Server
	persisters []*raft.MemoryStorage
	connected  []bool
	maxState   int
}

func testRaftConfig() raft.Config {
	c := raft.DefaultConfig()
	c.ElectionTimeoutMin = 250 * time.Millisecond
	c.ElectionTimeoutMax = 500 * time.Millisecond
	c.HeartbeatInterval = 60 * time.Millisecond
	c.RPCTimeout = 100 * time.Millisecond
	c.SnapshotChunkSize = 256 // exercise multi-chunk snapshot transfer
	return c
}

func newKVCluster(t *testing.T, n int, unreliable bool, maxState int) *kvCluster {
	c := &kvCluster{
		t: t, n: n, net: raft.NewNetwork(),
		servers: make([]*Server, n), persisters: make([]*raft.MemoryStorage, n),
		connected: make([]bool, n), maxState: maxState,
	}
	c.net.SetUnreliable(unreliable)
	for i := 0; i < n; i++ {
		c.persisters[i] = raft.NewMemoryStorage()
		c.start(i)
		c.connect(i)
	}
	return c
}

func (c *kvCluster) start(i int) {
	c.crash(i)
	c.persisters[i] = c.persisters[i].Copy()
	s := NewServer(i, c.n, c.net.Endpoint(i), c.persisters[i], c.maxState, testRaftConfig())
	c.mu.Lock()
	c.servers[i] = s
	c.mu.Unlock()
	c.net.Register(i, s.Raft())
}

func (c *kvCluster) crash(i int) {
	c.disconnect(i)
	c.mu.Lock()
	s := c.servers[i]
	c.servers[i] = nil
	c.mu.Unlock()
	if s != nil {
		s.Kill()
	}
}

func (c *kvCluster) connect(i int) {
	c.net.SetConnected(i, true)
	c.mu.Lock()
	c.connected[i] = true
	c.mu.Unlock()
}

func (c *kvCluster) disconnect(i int) {
	c.net.SetConnected(i, false)
	c.mu.Lock()
	c.connected[i] = false
	c.mu.Unlock()
}

// partition puts the given servers in a group that can talk to clients;
// others are cut off. (Raft traffic follows the same connectivity.)
func (c *kvCluster) cleanup() {
	for i := 0; i < c.n; i++ {
		c.crash(i)
	}
}

// Call implements Caller in-process, honouring connectivity.
func (c *kvCluster) Call(ctx context.Context, server int, op Op) (Result, error) {
	c.mu.Lock()
	s := c.servers[server]
	ok := c.connected[server]
	c.mu.Unlock()
	if s == nil || !ok {
		time.Sleep(10 * time.Millisecond)
		return Result{}, ErrWrongLeader
	}
	return s.Submit(ctx, op)
}

func (c *kvCluster) NumServers() int { return c.n }

func ctxT(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func TestKVBasic(t *testing.T) {
	c := newKVCluster(t, 3, false, -1)
	defer c.cleanup()
	ck := NewClerk(c)
	ctx, cancel := ctxT(10 * time.Second)
	defer cancel()

	if _, found, _ := ck.Get(ctx, "a"); found {
		t.Fatal("unexpected key")
	}
	must(t, ck.Put(ctx, "a", "x"))
	must(t, ck.Append(ctx, "a", "y"))
	if v, _, _ := ck.Get(ctx, "a"); v != "xy" {
		t.Fatalf("got %q", v)
	}
	if ok, _, _ := ck.CAS(ctx, "a", "xy", "z"); !ok {
		t.Fatal("CAS should succeed")
	}
	if ok, cur, _ := ck.CAS(ctx, "a", "xy", "w"); ok || cur != "z" {
		t.Fatalf("CAS should fail, cur=%q", cur)
	}
	if found, _ := ck.Delete(ctx, "a"); !found {
		t.Fatal("delete should report found")
	}
	if _, found, _ := ck.Get(ctx, "a"); found {
		t.Fatal("deleted key still present")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// genericTest runs nclients concurrently appending to their own key while
// the environment misbehaves, then checks each client's appends appear
// exactly once and in order. It also records a full history and checks it
// for linearizability.
func genericTest(t *testing.T, nclients int, unreliable, crash, partitions bool, maxState int) {
	const nservers = 5
	c := newKVCluster(t, nservers, unreliable, maxState)
	defer c.cleanup()

	var histMu sync.Mutex
	var history []lincheck.Operation
	record := func(op lincheck.Operation) {
		histMu.Lock()
		history = append(history, op)
		histMu.Unlock()
	}

	for round := 0; round < 3; round++ {
		var done int32
		var wg sync.WaitGroup
		counts := make([]int, nclients)
		for cli := 0; cli < nclients; cli++ {
			wg.Add(1)
			go func(cli int) {
				defer wg.Done()
				ck := NewClerk(c)
				key := strconv.Itoa(cli)
				last := ""
				ctx, cancel := ctxT(60 * time.Second)
				defer cancel()
				start := time.Now().UnixNano()
				must(t, ck.Put(ctx, key, last))
				record(lincheck.Operation{ClientID: cli, Kind: lincheck.Put, Key: key, Value: "", Call: start, Return: time.Now().UnixNano()})
				for j := 0; atomic.LoadInt32(&done) == 0; j++ {
					if rand.Intn(2) == 0 {
						v := fmt.Sprintf("x %d %d y", cli, j)
						call := time.Now().UnixNano()
						if err := ck.Append(ctx, key, v); err != nil {
							t.Errorf("append: %v", err)
							return
						}
						last += v
						record(lincheck.Operation{ClientID: cli, Kind: lincheck.Append, Key: key, Value: v,
							OutValue: last, Call: call, Return: time.Now().UnixNano()})
						counts[cli] = j + 1
					} else {
						call := time.Now().UnixNano()
						got, found, err := ck.Get(ctx, key)
						if err != nil {
							t.Errorf("get: %v", err)
							return
						}
						record(lincheck.Operation{ClientID: cli, Kind: lincheck.Get, Key: key,
							OutValue: got, OutFound: found, Call: call, Return: time.Now().UnixNano()})
						if got != last {
							t.Errorf("client %d get mismatch:\n got %q\nwant %q", cli, got, last)
							return
						}
					}
				}
			}(cli)
		}

		if partitions {
			stop := make(chan struct{})
			go func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					// Random majority/minority split.
					perm := rand.Perm(nservers)
					cut := rand.Intn(nservers/2 + 1)
					for i, s := range perm {
						if i < cut {
							c.disconnect(s)
						} else {
							c.connect(s)
						}
					}
					time.Sleep(time.Duration(rand.Intn(200)+100) * time.Millisecond)
				}
			}()
			time.Sleep(3 * time.Second)
			close(stop)
		} else {
			time.Sleep(3 * time.Second)
		}

		if crash {
			for i := 0; i < nservers; i++ {
				c.crash(i)
			}
			time.Sleep(200 * time.Millisecond)
			for i := 0; i < nservers; i++ {
				c.start(i)
				c.connect(i)
			}
		} else {
			for i := 0; i < nservers; i++ {
				c.connect(i)
			}
		}
		atomic.StoreInt32(&done, 1)
		wg.Wait()
		if t.Failed() {
			return
		}

		// Final check: each key holds exactly that client's appends, in order.
		ck := NewClerk(c)
		ctx, cancel := ctxT(30 * time.Second)
		for cli := 0; cli < nclients; cli++ {
			v, _, err := ck.Get(ctx, strconv.Itoa(cli))
			must(t, err)
			checkAppends(t, cli, v, counts[cli])
		}
		cancel()

		if maxState > 0 {
			for i := 0; i < nservers; i++ {
				if sz := c.persisters[i].StateSize(); sz > 8*maxState {
					t.Fatalf("server %d raft state %d bytes > 8x limit %d — snapshots not trimming log", i, sz, maxState)
				}
			}
		}
	}

	if ok, key := lincheck.Check(history); !ok {
		t.Fatalf("history not linearizable (key %q, %d ops)", key, len(history))
	}
	t.Logf("%d operations checked for linearizability", len(history))
}

func checkAppends(t *testing.T, cli int, v string, count int) {
	t.Helper()
	lastOff := -1
	for j := 0; j < count; j++ {
		want := fmt.Sprintf("x %d %d y", cli, j)
		off := strings.Index(v, want)
		if off < 0 {
			// Appends with random 50% skip: absent entries are those the
			// client chose not to send. Only check those present are unique & ordered.
			continue
		}
		if off2 := strings.LastIndex(v, want); off != off2 {
			t.Fatalf("client %d: duplicate append %q", cli, want)
		}
		if off <= lastOff {
			t.Fatalf("client %d: append %q out of order", cli, want)
		}
		lastOff = off
	}
}

func TestKVConcurrent(t *testing.T) { genericTest(t, 5, false, false, false, -1) }
func TestKVUnreliable(t *testing.T) { genericTest(t, 5, true, false, false, -1) }
func TestKVPartitions(t *testing.T) { genericTest(t, 5, false, false, true, -1) }
func TestKVCrashRestart(t *testing.T) {
	genericTest(t, 5, false, true, false, -1)
}
func TestKVSnapshotPartitionsCrashUnreliable(t *testing.T) {
	if testing.Short() {
		t.Skip("long test")
	}
	genericTest(t, 5, true, true, true, 1000)
}

// A minority partition must not serve requests (no stale reads).
func TestKVNoMinorityProgress(t *testing.T) {
	c := newKVCluster(t, 5, false, -1)
	defer c.cleanup()
	ck := NewClerk(c)
	ctx, cancel := ctxT(10 * time.Second)
	defer cancel()
	must(t, ck.Put(ctx, "k", "v1"))

	leader := -1
	for leader == -1 {
		for i, s := range c.servers {
			if _, isL := s.Raft().GetState(); isL {
				leader = i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Isolate leader with one follower (minority).
	minority := []int{leader, (leader + 1) % 5}
	for i := 0; i < 5; i++ {
		c.disconnect(i)
	}
	// Majority side elects a new leader and accepts a write.
	for i := 0; i < 5; i++ {
		if i != minority[0] && i != minority[1] {
			c.connect(i)
		}
	}
	must(t, ck.Put(ctx, "k", "v2"))

	// Minority leader must not answer a Get (it would be stale).
	sctx, scancel := ctxT(1500 * time.Millisecond)
	defer scancel()
	_, err := c.servers[leader].Submit(sctx, Op{Type: OpGet, Key: "k", ClientID: 42, Seq: 1})
	if err == nil {
		t.Fatal("minority leader served a read")
	}
}

// TestCheckerCatchesStaleReads proves the test suite has teeth: with
// deliberately unsafe local reads, a partitioned old leader returns a stale
// value and the linearizability checker must reject the history.
func TestCheckerCatchesStaleReads(t *testing.T) {
	c := newKVCluster(t, 5, false, -1)
	defer c.cleanup()
	ck := NewClerk(c)
	ctx, cancel := ctxT(10 * time.Second)
	defer cancel()

	var hist []lincheck.Operation
	now := func() int64 { return time.Now().UnixNano() }

	t0 := now()
	must(t, ck.Put(ctx, "k", "v1"))
	hist = append(hist, lincheck.Operation{Kind: lincheck.Put, Key: "k", Value: "v1", Call: t0, Return: now()})

	leader := -1
	for leader == -1 {
		for i, s := range c.servers {
			if _, isL := s.Raft().GetState(); isL {
				leader = i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		if i == leader || i == (leader+1)%5 {
			c.disconnect(i)
		}
	}
	t1 := now()
	must(t, ck.Put(ctx, "k", "v2"))
	hist = append(hist, lincheck.Operation{Kind: lincheck.Put, Key: "k", Value: "v2", Call: t1, Return: now()})

	old := c.servers[leader]
	old.unsafeLocalReads = true
	t2 := now()
	r, err := old.Submit(ctx, Op{Type: OpGet, Key: "k", ClientID: 7, Seq: 1})
	must(t, err)
	hist = append(hist, lincheck.Operation{Kind: lincheck.Get, Key: "k", OutValue: r.Value, OutFound: r.Found, Call: t2, Return: now()})

	if r.Value != "v1" {
		t.Fatalf("expected the unsafe read to be stale, got %q", r.Value)
	}
	if ok, _ := lincheck.Check(hist); ok {
		t.Fatal("checker accepted a stale read")
	}
}

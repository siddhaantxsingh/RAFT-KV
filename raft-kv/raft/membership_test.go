package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"
)

func (c *cluster) peer(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

// changeWithRetry runs a membership change on whichever peer is leader,
// retrying through leader changes and transient refusals.
func (c *cluster) changeWithRetry(what string, fn func(rf *Raft, ctx context.Context) error) {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			rf := c.peer(i)
			if rf == nil || !c.isConnected(i) {
				continue
			}
			if _, lead := rf.GetState(); !lead {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := fn(rf, ctx)
			cancel()
			if err == nil || errors.Is(err, ErrConfigNoop) {
				return
			}
			last = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("%s: did not complete: %v", what, last)
}

func (c *cluster) addServer(id int) {
	c.t.Helper()
	c.changeWithRetry(fmt.Sprintf("add %d", id), func(rf *Raft, ctx context.Context) error {
		return rf.AddServer(ctx, id, "")
	})
}

func (c *cluster) removeServer(id int) {
	c.t.Helper()
	c.changeWithRetry(fmt.Sprintf("remove %d", id), func(rf *Raft, ctx context.Context) error {
		return rf.RemoveServer(ctx, id)
	})
}

// waitVoters waits until every listed live peer's latest configuration
// has exactly the given voters.
func (c *cluster) waitVoters(peers, voters []int) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		bad := ""
		for _, i := range peers {
			rf := c.peer(i)
			if rf == nil {
				continue
			}
			conf, _ := rf.Configuration()
			if got := conf.Voters(); !slices.Equal(got, voters) {
				bad = fmt.Sprintf("peer %d has voters %v, want %v", i, got, voters)
			}
		}
		if bad == "" {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatal(bad)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Growing 3 -> 5: new servers catch up as learners, are promoted, and
// then really vote: with two original servers gone, the new ones are
// needed for a majority.
func TestMembershipAddServers(t *testing.T) {
	c := newClusterBoot(t, 5, 3, false, 0)
	defer c.cleanup()
	for i := 0; i < 20; i++ {
		c.one(100+i, 3, true)
	}
	c.addServer(3)
	c.waitVoters([]int{0, 1, 2, 3}, []int{0, 1, 2, 3})
	c.addServer(4)
	c.waitVoters([]int{0, 1, 2, 3, 4}, []int{0, 1, 2, 3, 4})

	// The new servers have the whole log.
	idx := c.one(500, 5, true)
	for i := 1; i <= idx; i++ {
		if n, _ := c.nCommitted(i); n != 5 {
			t.Fatalf("index %d applied on %d peers, want 5", i, n)
		}
	}
	// Two of the original three disconnected: the new voters form the majority.
	leader := c.checkOneLeader()
	var gone []int
	for _, i := range []int{0, 1, 2} {
		if i != leader && len(gone) < 2 {
			gone = append(gone, i)
		}
	}
	if len(gone) < 2 { // leader is a new server; drop two originals anyway
		gone = []int{0, 1}
	}
	for _, i := range gone {
		c.disconnect(i)
	}
	c.one(501, 3, true)
	for _, i := range gone {
		c.connect(i)
	}
	c.one(502, 5, true)
}

// Shrinking 5 -> 3, including removing the leader itself: the old leader
// steps down after the change commits, never leads again, and does not
// disrupt the remaining cluster.
func TestMembershipRemoveFollowerAndLeader(t *testing.T) {
	c := newCluster(t, 5, false, 0)
	defer c.cleanup()
	c.one(1, 5, true)
	leader := c.checkOneLeader()
	victim := (leader + 1) % 5
	c.removeServer(victim)
	var rest []int
	for i := 0; i < 5; i++ {
		if i != victim {
			rest = append(rest, i)
		}
	}
	c.waitVoters(rest, rest)
	c.one(2, 4, true)

	// Remove the leader itself.
	c.removeServer(leader)
	var rest2 []int
	for _, i := range rest {
		if i != leader {
			rest2 = append(rest2, i)
		}
	}
	c.waitVoters(rest2, rest2)
	newLeader := c.checkOneLeader()
	if newLeader == leader || newLeader == victim {
		t.Fatalf("removed server %d is leader", newLeader)
	}
	c.one(3, 3, true)

	// Removed servers stay connected but must not disrupt: no new terms.
	term0, _ := c.peer(newLeader).GetState()
	for i := 0; i < 10; i++ {
		c.one(10+i, 3, true)
		time.Sleep(100 * time.Millisecond)
	}
	if term1, lead := c.peer(newLeader).GetState(); term1 != term0 || !lead {
		t.Fatalf("leader disrupted: term %d -> %d, still leader=%v", term0, term1, lead)
	}
	for _, i := range []int{leader, victim} {
		if _, lead := c.peer(i).GetState(); lead {
			t.Fatalf("removed server %d became leader", i)
		}
	}
	// A 3-voter cluster tolerates one failure.
	c.disconnect(rest2[0])
	c.one(50, 2, true)
	c.connect(rest2[0])
}

// Only one change may be uncommitted at a time, and a new leader must
// commit an entry of its term first.
func TestMembershipOneChangeAtATime(t *testing.T) {
	c := newClusterBoot(t, 5, 3, false, 0)
	defer c.cleanup()
	c.one(1, 3, true)
	leader := c.checkOneLeader()
	for i := 0; i < 3; i++ {
		if i != leader {
			c.disconnect(i)
		}
	}
	rf := c.peer(leader)
	if _, _, err := rf.proposeConfig(func(conf *Configuration) error {
		conf.set(Member{ID: 3, Learner: true})
		return nil
	}); err != nil {
		t.Fatalf("first change: %v", err)
	}
	if _, _, err := rf.proposeConfig(func(conf *Configuration) error {
		conf.set(Member{ID: 4, Learner: true})
		return nil
	}); !errors.Is(err, ErrConfigInProgress) {
		t.Fatalf("second concurrent change: got %v, want ErrConfigInProgress", err)
	}
	// A change of more than one voter at once is refused.
	for i := 0; i < 3; i++ {
		c.connect(i)
	}
	c.one(2, 3, true)
	ldr := c.peer(c.checkOneLeader())
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, err := ldr.proposeConfig(func(conf *Configuration) error {
			conf.set(Member{ID: 3})
			conf.set(Member{ID: 4})
			return nil
		})
		if err == nil {
			t.Fatal("a change of two voters at once was accepted")
		}
		if strings.Contains(err.Error(), "more than one voter") {
			break // refused for the right reason
		}
		if time.Now().After(deadline) {
			t.Fatalf("two-voter change: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A configuration takes effect when appended; if that uncommitted entry is
// overwritten by a new leader, the server falls back to the previous one.
func TestMembershipUncommittedConfigReverts(t *testing.T) {
	c := newCluster(t, 3, false, 0)
	defer c.cleanup()
	c.one(1, 3, true)
	old := c.checkOneLeader()
	c.disconnect(old)
	victim := (old + 1) % 3
	if _, _, err := c.peer(old).proposeConfig(func(conf *Configuration) error {
		conf.remove(victim)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if conf, _ := c.peer(old).Configuration(); conf.Contains(victim) {
		t.Fatalf("config did not take effect on append: %v", conf)
	}
	// The majority side moves on and overwrites the old leader's tail.
	for i := 0; i < 5; i++ {
		c.one(10+i, 2, true)
	}
	c.connect(old)
	c.one(20, 3, true)
	c.waitVoters([]int{0, 1, 2}, []int{0, 1, 2})
}

// Configurations survive restarts (WAL), and compaction (snapshot config).
func TestMembershipPersistsAcrossRestartAndSnapshot(t *testing.T) {
	c := newClusterBoot(t, 5, 3, false, 10)
	defer c.cleanup()
	for i := 0; i < 25; i++ {
		c.one(i+1, 3, true)
	}
	// Server 3 joins after compaction: it must catch up via InstallSnapshot,
	// which carries the configuration.
	c.addServer(3)
	c.addServer(4)
	c.removeServer(0)
	want := []int{1, 2, 3, 4}
	c.waitVoters([]int{1, 2, 3, 4}, want)
	if n := c.peer(3).Metrics().SnapshotsInstalled; n == 0 {
		t.Fatal("server 3 caught up without InstallSnapshot; test does not cover the snapshot path")
	}
	for i := 0; i < 25; i++ { // push the config entries into snapshots
		c.one(100+i, 4, true)
	}
	for i := 0; i < 5; i++ {
		c.startPeer(i)
		c.connect(i)
	}
	c.waitVoters([]int{1, 2, 3, 4}, want)
	for i := 1; i < 5; i++ {
		if fi := c.peer(i).Status().FirstIndex; fi < 30 {
			t.Fatalf("peer %d did not compact (first index %d)", i, fi)
		}
	}
	c.one(999, 4, true)
	if _, lead := c.peer(0).GetState(); lead {
		t.Fatal("removed server 0 is leader after restart")
	}
}

// The leader crashes right after proposing a change. Whatever the new
// leader decides, all servers converge on one configuration and the
// cluster keeps committing. Repeated with an unreliable network.
func TestMembershipLeaderCrashDuringChange(t *testing.T) {
	c := newClusterBoot(t, 5, 3, true, 0)
	defer c.cleanup()
	c.one(1, 3, true)
	for round := 0; round < 4; round++ {
		leader := c.checkOneLeader()
		conf, _ := c.peer(leader).Configuration()
		var target int
		var change func(*Configuration) error
		if len(conf.Voters()) <= 3 {
			for target = 0; conf.Contains(target); target++ {
			}
			change = func(cf *Configuration) error { cf.set(Member{ID: target, Learner: true}); return nil }
		} else {
			target = conf.Voters()[0]
			if target == leader {
				target = conf.Voters()[1]
			}
			change = func(cf *Configuration) error { cf.remove(target); return nil }
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, _, err := c.peer(leader).proposeConfig(change); err == nil {
				break
			} else if time.Now().After(deadline) {
				t.Fatalf("propose: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
		c.crash(leader)
		c.one(100+round, 2, true)
		c.startPeer(leader)
		c.connect(leader)
		c.one(200+round, 2, true)
		// Promote any learner so the next round changes voters again.
		conf, _ = c.peer(c.checkOneLeader()).Configuration()
		for _, m := range conf.Members {
			if m.Learner {
				c.changeWithRetry("promote", func(rf *Raft, ctx context.Context) error {
					return rf.PromoteLearner(ctx, m.ID)
				})
			}
		}
		c.convergeConfig()
	}
}

// convergeConfig waits until every member agrees on the leader's config.
func (c *cluster) convergeConfig() {
	c.t.Helper()
	conf, _ := c.peer(c.checkOneLeader()).Configuration()
	var members []int
	for _, m := range conf.Members {
		members = append(members, m.ID)
	}
	c.one(rand.Int()%1000, len(conf.Voters())/2+1, true)
	c.waitVoters(members, conf.Voters())
}

// Randomised: membership changes interleaved with partitions, crashes and
// an unreliable network. The harness checks that no two servers ever apply
// different commands at one index and that each term has one leader.
func TestMembershipChurnUnderPartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed %d", seed)
	c := newClusterBoot(t, 6, 3, true, 25)
	defer c.cleanup()
	cmd := 1
	for round := 0; round < 12; round++ {
		leader := c.checkOneLeader()
		conf, _ := c.peer(leader).Configuration()
		voters := conf.Voters()
		// Partition one random voter (never a majority) during the change.
		cut := voters[rng.Intn(len(voters))]
		if len(voters) >= 3 {
			c.disconnect(cut)
		}
		if len(voters) < 5 && (len(voters) <= 3 || rng.Intn(2) == 0) {
			for id := 0; id < c.n; id++ {
				if !conf.Contains(id) {
					c.addServer(id)
					break
				}
			}
		} else {
			c.removeServer(voters[rng.Intn(len(voters))])
		}
		for k := 0; k < 3; k++ {
			c.one(cmd, 1, true)
			cmd++
		}
		c.connect(cut)
		if rng.Intn(3) == 0 { // crash and restart a random server
			v := rng.Intn(c.n)
			c.startPeer(v)
			c.connect(v)
		}
		c.convergeConfig()
	}
	c.net.SetUnreliable(false)
	c.convergeConfig()
}

// Learners receive the log but count neither toward commit nor elections.
func TestMembershipLearnersDoNotVoteOrCommit(t *testing.T) {
	c := newClusterBoot(t, 5, 3, false, 0)
	defer c.cleanup()
	c.one(1, 3, true)
	for _, id := range []int{3, 4} {
		c.changeWithRetry("add learner", func(rf *Raft, ctx context.Context) error {
			return rf.AddLearner(ctx, id, "")
		})
	}
	c.one(2, 5, true) // learners apply committed entries
	leader := c.checkOneLeader()
	var others []int
	for i := 0; i < 3; i++ {
		if i != leader {
			others = append(others, i)
		}
	}
	// Leader + two learners are 3 of 5 servers but 1 of 3 voters.
	for _, i := range others {
		c.disconnect(i)
	}
	idx, _, ok := c.peer(leader).Start(encInt(777))
	if !ok {
		t.Fatal("leader refused Start")
	}
	time.Sleep(1 * time.Second)
	if n, _ := c.nCommitted(idx); n != 0 {
		t.Fatalf("entry committed with only learners' acknowledgements (%d peers applied)", n)
	}
	// With the leader also gone, learners must not elect one of themselves.
	c.disconnect(leader)
	time.Sleep(1500 * time.Millisecond)
	for _, id := range []int{3, 4} {
		if _, lead := c.peer(id).GetState(); lead {
			t.Fatalf("learner %d became leader", id)
		}
	}
	for i := 0; i < 3; i++ {
		c.connect(i)
	}
	c.one(3, 5, true)
}

// A learner added while unreachable, after the configuration entry has
// been compacted into a snapshot, learns its configuration from the
// InstallSnapshot RPC alone.
func TestMembershipLearnerJoinsFromSnapshot(t *testing.T) {
	c := newClusterBoot(t, 4, 3, false, 10)
	defer c.cleanup()
	c.one(1, 3, true)
	c.disconnect(3)
	c.changeWithRetry("add learner", func(rf *Raft, ctx context.Context) error {
		return rf.AddLearner(ctx, 3, "")
	})
	for i := 0; i < 30; i++ { // compact past the config entry
		c.one(10+i, 3, true)
	}
	c.connect(3)
	deadline := time.Now().Add(10 * time.Second)
	for {
		conf, _ := c.peer(3).Configuration()
		if m, ok := conf.find(3); ok && m.Learner && len(conf.Voters()) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("learner did not get its configuration from the snapshot: %v", conf)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if c.peer(3).Metrics().SnapshotsInstalled == 0 {
		t.Fatal("learner caught up without a snapshot; test does not cover the path")
	}
}

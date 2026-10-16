// Command kvchaos runs a kvserver cluster as real OS processes, drives a
// concurrent client workload against it, injects faults, and finally
// checks every client-observed operation for linearizability with the
// lincheck package and verifies the replicas converge.
//
//	go build -o bin/ ./cmd/... && bin/kvchaos -duration 60s
//	bin/kvchaos -nodes 5 -faults kill,pause,partition,oneway,bridge,loss,membership
//	bin/kvchaos -nodes 5 -failover-trials 20     # leader failover timing
//
// Faults (-faults, comma-separated):
//
//	kill        SIGKILL a replica (half the time the leader) and restart it
//	pause       SIGSTOP/SIGCONT a replica
//	partition   split the cluster into a majority and a minority side
//	oneway      drop one direction of one link (asymmetric partition)
//	bridge      two halves that can only talk through one node
//	loss        drop 20% of peer RPCs and add up to 20 ms delay
//	membership  remove a voter, wipe it, and add a fresh server under a new ID
//
// Network faults are injected by a reverse proxy in front of every replica
// (netem.go); the replicas are separate processes talking over TCP.
//
// Exit status 0 means: the history is linearizable and the replicas agree.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/siddhaantxsingh/raft-kv/kv"
	"github.com/siddhaantxsingh/raft-kv/lincheck"
)

type replica struct {
	id    int
	url   string // proxy URL: the address everyone uses
	real  string // the process's own listener
	cmd   *exec.Cmd
	args  []string
	log   *os.File
	bin   string
	alive bool // false once removed from the cluster for good
}

func (r *replica) start() error {
	cmd := exec.Command(r.bin, r.args...)
	cmd.Stdout, cmd.Stderr = r.log, r.log
	if err := cmd.Start(); err != nil {
		return err
	}
	r.cmd = cmd
	return nil
}

func (r *replica) signal(s syscall.Signal) {
	if r.cmd != nil && r.cmd.Process != nil {
		r.cmd.Process.Signal(s)
	}
}

func (r *replica) kill() {
	r.signal(syscall.SIGCONT) // a paused process must be able to die
	r.signal(syscall.SIGKILL)
	if r.cmd != nil {
		r.cmd.Wait()
	}
	r.cmd = nil
}

type status struct {
	ID          int
	Role        string
	CommitIndex int
	LastApplied int
	Term        int
	Leader      int
}

var httpc = &http.Client{Timeout: 500 * time.Millisecond}

func getStatus(url string) (status, error) {
	var s status
	resp, err := httpc.Get(url + "/status")
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

type summary struct {
	Store           string  `json:"store"`
	Nodes           int     `json:"nodes"`
	Faults          string  `json:"faults"`
	Seed            int64   `json:"seed"`
	DurationSec     float64 `json:"duration_s"`
	Ops             int     `json:"ops"`
	OK              int     `json:"ok"`
	Unknown         int     `json:"unknown_outcome"`
	Kills           int     `json:"sigkills"`
	LeaderKills     int     `json:"leader_sigkills"`
	Pauses          int     `json:"pauses"`
	Partitions      int     `json:"partitions"`
	OneWay          int     `json:"oneway_partitions"`
	Bridges         int     `json:"bridge_partitions"`
	LossPeriods     int     `json:"loss_periods"`
	Replacements    int     `json:"membership_replacements"`
	ReplaceFailures int     `json:"membership_change_failures"`
	DroppedRPCs     int64   `json:"dropped_peer_rpcs"`
	FinalMembers    []int   `json:"final_members"`
	Linearizable    bool    `json:"linearizable"`
	FailedKey       string  `json:"failed_key,omitempty"`
	Converged       bool    `json:"replicas_converged"`
	FinalCommit     []int   `json:"final_commit_index"`
	CheckerSeconds  float64 `json:"checker_s"`
}

type harness struct {
	bin, dir, store string
	snapBytes       int
	basePort        int
	net             *netem
	mu              sync.Mutex
	reps            map[int]*replica
	bootURLs        []string
	extra           []string
}

func (h *harness) proxyPort(id int) int { return h.basePort + id }
func (h *harness) realPort(id int) int  { return h.basePort + 100 + id }

// newReplica creates the proxy and process description for id. Bootstrap
// replicas get the bootstrap peer list; joiners get -join.
func (h *harness) newReplica(id int, join bool) *replica {
	p, err := h.net.startProxy(id, h.proxyPort(id), fmt.Sprintf("http://127.0.0.1:%d", h.realPort(id)))
	must(err)
	lf, err := os.Create(filepath.Join(h.dir, fmt.Sprintf("node-%d.log", id)))
	must(err)
	peers := h.bootURLs
	if join {
		// -peers must contain our own URL at index id; other entries are
		// ignored once the leader's configuration arrives.
		peers = nil
		for i := 0; i <= id; i++ {
			peers = append(peers, fmt.Sprintf("http://127.0.0.1:%d", h.proxyPort(i)))
		}
	}
	args := []string{
		"-id", fmt.Sprint(id), "-peers", strings.Join(peers, ","), "-data", h.dir,
		"-listen", fmt.Sprintf("127.0.0.1:%d", h.realPort(id)),
		"-snapshot-bytes", fmt.Sprint(h.snapBytes), "-store", h.store,
	}
	if join {
		args = append(args, "-join")
	}
	args = append(args, h.extra...)
	r := &replica{id: id, url: p.addr, real: fmt.Sprintf("http://127.0.0.1:%d", h.realPort(id)),
		bin: h.bin, log: lf, args: args, alive: true}
	h.mu.Lock()
	h.reps[id] = r
	h.mu.Unlock()
	return r
}

func (h *harness) live() []*replica {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*replica
	for _, r := range h.reps {
		if r.alive {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func ids(rs []*replica) []int {
	var out []int
	for _, r := range rs {
		out = append(out, r.id)
	}
	return out
}

// leader asks every live replica directly (bypassing the proxies) and
// returns the leader with the highest term.
func (h *harness) leader() (*replica, int) {
	var best *replica
	bestTerm := -1
	for _, r := range h.live() {
		if s, err := getStatus(r.real); err == nil && s.Role == "leader" && s.Term > bestTerm {
			best, bestTerm = r, s.Term
		}
	}
	return best, bestTerm
}

type memberList struct {
	Members []struct {
		ID      int
		Addr    string
		Learner bool
	} `json:"members"`
}

// admin sends a membership request to the current leader, retrying.
func (h *harness) admin(method, path, body string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		l, _ := h.leader()
		if l == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		req, _ := http.NewRequest(method, l.real+path, bytes.NewBufferString(body))
		c := http.Client{Timeout: 30 * time.Second}
		resp, err := c.Do(req)
		if err != nil {
			last = err
			continue
		}
		b, _ := readAll(resp)
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		last = fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
		if resp.StatusCode == http.StatusNotFound && method == http.MethodDelete {
			return nil // already removed by an earlier attempt
		}
		time.Sleep(200 * time.Millisecond)
	}
	return last
}

func readAll(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	var b bytes.Buffer
	_, err := b.ReadFrom(resp.Body)
	return b.Bytes(), err
}

func (h *harness) members() ([]int, error) {
	l, _ := h.leader()
	if l == nil {
		return nil, errors.New("no leader")
	}
	resp, err := httpc.Get(l.real + "/admin/members")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var ml memberList
	if err := json.NewDecoder(resp.Body).Decode(&ml); err != nil {
		return nil, err
	}
	var out []int
	for _, m := range ml.Members {
		out = append(out, m.ID)
	}
	return out, nil
}

func main() {
	bin := flag.String("bin", "./bin/kvserver", "kvserver binary")
	dir := flag.String("dir", "./data/chaos", "data/log directory (wiped)")
	basePort := flag.Int("base-port", 7100, "first proxy port (real listeners use base+100+id)")
	nodes := flag.Int("nodes", 3, "bootstrap cluster size")
	duration := flag.Duration("duration", 30*time.Second, "fault-injection duration")
	clients := flag.Int("clients", 8, "concurrent clients")
	keys := flag.Int("keys", 6, "number of keys (shared by all clients)")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	faults := flag.String("faults", "kill,pause", "comma-separated faults: kill,pause,partition,oneway,bridge,loss,membership")
	snapBytes := flag.Int("snapshot-bytes", 64<<10, "replica -snapshot-bytes (small = frequent snapshots)")
	out := flag.String("json", "", "write the summary as JSON to this file")
	store := flag.String("store", "memory", "replica -store (lsm needs a kvserver built with -tags lsm)")
	extra := flag.String("server-args", "", "extra kvserver flags (space-separated), e.g. the negative control -unsafe-stale-reads-for-testing")
	failoverTrials := flag.Int("failover-trials", 0, "instead of chaos: kill the leader N times and measure recovery")
	flag.Parse()
	rng := rand.New(rand.NewSource(*seed))
	enabled := map[string]bool{}
	for _, f := range strings.Split(*faults, ",") {
		enabled[strings.TrimSpace(f)] = true
	}

	os.RemoveAll(*dir)
	must(os.MkdirAll(*dir, 0o755))
	h := &harness{bin: *bin, dir: *dir, store: *store, snapBytes: *snapBytes, basePort: *basePort,
		net: newNetem(rng.Int63()), reps: map[int]*replica{}, extra: strings.Fields(*extra)}
	for i := 0; i < *nodes; i++ {
		h.bootURLs = append(h.bootURLs, fmt.Sprintf("http://127.0.0.1:%d", h.proxyPort(i)))
	}
	for i := 0; i < *nodes; i++ {
		must(h.newReplica(i, false).start())
	}
	defer func() {
		for _, r := range h.reps {
			r.kill()
		}
	}()
	time.Sleep(1500 * time.Millisecond)

	if *failoverTrials > 0 {
		measureFailover(h, *failoverTrials, *out)
		return
	}

	// ---- workload
	var histMu sync.Mutex
	var history []lincheck.Operation
	var stop atomic.Bool
	var wg sync.WaitGroup
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		crng := rand.New(rand.NewSource(rng.Int63()))
		go func(cli int) {
			defer wg.Done()
			ck := kv.NewClerk(kv.NewHTTPCaller(append([]string(nil), h.bootURLs...)))
			ck.RetryTimeout = 500 * time.Millisecond
			lastSeen := map[string]string{}
			for n := 0; !stop.Load(); n++ {
				key := fmt.Sprintf("k%d", crng.Intn(*keys))
				op := lincheck.Operation{ClientID: cli, Key: key}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				op.Call = time.Now().UnixNano()
				var err error
				switch p := crng.Intn(100); {
				case p < 40:
					op.Kind = lincheck.Get
					op.OutValue, op.OutFound, err = ck.Get(ctx, key)
					if err == nil {
						lastSeen[key] = op.OutValue
					}
				case p < 65:
					op.Kind, op.Value = lincheck.Put, fmt.Sprintf("p%d.%d", cli, n)
					err = ck.Put(ctx, key, op.Value)
				case p < 80:
					op.Kind, op.Value = lincheck.Append, fmt.Sprintf("+%d.%d", cli, n)
					op.OutValue, err = ck.AppendResult(ctx, key, op.Value)
				default:
					op.Kind, op.Expected, op.Value = lincheck.CAS, lastSeen[key], fmt.Sprintf("c%d.%d", cli, n)
					op.OutOK, op.OutValue, err = ck.CAS(ctx, key, op.Expected, op.Value)
					op.OutFound = op.OutValue != ""
				}
				cancel()
				op.Return = time.Now().UnixNano()
				if err != nil {
					// Timed out, session expired, ...: the write may or may
					// not have taken effect.
					op.Unknown = true
				}
				histMu.Lock()
				history = append(history, op)
				histMu.Unlock()
			}
		}(c)
	}

	// ---- faults
	sum := summary{Seed: *seed, Store: *store, Nodes: *nodes, Faults: *faults}
	var kinds []string
	for _, k := range []string{"kill", "pause", "partition", "oneway", "bridge", "loss", "membership"} {
		if enabled[k] {
			kinds = append(kinds, k)
		}
	}
	nextID := *nodes
	start := time.Now()
	for time.Since(start) < *duration && len(kinds) > 0 {
		time.Sleep(time.Duration(800+rng.Intn(1700)) * time.Millisecond)
		live := h.live()
		victim := live[rng.Intn(len(live))]
		leaderTarget := rng.Intn(2) == 0
		if l, _ := h.leader(); leaderTarget && l != nil {
			victim = l
		}
		hold := time.Duration(500+rng.Intn(2500)) * time.Millisecond
		switch kind := kinds[rng.Intn(len(kinds))]; kind {
		case "pause":
			fmt.Printf("chaos: pausing replica %d for %v\n", victim.id, hold)
			victim.signal(syscall.SIGSTOP)
			time.Sleep(hold)
			victim.signal(syscall.SIGCONT)
			sum.Pauses++
		case "kill":
			fmt.Printf("chaos: SIGKILL replica %d (leader-targeted=%v)\n", victim.id, leaderTarget)
			victim.kill()
			sum.Kills++
			if leaderTarget {
				sum.LeaderKills++
			}
			time.Sleep(time.Duration(200+rng.Intn(2000)) * time.Millisecond)
			must(victim.start())
		case "partition":
			all := ids(live)
			rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
			minority := all[:max(1, (len(all)-1)/2)]
			if leaderTarget && !slices.Contains(minority, victim.id) {
				minority[0] = victim.id // put the leader on the minority side
				minority = dedup(minority)
			}
			majority := without(ids(live), minority)
			fmt.Printf("chaos: partition %v | %v for %v\n", minority, majority, hold)
			h.net.cut(minority, majority)
			time.Sleep(hold)
			h.net.heal()
			sum.Partitions++
		case "oneway":
			others := without(ids(live), []int{victim.id})
			to := others[rng.Intn(len(others))]
			from, dst := victim.id, to
			if rng.Intn(2) == 0 {
				from, dst = to, victim.id
			}
			fmt.Printf("chaos: drop link %d -> %d for %v\n", from, dst, hold)
			h.net.cutOneWay(from, dst)
			time.Sleep(hold)
			h.net.heal()
			sum.OneWay++
		case "bridge":
			if len(live) < 3 {
				continue
			}
			all := without(ids(live), []int{victim.id})
			rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
			a, b := all[:len(all)/2], all[len(all)/2:]
			fmt.Printf("chaos: bridge %v <-> %d <-> %v for %v\n", a, victim.id, b, hold)
			h.net.cut(a, b)
			time.Sleep(hold)
			h.net.heal()
			sum.Bridges++
		case "loss":
			fmt.Printf("chaos: 20%% RPC loss + 20ms jitter for %v\n", hold)
			h.net.setLoss(0.2, 20*time.Millisecond)
			time.Sleep(hold)
			h.net.heal()
			sum.LossPeriods++
		case "membership":
			// Replace a server: remove it (it may be the leader), wipe it,
			// and add a fresh server under a new ID.
			old := victim
			fmt.Printf("chaos: membership: remove %d, add %d\n", old.id, nextID)
			if err := h.admin(http.MethodDelete, fmt.Sprintf("/admin/members/%d", old.id), "", 15*time.Second); err != nil {
				fmt.Printf("chaos: remove %d failed: %v\n", old.id, err)
				sum.ReplaceFailures++
				continue
			}
			old.kill()
			h.mu.Lock()
			old.alive = false
			h.mu.Unlock()
			os.RemoveAll(filepath.Join(h.dir, fmt.Sprintf("node-%d", old.id)))
			nr := h.newReplica(nextID, true)
			must(nr.start())
			body := fmt.Sprintf(`{"id":%d,"addr":%q}`, nextID, nr.url)
			if err := h.admin(http.MethodPost, "/admin/members", body, 30*time.Second); err != nil {
				fmt.Printf("chaos: add %d failed: %v\n", nextID, err)
				sum.ReplaceFailures++
			}
			nextID++
			sum.Replacements++
		}
	}
	h.net.heal()
	sum.DurationSec = time.Since(start).Seconds()
	time.Sleep(2 * time.Second) // let clients make progress on a healthy cluster
	stop.Store(true)
	wg.Wait()

	// ---- convergence: every member reaches the same applied index
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		members, err := h.members()
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		sum.FinalMembers = members
		var commits []int
		agree := true
		for _, id := range members {
			h.mu.Lock()
			r := h.reps[id]
			h.mu.Unlock()
			if r == nil {
				agree = false
				break
			}
			s, err := getStatus(r.real)
			if err != nil {
				agree = false
				break
			}
			commits = append(commits, s.CommitIndex)
			if s.LastApplied != s.CommitIndex || s.CommitIndex != commits[0] {
				agree = false
			}
		}
		sum.FinalCommit = commits
		if agree && len(commits) == len(members) {
			sum.Converged = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	sum.DroppedRPCs = h.net.dropped

	// ---- linearizability
	sum.Ops = len(history)
	for _, op := range history {
		if op.Unknown {
			sum.Unknown++
		} else {
			sum.OK++
		}
	}
	t := time.Now()
	sum.Linearizable, sum.FailedKey = lincheck.Check(history)
	sum.CheckerSeconds = time.Since(t).Seconds()

	b, _ := json.MarshalIndent(sum, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		os.WriteFile(*out, b, 0o644)
	}
	if !sum.Linearizable || !sum.Converged || sum.OK == 0 {
		fmt.Println("CHAOS FAIL")
		os.Exit(1)
	}
	fmt.Println("CHAOS PASS")
}

func dedup(xs []int) []int {
	var out []int
	for _, x := range xs {
		if !slices.Contains(out, x) {
			out = append(out, x)
		}
	}
	return out
}

func without(xs, drop []int) []int {
	var out []int
	for _, x := range xs {
		if !slices.Contains(drop, x) {
			out = append(out, x)
		}
	}
	return out
}

func must(err error) {
	if err != nil && !errors.Is(err, os.ErrExist) {
		fmt.Fprintln(os.Stderr, "kvchaos:", err)
		os.Exit(2)
	}
}

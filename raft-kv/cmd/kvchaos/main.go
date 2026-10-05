// Command kvchaos runs a 3-replica kvserver cluster as real OS processes,
// drives a concurrent client workload against it, injects process faults
// (SIGKILL + restart, SIGSTOP/SIGCONT pauses, leader-targeted or random),
// and finally checks every client-observed operation for linearizability
// with the lincheck package and verifies the replicas converge.
//
//	go build -o bin/ ./cmd/... && bin/kvchaos -duration 60s
//
// Exit status 0 means: the history is linearizable and the replicas agree.
// It does not inject network partitions or disk faults (see docs).
package main

import (
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/siddhaantxsingh/raft-kv/kv"
	"github.com/siddhaantxsingh/raft-kv/lincheck"
)

type replica struct {
	id   int
	url  string
	cmd  *exec.Cmd
	args []string
	log  *os.File
	bin  string
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
	Role        string
	CommitIndex int
	LastApplied int
	Term        int
}

func getStatus(url string) (status, error) {
	var s status
	c := http.Client{Timeout: 500 * time.Millisecond}
	resp, err := c.Get(url + "/status")
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

type summary struct {
	Store          string  `json:"store"`
	Seed           int64   `json:"seed"`
	DurationSec    float64 `json:"duration_s"`
	Ops            int     `json:"ops"`
	OK             int     `json:"ok"`
	Unknown        int     `json:"unknown_outcome"`
	Kills          int     `json:"sigkills"`
	LeaderKills    int     `json:"leader_sigkills"`
	Pauses         int     `json:"pauses"`
	Linearizable   bool    `json:"linearizable"`
	FailedKey      string  `json:"failed_key,omitempty"`
	Converged      bool    `json:"replicas_converged"`
	FinalCommit    []int   `json:"final_commit_index"`
	CheckerSeconds float64 `json:"checker_s"`
}

func main() {
	bin := flag.String("bin", "./bin/kvserver", "kvserver binary")
	dir := flag.String("dir", "./data/chaos", "data/log directory (wiped)")
	basePort := flag.Int("base-port", 7100, "first replica port")
	duration := flag.Duration("duration", 30*time.Second, "fault-injection duration")
	clients := flag.Int("clients", 8, "concurrent clients")
	keys := flag.Int("keys", 6, "number of keys (shared by all clients)")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	pause := flag.Bool("pause", true, "also inject SIGSTOP/SIGCONT pauses")
	snapBytes := flag.Int("snapshot-bytes", 64<<10, "replica -snapshot-bytes (small = frequent snapshots)")
	out := flag.String("json", "", "write the summary as JSON to this file")
	store := flag.String("store", "memory", "replica -store (lsm needs a kvserver built with -tags lsm)")
	flag.Parse()
	rng := rand.New(rand.NewSource(*seed))

	os.RemoveAll(*dir)
	must(os.MkdirAll(*dir, 0o755))
	var urls []string
	for i := 0; i < 3; i++ {
		urls = append(urls, fmt.Sprintf("http://127.0.0.1:%d", *basePort+i))
	}
	reps := make([]*replica, 3)
	for i := range reps {
		lf, err := os.Create(filepath.Join(*dir, fmt.Sprintf("node-%d.log", i)))
		must(err)
		reps[i] = &replica{id: i, url: urls[i], bin: *bin, log: lf, args: []string{
			"-id", fmt.Sprint(i), "-peers", strings.Join(urls, ","), "-data", *dir,
			"-snapshot-bytes", fmt.Sprint(*snapBytes), "-store", *store,
		}}
		must(reps[i].start())
	}
	defer func() {
		for _, r := range reps {
			r.kill()
		}
	}()
	time.Sleep(1500 * time.Millisecond)

	// ---- workload
	var histMu sync.Mutex
	var history []lincheck.Operation
	var stop atomic.Bool
	var wg sync.WaitGroup
	caller := kv.NewHTTPCaller(urls)
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		crng := rand.New(rand.NewSource(rng.Int63()))
		go func(cli int) {
			defer wg.Done()
			ck := kv.NewClerk(caller)
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
					var r string
					r, err = appendRet(ctx, ck, key, op.Value)
					op.OutValue = r
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
	sum := summary{Seed: *seed, Store: *store}
	start := time.Now()
	for time.Since(start) < *duration {
		time.Sleep(time.Duration(800+rng.Intn(1700)) * time.Millisecond)
		victim := rng.Intn(3)
		leaderTarget := rng.Intn(2) == 0
		if leaderTarget {
			for i, r := range reps {
				if s, err := getStatus(r.url); err == nil && s.Role == "leader" {
					victim = i
				}
			}
		}
		if *pause && rng.Intn(3) == 0 {
			fmt.Printf("chaos: pausing replica %d\n", victim)
			reps[victim].signal(syscall.SIGSTOP)
			time.Sleep(time.Duration(500+rng.Intn(2500)) * time.Millisecond)
			reps[victim].signal(syscall.SIGCONT)
			sum.Pauses++
			continue
		}
		fmt.Printf("chaos: SIGKILL replica %d (leader-targeted=%v)\n", victim, leaderTarget)
		reps[victim].kill()
		sum.Kills++
		if leaderTarget {
			sum.LeaderKills++
		}
		time.Sleep(time.Duration(200+rng.Intn(2000)) * time.Millisecond)
		must(reps[victim].start())
	}
	sum.DurationSec = time.Since(start).Seconds()
	time.Sleep(2 * time.Second) // let clients make progress on a healthy cluster
	stop.Store(true)
	wg.Wait()

	// ---- convergence: all replicas reach the same applied index
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var commits []int
		agree := true
		for _, r := range reps {
			s, err := getStatus(r.url)
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
		if agree && len(commits) == 3 {
			sum.Converged = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

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

// appendRet appends and returns the new value via the HTTP API's append
// result, which the Clerk API does not expose.
func appendRet(ctx context.Context, ck *kv.Clerk, key, v string) (string, error) {
	return ck.AppendResult(ctx, key, v)
}

func must(err error) {
	if err != nil && !errors.Is(err, os.ErrExist) {
		fmt.Fprintln(os.Stderr, "kvchaos:", err)
		os.Exit(2)
	}
}

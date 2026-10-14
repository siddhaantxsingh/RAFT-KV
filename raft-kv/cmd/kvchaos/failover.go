package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

// Default Raft timing in kvserver (raft.DefaultConfig).
const (
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
)

type failoverTrial struct {
	OldLeader  int     `json:"old_leader"`
	NewLeader  int     `json:"new_leader"`
	ElectedMs  float64 `json:"new_leader_elected_ms"`
	FirstPutMs float64 `json:"first_committed_put_ms"`
}

type failoverReport struct {
	Nodes                int             `json:"nodes"`
	ElectionTimeoutMinMs float64         `json:"election_timeout_min_ms"`
	ElectionTimeoutMaxMs float64         `json:"election_timeout_max_ms"`
	Trials               []failoverTrial `json:"trials"`
	ElectedP50Ms         float64         `json:"elected_p50_ms"`
	ElectedP90Ms         float64         `json:"elected_p90_ms"`
	ElectedMaxMs         float64         `json:"elected_max_ms"`
	PutP50Ms             float64         `json:"put_p50_ms"`
	PutP90Ms             float64         `json:"put_p90_ms"`
	PutMaxMs             float64         `json:"put_max_ms"`
	PutWithin2xMinPct    float64         `json:"put_within_2x_min_timeout_pct"`
	PutWithin2xMaxPct    float64         `json:"put_within_2x_max_timeout_pct"`
}

// measureFailover SIGKILLs the leader n times. For each trial it records
// the time from the kill until (a) another replica reports itself leader
// in a higher term and (b) a PUT sent to that leader commits. Recovery
// includes failure detection: followers wait one randomised election
// timeout (300-600 ms) after the last heartbeat before campaigning.
func measureFailover(h *harness, n int, out string) {
	rep := failoverReport{Nodes: len(h.live()),
		ElectionTimeoutMinMs: ms(electionTimeoutMin), ElectionTimeoutMaxMs: ms(electionTimeoutMax)}
	put := func(url string) bool {
		req, _ := http.NewRequest(http.MethodPut, url+"/kv/failover", strings.NewReader("x"))
		c := http.Client{Timeout: 300 * time.Millisecond}
		resp, err := c.Do(req)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusNoContent
	}
	for trial := 0; trial < n; trial++ {
		// Wait for a stable leader that commits writes.
		var old *replica
		var term int
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
			if l, t := h.leader(); l != nil && put(l.real) {
				old, term = l, t
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if old == nil {
			fmt.Println("failover: no stable leader")
			os.Exit(1)
		}
		time.Sleep(300 * time.Millisecond) // a few heartbeats in the new state
		t0 := time.Now()
		old.kill()
		tr := failoverTrial{OldLeader: old.id, NewLeader: -1}
		var newL *replica
		for time.Since(t0) < 10*time.Second && newL == nil {
			for _, r := range h.live() {
				if r == old {
					continue
				}
				if s, err := getStatus(r.real); err == nil && s.Role == "leader" && s.Term > term {
					newL = r
					tr.ElectedMs = ms(time.Since(t0))
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
		if newL != nil {
			tr.NewLeader = newL.id
			for time.Since(t0) < 10*time.Second {
				if put(newL.real) {
					tr.FirstPutMs = ms(time.Since(t0))
					break
				}
			}
		}
		fmt.Printf("failover trial %d: leader %d killed; new leader %d elected after %.0f ms, first PUT committed after %.0f ms\n",
			trial, tr.OldLeader, tr.NewLeader, tr.ElectedMs, tr.FirstPutMs)
		rep.Trials = append(rep.Trials, tr)
		must(old.start())
		time.Sleep(2 * time.Second)
	}
	var el, pt []float64
	within := [2]int{}
	for _, t := range rep.Trials {
		el = append(el, t.ElectedMs)
		pt = append(pt, t.FirstPutMs)
		if t.FirstPutMs > 0 && t.FirstPutMs <= 2*ms(electionTimeoutMin) {
			within[0]++
		}
		if t.FirstPutMs > 0 && t.FirstPutMs <= 2*ms(electionTimeoutMax) {
			within[1]++
		}
	}
	rep.ElectedP50Ms, rep.ElectedP90Ms, rep.ElectedMaxMs = pctl(el, 0.5), pctl(el, 0.9), pctl(el, 1)
	rep.PutP50Ms, rep.PutP90Ms, rep.PutMaxMs = pctl(pt, 0.5), pctl(pt, 0.9), pctl(pt, 1)
	rep.PutWithin2xMinPct = 100 * float64(within[0]) / float64(len(rep.Trials))
	rep.PutWithin2xMaxPct = 100 * float64(within[1]) / float64(len(rep.Trials))
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if out != "" {
		os.WriteFile(out, b, 0o644)
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func pctl(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[int(float64(len(s)-1)*p)]
}

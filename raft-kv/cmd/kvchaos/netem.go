package main

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// netem is a fault-injecting network between real kvserver processes.
// Every replica is reached through its own HTTP reverse proxy: replicas
// use the proxy URLs as peer addresses and listen on separate real ports.
// The Raft HTTP transport tags each RPC with X-Raft-From, so the proxy in
// front of node j knows the link (from -> j) and can drop the request,
// drop the reply (j -> from), or delay it. Requests without the header
// (clients) are never faulted.
//
// A dropped request is held until the caller gives up (its RPC timeout)
// and then the connection is closed, like a lost packet rather than a
// fast "connection refused".
type netem struct {
	mu      sync.Mutex
	blocked map[[2]int]bool // directed link from -> to is cut
	loss    float64         // probability of dropping any peer request
	delay   time.Duration   // max random delay per peer request
	rng     *rand.Rand
	dropped int64
}

func newNetem(seed int64) *netem {
	return &netem{blocked: map[[2]int]bool{}, rng: rand.New(rand.NewSource(seed))}
}

func (n *netem) heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = map[[2]int]bool{}
	n.loss, n.delay = 0, 0
}

// cut blocks both directions between every node in a and every node in b.
func (n *netem) cut(a, b []int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, x := range a {
		for _, y := range b {
			n.blocked[[2]int{x, y}] = true
			n.blocked[[2]int{y, x}] = true
		}
	}
}

func (n *netem) cutOneWay(from, to int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[[2]int{from, to}] = true
}

func (n *netem) setLoss(p float64, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.loss, n.delay = p, d
}

// decide returns (drop request, drop reply, delay) for one RPC.
func (n *netem) decide(from, to int) (bool, bool, time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	dropReq := n.blocked[[2]int{from, to}] || n.loss > 0 && n.rng.Float64() < n.loss
	dropRep := n.blocked[[2]int{to, from}] || n.loss > 0 && n.rng.Float64() < n.loss
	var d time.Duration
	if n.delay > 0 {
		d = time.Duration(n.rng.Int63n(int64(n.delay)))
	}
	if dropReq || dropRep {
		n.dropped++
	}
	return dropReq, dropRep, d
}

type proxy struct {
	id   int
	ln   net.Listener
	srv  *http.Server
	addr string // proxy URL (what replicas and clients use)
}

// startProxy listens on 127.0.0.1:port and forwards to target.
func (n *netem) startProxy(id, port int, target string) (*proxy, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	tu, _ := url.Parse(target)
	rp := httputil.NewSingleHostReverseProxy(tu)
	rp.Transport = &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 30 * time.Second}
	rp.ErrorLog = nil
	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		w.WriteHeader(http.StatusBadGateway)
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fs := r.Header.Get("X-Raft-From")
		if fs == "" {
			rp.ServeHTTP(w, r)
			return
		}
		from, _ := strconv.Atoi(fs)
		dropReq, dropRep, d := n.decide(from, id)
		if d > 0 {
			time.Sleep(d)
		}
		if dropReq {
			blackhole(w, r)
			return
		}
		if !dropRep {
			rp.ServeHTTP(w, r)
			return
		}
		// Deliver the request but lose the reply.
		rec := &discardWriter{h: http.Header{}}
		rp.ServeHTTP(rec, r)
		blackhole(w, r)
	})
	p := &proxy{id: id, ln: ln, addr: "http://" + ln.Addr().String(),
		srv: &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}}
	go p.srv.Serve(ln)
	return p, nil
}

// blackhole waits until the caller gives up, then drops the connection.
func blackhole(w http.ResponseWriter, r *http.Request) {
	select {
	case <-r.Context().Done():
	case <-time.After(2 * time.Second):
	}
	if hj, ok := w.(http.Hijacker); ok {
		if c, _, err := hj.Hijack(); err == nil {
			c.Close()
			return
		}
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

type discardWriter struct{ h http.Header }

func (d *discardWriter) Header() http.Header         { return d.h }
func (d *discardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (d *discardWriter) WriteHeader(int)             {}

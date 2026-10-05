package kv

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func bareServer(maxSessions int) *Server {
	return &Server{store: NewMemStore(), lastApplied: map[int64]SessionEntry{}, maxSessions: maxSessions}
}

// Audit R-M4: header-less HTTP requests got a fresh random ClientID, and
// every ClientID stayed in the dedup table forever (unbounded growth, a
// cheap memory DoS). Anonymous writes now keep no session state.
func TestAnonymousWritesKeepNoSessionState(t *testing.T) {
	s := bareServer(DefaultMaxSessions)
	for i := 0; i < 1000; i++ {
		s.applyOp(Op{Type: OpPut, Key: "k", Value: "v"}, i+1)
	}
	if n := len(s.lastApplied); n != 0 {
		t.Fatalf("anonymous writes created %d sessions", n)
	}
	// Anonymous writes are at-least-once: each one applies.
	for i := 0; i < 3; i++ {
		s.applyOp(Op{Type: OpAppend, Key: "a", Value: "x"}, 2000+i)
	}
	if v, _, _ := s.store.Get("a"); v != "xxx" {
		t.Fatalf("anonymous appends: %q", v)
	}
}

func TestSessionTableIsBoundedAndDeterministic(t *testing.T) {
	a, b := bareServer(100), bareServer(100)
	for i := 1; i <= 1000; i++ {
		op := Op{Type: OpPut, Key: "k", Value: "v", ClientID: int64(i), Seq: 1}
		a.applyOp(op, i)
		b.applyOp(op, i)
		if len(a.lastApplied) > 100 {
			t.Fatalf("table grew to %d", len(a.lastApplied))
		}
	}
	if len(a.lastApplied) != len(b.lastApplied) {
		t.Fatalf("replicas diverged: %d vs %d", len(a.lastApplied), len(b.lastApplied))
	}
	for id := range a.lastApplied {
		if _, ok := b.lastApplied[id]; !ok {
			t.Fatalf("replicas evicted different sessions")
		}
	}
	// Most recently active sessions survive.
	if _, ok := a.lastApplied[1000]; !ok {
		t.Fatalf("newest session evicted")
	}
	if _, ok := a.lastApplied[1]; ok {
		t.Fatalf("oldest session kept")
	}
}

func TestEvictedSessionRetryIsRejectedNotReapplied(t *testing.T) {
	s := bareServer(10)
	s.applyOp(Op{Type: OpAppend, Key: "k", Value: "x", ClientID: 1, Seq: 1}, 1)
	s.applyOp(Op{Type: OpAppend, Key: "k", Value: "x", ClientID: 1, Seq: 2}, 2)
	// Duplicate while the session is known: not applied twice.
	if r := s.applyOp(Op{Type: OpAppend, Key: "k", Value: "x", ClientID: 1, Seq: 2}, 3); r.Value != "xx" {
		t.Fatalf("duplicate result %+v", r)
	}
	for i := int64(2); i <= 20; i++ { // evict client 1
		s.applyOp(Op{Type: OpPut, Key: "o", Value: "v", ClientID: i, Seq: 1}, int(i)+10)
	}
	if _, ok := s.lastApplied[1]; ok {
		t.Fatalf("client 1 should have been evicted")
	}
	r := s.applyOp(Op{Type: OpAppend, Key: "k", Value: "x", ClientID: 1, Seq: 2}, 100)
	if !r.SessionExpired {
		t.Fatalf("retry after eviction not rejected: %+v", r)
	}
	if v, _, _ := s.store.Get("k"); v != "xx" {
		t.Fatalf("retry after eviction was re-applied: %q", v)
	}
}

// End to end: a Clerk whose session expired gets ErrSessionExpired once,
// then continues on a fresh session.
func TestClerkRecoversFromExpiredSession(t *testing.T) {
	c := newKVCluster(t, 3, false, -1)
	defer c.cleanup()
	ck := NewClerk(c)
	ctx, cancel := ctxT(10 * time.Second)
	defer cancel()
	must(t, ck.Put(ctx, "a", "1"))
	ck.seq = 5 // pretend the server forgot us after several writes
	ck.clientID = randomID()
	if err := ck.Put(ctx, "a", "2"); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("want ErrSessionExpired, got %v", err)
	}
	must(t, ck.Put(ctx, "a", "3"))
	v, _, err := ck.Get(ctx, "a")
	must(t, err)
	if v != "3" {
		t.Fatalf("got %q", v)
	}
}

func TestHTTPAnonymousAndBadSessionHeaders(t *testing.T) {
	c := newKVCluster(t, 1, false, -1)
	defer c.cleanup()
	deadline := time.Now().Add(5 * time.Second)
	for c.servers[0].Raft().Leader() != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	mux := http.NewServeMux()
	(&API{Server: c.servers[0], Timeout: 2 * time.Second}).Register(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	for i := 0; i < 50; i++ {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPut, ts.URL+"/kv/k", strings.NewReader("v"))
		resp, err := http.DefaultClient.Do(req)
		must(t, err)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
	if n := c.servers[0].Sessions(); n != 0 {
		t.Fatalf("header-less requests created %d sessions", n)
	}
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/k", strings.NewReader("v"))
	req.Header.Set("X-Client-ID", "abc")
	resp, err := http.DefaultClient.Do(req)
	must(t, err)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad X-Client-ID: status %d", resp.StatusCode)
	}
}

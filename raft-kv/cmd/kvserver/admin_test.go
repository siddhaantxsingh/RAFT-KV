package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/siddhaantxsingh/raft-kv/kv"
)

// End to end over real HTTP listeners: grow a 3-replica cluster to 4 with
// a -join server through /admin/members, use it, then remove the leader.
func TestAdminMembershipEndToEnd(t *testing.T) {
	dir := t.TempDir()
	var lns []net.Listener
	var urls []string
	for i := 0; i < 4; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns = append(lns, ln)
		urls = append(urls, "http://"+ln.Addr().String())
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodes := map[int]*node{}
	start := func(i int, args ...string) {
		c, err := parseFlags(append([]string{"-id", fmt.Sprint(i), "-data", dir}, args...))
		if err != nil {
			t.Fatal(err)
		}
		n, err := newNode(c, logger)
		if err != nil {
			t.Fatal(err)
		}
		srv, _ := newHTTPServer(c, n.handler)
		go srv.Serve(lns[i])
		nodes[i] = n
		t.Cleanup(func() { srv.Close(); n.server.Kill() })
	}
	for i := 0; i < 3; i++ {
		start(i, "-peers", strings.Join(urls[:3], ","))
	}
	start(3, "-peers", strings.Join(urls, ","), "-join")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ck := kv.NewClerk(kv.NewHTTPCaller(urls[:3]))
	if err := ck.Put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	leader := func() int {
		for i, n := range nodes {
			if n.server.Raft().Status().Role == "leader" {
				return i
			}
		}
		return -1
	}
	do := func(method, url, body string) (int, string) {
		req, _ := http.NewRequest(method, url, strings.NewReader(body))
		resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
		if err != nil {
			return -1, err.Error()
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	// A follower redirects membership requests.
	l := leader()
	f := (l + 1) % 3
	if code, _ := do("POST", urls[f]+"/admin/members", fmt.Sprintf(`{"id":3,"addr":%q}`, urls[3])); code != http.StatusMisdirectedRequest {
		t.Fatalf("follower answered %d, want 421", code)
	}
	if code, body := do("POST", urls[l]+"/admin/members", fmt.Sprintf(`{"id":3,"addr":%q}`, urls[3])); code != 200 {
		t.Fatalf("add server: %d %s", code, body)
	}
	var ml struct {
		Members []struct {
			ID      int
			Learner bool
		} `json:"members"`
	}
	_, body := do("GET", urls[3]+"/admin/members", "")
	json.Unmarshal([]byte(body), &ml)
	if len(ml.Members) != 4 || ml.Members[3].Learner {
		t.Fatalf("new server's view of membership: %s", body)
	}
	// The new server serves (redirects) clients and has the data.
	ck3 := kv.NewClerk(kv.NewHTTPCaller(urls[3:]))
	if v, _, err := ck3.Get(ctx, "a"); err != nil || v != "1" {
		t.Fatalf("get via new server: %q %v", v, err)
	}
	// Remove the leader; the remaining three elect a new one and keep going.
	if code, body := do("DELETE", fmt.Sprintf("%s/admin/members/%d", urls[l], l), ""); code != 200 {
		t.Fatalf("remove leader: %d %s", code, body)
	}
	if err := ck3.Put(ctx, "b", "2"); err != nil {
		t.Fatalf("put after removing the leader: %v", err)
	}
	if nl := leader(); nl == l || nl == -1 {
		t.Fatalf("leader after removal: %d (removed %d)", nl, l)
	}
}

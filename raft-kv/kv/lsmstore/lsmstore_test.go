//go:build lsm

package lsmstore

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/siddhaantxsingh/raft-kv/kv"
	"github.com/siddhaantxsingh/raft-kv/raft"
)

func TestStoreContract(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e := kv.SessionEntry{Seq: 3, Index: 7, Result: kv.Result{Value: "v", OK: true}}
	must(t, s.Apply(7, []kv.Write{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}},
		[]kv.SessionUpdate{{ClientID: 42, Entry: &e}, {ClientID: 43, Entry: &e}}))
	must(t, s.Apply(8, []kv.Write{{Key: "a", Delete: true, Existed: true}}, []kv.SessionUpdate{{ClientID: 43}}))
	check := func(s *Store) {
		t.Helper()
		if v, ok, _ := s.Get("b"); !ok || v != "2" {
			t.Fatalf("get b: %q %v", v, ok)
		}
		if _, ok, _ := s.Get("a"); ok {
			t.Fatal("deleted key present")
		}
		if i, _ := s.AppliedIndex(); i != 8 {
			t.Fatalf("applied %d", i)
		}
		ss, err := s.LoadSessions()
		if err != nil || len(ss) != 1 || ss[42].Seq != 3 || ss[42].Result.Value != "v" {
			t.Fatalf("sessions %+v %v", ss, err)
		}
		if s.Len() != 1 {
			t.Fatalf("len %d", s.Len())
		}
	}
	check(s)
	s.Close()
	s, err = Open(dir) // persists across reopen
	if err != nil {
		t.Fatal(err)
	}
	check(s)

	must(t, s.Restore(map[string]string{"x": "9", "y": "8"}, map[int64]kv.SessionEntry{5: e}, 100))
	if d, _ := s.Dump(); len(d) != 2 || d["x"] != "9" {
		t.Fatalf("dump after restore %v", d)
	}
	if i, _ := s.AppliedIndex(); i != 100 {
		t.Fatalf("applied after restore %d", i)
	}
	if _, ok, _ := s.Get("b"); ok {
		t.Fatal("old data survived restore")
	}
	s.Close()
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// node starts a 1-replica kv.Server on FileStorage + an lsm store in dir.
func node(t *testing.T, dir string, maxState int) *kv.Server {
	t.Helper()
	st, err := raft.NewFileStorage(filepath.Join(dir, "raft"))
	must(t, err)
	store, err := Open(filepath.Join(dir, "lsm"))
	must(t, err)
	cfg := raft.DefaultConfig()
	cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax = 50*time.Millisecond, 100*time.Millisecond
	net := raft.NewNetwork()
	s := kv.NewServer(0, 1, net.Endpoint(0), st, maxState, cfg, kv.WithStore(store))
	net.Register(0, s.Raft())
	net.SetConnected(0, true)
	return s
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	os.RemoveAll(dst)
	out, err := exec.Command("cp", "-a", src, dst).CombinedOutput()
	if err != nil {
		t.Fatalf("cp: %v %s", err, out)
	}
}

type caller struct{ s *kv.Server }

func (c caller) Call(ctx context.Context, _ int, op kv.Op) (kv.Result, error) {
	return c.s.Submit(ctx, op)
}
func (c caller) NumServers() int { return 1 }

// The Raft log is the write-ahead log of record and the store is written
// without fsync, so after a crash the store may be BEHIND the Raft log.
// Simulate the worst case by replacing the store with an old copy: the
// server must re-apply the missing entries from the log (no snapshot) or
// restore from Raft's snapshot (when the log was compacted past the copy).
func TestStaleStoreIsBroughtForwardFromRaftLog(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxState int
	}{{"log replay", -1}, {"snapshot restore", 4 << 10}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := node(t, dir, tc.maxState)
			ck := kv.NewClerk(caller{s})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for i := 0; i < 50; i++ {
				must(t, ck.Append(ctx, "k", fmt.Sprintf("<%d>", i)))
			}
			s.Kill() // closes the store cleanly
			copyDir(t, filepath.Join(dir, "lsm"), filepath.Join(dir, "lsm-old"))

			s = node(t, dir, tc.maxState)
			ck = kv.NewClerk(caller{s})
			want := ""
			for i := 0; i < 50; i++ {
				want += fmt.Sprintf("<%d>", i)
			}
			for i := 50; i < 300; i++ {
				must(t, ck.Append(ctx, "k", fmt.Sprintf("<%d>", i)))
				want += fmt.Sprintf("<%d>", i)
			}
			must(t, ck.Put(ctx, "other", "x"))
			s.Kill()
			// "Crash": the store lost everything after the old copy.
			os.RemoveAll(filepath.Join(dir, "lsm"))
			copyDir(t, filepath.Join(dir, "lsm-old"), filepath.Join(dir, "lsm"))

			s = node(t, dir, tc.maxState)
			defer s.Kill()
			ck = kv.NewClerk(caller{s})
			got, _, err := ck.Get(ctx, "k")
			must(t, err)
			if got != want {
				t.Fatalf("after stale-store recovery: got %d bytes, want %d", len(got), len(want))
			}
			if v, _, _ := ck.Get(ctx, "other"); v != "x" {
				t.Fatalf("other = %q", v)
			}
			if tc.maxState > 0 && s.Stats().Restored == 0 && s.Raft().Status().FirstIndex == 0 {
				t.Fatal("expected the log to be compacted in the snapshot case")
			}
		})
	}
}

package raft

import (
	"hash/crc32"
	"testing"
	"time"
)

// newFollower returns a lone Raft node that stays a follower.
func newFollower(t *testing.T, st Storage) (*Raft, chan ApplyMsg) {
	t.Helper()
	ch := make(chan ApplyMsg, 100)
	cfg := DefaultConfig()
	cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax = time.Hour, 2*time.Hour
	rf := New(0, 3, NewNetwork().Endpoint(0), st, ch, cfg)
	t.Cleanup(rf.Kill)
	return rf, ch
}

func chunkArgs(data []byte, off, size int64, index int) *InstallSnapshotArgs {
	end := min(off+size, int64(len(data)))
	return &InstallSnapshotArgs{
		Term: 1, LeaderID: 1, LastIncludedIndex: index, LastIncludedTerm: 1,
		Offset: off, Data: data[off:end], Done: end == int64(len(data)),
		Total: int64(len(data)), Checksum: crc32.Checksum(data, crcTable),
	}
}

// Audit R-H2: snapshots were sent in one RPC with a 4×RPCTimeout deadline,
// so a snapshot too large to transfer in that time could never be
// installed. Chunks are now reassembled, verified and resumable.
func TestInstallSnapshotChunksResumeAndVerify(t *testing.T) {
	st := NewMemoryStorage()
	rf, ch := newFollower(t, st)
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i * 7)
	}

	r := rf.HandleInstallSnapshot(chunkArgs(data, 0, 300, 50))
	if r.Installed || r.NextOffset != 300 {
		t.Fatalf("first chunk: %+v", r)
	}
	// A duplicate/out-of-order chunk reports where to resume.
	if r := rf.HandleInstallSnapshot(chunkArgs(data, 600, 300, 50)); r.NextOffset != 300 {
		t.Fatalf("out-of-order chunk: %+v", r)
	}
	// A chunk of a different snapshot (no offset 0) restarts the transfer.
	if r := rf.HandleInstallSnapshot(chunkArgs(data, 300, 300, 51)); r.NextOffset != 0 {
		t.Fatalf("unknown transfer: %+v", r)
	}
	// Resume the original transfer where the follower says.
	if r := rf.HandleInstallSnapshot(chunkArgs(data, 300, 300, 50)); r.NextOffset != 600 {
		t.Fatalf("resume: %+v", r)
	}
	r = rf.HandleInstallSnapshot(chunkArgs(data, 600, 400, 50))
	if !r.Installed {
		t.Fatalf("final chunk not installed: %+v", r)
	}
	select {
	case m := <-ch:
		if !m.SnapshotValid || m.SnapshotIndex != 50 || string(m.Snapshot) != string(data) {
			t.Fatalf("applied snapshot wrong: index %d, %d bytes", m.SnapshotIndex, len(m.Snapshot))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot not delivered to the service")
	}
	if idx, _, d := st.Snapshot(); idx != 50 || string(d) != string(data) {
		t.Fatalf("snapshot not persisted: index %d", idx)
	}
	if s := rf.Status(); s.FirstIndex != 50 || s.CommitIndex != 50 {
		t.Fatalf("status after install: %+v", s)
	}
}

func TestInstallSnapshotRejectsCorruptAssembly(t *testing.T) {
	st := NewMemoryStorage()
	rf, _ := newFollower(t, st)
	data := []byte("0123456789abcdefghij")
	a := chunkArgs(data, 0, 10, 20)
	rf.HandleInstallSnapshot(a)
	b := chunkArgs(data, 10, 10, 20)
	b.Data = []byte("XXXXXXXXXX") // corrupted in transit
	r := rf.HandleInstallSnapshot(b)
	if r.Installed || r.NextOffset != 0 {
		t.Fatalf("corrupt snapshot accepted: %+v", r)
	}
	if idx, _, _ := st.Snapshot(); idx != 0 {
		t.Fatalf("corrupt snapshot persisted at %d", idx)
	}
	if rf.Metrics().SnapshotChecksumFails != 1 {
		t.Fatalf("checksum failure not counted")
	}
	// Oversized snapshots are refused up front.
	big := chunkArgs(data, 0, 10, 20)
	big.Total = 1 << 62
	if r := rf.HandleInstallSnapshot(big); r.Installed || r.NextOffset != 0 {
		t.Fatalf("oversized snapshot accepted: %+v", r)
	}
}

type slowSnapshotStorage struct {
	*MemoryStorage
	started chan struct{}
	release chan struct{}
}

func (s *slowSnapshotStorage) SaveSnapshot(index, term int, data []byte) error {
	close(s.started)
	<-s.release
	return s.MemoryStorage.SaveSnapshot(index, term, data)
}

// Audit R-M2: Snapshot wrote the snapshot to disk while holding rf.mu, so
// a slow disk stalled heartbeats, votes and client calls on that node.
func TestSnapshotWriteDoesNotHoldRaftLock(t *testing.T) {
	st := &slowSnapshotStorage{NewMemoryStorage(), make(chan struct{}), make(chan struct{})}
	rf, _ := newFollower(t, st)
	var es []LogEntry
	for i := 1; i <= 10; i++ {
		es = append(es, LogEntry{Term: 1, Index: i, Command: []byte{byte(i)}})
	}
	rf.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: es, LeaderCommit: 10})

	done := make(chan struct{})
	go func() { rf.Snapshot(8, []byte("snap")); close(done) }()
	<-st.started
	got := make(chan struct{})
	go func() {
		rf.GetState()
		rf.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 10, PrevLogTerm: 1, LeaderCommit: 10})
		close(got)
	}()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		close(st.release)
		t.Fatal("raft lock held during snapshot write")
	}
	close(st.release)
	<-done
	if s := rf.Status(); s.FirstIndex != 8 {
		t.Fatalf("log not compacted after snapshot: first index %d", s.FirstIndex)
	}
}

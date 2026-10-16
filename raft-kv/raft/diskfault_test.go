package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
)

// faultFS wraps real files and injects disk faults:
//   - fsync failure (EIO) on chosen calls,
//   - short writes (part of the buffer written, then an error),
//   - crashes that keep only what was fsynced plus an arbitrary part of
//     the unsynced tail, optionally torn (garbage) or with an unsynced
//     block lost while later unsynced data survives (out-of-order
//     write-back).
//
// Only WAL segments are subject to crash loss; hardstate and snapshot are
// written by temp file + fsync + rename, which the crash model treats as
// atomic once fsynced.
type faultFS struct {
	mu          sync.Mutex
	failSync    func(path string, n int) bool // n counts Sync calls on WAL segments
	shortWrite  func(path string, n int) bool // n counts Write calls on WAL segments
	syncs       int
	writes      int
	synced      map[string]int64 // WAL path -> length known durable
	injectedErr int
}

func newFaultFS() *faultFS { return &faultFS{synced: map[string]int64{}} }

func isWAL(path string) bool { return strings.HasPrefix(filepath.Base(path), "wal-") }

func (fs *faultFS) open(name string, flag int, perm os.FileMode) (walFile, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	if isWAL(name) {
		fs.mu.Lock()
		if flag&os.O_TRUNC != 0 {
			fs.synced[name] = 0
		} else if _, ok := fs.synced[name]; !ok {
			st, _ := f.Stat()
			fs.synced[name] = st.Size() // existing data was durable at open
		}
		fs.mu.Unlock()
	}
	return &faultFile{File: f, fs: fs, path: name}, nil
}

type faultFile struct {
	*os.File
	fs   *faultFS
	path string
}

func (f *faultFile) Write(p []byte) (int, error) {
	if isWAL(f.path) {
		f.fs.mu.Lock()
		f.fs.writes++
		short := f.fs.shortWrite != nil && f.fs.shortWrite(f.path, f.fs.writes)
		if short {
			f.fs.injectedErr++
		}
		f.fs.mu.Unlock()
		if short {
			n, _ := f.File.Write(p[:len(p)/2])
			return n, fmt.Errorf("injected short write: %w", syscall.ENOSPC)
		}
	}
	return f.File.Write(p)
}

func (f *faultFile) Sync() error {
	if isWAL(f.path) {
		f.fs.mu.Lock()
		f.fs.syncs++
		fail := f.fs.failSync != nil && f.fs.failSync(f.path, f.fs.syncs)
		if fail {
			f.fs.injectedErr++
		}
		f.fs.mu.Unlock()
		if fail {
			return fmt.Errorf("injected fsync failure: %w", syscall.EIO)
		}
	}
	if err := f.File.Sync(); err != nil {
		return err
	}
	if isWAL(f.path) {
		st, err := f.Stat()
		if err == nil {
			f.fs.mu.Lock()
			f.fs.synced[f.path] = st.Size()
			f.fs.mu.Unlock()
		}
	}
	return nil
}

func (f *faultFile) Truncate(size int64) error {
	if err := f.File.Truncate(size); err != nil {
		return err
	}
	f.fs.mu.Lock()
	if s := f.fs.synced[f.path]; s > size {
		f.fs.synced[f.path] = size
	}
	f.fs.mu.Unlock()
	return nil
}

type crashMode int

const (
	crashCut       crashMode = iota // keep a random prefix of the unsynced tail
	crashTorn                       // ... and overwrite its last bytes with garbage
	crashReordered                  // zero a block inside the unsynced tail, keep data after it
)

// crash applies the crash model to every WAL segment in dir.
func (fs *faultFS) crash(dir string, mode crashMode, rng *rand.Rand) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	names, _ := filepath.Glob(filepath.Join(dir, "wal-*"))
	for _, name := range names {
		data, err := os.ReadFile(name)
		if err != nil {
			continue
		}
		durable := min(fs.synced[name], int64(len(data)))
		unsynced := int64(len(data)) - durable
		if unsynced == 0 {
			continue
		}
		keep := durable + rng.Int63n(unsynced+1)
		out := append([]byte(nil), data[:keep]...)
		switch mode {
		case crashTorn:
			for i := keep - 1; i >= durable && i >= keep-16; i-- {
				out[i] = byte(rng.Intn(256))
			}
		case crashReordered:
			if keep > durable {
				a := durable + rng.Int63n(keep-durable)
				b := min(keep, a+1+rng.Int63n(64))
				for i := a; i < b; i++ {
					out[i] = 0
				}
			}
		}
		os.WriteFile(name, out, 0o644)
		fs.synced[name] = 0
	}
	fs.synced = map[string]int64{}
}

func faultyStorage(t *testing.T, dir string, fs *faultFS) *FileStorage {
	t.Helper()
	st, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.open = fs.open
	if _, _, _, _, err := st.Load(); err != nil {
		t.Fatal(err)
	}
	return st
}

func ent(i, term int) LogEntry {
	return LogEntry{Term: term, Index: i, Command: []byte(fmt.Sprintf("cmd-%d-%d", i, term))}
}

// An fsync failure must be reported, and must poison the storage: after
// a failed fsync, Linux may drop the dirty pages and report a later fsync
// as successful ("fsyncgate"), so a retry could acknowledge lost data.
func TestDiskFaultFsyncFailureIsStickyAndReported(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	st := faultyStorage(t, dir, fs)
	if err := st.Append([]LogEntry{ent(1, 1)}, true); err != nil {
		t.Fatal(err)
	}
	fs.failSync = func(_ string, n int) bool { return n == 2 }
	if err := st.Append([]LogEntry{ent(2, 1)}, true); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Append with failing fsync returned %v, want EIO", err)
	}
	// The next fsync would "succeed" on real Linux; storage must not
	// acknowledge anything after the failure.
	if err := st.Append([]LogEntry{ent(3, 1)}, true); err == nil {
		t.Fatal("Append succeeded after an earlier fsync failure")
	}
	if err := st.Sync(); err == nil {
		t.Fatal("Sync succeeded after an earlier fsync failure")
	}
}

// A short write (e.g. ENOSPC) leaves a partial record. If later appends
// were accepted, they would land after the garbage and the next Load
// would refuse the whole WAL, losing acknowledged entries.
func TestDiskFaultShortWriteIsStickyAndRecoverable(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	st := faultyStorage(t, dir, fs)
	for i := 1; i <= 3; i++ {
		if err := st.Append([]LogEntry{ent(i, 1)}, true); err != nil {
			t.Fatal(err)
		}
	}
	fs.shortWrite = func(_ string, n int) bool { return n == 4 }
	if err := st.Append([]LogEntry{ent(4, 1)}, true); err == nil {
		t.Fatal("short write not reported")
	}
	if err := st.Append([]LogEntry{ent(5, 1)}, true); err == nil {
		t.Fatal("Append succeeded after a short write left a partial record")
	}
	// Restart: the partial record is a torn tail and is dropped; every
	// acknowledged entry is intact.
	st2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, log, err := st2.Load()
	if err != nil {
		t.Fatalf("load after short write: %v", err)
	}
	if len(log) != 3 || log[2].Index != 3 {
		t.Fatalf("recovered %d entries, want 3", len(log))
	}
	if err := st2.Append([]LogEntry{ent(4, 2)}, true); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
}

// Raft treats storage errors as fatal (fail-stop): a follower whose fsync
// fails must not acknowledge the entries.
func TestDiskFaultRaftFailStopOnFsyncError(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	st := faultyStorage(t, dir, fs)
	rf, _ := newFollower(t, st)
	fs.failSync = func(string, int) bool { return true }
	var reply *AppendEntriesReply
	panicked := func() (p bool) {
		defer func() { p = recover() != nil }()
		reply = rf.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: []LogEntry{ent(1, 1)}})
		return false
	}()
	if !panicked || reply != nil {
		t.Fatalf("follower acknowledged entries after a failed fsync (reply %+v)", reply)
	}
}

// Randomised crash test: appends (synced and buffered), truncations,
// snapshots and compactions, then a crash under each crash model. After
// restart, either every acknowledged entry is present with the right
// content, or Load refuses to start (ErrCorruptWAL) — never a silent loss.
func TestDiskFaultCrashConsistency(t *testing.T) {
	iters := 300
	if testing.Short() {
		iters = 60
	}
	refused := map[crashMode]int{}
	for it := 0; it < iters; it++ {
		seed := int64(it)
		rng := rand.New(rand.NewSource(seed))
		mode := crashMode(it % 3)
		dir := t.TempDir()
		fs := newFaultFS()
		st := faultyStorage(t, dir, fs)

		// model: what the log holds, and the acknowledged (durable) prefix.
		var log []LogEntry
		acked := 0 // entries log[:acked] are acknowledged durable
		snapIndex := 0
		term := 1
		ops := 5 + rng.Intn(40)
		for k := 0; k < ops; k++ {
			switch p := rng.Intn(100); {
			case p < 60: // append a batch
				n := 1 + rng.Intn(4)
				first := snapIndex + len(log) + 1
				if rng.Intn(6) == 0 && len(log) > acked {
					// truncate unacknowledged suffix (new leader overwrote it)
					first = snapIndex + acked + 1 + rng.Intn(len(log)-acked)
					term++
				}
				var batch []LogEntry
				for i := 0; i < n; i++ {
					batch = append(batch, ent(first+i, term))
				}
				sync := rng.Intn(2) == 0
				if err := st.Append(batch, sync); err != nil {
					t.Fatal(err)
				}
				log = append(log[:first-snapIndex-1], batch...)
				if sync {
					acked = len(log)
				} else if acked > len(log) {
					acked = len(log)
				}
			case p < 85:
				if err := st.Sync(); err != nil {
					t.Fatal(err)
				}
				acked = len(log)
			default: // snapshot + compact over the acknowledged prefix
				if acked == 0 {
					continue
				}
				upto := snapIndex + 1 + rng.Intn(acked)
				e := log[upto-snapIndex-1]
				if err := st.SaveSnapshot(upto, e.Term, []byte("snap"), nil); err != nil {
					t.Fatal(err)
				}
				retained := append([]LogEntry(nil), log[upto-snapIndex:]...)
				if err := st.CompactLog(upto, retained); err != nil {
					t.Fatal(err)
				}
				acked -= upto - snapIndex
				log = retained
				acked = len(log) // CompactLog fsyncs the retained suffix
				snapIndex = upto
			}
		}
		fs.crash(dir, mode, rng)

		st2, _ := NewFileStorage(dir)
		_, si, _, got, err := st2.Load()
		if err != nil {
			if !errors.Is(err, ErrCorruptWAL) {
				t.Fatalf("seed %d mode %d: load: %v", seed, mode, err)
			}
			refused[mode]++
			continue
		}
		if si != snapIndex {
			t.Fatalf("seed %d: snapshot index %d, want %d", seed, si, snapIndex)
		}
		if len(got) < acked {
			t.Fatalf("seed %d mode %d: lost acknowledged entries: have %d, acknowledged %d", seed, mode, len(got), acked)
		}
		// The acknowledged prefix must be exact. Beyond it, any entries that
		// were written at some point may survive (unacknowledged).
		for i := range got {
			if got[i].Index != snapIndex+i+1 {
				t.Fatalf("seed %d mode %d: entry %d has index %d (snap %d): log not contiguous", seed, mode, i, got[i].Index, snapIndex)
			}
			if i < acked && string(got[i].Command) != string(log[i].Command) {
				t.Fatalf("seed %d mode %d: acknowledged entry %d differs: got %+v want %+v", seed, mode, i, got[i], log[i])
			}
		}
	}
	if refused[crashCut] != 0 || refused[crashTorn] != 0 {
		t.Fatalf("a torn or cut tail must always load; refused: %v", refused)
	}
	t.Logf("load refused (ErrCorruptWAL, no data loss) in %d of %d out-of-order write-back crashes", refused[crashReordered], iters/3)
}

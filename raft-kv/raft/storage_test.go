package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func ents(term int, from, to int) []LogEntry {
	var out []LogEntry
	for i := from; i <= to; i++ {
		out = append(out, LogEntry{Term: term, Index: i, Command: []byte{byte(i)}})
	}
	return out
}

func reopen(t *testing.T, dir string) (*FileStorage, HardState, int, int, []LogEntry) {
	t.Helper()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	hs, si, st, log, err := fs.Load()
	if err != nil {
		t.Fatal(err)
	}
	return fs, hs, si, st, log
}

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fs, hs, _, _, log := reopen(t, dir)
	if hs.VotedFor != -1 || len(log) != 0 {
		t.Fatalf("fresh storage not empty: %+v %v", hs, log)
	}
	must(fs.SetHardState(HardState{Term: 3, VotedFor: 1}))
	must(fs.Append(ents(1, 1, 5), true))
	must(fs.Append(ents(2, 6, 8), false))
	must(fs.Sync())

	_, hs, _, _, log = reopen(t, dir)
	if hs.Term != 3 || hs.VotedFor != 1 {
		t.Fatalf("hardstate %+v", hs)
	}
	if len(log) != 8 || log[7].Index != 8 || log[7].Term != 2 {
		t.Fatalf("log %+v", log)
	}
}

func TestFileStorageTruncationReplay(t *testing.T) {
	dir := t.TempDir()
	fs, _, _, _, _ := reopen(t, dir)
	must(fs.Append(ents(1, 1, 10), true))
	// Leader in term 2 overwrites from index 6.
	must(fs.Append(ents(2, 6, 7), true))
	_, _, _, _, log := reopen(t, dir)
	if len(log) != 7 {
		t.Fatalf("want 7 entries after truncation, got %d", len(log))
	}
	if log[5].Term != 2 || log[6].Term != 2 || log[4].Term != 1 {
		t.Fatalf("wrong terms after truncation: %+v", log)
	}
}

func TestFileStorageTornTail(t *testing.T) {
	dir := t.TempDir()
	fs, _, _, _, _ := reopen(t, dir)
	must(fs.Append(ents(1, 1, 3), true))
	must(fs.Append(ents(1, 4, 6), true))
	// Simulate a crash mid-write: chop bytes off the last record.
	p := filepath.Join(dir, segName(0))
	st, _ := os.Stat(p)
	must(os.Truncate(p, st.Size()-5))

	fs2, _, _, _, log := reopen(t, dir)
	if len(log) != 3 {
		t.Fatalf("torn record should be dropped; got %d entries", len(log))
	}
	// Storage must be writable after recovery, and the new data must survive.
	must(fs2.Append(ents(2, 4, 4), true))
	_, _, _, _, log = reopen(t, dir)
	if len(log) != 4 || log[3].Term != 2 {
		t.Fatalf("append after torn-tail recovery lost: %+v", log)
	}
}

func TestFileStorageCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	fs, _, _, _, _ := reopen(t, dir)
	must(fs.Append(ents(1, 1, 3), true))
	must(fs.Append(ents(1, 4, 6), true))
	p := filepath.Join(dir, segName(0))
	b, _ := os.ReadFile(p)
	b[len(b)-3] ^= 0xFF // flip bits in last record's payload
	os.WriteFile(p, b, 0o644)
	_, _, _, _, log := reopen(t, dir)
	if len(log) != 3 {
		t.Fatalf("CRC mismatch should drop record; got %d entries", len(log))
	}
}

func TestFileStorageSnapshotCompaction(t *testing.T) {
	dir := t.TempDir()
	fs, _, _, _, _ := reopen(t, dir)
	must(fs.Append(ents(1, 1, 100), true))
	before := fs.StateSize()
	must(fs.SaveSnapshot(90, 1, []byte("snap@90")))
	must(fs.CompactLog(90, ents(1, 91, 100)))
	if fs.StateSize() >= before {
		t.Fatalf("compaction did not shrink WAL: %d -> %d", before, fs.StateSize())
	}
	must(fs.Append(ents(1, 101, 105), true))

	fs2, _, si, st, log := reopen(t, dir)
	if _, _, d := fs2.Snapshot(); si != 90 || st != 1 || string(d) != "snap@90" {
		t.Fatalf("snapshot meta lost: %d %d %q", si, st, d)
	}
	if len(log) != 15 || log[0].Index != 91 || log[14].Index != 105 {
		t.Fatalf("log after compaction: first=%d len=%d", log[0].Index, len(log))
	}
}

// Audit R-H1: damage in the middle of the WAL used to be treated like a
// torn tail — Load returned 10 of 50 entries and truncated the file from
// 3,260 to 652 bytes, destroying acknowledged entries without an error.
func TestFileStorageMidWALCorruptionFailsLoad(t *testing.T) {
	dir := t.TempDir()
	fs, _, _, _, _ := reopen(t, dir)
	for i := 0; i < 5; i++ {
		must(fs.Append(ents(1, i*10+1, i*10+10), true))
	}
	p := filepath.Join(dir, segName(0))
	b, _ := os.ReadFile(p)
	b[len(b)/3] ^= 0xFF // inside record 1 or 2 of 5
	must(os.WriteFile(p, b, 0o644))

	fs2, err := NewFileStorage(dir)
	must(err)
	_, _, _, _, err = fs2.Load()
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("want ErrCorruptWAL, got %v", err)
	}
	after, _ := os.ReadFile(p)
	if !bytes.Equal(after, b) {
		t.Fatalf("Load modified a corrupted WAL (%d -> %d bytes)", len(b), len(after))
	}
}

// A corrupted length field mid-log must not masquerade as a torn tail.
func TestFileStorageCorruptLengthFailsLoad(t *testing.T) {
	dir := t.TempDir()
	fs, _, _, _, _ := reopen(t, dir)
	must(fs.Append(ents(1, 1, 3), true))
	must(fs.Append(ents(1, 4, 6), true))
	must(fs.Append(ents(1, 7, 9), true))
	p := filepath.Join(dir, segName(0))
	b, _ := os.ReadFile(p)
	b[3] = 0x3f // first record's length becomes ~1 GiB
	must(os.WriteFile(p, b, 0o644))
	fs2, _ := NewFileStorage(dir)
	if _, _, _, _, err := fs2.Load(); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("want ErrCorruptWAL, got %v", err)
	}
}

// A zero-filled tail (file extended without its data reaching disk) is a
// torn tail and is truncated.
func TestFileStorageZeroTailIsTorn(t *testing.T) {
	dir := t.TempDir()
	fs, _, _, _, _ := reopen(t, dir)
	must(fs.Append(ents(1, 1, 3), true))
	p := filepath.Join(dir, segName(0))
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write(make([]byte, 4096))
	f.Close()
	_, _, _, _, log := reopen(t, dir)
	if len(log) != 3 {
		t.Fatalf("got %d entries", len(log))
	}
}

// hardstate and snapshot files are checksummed; legacy (unframed) files
// still load.
func TestFileStorageStateFilesChecksummed(t *testing.T) {
	dir := t.TempDir()
	fs, _, _, _, _ := reopen(t, dir)
	must(fs.SetHardState(HardState{Term: 7, VotedFor: 2}))
	must(fs.Append(ents(1, 1, 10), true))
	must(fs.SaveSnapshot(5, 1, []byte("snapshot-data")))
	must(fs.CompactLog(5, ents(1, 6, 10)))

	// Flip a byte that still decodes as valid gob (silent corruption
	// without a checksum): the snapshot payload text, and the hardstate's
	// final value byte.
	for _, name := range []string{"hardstate", "snapshot"} {
		p := filepath.Join(dir, name)
		orig, _ := os.ReadFile(p)
		bad := append([]byte(nil), orig...)
		if i := bytes.Index(bad, []byte("snapshot-data")); i >= 0 {
			bad[i] ^= 0x01
		} else {
			bad[len(bad)-2] ^= 0x02
		}
		must(os.WriteFile(p, bad, 0o644))
		fs2, _ := NewFileStorage(dir)
		if _, _, _, _, err := fs2.Load(); err == nil {
			t.Fatalf("corrupted %s loaded without error", name)
		}
		must(os.WriteFile(p, orig, 0o644))
	}

	// Legacy format: bare gob, as written before framing was added.
	var buf bytes.Buffer
	must(gob.NewEncoder(&buf).Encode(HardState{Term: 9, VotedFor: 1}))
	must(os.WriteFile(filepath.Join(dir, "hardstate"), buf.Bytes(), 0o644))
	_, hs, si, _, _ := reopen(t, dir)
	if hs.Term != 9 || hs.VotedFor != 1 || si != 5 {
		t.Fatalf("legacy hardstate: %+v snap %d", hs, si)
	}
}

// Audit R-M3: Sync captured the WAL handle and fsynced it after releasing
// the lock while snapshot compaction closed that handle, so a concurrent Sync
// could fail with "file already closed" (which Raft treats as fatal). The
// hook parks Sync inside that window while a snapshot is attempted.
func TestFileStorageSyncConcurrentWithSnapshot(t *testing.T) {
	dir := t.TempDir()
	fs, _, _, _, _ := reopen(t, dir)
	must(fs.Append(ents(1, 1, 10), true))
	inWindow := make(chan struct{})
	release := make(chan struct{})
	fs.testAfterSyncCapture = func() {
		close(inWindow)
		<-release
	}
	syncErr := make(chan error, 1)
	go func() { syncErr <- fs.Sync() }()
	<-inWindow
	snapDone := make(chan error, 1)
	go func() {
		if err := fs.SaveSnapshot(5, 1, []byte("s")); err != nil {
			snapDone <- err
			return
		}
		snapDone <- fs.CompactLog(5, ents(1, 6, 10)) // rotates the segment under Sync
	}()
	time.Sleep(50 * time.Millisecond) // let SaveSnapshot run (or block)
	close(release)
	if err := <-syncErr; err != nil {
		t.Fatalf("Sync failed during SaveSnapshot: %v", err)
	}
	must(<-snapDone)
	fs.testAfterSyncCapture = nil
	must(fs.Append(ents(1, 11, 12), true))
	_, _, si, _, log := reopen(t, dir)
	if si != 5 || len(log) != 7 {
		t.Fatalf("after snapshot: snap %d, %d entries", si, len(log))
	}
}

// Data directories written before WAL segmentation have a single "wal"
// file; it must load, never be appended to, and go away on compaction.
func TestFileStorageLoadsLegacyWAL(t *testing.T) {
	dir := t.TempDir()
	var legacy []byte
	legacy = append(legacy, encodeRecord(ents(1, 1, 5))...)
	legacy = append(legacy, encodeRecord(ents(2, 4, 6))...)
	must(os.WriteFile(filepath.Join(dir, "wal"), legacy, 0o644))

	fs, _, _, _, log := reopen(t, dir)
	if len(log) != 6 || log[3].Term != 2 {
		t.Fatalf("legacy log: %+v", log)
	}
	must(fs.Append(ents(2, 7, 8), true))
	if b, _ := os.ReadFile(filepath.Join(dir, "wal")); !bytes.Equal(b, legacy) {
		t.Fatalf("legacy file was modified")
	}
	_, _, _, _, log = reopen(t, dir)
	if len(log) != 8 {
		t.Fatalf("after append: %d entries", len(log))
	}
	must(fs.SaveSnapshot(6, 2, []byte("s")))
	must(fs.CompactLog(6, ents(2, 7, 8)))
	if _, err := os.Stat(filepath.Join(dir, "wal")); !os.IsNotExist(err) {
		t.Fatalf("legacy wal not removed by compaction")
	}
	_, _, si, _, log := reopen(t, dir)
	if si != 6 || len(log) != 2 || log[0].Index != 7 {
		t.Fatalf("after compaction: snap %d log %+v", si, log)
	}
}

// A crash during CompactLog can leave old segments next to the new one.
// Replay must still produce exactly the retained log.
func TestFileStorageCompactionCrashLeavesOldSegments(t *testing.T) {
	dir := t.TempDir()
	fs, _, _, _, _ := reopen(t, dir)
	must(fs.Append(ents(1, 1, 10), true))
	must(fs.Append(ents(2, 8, 12), true)) // truncates 8..10, term 2
	old, _ := os.ReadFile(filepath.Join(dir, segName(0)))
	must(fs.SaveSnapshot(9, 2, []byte("s")))
	must(fs.CompactLog(9, ents(2, 10, 12)))
	// "Un-delete" the old segment, as if the crash beat the unlink.
	must(os.WriteFile(filepath.Join(dir, segName(0)), old, 0o644))
	_, _, si, _, log := reopen(t, dir)
	if si != 9 || len(log) != 3 || log[0].Index != 10 || log[2].Index != 12 || log[2].Term != 2 {
		t.Fatalf("snap %d, log %+v", si, log)
	}
}

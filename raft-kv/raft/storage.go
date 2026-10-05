package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Storage is Raft's durable state. Unlike a "save everything" persister,
// it is incremental: appending one entry costs one small write, not a
// rewrite of the whole log.
//
// Durability contract:
//   - SetHardState is durable when it returns.
//   - Append(entries, true) is durable when it returns.
//   - Append(entries, false) is buffered; it becomes durable after Sync().
//     The leader uses this to fsync in parallel with replication (group
//     commit): its own matchIndex only advances after Sync.
//   - The first appended entry may have an index <= the last stored index;
//     everything from that index onward is replaced (log truncation).
//
// Snapshots are two-phase so the large write can happen without Raft's
// lock held:
//   - SaveSnapshot durably records a snapshot. It never discards log
//     entries, so the stored log stays a superset of what the snapshot does
//     not cover and a crash at any point is safe. Saving a snapshot at or
//     below the stored snapshot's index is a no-op.
//   - CompactLog then replaces the stored log with retained (the entries
//     after index; empty when an installed snapshot replaced a conflicting
//     log). Only the retained suffix is written — normally a few entries —
//     so it is cheap enough to call with Raft's lock held, which also
//     guarantees no Append runs concurrently.
type Storage interface {
	Load() (hs HardState, snapIndex, snapTerm int, entries []LogEntry, err error)
	SetHardState(hs HardState) error
	Append(entries []LogEntry, sync bool) error
	Sync() error
	SaveSnapshot(index, term int, data []byte) error
	CompactLog(index int, retained []LogEntry) error
	// Snapshot returns the latest saved snapshot with its metadata. The
	// returned slice is shared and must not be modified.
	Snapshot() (index, term int, data []byte)
	StateSize() int // bytes of log/state (excluding snapshot), for compaction triggers
}

type HardState struct {
	Term     int
	VotedFor int
}

// ------------------------------------------------------------ MemoryStorage

// MemoryStorage keeps everything in memory. Copy() models a crash/restart:
// the new instance sees exactly what was durably stored. Buffered appends
// are treated as durable (tests exercise logic, not fsync timing).
type MemoryStorage struct {
	mu        sync.Mutex
	hs        HardState
	snapIndex int
	snapTerm  int
	snapshot  []byte
	base      int // index just below log[0]
	log       []LogEntry
	size      int
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{hs: HardState{VotedFor: -1}} }

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func (m *MemoryStorage) Copy() *MemoryStorage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &MemoryStorage{
		hs: m.hs, snapIndex: m.snapIndex, snapTerm: m.snapTerm, snapshot: m.snapshot,
		base: m.base, log: append([]LogEntry(nil), m.log...), size: m.size,
	}
}

func (m *MemoryStorage) Load() (HardState, int, int, []LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []LogEntry
	for _, e := range m.log {
		if e.Index > m.snapIndex {
			out = append(out, e)
		}
	}
	return m.hs, m.snapIndex, m.snapTerm, out, nil
}

func (m *MemoryStorage) SetHardState(hs HardState) error {
	m.mu.Lock()
	m.hs = hs
	m.mu.Unlock()
	return nil
}

func entrySize(e LogEntry) int { return len(e.Command) + 24 }

func (m *MemoryStorage) recomputeSize() {
	m.size = 0
	for _, e := range m.log {
		m.size += entrySize(e)
	}
}

func (m *MemoryStorage) Append(entries []LogEntry, _ bool) error {
	if len(entries) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	first := entries[0].Index
	cut := first - m.base - 1
	if cut < 0 {
		return fmt.Errorf("append at %d below compacted index %d", first, m.base)
	}
	if cut < len(m.log) {
		m.log = m.log[:cut]
	}
	if cut > len(m.log) {
		return fmt.Errorf("append at %d leaves a gap (last %d)", first, m.base+len(m.log))
	}
	m.log = append(m.log, entries...)
	m.recomputeSize()
	return nil
}

func (m *MemoryStorage) Sync() error { return nil }

func (m *MemoryStorage) SaveSnapshot(index, term int, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index <= m.snapIndex {
		return nil
	}
	m.snapIndex, m.snapTerm, m.snapshot = index, term, clone(data)
	return nil
}

func (m *MemoryStorage) CompactLog(index int, retained []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index > m.snapIndex {
		return fmt.Errorf("compact to %d beyond saved snapshot %d", index, m.snapIndex)
	}
	m.log, m.base = append([]LogEntry(nil), retained...), index
	m.recomputeSize()
	return nil
}

func (m *MemoryStorage) Snapshot() (int, int, []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapIndex, m.snapTerm, m.snapshot
}

func (m *MemoryStorage) StateSize() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.size
}

// ------------------------------------------------------------ FileStorage

// FileStorage layout in dir:
//
//	hardstate        tiny file, replaced atomically (temp + fsync + rename)
//	snapshot         {index, term, data}, replaced atomically
//	wal-<seq>        WAL segments, replayed in seq order. A legacy single
//	                 "wal" file (older versions) is treated as segment 0.
//
// Each segment holds records [len u32][crc32c u32][gob []LogEntry]; each
// record is a batch of entries that replaces the log from its first index
// onward. hardstate and snapshot are framed as [magic u32][crc32c u32][gob]
// so bit rot is detected; files written by older versions (bare gob) load.
//
// On load, a torn tail — a damaged final record of the *last* segment with
// no intact record after it (crash during an append) — is truncated away:
// it was never acknowledged, so dropping it is safe. Damage anywhere else
// is corruption of acknowledged data and fails Load (ErrCorruptWAL) without
// modifying any file. Segments are fsynced before a new one is started.
//
// Compaction starts a new segment holding only the retained suffix, fsyncs
// it, and then deletes every older segment.
type FileStorage struct {
	// syncMu serialises WAL fsyncs with segment rotation, so a Sync never
	// runs on a handle that was closed under it. snapMu serialises
	// snapshot saves (the large write runs under snapMu only).
	// Lock order: snapMu, then syncMu, then mu.
	snapMu sync.Mutex
	syncMu sync.Mutex
	mu     sync.Mutex
	dir    string
	segs   []segment // oldest first; the last one is open for appends
	cur    *os.File
	// Latest saved snapshot (data shared, never modified).
	snapIndex, snapTerm int
	snapshot            []byte
	// testAfterSyncCapture, if set, runs in Sync between reading the WAL
	// handle and fsyncing it (tests use it to widen race windows).
	testAfterSyncCapture func()
}

type segment struct {
	name string
	size int64
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir}, nil
}

func (f *FileStorage) path(n string) string { return filepath.Join(f.dir, n) }

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (f *FileStorage) atomicWrite(name string, data []byte) error {
	tmp := f.path(name + ".tmp")
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := fh.Write(data); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path(name)); err != nil {
		return err
	}
	return syncDir(f.dir)
}

const fileMagic = 0x52464b31 // "RFK1"

// frame wraps a small state file as [magic][crc32c][payload].
func frame(payload []byte) []byte {
	out := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(out[0:4], fileMagic)
	binary.LittleEndian.PutUint32(out[4:8], crc32.Checksum(payload, crcTable))
	copy(out[8:], payload)
	return out
}

// unframe returns the payload of a framed file, or the whole file if it
// predates framing (legacy bare gob).
func unframe(name string, b []byte) ([]byte, error) {
	if len(b) < 8 || binary.LittleEndian.Uint32(b[0:4]) != fileMagic {
		return b, nil
	}
	if crc32.Checksum(b[8:], crcTable) != binary.LittleEndian.Uint32(b[4:8]) {
		return nil, fmt.Errorf("%s: checksum mismatch (file corrupted)", name)
	}
	return b[8:], nil
}

type snapFile struct {
	Index, Term int
	Data        []byte
}

func encodeRecord(entries []LogEntry) []byte {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(entries); err != nil {
		panic(err)
	}
	rec := make([]byte, 8+payload.Len())
	binary.LittleEndian.PutUint32(rec[0:4], uint32(payload.Len()))
	binary.LittleEndian.PutUint32(rec[4:8], crc32.Checksum(payload.Bytes(), crcTable))
	copy(rec[8:], payload.Bytes())
	return rec
}

// ErrCorruptWAL means the WAL is damaged somewhere other than its tail:
// intact records follow the damage, so acknowledged entries would be lost
// by truncating. The node must not start on this log; recover it by
// restoring the directory or by wiping it and re-joining the cluster.
var ErrCorruptWAL = errors.New("raft wal corrupted")

const maxRecord = 1 << 30

// recordAt checks the record at data[p:] (length, checksum). A zero-length
// record is never written, so it counts as damage (zero-filled tail).
func recordAt(data []byte, p int) ([]byte, bool) {
	if p+8 > len(data) {
		return nil, false
	}
	n := int(binary.LittleEndian.Uint32(data[p : p+4]))
	sum := binary.LittleEndian.Uint32(data[p+4 : p+8])
	if n == 0 || n > maxRecord || p+8+n > len(data) {
		return nil, false
	}
	payload := data[p+8 : p+8+n]
	if crc32.Checksum(payload, crcTable) != sum {
		return nil, false
	}
	return payload, true
}

func segName(seq uint64) string { return fmt.Sprintf("wal-%016d", seq) }

// listSegments returns segment names in replay order.
func (f *FileStorage) listSegments() ([]string, error) {
	des, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}
	type s struct {
		seq  uint64
		name string
	}
	var out []s
	for _, d := range des {
		n := d.Name()
		switch {
		case n == "wal":
			out = append(out, s{0, n})
		case strings.HasPrefix(n, "wal-") && !strings.HasSuffix(n, ".tmp"):
			seq, err := strconv.ParseUint(n[4:], 10, 64)
			if err == nil {
				out = append(out, s{seq + 1, n}) // +1: after the legacy file
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	names := make([]string, len(out))
	for i, x := range out {
		names[i] = x.name
	}
	return names, nil
}

func (f *FileStorage) Load() (HardState, int, int, []LogEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	hs := HardState{VotedFor: -1}
	if b, err := os.ReadFile(f.path("hardstate")); err == nil {
		p, err := unframe("hardstate", b)
		if err != nil {
			return hs, 0, 0, nil, err
		}
		if err := gob.NewDecoder(bytes.NewReader(p)).Decode(&hs); err != nil {
			return hs, 0, 0, nil, fmt.Errorf("hardstate: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return hs, 0, 0, nil, err
	}
	var sf snapFile
	if b, err := os.ReadFile(f.path("snapshot")); err == nil {
		p, err := unframe("snapshot", b)
		if err != nil {
			return hs, 0, 0, nil, err
		}
		if err := gob.NewDecoder(bytes.NewReader(p)).Decode(&sf); err != nil {
			return hs, 0, 0, nil, fmt.Errorf("snapshot: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return hs, 0, 0, nil, err
	}
	f.snapIndex, f.snapTerm, f.snapshot = sf.Index, sf.Term, sf.Data

	names, err := f.listSegments()
	if err != nil {
		return hs, 0, 0, nil, err
	}
	var log []LogEntry
	f.segs = nil
	tornAt := int64(-1) // valid length of the last segment if its tail is torn
	for si, name := range names {
		data, err := os.ReadFile(f.path(name))
		if err != nil {
			return hs, 0, 0, nil, err
		}
		seg := segment{name: name}
		p := 0
		for p < len(data) {
			payload, ok := recordAt(data, p)
			if !ok {
				last := si == len(names)-1
				for q := p + 1; last && q+8 <= len(data); q++ {
					if _, ok := recordAt(data, q); ok {
						last = false
					}
				}
				if !last {
					return hs, 0, 0, nil, fmt.Errorf("%w: damaged record in %s at offset %d of %d bytes with intact data after it", ErrCorruptWAL, name, p, len(data))
				}
				tornAt = int64(p)
				break
			}
			var batch []LogEntry
			if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&batch); err != nil {
				// Checksummed but undecodable: not a torn write.
				return hs, 0, 0, nil, fmt.Errorf("%w: undecodable record in %s at offset %d: %v", ErrCorruptWAL, name, p, err)
			}
			p += 8 + len(payload)
			for _, e := range batch {
				if e.Index <= sf.Index {
					continue
				}
				cut := e.Index - sf.Index - 1
				if cut < len(log) {
					log = log[:cut]
				}
				if cut == len(log) {
					log = append(log, e)
				}
			}
		}
		seg.size = int64(p)
		f.segs = append(f.segs, seg)
	}

	// Open the last segment for appends (truncating a torn tail), or start
	// a fresh one. The legacy "wal" file is never appended to.
	if n := len(f.segs); n > 0 && f.segs[n-1].name != "wal" {
		last := &f.segs[n-1]
		fh, err := os.OpenFile(f.path(last.name), os.O_RDWR, 0o644)
		if err != nil {
			return hs, 0, 0, nil, err
		}
		if tornAt >= 0 {
			if err := fh.Truncate(tornAt); err != nil {
				fh.Close()
				return hs, 0, 0, nil, err
			}
			if err := fh.Sync(); err != nil {
				fh.Close()
				return hs, 0, 0, nil, err
			}
		}
		if _, err := fh.Seek(last.size, io.SeekStart); err != nil {
			fh.Close()
			return hs, 0, 0, nil, err
		}
		f.cur = fh
	} else {
		if n > 0 && tornAt >= 0 { // torn legacy file
			if err := os.Truncate(f.path("wal"), tornAt); err != nil {
				return hs, 0, 0, nil, err
			}
		}
		if err := f.newSegmentLocked(); err != nil {
			return hs, 0, 0, nil, err
		}
	}
	return hs, sf.Index, sf.Term, log, nil
}

// newSegmentLocked starts a new, empty segment and makes it current. The
// caller holds mu (and syncMu if Syncs may be running).
func (f *FileStorage) newSegmentLocked() error {
	var seq uint64
	if n := len(f.segs); n > 0 && f.segs[n-1].name != "wal" {
		prev, _ := strconv.ParseUint(f.segs[n-1].name[4:], 10, 64)
		seq = prev + 1
	}
	name := segName(seq)
	fh, err := os.OpenFile(f.path(name), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := syncDir(f.dir); err != nil {
		fh.Close()
		return err
	}
	if f.cur != nil {
		// Everything in the old segment must be durable before later
		// records exist in the new one (torn tails only in the last).
		if err := f.cur.Sync(); err != nil {
			fh.Close()
			return err
		}
		f.cur.Close()
	}
	f.cur = fh
	f.segs = append(f.segs, segment{name: name})
	return nil
}

func (f *FileStorage) SetHardState(hs HardState) error {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(hs)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.atomicWrite("hardstate", frame(b.Bytes()))
}

func (f *FileStorage) Append(entries []LogEntry, sync bool) error {
	if len(entries) == 0 {
		return nil
	}
	rec := encodeRecord(entries)
	f.mu.Lock()
	_, err := f.cur.Write(rec)
	f.segs[len(f.segs)-1].size += int64(len(rec))
	f.mu.Unlock()
	if err != nil {
		return err
	}
	if sync {
		return f.Sync()
	}
	return nil
}

// Sync makes every appended record durable. If CompactLog rotated the
// segment meanwhile, the old segment was fsynced by the rotation.
func (f *FileStorage) Sync() error {
	f.syncMu.Lock()
	defer f.syncMu.Unlock()
	f.mu.Lock()
	wal := f.cur
	hook := f.testAfterSyncCapture
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return wal.Sync()
}

// SaveSnapshot durably writes the snapshot file. Log entries are not
// touched, so concurrent appends are unaffected; only snapMu is held
// during the write.
func (f *FileStorage) SaveSnapshot(index, term int, data []byte) error {
	f.snapMu.Lock()
	defer f.snapMu.Unlock()
	f.mu.Lock()
	stale := index <= f.snapIndex
	f.mu.Unlock()
	if stale {
		return nil
	}
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(snapFile{index, term, data}); err != nil {
		return err
	}
	if err := f.atomicWrite("snapshot", frame(b.Bytes())); err != nil {
		return err
	}
	f.mu.Lock()
	f.snapIndex, f.snapTerm, f.snapshot = index, term, clone(data)
	f.mu.Unlock()
	return nil
}

// CompactLog starts a new segment containing retained, makes it durable,
// then deletes every older segment. If a crash leaves some old segments
// behind, replay skips their entries at or below the snapshot and the
// retained record (replayed last) replaces anything above it.
func (f *FileStorage) CompactLog(index int, retained []LogEntry) error {
	f.syncMu.Lock()
	defer f.syncMu.Unlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	if index > f.snapIndex {
		return fmt.Errorf("compact to %d beyond saved snapshot %d", index, f.snapIndex)
	}
	if len(retained) > 0 && retained[0].Index != index+1 {
		return fmt.Errorf("retained log starts at %d, want %d", retained[0].Index, index+1)
	}
	old := len(f.segs)
	if err := f.newSegmentLocked(); err != nil {
		return err
	}
	if len(retained) > 0 {
		rec := encodeRecord(retained)
		if _, err := f.cur.Write(rec); err != nil {
			return err
		}
		if err := f.cur.Sync(); err != nil {
			return err
		}
		f.segs[len(f.segs)-1].size = int64(len(rec))
	}
	for _, s := range f.segs[:old] {
		if err := os.Remove(f.path(s.name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	f.segs = append([]segment(nil), f.segs[old:]...)
	return nil
}

func (f *FileStorage) Snapshot() (int, int, []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapIndex, f.snapTerm, f.snapshot
}

func (f *FileStorage) StateSize() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, s := range f.segs {
		n += s.size
	}
	return int(n)
}

//go:build lsm

// Package lsmstore is a kv.StateStore backed by lsm-engine through its C
// ABI (cgo). Build with -tags lsm and point the linker at the engine:
//
//	(cd ../lsm-engine && cargo build --release -p lsm-capi)
//	CGO_LDFLAGS="-L../lsm-engine/target/release" go build -tags lsm ./...
//
// Layout inside the LSM (one namespace byte per kind of record):
//
//	'd' + key            user value
//	's' + 8-byte BE id   gob(kv.SessionEntry)
//	'm' + "applied"      8-byte BE index of the last applied entry
//
// Every Apply is a single atomic lsm write batch containing the data
// writes, session changes and the new applied index, written WITHOUT
// fsync: the Raft log is the write-ahead log of record. After a crash the
// store recovers some prefix of applied entries (atomically per entry),
// and kv.Server re-applies the rest from the Raft log or restores from
// Raft's snapshot if the store is older than it.
package lsmstore

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -l:liblsm_capi.a -ldl -lpthread -lm
#include <stdlib.h>
#include "lsm.h"
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"unsafe"

	"github.com/siddhaantxsingh/raft-kv/kv"
)

var appliedKey = []byte("mapplied")

type Store struct {
	dir  string
	db   *C.lsm_db
	keys atomic.Int64
}

func cerr(e *C.char) error {
	if e == nil {
		return nil
	}
	defer C.lsm_free_err(e)
	return errors.New(C.GoString(e))
}

func ptr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

func open(dir string) (*C.lsm_db, error) {
	cdir := C.CString(dir)
	defer C.free(unsafe.Pointer(cdir))
	var e *C.char
	db := C.lsm_open(cdir, 0, &e)
	if db == nil {
		return nil, fmt.Errorf("lsm open %s: %w", dir, cerr(e))
	}
	return db, nil
}

// Open opens (or creates) the store in dir and counts its keys.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db, err := open(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{dir: dir, db: db}
	n := 0
	if err := s.scan([]byte("d"), []byte("e"), func(k, v []byte) { n++ }); err != nil {
		s.Close()
		return nil, err
	}
	s.keys.Store(int64(n))
	return s, nil
}

func (s *Store) get(k []byte) ([]byte, bool, error) {
	var v *C.uint8_t
	var vl C.size_t
	var e *C.char
	r := C.lsm_get(s.db, ptr(k), C.size_t(len(k)), &v, &vl, &e)
	switch r {
	case 1:
		defer C.lsm_free(v, vl)
		return C.GoBytes(unsafe.Pointer(v), C.int(vl)), true, nil
	case 0:
		return nil, false, nil
	default:
		return nil, false, cerr(e)
	}
}

func (s *Store) scan(start, end []byte, f func(k, v []byte)) error {
	it := C.lsm_scan(s.db, ptr(start), C.size_t(len(start)), ptr(end), C.size_t(len(end)))
	if it == nil {
		return errors.New("lsm scan failed")
	}
	defer C.lsm_iter_free(it)
	var k, v *C.uint8_t
	var kl, vl C.size_t
	for C.lsm_iter_next(it, &k, &kl, &v, &vl) == 1 {
		f(C.GoBytes(unsafe.Pointer(k), C.int(kl)), C.GoBytes(unsafe.Pointer(v), C.int(vl)))
	}
	var e *C.char
	if C.lsm_iter_status(it, &e) != 0 {
		return cerr(e)
	}
	return nil
}

func (s *Store) Get(key string) (string, bool, error) {
	v, ok, err := s.get(append([]byte("d"), key...))
	return string(v), ok, err
}

type batch struct{ b *C.lsm_batch }

func newBatch() batch { return batch{C.lsm_batch_new()} }
func (b batch) put(k, v []byte) {
	C.lsm_batch_put(b.b, ptr(k), C.size_t(len(k)), ptr(v), C.size_t(len(v)))
}
func (b batch) del(k []byte) { C.lsm_batch_delete(b.b, ptr(k), C.size_t(len(k))) }
func (b batch) free()        { C.lsm_batch_free(b.b) }

func (s *Store) write(b batch) error {
	var e *C.char
	if C.lsm_write(s.db, b.b, 0, &e) != 0 {
		return cerr(e)
	}
	return nil
}

func sessKey(id int64) []byte {
	k := make([]byte, 9)
	k[0] = 's'
	binary.BigEndian.PutUint64(k[1:], uint64(id))
	return k
}

func idxVal(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func (s *Store) Apply(index int, writes []kv.Write, sessions []kv.SessionUpdate) error {
	b := newBatch()
	defer b.free()
	delta := int64(0)
	for _, w := range writes {
		k := append([]byte("d"), w.Key...)
		if w.Delete {
			b.del(k)
			if w.Existed {
				delta--
			}
		} else {
			b.put(k, []byte(w.Value))
			if !w.Existed {
				delta++
			}
		}
	}
	for _, su := range sessions {
		if su.Entry == nil {
			b.del(sessKey(su.ClientID))
			continue
		}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(su.Entry); err != nil {
			return err
		}
		b.put(sessKey(su.ClientID), buf.Bytes())
	}
	b.put(appliedKey, idxVal(index))
	if err := s.write(b); err != nil {
		return err
	}
	s.keys.Add(delta)
	return nil
}

func (s *Store) AppliedIndex() (int, error) {
	v, ok, err := s.get(appliedKey)
	if err != nil || !ok {
		return 0, err
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("lsmstore: malformed applied index (%d bytes)", len(v))
	}
	return int(binary.BigEndian.Uint64(v)), nil
}

func (s *Store) LoadSessions() (map[int64]kv.SessionEntry, error) {
	out := map[int64]kv.SessionEntry{}
	var derr error
	err := s.scan([]byte("s"), []byte("t"), func(k, v []byte) {
		var e kv.SessionEntry
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&e); err != nil && derr == nil {
			derr = err
		}
		out[int64(binary.BigEndian.Uint64(k[1:]))] = e
	})
	if err == nil {
		err = derr
	}
	return out, err
}

func (s *Store) Dump() (map[string]string, error) {
	out := map[string]string{}
	err := s.scan([]byte("d"), []byte("e"), func(k, v []byte) { out[string(k[1:])] = string(v) })
	return out, err
}

// Restore replaces the store's contents. It builds the new state in a
// fresh directory, writing the applied index last, then swaps directories,
// so a crash at any point leaves either the old state or a state whose
// applied index is 0 (which makes kv.Server restore from the snapshot
// again).
func (s *Store) Restore(data map[string]string, sessions map[int64]kv.SessionEntry, index int) error {
	tmp := s.dir + ".restore"
	os.RemoveAll(tmp)
	ndb, err := open(tmp)
	if err != nil {
		return err
	}
	ns := &Store{dir: tmp, db: ndb}
	b := newBatch()
	n := 0
	flush := func() error {
		err := ns.write(b)
		b.free()
		b = newBatch()
		n = 0
		return err
	}
	for k, v := range data {
		b.put(append([]byte("d"), k...), []byte(v))
		if n++; n == 1000 {
			if err := flush(); err != nil {
				ns.Close()
				return err
			}
		}
	}
	for id, e := range sessions {
		var buf bytes.Buffer
		gob.NewEncoder(&buf).Encode(e)
		b.put(sessKey(id), buf.Bytes())
	}
	if err := flush(); err != nil {
		b.free()
		ns.Close()
		return err
	}
	b.put(appliedKey, idxVal(index))
	if err := flush(); err != nil {
		b.free()
		ns.Close()
		return err
	}
	b.free()
	var e *C.char
	if C.lsm_flush(ndb, &e) != 0 { // make the new state durable before the swap
		ns.Close()
		return cerr(e)
	}
	ns.Close()
	s.Close()
	old := s.dir + ".old"
	os.RemoveAll(old)
	if err := os.Rename(s.dir, old); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.dir); err != nil {
		return err
	}
	syncDir(filepath.Dir(s.dir))
	os.RemoveAll(old)
	if s.db, err = open(s.dir); err != nil {
		return err
	}
	s.keys.Store(int64(len(data)))
	return nil
}

func syncDir(d string) {
	if f, err := os.Open(d); err == nil {
		f.Sync()
		f.Close()
	}
}

func (s *Store) Len() int { return int(s.keys.Load()) }

func (s *Store) Close() error {
	if s.db != nil {
		C.lsm_close(s.db)
		s.db = nil
	}
	return nil
}

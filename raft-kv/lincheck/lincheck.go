// Package lincheck checks whether a concurrent history of key/value
// operations is linearizable, using the Wing & Gong search with Lowe's
// memoisation ("Testing for linearizability", 2017). Because operations on
// different keys commute, the history is partitioned by key and each
// partition is checked independently (P-compositionality), which keeps the
// exponential search tractable.
package lincheck

import (
	"math"
	"sort"
)

type Kind uint8

const (
	Get Kind = iota
	Put
	Append
	Delete
	CAS
)

// Operation is one client request as observed from outside the system.
// Call/Return are timestamps (e.g. time.Now().UnixNano()). If the client
// never learned the outcome (timeout), set Unknown=true; such an operation
// may take effect at any point after Call, or never.
type Operation struct {
	ClientID int
	Kind     Kind
	Key      string
	Value    string // Put/Append/CAS new value
	Expected string // CAS expected value ("" = absent)

	// Observed output.
	OutValue string
	OutFound bool
	OutOK    bool // CAS swapped
	Unknown  bool

	Call, Return int64
}

type state struct {
	val    string
	exists bool
}

// step applies op to s, returning the new state and whether the observed
// output is consistent with the model.
func step(s state, op *Operation) (state, bool) {
	switch op.Kind {
	case Get:
		if op.Unknown {
			return s, true
		}
		if !s.exists {
			return s, !op.OutFound && op.OutValue == ""
		}
		return s, op.OutFound && op.OutValue == s.val
	case Put:
		return state{op.Value, true}, true
	case Append:
		ns := state{s.val + op.Value, true}
		return ns, op.Unknown || op.OutValue == ns.val
	case Delete:
		return state{}, op.Unknown || op.OutFound == s.exists
	case CAS:
		match := (s.exists && s.val == op.Expected) || (!s.exists && op.Expected == "")
		if op.Unknown {
			if match {
				return state{op.Value, true}, true
			}
			return s, true
		}
		if match != op.OutOK {
			return s, false
		}
		if match {
			return state{op.Value, true}, true
		}
		return s, true
	}
	return s, false
}

// Check returns true if the history is linearizable. It also returns the
// first key whose sub-history failed, for debugging.
func Check(history []Operation) (bool, string) {
	byKey := map[string][]*Operation{}
	for i := range history {
		op := &history[i]
		if op.Unknown {
			op.Return = math.MaxInt64
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !checkPartition(byKey[k]) {
			return false, k
		}
	}
	return true, ""
}

// entry is a node in the doubly linked list of call/return events.
type entry struct {
	isCall     bool
	id         int
	op         *Operation
	match      *entry // call -> its return
	prev, next *entry
}

type bitset []uint64

func newBitset(n int) bitset   { return make(bitset, (n+63)/64) }
func (b bitset) set(i int)     { b[i/64] |= 1 << (uint(i) % 64) }
func (b bitset) clear(i int)   { b[i/64] &^= 1 << (uint(i) % 64) }
func (b bitset) clone() bitset { c := make(bitset, len(b)); copy(c, b); return c }
func (b bitset) equals(o bitset) bool {
	for i := range b {
		if b[i] != o[i] {
			return false
		}
	}
	return true
}
func (b bitset) hash() uint64 {
	var h uint64 = 14695981039346656037
	for _, w := range b {
		h ^= w
		h *= 1099511628211
	}
	return h
}

type cacheEntry struct {
	bits  bitset
	state state
}

func checkPartition(ops []*Operation) bool {
	n := len(ops)
	type event struct {
		t      int64
		isCall bool
		id     int
	}
	events := make([]event, 0, 2*n)
	for i, op := range ops {
		events = append(events, event{op.Call, true, i}, event{op.Return, false, i})
	}
	// Sort by time; at equal times, returns before calls (ops that touch
	// at an instant are not considered concurrent).
	sort.Slice(events, func(a, b int) bool {
		if events[a].t != events[b].t {
			return events[a].t < events[b].t
		}
		return !events[a].isCall && events[b].isCall
	})

	head := &entry{}
	cur := head
	calls := make([]*entry, n)
	for _, ev := range events {
		e := &entry{isCall: ev.isCall, id: ev.id, op: ops[ev.id], prev: cur}
		cur.next = e
		cur = e
		if ev.isCall {
			calls[ev.id] = e
		} else {
			calls[ev.id].match = e
		}
	}

	lift := func(e *entry) {
		e.prev.next = e.next
		if e.next != nil {
			e.next.prev = e.prev
		}
		m := e.match
		m.prev.next = m.next
		if m.next != nil {
			m.next.prev = m.prev
		}
	}
	unlift := func(e *entry) {
		m := e.match
		m.prev.next = m
		if m.next != nil {
			m.next.prev = m
		}
		e.prev.next = e
		if e.next != nil {
			e.next.prev = e
		}
	}

	type frame struct {
		e     *entry
		state state
	}
	cache := map[uint64][]cacheEntry{}
	seen := func(b bitset, s state) bool {
		h := b.hash() ^ uint64(len(s.val))*31
		for _, c := range cache[h] {
			if c.state == s && c.bits.equals(b) {
				return true
			}
		}
		cache[h] = append(cache[h], cacheEntry{b.clone(), s})
		return false
	}

	linearized := newBitset(n)
	var stack []frame
	s := state{}
	e := head.next
	for head.next != nil {
		if e.isCall {
			ns, ok := step(s, e.op)
			if ok {
				linearized.set(e.id)
				if !seen(linearized, ns) {
					stack = append(stack, frame{e, s})
					s = ns
					lift(e)
					e = head.next
					continue
				}
				linearized.clear(e.id)
			}
			e = e.next
		} else {
			// Reached a return whose call isn't linearized: backtrack.
			if len(stack) == 0 {
				return false
			}
			top := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			s = top.state
			linearized.clear(top.e.id)
			unlift(top.e)
			e = top.e.next
		}
	}
	return true
}

package kv

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// Caller sends one operation to one server. Implemented over HTTP by
// HTTPCaller and in-process by the test harness.
type Caller interface {
	Call(ctx context.Context, server int, op Op) (Result, error)
	NumServers() int
}

// LeaderHinter is optionally returned by a Caller error to point the client
// at the current leader, saving a round of probing.
type LeaderHinter interface{ LeaderHint() int }

// Clerk is a client handle. A Clerk is safe for concurrent use, but each
// call is serialised so (ClientID, Seq) stays monotonically increasing —
// open one Clerk per logical client for parallelism. Seq counts writes
// only (reads are not deduplicated), starting at 1.
type Clerk struct {
	mu       sync.Mutex
	caller   Caller
	clientID int64
	seq      int64
	leader   int

	// RetryTimeout bounds a single attempt at one server.
	RetryTimeout time.Duration
}

func randomID() int64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return int64(binary.LittleEndian.Uint64(b[:]) >> 1)
}

func NewClerk(c Caller) *Clerk {
	return &Clerk{caller: c, clientID: randomID(), RetryTimeout: 1 * time.Second}
}

func (ck *Clerk) do(ctx context.Context, op Op) (Result, error) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if op.Type != OpGet {
		ck.seq++
	}
	op.ClientID, op.Seq = ck.clientID, ck.seq

	backoff := 10 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		actx, cancel := context.WithTimeout(ctx, ck.RetryTimeout)
		res, err := ck.caller.Call(actx, ck.leader, op)
		cancel()
		if err == nil {
			return res, nil
		}
		if errors.Is(err, ErrSessionExpired) {
			// The server forgot this session; continuing with it could
			// apply a write twice. Start a fresh session and report the
			// (ambiguous) outcome to the caller.
			ck.clientID, ck.seq = randomID(), 0
			return Result{}, err
		}
		var h LeaderHinter
		if errors.As(err, &h) && h.LeaderHint() >= 0 && h.LeaderHint() != ck.leader {
			ck.leader = h.LeaderHint()
			continue
		}
		ck.leader = (ck.leader + 1) % ck.caller.NumServers()
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
}

func (ck *Clerk) Get(ctx context.Context, key string) (string, bool, error) {
	r, err := ck.do(ctx, Op{Type: OpGet, Key: key})
	return r.Value, r.Found, err
}

func (ck *Clerk) Put(ctx context.Context, key, value string) error {
	_, err := ck.do(ctx, Op{Type: OpPut, Key: key, Value: value})
	return err
}

func (ck *Clerk) Append(ctx context.Context, key, value string) error {
	_, err := ck.do(ctx, Op{Type: OpAppend, Key: key, Value: value})
	return err
}

// AppendResult appends and returns the key's value after the append.
func (ck *Clerk) AppendResult(ctx context.Context, key, value string) (string, error) {
	r, err := ck.do(ctx, Op{Type: OpAppend, Key: key, Value: value})
	return r.Value, err
}

func (ck *Clerk) Delete(ctx context.Context, key string) (bool, error) {
	r, err := ck.do(ctx, Op{Type: OpDelete, Key: key})
	return r.Found, err
}

// CAS sets key to value only if its current value equals expected ("" means
// "key absent"). Returns whether the swap happened and the current value.
func (ck *Clerk) CAS(ctx context.Context, key, expected, value string) (bool, string, error) {
	r, err := ck.do(ctx, Op{Type: OpCAS, Key: key, Expected: expected, Value: value})
	return r.OK, r.Value, err
}

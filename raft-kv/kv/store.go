package kv

import "sync"

// Write is one key mutation produced by applying a log entry. Existed
// reports whether the key existed before (for key counting).
type Write struct {
	Key, Value string
	Delete     bool
	Existed    bool
}

// SessionUpdate records (Entry != nil) or evicts (Entry == nil) a client
// session in the dedup table.
type SessionUpdate struct {
	ClientID int64
	Entry    *SessionEntry
}

// StateStore holds the replicated state machine: user data, the client
// session table and the index of the last applied log entry.
//
// Contract:
//   - Apply makes the effects of log entry `index` visible atomically:
//     either all of writes+sessions+index are recovered after a crash, or
//     none are. It need not be durable when it returns: the Raft log is the
//     write-ahead log of record, and entries after the recovered
//     AppliedIndex are re-applied on restart.
//   - Restore replaces the entire state with a snapshot's contents.
//   - Implementations are called with the Server's lock held (no
//     concurrent calls), except Len.
type StateStore interface {
	Get(key string) (value string, found bool, err error)
	Apply(index int, writes []Write, sessions []SessionUpdate) error
	AppliedIndex() (int, error)
	LoadSessions() (map[int64]SessionEntry, error)
	Dump() (map[string]string, error)
	Restore(data map[string]string, sessions map[int64]SessionEntry, index int) error
	Len() int
	Close() error
}

// MemStore is the default in-memory StateStore. Nothing survives a
// restart: state is rebuilt from the Raft snapshot and log.
type MemStore struct {
	mu      sync.Mutex
	data    map[string]string
	applied int
}

func NewMemStore() *MemStore { return &MemStore{data: map[string]string{}} }

func (m *MemStore) Get(k string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[k]
	return v, ok, nil
}

func (m *MemStore) Apply(index int, writes []Write, _ []SessionUpdate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range writes {
		if w.Delete {
			delete(m.data, w.Key)
		} else {
			m.data[w.Key] = w.Value
		}
	}
	m.applied = index
	return nil
}

func (m *MemStore) AppliedIndex() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applied, nil
}

// LoadSessions returns nothing: in memory, sessions are not persisted
// separately (they come back with the snapshot).
func (m *MemStore) LoadSessions() (map[int64]SessionEntry, error) {
	return map[int64]SessionEntry{}, nil
}

func (m *MemStore) Dump() (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.data))
	for k, v := range m.data {
		out[k] = v
	}
	return out, nil
}

func (m *MemStore) Restore(data map[string]string, _ map[int64]SessionEntry, index int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]string, len(data))
	for k, v := range data {
		m.data[k] = v
	}
	m.applied = index
	return nil
}

func (m *MemStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.data)
}

func (m *MemStore) Close() error { return nil }

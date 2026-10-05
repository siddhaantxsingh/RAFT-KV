package raft

// LogEntry is a single entry in the replicated log.
type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

// ApplyMsg is delivered on the apply channel once an entry is committed,
// or when a snapshot must be installed into the state machine.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	PreVote      bool // true for a pre-vote probe (does not change any state)
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply carries the "fast backup" hints from the extended
// Raft paper so a leader can skip a whole conflicting term per round trip.
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term of the conflicting entry (-1 if follower log is too short)
	XIndex  int // first index the follower has for XTerm
	XLen    int // follower's log length (last index + 1)
}

// InstallSnapshotArgs carries one chunk of a snapshot (Raft paper, Fig. 13).
// Chunks are sent in order; Offset is the chunk's byte position, Total and
// Checksum (CRC-32C of the whole snapshot) let the follower verify the
// reassembled snapshot before installing it.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int64
	Data              []byte
	Done              bool
	Total             int64
	Checksum          uint32
}

// InstallSnapshotReply tells the leader where to resume (NextOffset) so an
// interrupted transfer continues instead of restarting, or that the
// follower already has everything the snapshot covers (Installed).
type InstallSnapshotReply struct {
	Term       int
	NextOffset int64
	Installed  bool
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	default:
		return "leader"
	}
}

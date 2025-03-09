package raft

import (
	"sync"
	"time"
)

type Write struct {
	Key string
	Val int
}

type Read struct {
	Key string
}

type AddServer struct {
	ServerId uint64
	Addr     string
}

type RemoveServer struct {
	ServerId uint64
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
	Dead
)

type CommitEntry struct {
	Command interface{}
	Term    uint64
	Index   uint64
}

type LogEntry struct {
	Command interface{}
	Term    uint64
}

type Node struct {
	id             uint64
	mu             sync.Mutex
	peerList       Set
	server         *Server
	db             *Database
	commitChan     chan CommitEntry
	newCommitReady chan struct{}
	trigger        chan struct{}

	currentTerm     uint64
	potentialLeader int64
	votedFor        int64
	log             []LogEntry

	commitLength       uint64
	lastApplied        uint64
	state              NodeState
	electionResetEvent time.Time

	nextIndex    map[uint64]uint64
	matchedIndex map[uint64]uint64
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderId     uint64
	LastLogIndex uint64
	LastLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	RecoveryIndex uint64
	RecoveryTerm  uint64
}

type JoinClusterArgs struct {
	ServerId   uint64
	ServerAddr string
}

type JoinClusterReply struct {
	Success    bool
	LeaderId   int64
	LeaderAddr string
	Term       uint64
}

type LeaveClusterArgs struct {
	ServerId uint64
}

type LeaveClusterReply struct {
	Success    bool
	Term       uint64
	LeaderId   uint64
	LeaderAddr string
}

type FetchPeerListArgs struct {
	Term uint64
}

type FetchPeerListReply struct {
	Success     bool
	Term        uint64
	LeaderId    int64
	LeaderAddr  string
	PeerSet     map[uint64]struct{}
	PeerAddress map[uint64]string
}

type SendDataArgs struct {
	Cmd  interface{}
	Term uint64
}

type SendDataReply struct {
	Success  bool
	Term     uint64
	LeaderId int64
	Result   struct {
		Success bool
		Value   interface{}
		Error   error
	}
}

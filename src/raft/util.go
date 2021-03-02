package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//
// a structure of log entry that can be added
//
type Entry struct {
	Command interface{}
	Term    int // term when entry was received by leader
}

// Raft state
const (
	CANDIDATE = iota
	FOLLOWER
	LEADER
)

//
// AppendEntry struct args that can be used for leader to replicate log
//
type AppednEntryArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader's commitIndex
}

//
// results return from the RPC
//
type AppendEntryReply struct {
	Term           int  // currentTerm, for leader to update itself
	Success        bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	AppendedLength int
}

//
// a function used for compare two int
//
func min(i int, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}

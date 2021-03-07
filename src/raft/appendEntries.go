package raft

import (
	"time"
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
	Term            int  // currentTerm, for leader to update itself
	Success         bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	AppendedLength  int
	DecrementLength int
}

//
// A function of AppendEntries handler
//
func (rf *Raft) AppendEntryHandler(args *AppednEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	// if old leader send something to new group
	// do nothing but return the current term to it
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	// If a candidate or leader discovers that its term is out of date,
	// it should immediately reverts to follower state
	if rf.state != FOLLOWER {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.voteCount = 0
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		rf.persist()
	}

	rf.currentTerm = args.Term
	rf.electionTimeout = false
	reply.Term = rf.currentTerm
	lastIndex := len(rf.log) - 1
	baseIndex := rf.log[0].Index
	if args.PrevLogIndex > rf.log[lastIndex].Index {
		reply.Success = false
		reply.DecrementLength += (args.PrevLogIndex - rf.log[lastIndex].Index)
	} else if rf.log[args.PrevLogIndex-baseIndex].Term != args.PrevLogTerm {
		reply.Success = false
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex - baseIndex; i > 0; i-- {
			if rf.log[i].Term != term {
				break
			}
			reply.DecrementLength++
		}
	} else {
		reply.Success = true
		// if an existing entry conflicts with a new one (same index but different term),
		// delete the existing entry and all that follow it, $5.3
		if len(args.Entries) != 0 {
			rf.log = rf.log[:args.PrevLogIndex-baseIndex+1]
			rf.log = append(rf.log, args.Entries...)
		}

		// if LeaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

		if args.LeaderCommit > rf.commitIndex {
			lastIndex = len(rf.log) - 1
			rf.commitIndex = min(args.LeaderCommit, rf.log[lastIndex].Index)
		}
		rf.persist()
	}
	rf.mu.Unlock()
	return
}

//
// send heartbeat to other server in a specific interval
//
func (rf *Raft) sendAppendEntries(server int, args *AppednEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntryHandler", args, reply)
	rf.mu.Lock()
	baseIndex := rf.log[0].Index
	if ok == false || rf.state != LEADER {
		// if the other server cannot be connected to or the state is not leader anymore
		// , nothing need to be done
	} else if reply.Success == false {
		if reply.Term > rf.currentTerm {
			// reset everything and turn state to follower
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.voteCount = 0
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			rf.electionTimeout = false
			rf.persist()
			rf.mu.Unlock()
			return ok
		} else {
			if args.PrevLogIndex >= baseIndex && rf.nextIndex[server] == args.PrevLogIndex+1 && rf.log[args.PrevLogIndex-baseIndex].Term == args.PrevLogTerm {
				rf.nextIndex[server] -= reply.DecrementLength
			}
		}
	} else if reply.Success == true {
		if args.PrevLogIndex >= baseIndex && rf.nextIndex[server] == args.PrevLogIndex+1 && rf.log[args.PrevLogIndex-baseIndex].Term == args.PrevLogTerm {
			rf.nextIndex[server] += len(args.Entries)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	}

	rf.mu.Unlock()
	return ok
}

//
// if the server is leader, then it will send heartbeat to others in case of election timeout
//
func (rf *Raft) heartbeatTimeout() {
	for {
		if rf.killed() {
			return
		}

		if !rf.AppendEntriesWrapping(false) {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// a wrapper that could send either normal hearbeat or logs
func (rf *Raft) AppendEntriesWrapping(heartbeat bool) bool {

	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return false
	}
	baseIndex := rf.log[0].Index
	lastIndex := len(rf.log) - 1
	for idx, value := range rf.nextIndex {
		if idx != rf.me && value-baseIndex <= 0 {
			args := SnapshotArgs{}
			args.Term = rf.currentTerm
			args.LeaderID = rf.me
			args.LastIncludedIndex = rf.log[0].Index
			args.LastIncludedTerm = rf.log[0].Term
			args.Data = rf.persister.ReadSnapshot()
			reply := SnapshotReply{}
			go rf.SendInstallSnapshot(idx, &args, &reply)
		} else if idx != rf.me {
			args := AppednEntryArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.log[lastIndex].Index
			args.PrevLogTerm = rf.log[lastIndex].Term
			args.Entries = make([]Entry, 0)
			reply := AppendEntryReply{}
			if !heartbeat && lastIndex >= value-baseIndex {
				args.Entries = append(args.Entries, rf.log[value-baseIndex:]...)
				args.PrevLogIndex = value - 1
				args.PrevLogTerm = rf.log[value-baseIndex-1].Term
			}
			// fmt.Printf("nextindex is %v\n", rf.nextIndex)
			// fmt.Printf("rf.log is %v, PrevLogIndex is %v, PrevLogTerm is %v\n", rf.log, rf.log[lastIndex].Index, rf.log[lastIndex].Term)
			go rf.sendAppendEntries(idx, &args, &reply)
		}
	}

	rf.mu.Unlock()

	return true
}

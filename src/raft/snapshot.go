package raft

import (
	"bytes"

	"6.824/labgob"
)

type SnapshotArgs struct {
	Term              int    // leader's term
	LeaderID          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including the index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type SnapshotReply struct {
	Term int // currentTerm, for leader to update itself.
}

// CondInstallSnapshot :
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

// Snapshot :
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	baseIndex := rf.log[0].Index
	lastIndex := rf.log[len(rf.log)-1].Index
	if index <= baseIndex || index > lastIndex {
		// can't trim log since index is invalid
		return
	}
	var log []Entry

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	snapshot = rf.persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&log)

	log = append(log, rf.log[1:index-baseIndex+1]...)

	e.Encode(log)

	rf.trimLogWithoutLock(index, rf.log[index-baseIndex].Term)
	rf.persister.SaveStateAndSnapshot(rf.getRaftStateWithoutLock(), w.Bytes())
	rf.mu.Unlock()
}

//
// recover from previous raft snapshot.
//
func (rf *Raft) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var log []Entry
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	// fmt.Printf("server %v tries to read, the rf.lastApplied is %v\n!!!!!!!!!!!!!!!!!!!!!!!!", rf.me, rf.lastApplied)

	if d.Decode(&log) == nil {
		baseIndex := log[0].Index
		for idx := rf.lastApplied + 1; idx <= log[len(log)-1].Index; idx++ {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(log[idx-baseIndex].Command)
			message := ApplyMsg{}
			message.SnapshotValid = true
			message.Snapshot = w.Bytes()
			message.SnapshotTerm = log[idx-baseIndex].Term
			message.SnapshotIndex = log[idx-baseIndex].Index
			rf.applyCh <- message
		}
	}

	lastIncludedIndex := log[len(log)-1].Index
	lastIncludedTerm := log[len(log)-1].Term
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	newLog := make([]Entry, 0)
	newLog = append(newLog, Entry{"", lastIncludedTerm, lastIncludedIndex})
	rf.log = newLog
}

// SendInstallSnapshot :
// Invoked by leader to send chunks of a snapshot to a follower.
// Leader always send chunks in order.
func (rf *Raft) SendInstallSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallsnapshotHandler", args, reply)

	rf.mu.Lock()

	defer rf.mu.Unlock()
	if ok == false || rf.state != LEADER || args.Term != rf.currentTerm {
		// do nothing
	} else if reply.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		rf.electionTimeout = false
		rf.persist()
	} else {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}

	return ok
}

// InstallsnapshotHandler :
// handler for followers to update their snapshot
func (rf *Raft) InstallsnapshotHandler(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()

	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	rf.electionTimeout = false

	if rf.currentTerm < args.Term {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		reply.Term = args.Term
		rf.persist()
		return
	}

	if args.LastIncludedIndex > rf.lastApplied {
		var log []Entry
		r := bytes.NewBuffer(args.Data)
		d := labgob.NewDecoder(r)

		if d.Decode(&log) == nil {
			// fmt.Printf("*****************************************\n")
			// fmt.Printf("the rf.lastApplied is %v\n", rf.lastApplied)
			// fmt.Printf("the snapshot is %v\n", log)
			// fmt.Printf("the leader is %v, the term is %v, the lastidx is %v and the lastterm is %v\n", args.LeaderID, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
			baseIndex := log[0].Index
			for idx := rf.lastApplied + 1; idx <= args.LastIncludedIndex && idx <= log[len(log)-1].Index; idx++ {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(log[idx-baseIndex].Command)
				message := ApplyMsg{}
				message.SnapshotValid = true
				message.Snapshot = w.Bytes()
				message.SnapshotTerm = log[idx-baseIndex].Term
				message.SnapshotIndex = log[idx-baseIndex].Index
				rf.applyCh <- message
			}
		}
		rf.persister.SaveStateAndSnapshot(rf.getRaftStateWithoutLock(), args.Data)
		newLog := make([]Entry, 0)
		newLog = append(newLog, Entry{"", args.LastIncludedTerm, args.LastIncludedIndex})
		rf.log = newLog
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex

	}
	return

}

//
// discard old log entries up to lastIncludedIndex.
//
func (rf *Raft) trimLogWithoutLock(lastIncludedIndex int, lastIncludedTerm int) {
	newLog := make([]Entry, 0)
	newLog = append(newLog, Entry{"", lastIncludedTerm, lastIncludedIndex})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newLog
}

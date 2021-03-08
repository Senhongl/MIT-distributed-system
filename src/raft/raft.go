package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// My definition
	state int // it could be either leader, candidae, follower
	// electionTimer *time.Timer   // create a ticker for election timeout
	electionTimeout bool          // an boolean value that indicate that if the server should start an election
	voteCount       int           // count the vote from other server
	applyCh         chan ApplyMsg // a chanel for apply message

	// Persistent state for all server:
	currentTerm int     // latest server has been seen (initialization should be 0 on first boot, increase monotonically)
	votedFor    int     // candidateID that received vote in current term (or -1 if none)
	log         []Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be commiteed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// (The value below will be reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

//
// a structure of log entry that can be added
//
type Entry struct {
	Command interface{}
	Term    int // term when entry was received by leader
	Index   int // the index of the entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	rf.mu.Unlock()
	return term, isleader
}

//
// encode current raft state.
//
func (rf *Raft) getRaftStateWithoutLock() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.getRaftStateWithoutLock())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm, votedFor, lastApplied, commitIndex int
	var log []Entry
	if d.Decode(&currentTerm) == nil && d.Decode(&votedFor) == nil && d.Decode(&log) == nil && d.Decode(&lastApplied) == nil && d.Decode(&commitIndex) == nil {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastApplied = lastApplied
		rf.commitIndex = commitIndex
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	if rf.killed() { // if the raft has been killed, return immediately
		return index, term, false
	}
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = (rf.state == LEADER)
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	// when there are multiple command sending to leader concurrently,
	// we must need a lock here
	index = rf.log[len(rf.log)-1].Index + 1
	newLog := Entry{command, term, index}
	rf.log = append(rf.log, newLog)
	rf.persist()
	// go rf.AppendEntriesWrapping(true)
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.log = []Entry{{"", -1, 0}}
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = FOLLOWER
	rf.electionTimeout = false
	go rf.ticker()
	go rf.sendApplyMsg()
	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	rf.persist()
	rf.mu.Unlock()
	return rf
}

//
// if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and
// log[N].term == currentTerm: set commitIndex = N. For the reason why we want to make sure
// that if the log[N].term == currentTerm is that an old log entry might be overwritten by a future leader.
// The case can be seen in paper $5.4.2.
//
func (rf *Raft) updateCommitIndexForLeader() {
	for {
		rf.mu.Lock()
		if rf.state != LEADER { // if the server convert to follower, the function should stop running
			rf.mu.Unlock()
			break
		}
		baseIndex := rf.log[0].Index
		N := rf.commitIndex + 1
		for ; N <= rf.log[len(rf.log)-1].Index; N++ {
			count := 0
			for idx, value := range rf.matchIndex {
				if idx != rf.me && value >= N {
					count++
				}
			}
			if count+1 > len(rf.peers)/2 {
				if rf.log[N-baseIndex].Term == rf.currentTerm {
					rf.commitIndex = N
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) printmsg() {
	rf.mu.Lock()
	fmt.Printf("******************************************************************************************\n")
	fmt.Printf("the server is %v, the state is %v, the term is %v\n", rf.me, rf.state, rf.currentTerm)
	fmt.Printf("the length of log is %v and the log is %v\n", len(rf.log), rf.log)
	fmt.Printf("the commitIndex is %v and the lastapplied is %v\n", rf.commitIndex, rf.lastApplied)
	fmt.Printf("the nextIndex is %v\n", rf.nextIndex)
	fmt.Printf("the matchIndex is %v\n", rf.matchIndex)
	rf.mu.Unlock()
}

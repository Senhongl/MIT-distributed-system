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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) == nil && d.Decode(&votedFor) == nil && d.Decode(&log) == nil {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.mu.Unlock()
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()

	defer rf.mu.Unlock()
	rf.electionTimeout = false
	if args.Term < rf.currentTerm {
		// According to $5.1, if a candidate or leader discovers that its term is out of date,
		// it immediately reverts to follower state. If a server receives a request with a stale
		// number, it rejects the request.
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.voteCount = 0
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		rf.persist()
	} else if rf.state == LEADER {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// now, args.Term == rf.currentTerm
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 || args.CandidateID == rf.votedFor {
		if (rf.log[len(rf.log)-1].Term < args.LastLogTerm) || // if the logs have last entries with different terms
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 <= args.LastLogIndex) {
			// if the logs end with the same term, then whichever log is longer is more up-to-date
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			rf.state = FOLLOWER
			rf.persist()
		} else {
			// the server should not vote for candidate because its log is not up-to-date
			reply.VoteGranted = false
		}
	} else {
		// the server has already vote for someone else
		reply.VoteGranted = false
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()

	if ok == false || rf.state != CANDIDATE {
		// the server lost connect or it is not candidate anymore
		rf.mu.Unlock()
		return false
	} else if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.state = FOLLOWER
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		rf.persist()
		rf.mu.Unlock()
		return false
	} else if reply.VoteGranted == true {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 && rf.state == CANDIDATE {
			rf.state = LEADER
			for idx, _ := range rf.nextIndex {
				if idx != rf.me {
					rf.nextIndex[idx] = len(rf.log)
				}
			}

			rf.mu.Unlock()
			go rf.heartbeatTimeout()
			go rf.updateCommitIndexForLeader()

			return true
		}
	}

	rf.mu.Unlock()
	return true
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
	term, isLeader = rf.GetState()
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	// when there are multiple command sending to leader concurrently,
	// we must need a lock here
	index = len(rf.log)
	newLog := Entry{command, term}
	rf.log = append(rf.log, newLog)
	rf.persist()
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.electionTimeout = false
			rf.mu.Unlock()
		} else if rf.electionTimeout {
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.state = CANDIDATE
			rf.voteCount = 1
			rf.persist()
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateID = rf.me
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
			rf.mu.Unlock()
			go rf.election(args)
		} else {
			rf.electionTimeout = true
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(rand.Int63()%300+200) * time.Millisecond)
	}
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
	rf.log = []Entry{Entry{"", -1}}
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = FOLLOWER
	rf.electionTimeout = false
	go rf.ticker()
	go rf.sendApplyMsg()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
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
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
	}

	if args.PrevLogIndex >= len(rf.log) {
		rf.electionTimeout = false
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.DecrementLength += (args.PrevLogIndex - len(rf.log) + 1)
		rf.mu.Unlock()
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.electionTimeout = false
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = false
		term := rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i].Term != term {
				break
			}
			reply.DecrementLength += 1
		}
		rf.mu.Unlock()
	} else {
		rf.electionTimeout = false
		reply.Term = rf.currentTerm
		reply.Success = true
		// if an existing entry conflicts with a new one (same index but different term),
		// delete the existing entry and all that follow it, $5.3
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		reply.AppendedLength = len(rf.log) - 1 - args.PrevLogIndex
		// if LeaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
		rf.persist()
		rf.mu.Unlock()
	}
}

//
// send heartbeat to other server in a specific interval
//
func (rf *Raft) sendAppendEntries(server int, args *AppednEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntryHandler", args, reply)

	rf.mu.Lock()

	if ok == false || rf.state != LEADER {
		// if the other server cannot be connected to or the state is not leader anymore
		// , nothing need to be done
	} else if reply.Success == false {
		if reply.Term > rf.currentTerm {
			// reset everything and turn state to follower
			// rf.state = FOLLOWER
			// rf.currentTerm = reply.Term
			// rf.votedFor = -1
			// rf.voteCount = 0
			// rf.nextIndex = make([]int, len(rf.peers))
			// rf.matchIndex = make([]int, len(rf.peers))
			// rf.electionTimeout = false
			// rf.mu.Unlock()
			// return ok
		} else {
			if rf.nextIndex[server] == args.PrevLogIndex+1 {
				rf.nextIndex[server] -= reply.DecrementLength
			}
		}
	} else if reply.Success == true {
		if rf.nextIndex[server] == args.PrevLogIndex+1 {
			rf.nextIndex[server] += reply.AppendedLength
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

		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}

		arguments := make([]AppednEntryArgs, len(rf.peers))
		replications := make([]AppendEntryReply, len(rf.peers))

		for idx, value := range rf.nextIndex {
			if idx != rf.me {
				args := AppednEntryArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = len(rf.log) - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.Entries = make([]Entry, 0)
				reply := AppendEntryReply{}
				if len(rf.log)-1 >= value {
					args.Entries = append(args.Entries, rf.log[value:]...)
					args.PrevLogIndex = value - 1
					args.PrevLogTerm = rf.log[value-1].Term
				}
				arguments[idx] = args
				replications[idx] = reply
			}
		}

		rf.mu.Unlock()

		for idx, _ := range rf.peers {
			if idx != rf.me {
				args := arguments[idx]
				reply := replications[idx]
				go rf.sendAppendEntries(idx, &args, &reply)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

//
// if commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
//
func (rf *Raft) sendApplyMsg() {
	for {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			message := ApplyMsg{}
			message.Command = rf.log[rf.lastApplied].Command
			message.CommandIndex = rf.lastApplied
			message.CommandValid = true
			// fmt.Printf("the server is %v, the command is %v and the current term is %v\n", rf.me, message.Command, rf.currentTerm)
			rf.applyCh <- message
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
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
		N := rf.commitIndex + 1
		for ; N < len(rf.log); N++ {
			count := 0
			for idx, value := range rf.matchIndex {
				if idx != rf.me && value >= N {
					count++
				}
			}
			if count+1 > len(rf.peers)/2 {
				if rf.log[N].Term == rf.currentTerm {
					rf.commitIndex = N
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

//
// start-up a election
//
func (rf *Raft) election(args RequestVoteArgs) {
	// whenever we start an election, we should reset the electiontimer

	for idx, _ := range rf.peers {
		if idx != rf.me {
			// initialize a reply
			reply := RequestVoteReply{}
			go rf.sendRequestVote(idx, &args, &reply)
		}
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

package raft

import (
	"math/rand"
	"time"
)

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
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && rf.log[len(rf.log)-1].Index <= args.LastLogIndex) {
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
	} else if args.Term == rf.currentTerm && reply.VoteGranted == true {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 && rf.state == CANDIDATE {
			rf.state = LEADER
			for idx, _ := range rf.nextIndex {
				if idx != rf.me {
					rf.nextIndex[idx] = rf.log[len(rf.log)-1].Index + 1
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
			args.LastLogIndex = rf.log[len(rf.log)-1].Index
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
// start-up a election
//
func (rf *Raft) election(args RequestVoteArgs) {
	// whenever we start an election, we should reset the electiontimer
	for idx := range rf.peers {
		if idx != rf.me {
			// initialize a reply
			reply := RequestVoteReply{}
			go rf.sendRequestVote(idx, &args, &reply)
		}
	}

}

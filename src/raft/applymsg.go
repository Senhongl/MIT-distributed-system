package raft

import "time"

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
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// if commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
//
func (rf *Raft) sendApplyMsg() {
	for {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			baseIndex := rf.log[0].Index
			rf.lastApplied++
			message := ApplyMsg{}
			// command := rf.log[rf.lastApplied-baseIndex].Command
			message.Command = rf.log[rf.lastApplied-baseIndex].Command
			message.CommandIndex = rf.lastApplied
			message.CommandTerm = rf.log[rf.lastApplied-baseIndex].Term
			message.CommandValid = true
			message.SnapshotValid = false
			// fmt.Printf("the server is %v, the command is %v and the current term is %v\n", rf.me, message.Command, rf.currentTerm)
			rf.applyCh <- message
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
}

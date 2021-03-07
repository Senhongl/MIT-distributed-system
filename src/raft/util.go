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

// Raft state
const (
	CANDIDATE = iota
	FOLLOWER
	LEADER
)

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

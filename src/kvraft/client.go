package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	requestID int64
	clientID  int64
	mu        sync.Mutex
	leaderID  int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientID = nrand()
	ck.requestID = 0
	ck.leaderID = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientID = ck.clientID
	// fmt.Printf("hello, this is get\n")
	ck.mu.Lock()
	args.RequestID = ck.requestID
	ck.requestID++
	ck.mu.Unlock()
	reply := GetReply{}
	// fmt.Printf("client %v with reqeustID %v: we should get the value of %v\n", ck.clientID, ck.requestID, key)
	// You will have to modify this function.
	leaderID := ck.leaderID
	for i := 0; i <= 1000; i++ {
		server := ck.servers[leaderID]
		ok := server.Call("KVServer.Get", &args, &reply)
		if ok == false {
			leaderID = (leaderID + 1) % len(ck.servers)
			// do nothing
		} else if reply.Err == ErrNoKey {
			ck.leaderID = leaderID
			fmt.Println("key does not exist")
			return ""
		} else if reply.Err == ErrWrongLeader {
			leaderID = (leaderID + 1) % len(ck.servers)
		} else if reply.Err == ErrTerm {
			return ""
		} else if reply.Err == ErrNoAgree {
			continue
		} else {
			ck.leaderID = leaderID
			return reply.Value
		}
		time.Sleep(20 * time.Millisecond)
	}

	fmt.Println("Get timeout")
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Op = op
	args.Key = key
	args.Value = value
	args.ClientID = ck.clientID
	ck.mu.Lock()
	args.RequestID = ck.requestID
	ck.requestID++
	ck.mu.Unlock()
	reply := PutAppendReply{}
	// fmt.Printf("client %v with requestID %v: we should %v %v to %v\n", ck.clientID, ck.requestID, op, value, key)
	leaderID := ck.leaderID
	for i := 0; i <= 1000; i++ {
		// for {
		server := ck.servers[leaderID]
		ok := server.Call("KVServer.PutAppend", &args, &reply)
		if ok == false {
			leaderID = (leaderID + 1) % len(ck.servers)
			// do nothing
		} else if reply.Err == ErrWrongLeader {
			leaderID = (leaderID + 1) % len(ck.servers)
		} else if reply.Err == ErrTerm {
			// fmt.Println("hey 120!")
			return
		} else if reply.Err == ErrNoAgree {
			// fmt.Println("hey 124!")
		} else {
			ck.leaderID = leaderID
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Println("Put timeout")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

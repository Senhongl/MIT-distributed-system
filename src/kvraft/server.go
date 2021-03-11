package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

type KVReply struct {
	Success bool
	Term    int
	Value   string
	Err     Err
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int                  // snapshot if log grows this big
	resultCh     map[int]chan KVReply // an array of channel to receive the result from raft
	requestID    map[int64]int64
	// ack          map[int64]int64
	// Your definitions here.
	log map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isRepeated(args.ClientID, args.RequestID) {
		reply.Success = true
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{Operation: "Get", Key: args.Key, ClientID: args.ClientID, RequestID: args.RequestID}
	idx, term, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Success = false
		reply.Err = ErrWrongLeader
		return
	} else {
		// fmt.Printf("%v: someone ask me to get the value of %v\n", idx, args.Key)
		if _, ok := kv.resultCh[idx]; !ok {
			kv.resultCh[idx] = make(chan KVReply, 1)
		} else {
			fmt.Println("impossible!")
		}
		select {
		case res := <-kv.resultCh[idx]:
			if res.Success == true && res.Term == term {
				reply.Success = true
				reply.Value = res.Value
				reply.Err = OK
				// fmt.Printf("%v: yep, got the result from %v, which is %v\n", idx, kv.me, res.Value)
			} else if res.Success == false {
				reply.Success = false
				reply.Err = res.Err
				// fmt.Printf("%v: nope, we could not achieve it.\n", idx)
			} else {
				reply.Success = false
				reply.Err = ErrTerm
				// fmt.Printf("%v: well, inequal term\n", idx)
			}
			kv.mu.Lock()
			close(kv.resultCh[idx])
			delete(kv.resultCh, idx)
			kv.mu.Unlock()
			return
		case <-time.After(time.Second):
			reply.Success = false
			reply.Err = ErrNoAgree
			kv.mu.Lock()
			close(kv.resultCh[idx])
			delete(kv.resultCh, idx)
			kv.mu.Unlock()
			// fmt.Println("Get timeout")
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// t := time.Now()
	kv.mu.Lock()
	if kv.isRepeated(args.ClientID, args.RequestID) {
		reply.Success = true
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{Operation: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID, RequestID: args.RequestID}
	idx, term, isLeader := kv.rf.Start(op)
	if isLeader == false {
		// fmt.Printf("%v: not leader\n", kv.me)
		reply.Success = false
		reply.Err = ErrWrongLeader
		// fmt.Printf("not leader, take %v\n", time.Since(t))
		return
	} else {
		// fmt.Printf("%v: someone ask me to %v the value of %v to %v\n", idx, args.Op, args.Value, args.Key)
		if _, ok := kv.resultCh[idx]; !ok {
			kv.resultCh[idx] = make(chan KVReply, 1)
		} else {
			fmt.Println("imporssible!")
		}

		select {
		case res := <-kv.resultCh[idx]:
			if res.Success == true && res.Term == term {
				reply.Success = true
				// fmt.Printf("%v: we made it!\n", idx)
			} else {
				reply.Success = false
				reply.Err = ErrTerm
				// fmt.Printf("%v: line 143!\n", idx)
			}
			kv.mu.Lock()
			close(kv.resultCh[idx])
			delete(kv.resultCh, idx)
			kv.mu.Unlock()
			// fmt.Printf("Success, take %v\n", time.Since(t))
			return
		case <-time.After(time.Second):
			reply.Success = false
			reply.Err = ErrNoAgree
			kv.mu.Lock()
			close(kv.resultCh[idx])
			delete(kv.resultCh, idx)
			kv.mu.Unlock()
			// fmt.Println("Put timeout")
			return
		}

	}
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if m.CommandValid == false {
			// ignore other types of ApplyMsg
		} else {
			kv.mu.Lock()
			cmd := m.Command.(Op)
			if !kv.isRepeated(cmd.ClientID, cmd.RequestID) {
				// fmt.Printf("1: the cmd is %v, and the requestid is %v\n", cmd, kv.requestID[cmd.ClientID])
				if cmd.Operation == "Put" {
					kv.log[cmd.Key] = cmd.Value
				} else if cmd.Operation == "Append" {
					if val, ok := kv.log[cmd.Key]; ok {
						kv.log[cmd.Key] = val + cmd.Value
					} else {
						kv.log[cmd.Key] = cmd.Value
					}
				}
				kv.requestID[cmd.ClientID] = cmd.RequestID
			}

			if ch, ok := kv.resultCh[m.CommandIndex]; ok {
				if val, logExist := kv.log[cmd.Key]; logExist {
					reply := KVReply{Success: true, Term: m.CommandTerm, Value: val}
					ch <- reply
				} else {
					reply := KVReply{Success: false, Term: m.CommandTerm, Err: ErrNoKey}
					ch <- reply
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) isRepeated(clientID int64, requestID int64) bool {
	if lastRequstID, ok := kv.requestID[clientID]; ok {
		if lastRequstID >= requestID {
			return true
		}
	}

	return false
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.log = make(map[string]string)
	kv.resultCh = make(map[int]chan KVReply)
	kv.requestID = make(map[int64]int64)
	go kv.applier()
	return kv
}

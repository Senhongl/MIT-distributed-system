package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTerm        = "Incompatible Term"
	ErrNoAgree     = "Raft cannot make agreement"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientID  int64
	RequestID int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Success bool
	Err     Err
}

type GetArgs struct {
	Key       string
	ClientID  int64
	RequestID int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Success bool
	Err     Err
	Value   string
}

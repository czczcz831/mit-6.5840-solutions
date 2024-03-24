package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrAgreement   = "ErrAgreement"
	ErrStaleReq    = "ErrStaleReq"
	GetType        = 1
	PutType        = 2
	AppendType     = 3
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientID string
	Seq      int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err    Err
	Leader int64
}

type GetArgs struct {
	Key      string
	ClientID string
	Seq      int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err    Err
	Value  string
	Leader int64
}

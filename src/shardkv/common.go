package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrAgreement    = "ErrAgreement"
	ErrStaleConfig  = "ErrStaleConfig"
	GetType         = 1
	PutType         = 2
	AppendType      = 3
	StartConfigType = 4
	NeedOpType      = 5
	SendOpType      = 6
	EmptyOp         = 7
	//ShardState
	ShardNoHas = 1
	ShardHas   = 2
	ShardNeed  = 3
	ShardSend  = 4
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	ClientID string
	Seq      int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err    Err
	Leader int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID string
	Seq      int64
}

type GetReply struct {
	Err    Err
	Value  string
	Leader int64
}

type RequestMapArgs struct {
	Shard     int
	ConfigNum int
	ClientID  string
	Seq       int64
}

type RequestMapReply struct {
	ShardData map[string]string
	DupGetTab map[string]map[int64]GetReply
	ConfigNum int
	Shard     int
	Err       Err
	Leader    int64
}

type ReceiveMapReply struct {
	Receive bool
	Shard   int
	Err     Err
}

type ReceiveMapArgs struct {
	ConfigNum int
	Shard     int
}

type StartConfigArgs struct {
	NewConfig shardctrler.Config
}

type NeedOp struct {
	Reply RequestMapReply
}

type SendOp struct {
	ConfigNum int
	Shard     int
}

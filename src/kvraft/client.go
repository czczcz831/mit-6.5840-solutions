package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"github.com/google/uuid"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LastLeader int64
	mu         sync.Mutex
	Seq        int64
	UUID       string
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
	// You'll have to add code here.
	ck.UUID = uuid.New().String()
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	ok := false
	var reply GetReply
	i := ck.LastLeader
	args := GetArgs{
		Key:      key,
		ClientID: ck.UUID,
		Seq:      ck.Seq,
	}
	for !ok || reply.Err != OK {
		reply.Err = ""

		ck.mu.Unlock()
		flag := make(chan bool)
		go func() {
			ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
			flag <- true
		}()
		select {
		case <-flag:
		case <-time.After(500 * time.Millisecond):
		}
		// DPrintf("Client %v 发送给 %v Get key:%v", ck.UUID, i, key)
		i = nrand() % int64(len(ck.servers))
		ck.mu.Lock()
		if reply.Err == ErrStaleReq {
			break
		}
	}
	ck.Seq++
	ck.LastLeader = reply.Leader
	ck.mu.Unlock()

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ok := false
	var reply PutAppendReply
	i := ck.LastLeader
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.UUID,
		Seq:      ck.Seq,
	}
	for !ok || reply.Err != OK {
		reply.Err = ""
		ck.mu.Unlock()
		flag := make(chan bool)
		go func() {
			ok = ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			flag <- true
		}()
		select {
		case <-flag:
		case <-time.After(500 * time.Millisecond):
		}
		// DPrintf("Client %v 发送给 %v PutAppend key:%v value:%v", ck.UUID, i, key, value)
		// DPrintf("Client %v 发送给 %v PutAppend key:%v value:%v", ck.UUID, i, key, value)
		// DPrintf("Client %v 收到来自 %v 的reply %v", ck.UUID, i, reply)
		i = nrand() % int64(len(ck.servers))
		ck.mu.Lock()
		if reply.Err == ErrStaleReq {
			args.Seq = ck.Seq
		}
	}

	ck.Seq++
	ck.LastLeader = reply.Leader
	ck.mu.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

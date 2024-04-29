package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"github.com/google/uuid"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	lastLeader map[int]int   // gid -> leader
	Seq        map[int]int64 //gid -> Seq
	UUID       string
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.UUID = uuid.New().String()
	ck.Seq = make(map[int]int64)
	ck.lastLeader = make(map[int]int)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	DPrintf("客户端Get key %v", key)
	shard := key2shard(key)
	ck.mu.Lock()
	var reply *GetReply

	gid := ck.config.Shards[shard]
	leader := ck.lastLeader[ck.config.Shards[shard]]
	if ck.Seq[gid] == 0 {
		ck.Seq[gid] = 1
	}
	args := GetArgs{
		Key:      key,
		ClientID: ck.UUID,
		Seq:      ck.Seq[gid],
	}
	for {
		if servers, ok := ck.config.Groups[gid]; ok {
			reply = &GetReply{}
			reply.Err = ""
			flag := make(chan bool)
			go func() {
				srv := ck.make_end(servers[leader])
				ok = srv.Call("ShardKV.Get", &args, &reply)
				flag <- true
			}()
			select {
			case <-flag:
			case <-time.After(500 * time.Millisecond):
			}
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				break
			}
			if ok && (reply.Err == ErrWrongGroup) {
				time.Sleep(100 * time.Millisecond)
				ck.config = ck.sm.Query(-1)
				gid = ck.config.Shards[shard]
				leader = ck.lastLeader[gid]
				if ck.Seq[gid] == 0 {
					ck.Seq[gid] = 1
				}
				args.Seq = ck.Seq[gid]
				continue
			}
			//not ok or wrong leader
			leader = int(nrand()) % len(ck.config.Groups[gid])
		} else {
			time.Sleep(100 * time.Millisecond)
			ck.config = ck.sm.Query(-1)
			gid = ck.config.Shards[shard]
			leader = ck.lastLeader[gid]
			if ck.Seq[gid] == 0 {
				ck.Seq[gid] = 1
			}
			args.Seq = ck.Seq[gid]
			continue
		}
	}

	ck.Seq[gid]++
	ck.lastLeader[gid] = leader
	ck.mu.Unlock()
	return reply.Value
	// if servers, ok := ck.config.Groups[gid]; ok {
	// 	// try each server for the shard.
	// 	for si := 0; si < len(servers); si++ {
	// 		srv := ck.make_end(servers[si])
	// 		var reply GetReply
	// 		ok := srv.Call("ShardKV.Get", &args, &reply)
	// 		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
	// 			return reply.Value
	// 		}
	// 		if ok && (reply.Err == ErrWrongGroup) {
	// 			break
	// 		}
	// 		// ... not ok, or ErrWrongLeader
	// 	}
	// }
	// time.Sleep(100 * time.Millisecond)
	// ask controler for the latest configuration.
	// 	ck.config = ck.sm.Query(-1)
	// }

	// return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	shard := key2shard(key)
	ck.mu.Lock()
	var reply *PutAppendReply

	gid := ck.config.Shards[shard]
	leader := ck.lastLeader[ck.config.Shards[shard]]
	if ck.Seq[gid] == 0 {
		ck.Seq[gid] = 1
	}
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.UUID,
		Seq:      ck.Seq[gid],
	}
	for {
		if servers, ok := ck.config.Groups[gid]; ok {
			reply = &PutAppendReply{}
			reply.Err = ""
			flag := make(chan bool)
			go func() {
				srv := ck.make_end(servers[leader])
				ok = srv.Call("ShardKV.PutAppend", &args, &reply)
				flag <- true
			}()
			select {
			case <-flag:
			case <-time.After(500 * time.Millisecond):
			}
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				DPrintf("客户端PutAppend 成功 key %v value %v ", key, value)
				break
			}
			if ok && (reply.Err == ErrWrongGroup) {
				time.Sleep(100 * time.Millisecond)
				ck.config = ck.sm.Query(-1)
				gid = ck.config.Shards[shard]
				leader = ck.lastLeader[gid]
				if ck.Seq[gid] == 0 {
					ck.Seq[gid] = 1
				}
				args.Seq = ck.Seq[gid]
				continue
			}
			//not ok or wrong leader
			leader = int(nrand()) % len(ck.config.Groups[gid])
		} else {
			time.Sleep(100 * time.Millisecond)
			ck.config = ck.sm.Query(-1)
			gid = ck.config.Shards[shard]
			leader = ck.lastLeader[gid]
			if ck.Seq[gid] == 0 {
				ck.Seq[gid] = 1
			}
			args.Seq = ck.Seq[gid]
		}
	}

	ck.Seq[gid]++
	ck.lastLeader[gid] = leader
	ck.mu.Unlock()
	// args := PutAppendArgs{}
	// args.Key = key
	// args.Value = value
	// args.Op = op

	// for {
	// 	shard := key2shard(key)
	// 	gid := ck.config.Shards[shard]
	// 	if servers, ok := ck.config.Groups[gid]; ok {
	// 		for si := 0; si < len(servers); si++ {
	// 			srv := ck.make_end(servers[si])
	// 			var reply PutAppendReply
	// 			ok := srv.Call("ShardKV.PutAppend", &args, &reply)
	// 			if ok && reply.Err == OK {
	// 				return
	// 			}
	// 			if ok && reply.Err == ErrWrongGroup {
	// 				break
	// 			}
	// 			// ... not ok, or ErrWrongLeader
	// 		}
	// 	}
	// 	time.Sleep(100 * time.Millisecond)
	// 	// ask controler for the latest configuration.
	// 	ck.config = ck.sm.Query(-1)
	// }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
	DPrintf("客户端Put key %v value %v", key, value)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
	DPrintf("客户端Append key %v value %v", key, value)
}

package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
	"github.com/google/uuid"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.UUID = uuid.New().String()
	ck.Seq = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.mu.Lock()
	ok := false
	var reply *QueryReply
	i := ck.LastLeader

	args := QueryArgs{
		ClientID: ck.UUID,
		Num:      num,
		Seq:      ck.Seq,
	}

	for !ok || reply.Err != OK {
		reply = &QueryReply{}
		reply.Err = ""
		// ck.mu.Unlock()
		flag := make(chan bool)
		go func() {
			ok = ck.servers[i].Call("ShardCtrler.Query", &args, &reply)
			flag <- true
		}()
		select {
		case <-flag:
		case <-time.After(500 * time.Millisecond):
		}
		// DPrintf("Client %v 发送给 %v Get key:%v", ck.UUID, i, key)
		i = nrand() % int64(len(ck.servers))
		// ck.mu.Lock()
	}
	ck.Seq++
	ck.LastLeader = reply.Leader
	ck.mu.Unlock()

	return reply.Config
	// for {
	// 	// try each known server.
	// 	for _, srv := range ck.servers {
	// 		var reply QueryReply
	// 		ok := srv.Call("ShardCtrler.Query", args, &reply)
	// 		if ok && reply.WrongLeader == false {
	// 			return reply.Config
	// 		}
	// 	}
	// 	time.Sleep(100 * time.Millisecond)
	// }
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.mu.Lock()
	ok := false
	var reply *JoinReply
	i := ck.LastLeader
	args := JoinArgs{
		Servers:  servers,
		ClientID: ck.UUID,
		Seq:      ck.Seq,
	}
	for !ok || reply.Err != OK {
		reply = &JoinReply{}
		reply.Err = ""
		// ck.mu.Unlock()
		flag := make(chan bool)
		go func() {
			ok = ck.servers[i].Call("ShardCtrler.Join", &args, &reply)
			flag <- true
		}()
		select {
		case <-flag:
		case <-time.After(500 * time.Millisecond):
		}
		// DPrintf("Client %v 发送给 %v Get key:%v", ck.UUID, i, key)
		i = nrand() % int64(len(ck.servers))
		// ck.mu.Lock()
	}
	ck.Seq++
	ck.LastLeader = reply.Leader
	ck.mu.Unlock()

	// for {
	// 	// try each known server.
	// 	for _, srv := range ck.servers {
	// 		var reply JoinReply
	// 		ok := srv.Call("ShardCtrler.Join", args, &reply)
	// 		if ok && reply.WrongLeader == false {
	// 			return
	// 		}
	// 	}
	// 	time.Sleep(100 * time.Millisecond)
	// }
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	ok := false
	var reply *LeaveReply
	i := ck.LastLeader
	args := LeaveArgs{
		GIDs:     gids,
		ClientID: ck.UUID,
		Seq:      ck.Seq,
	}
	for !ok || reply.Err != OK {
		reply = &LeaveReply{}
		reply.Err = ""
		// ck.mu.Unlock()
		flag := make(chan bool)
		go func() {
			ok = ck.servers[i].Call("ShardCtrler.Leave", &args, &reply)
			flag <- true
		}()
		select {
		case <-flag:
		case <-time.After(500 * time.Millisecond):
		}
		// DPrintf("Client %v 发送给 %v Get key:%v", ck.UUID, i, key)
		i = nrand() % int64(len(ck.servers))
		// ck.mu.Lock()
	}
	ck.Seq++
	ck.LastLeader = reply.Leader
	ck.mu.Unlock()

	// for {
	// 	// try each known server.
	// 	for _, srv := range ck.servers {
	// 		var reply LeaveReply
	// 		ok := srv.Call("ShardCtrler.Leave", args, &reply)
	// 		if ok && reply.WrongLeader == false {
	// 			return
	// 		}
	// 	}
	// 	time.Sleep(100 * time.Millisecond)
	// }
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	ok := false
	var reply *MoveReply
	i := ck.LastLeader
	args := MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientID: ck.UUID,
		Seq:      ck.Seq,
	}
	for !ok || reply.Err != OK {
		reply = &MoveReply{}
		reply.Err = ""
		// ck.mu.Unlock()
		flag := make(chan bool)
		go func() {
			ok = ck.servers[i].Call("ShardCtrler.Move", &args, &reply)
			flag <- true
		}()
		select {
		case <-flag:
		case <-time.After(500 * time.Millisecond):
		}
		// DPrintf("Client %v 发送给 %v Get key:%v", ck.UUID, i, key)
		i = nrand() % int64(len(ck.servers))
		// ck.mu.Lock()
	}
	ck.Seq++
	ck.LastLeader = reply.Leader
	ck.mu.Unlock()

	// for {
	// 	// try each known server.
	// 	for _, srv := range ck.servers {
	// 		var reply MoveReply
	// 		ok := srv.Call("ShardCtrler.Move", args, &reply)
	// 		if ok && reply.WrongLeader == false {
	// 			return
	// 		}
	// 	}
	// 	time.Sleep(100 * time.Millisecond)
	// }
}

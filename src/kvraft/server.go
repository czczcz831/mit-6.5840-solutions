package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

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
	Type     int
	Cmd      interface{}
	ClientID int
	Seq      int
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	data    map[string]string
	DupTab  map[string]map[int64]TableRes
	LastSeq map[string]int64

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

type TableRes struct {
	Type         int
	GetRpl       GetReply
	PutAppendRpl PutAppendReply
}

func (kv *KVServer) ExectuteOp() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			DPrintf("Server %d receive msg %v", kv.me, msg)
			op := msg.Command.(Op)
			kv.mu.Lock()
			//泛型判断操作类型
			if op.Type == GetType {
				args := op.Cmd.(GetArgs)
				if _, ok := kv.DupTab[args.ClientID]; !ok {
					kv.DupTab[args.ClientID] = make(map[int64]TableRes)
				}
				if kv.DupTab[args.ClientID][args.Seq].Type != GetType {
					kv.DupTab[args.ClientID][args.Seq] = TableRes{Type: GetType, GetRpl: GetReply{Value: kv.data[args.Key], Err: OK}}
					// DPrintf("Server %d get 成功 key:%v value:%v ", kv.me, args.Key, kv.data[args.Key])
				}
			} else if op.Type == PutType {
				args := op.Cmd.(PutAppendArgs)
				if _, ok := kv.DupTab[args.ClientID]; !ok {
					kv.DupTab[args.ClientID] = make(map[int64]TableRes)
				}
				if kv.DupTab[args.ClientID][args.Seq].Type != PutType {
					kv.data[args.Key] = args.Value
					kv.DupTab[args.ClientID][args.Seq] = TableRes{Type: PutType, PutAppendRpl: PutAppendReply{Err: OK}}
					// DPrintf("Server %d put 成功 key:%v value:%v ", kv.me, args.Key, kv.data[args.Key])
				}
			} else if op.Type == AppendType {
				args := op.Cmd.(PutAppendArgs)
				if _, ok := kv.DupTab[args.ClientID]; !ok {
					kv.DupTab[args.ClientID] = make(map[int64]TableRes)
				}
				if kv.DupTab[args.ClientID][args.Seq].Type != AppendType {
					kv.data[args.Key] += args.Value
					// DPrintf("Server %d Append 成功 key:%v value:%v ", kv.me, args.Key, kv.data[args.Key])
					kv.DupTab[args.ClientID][args.Seq] = TableRes{Type: AppendType, PutAppendRpl: PutAppendReply{Err: OK}}
				}
			}
			kv.mu.Unlock()
		}

	}
}

// 简单垃圾回收
func (kv *KVServer) GC() {
	ticker := time.NewTicker(200 * time.Millisecond)
	for {
		<-ticker.C
		//STW !!!
		kv.mu.Lock()
		for client, seq := range kv.DupTab {
			for s, _ := range seq {
				if kv.LastSeq[client]-200 > s {
					delete(kv.DupTab[client], s)
				}
			}
		}
		kv.mu.Unlock()

	}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK
	cmd := Op{Type: GetType, Cmd: *args}
	//说明是很久以前的请求，让client重新发送
	if kv.LastSeq[args.ClientID] > args.Seq {
		reply.Err = ErrStaleReq
		return
	}

	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Leader = int64(kv.me)

	for {
		select {
		case <-time.After(1 * time.Second):
			reply.Err = ErrAgreement
			return
		default:
			kv.mu.RLock()
			if kv.DupTab[args.ClientID][args.Seq].Type == GetType {
				reply.Err = OK
				reply.Value = kv.DupTab[args.ClientID][args.Seq].GetRpl.Value
				kv.mu.RUnlock()
				return
			}
			kv.mu.RUnlock()
			time.Sleep(10 * time.Millisecond)
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK
	cmd := Op{}
	cmd.Cmd = *args
	//说明是很久以前的请求，让client重新发送
	if kv.LastSeq[args.ClientID] > args.Seq {
		reply.Err = ErrStaleReq
		return
	}
	if args.Op == "Put" {
		cmd.Type = PutType
	} else {
		cmd.Type = AppendType
	}
	_, _, IsLeader := kv.rf.Start(cmd)
	if !IsLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Leader = int64(kv.me)

	for {
		select {
		case <-time.After(1 * time.Second):
			reply.Err = ErrAgreement
			return
		default:
			kv.mu.RLock()
			if kv.DupTab[args.ClientID][args.Seq].Type == PutType || kv.DupTab[args.ClientID][args.Seq].Type == AppendType {
				reply.Err = OK
				kv.mu.RUnlock()
				return
			}
			kv.mu.RUnlock()
			time.Sleep(10 * time.Millisecond)
		}
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.data = make(map[string]string)
	kv.DupTab = make(map[string]map[int64]TableRes)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.ExectuteOp()
	go kv.GC()

	// You may need initialization code here.

	return kv
}

package kvraft

import (
	"bytes"
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
	mu                sync.RWMutex
	me                int
	rf                *raft.Raft
	applyCh           chan raft.ApplyMsg
	dead              int32 // set by Kill()
	Data              map[string]string
	DupGetTab         map[string]map[int64]GetReply
	LastSeq           map[string]int64
	LastApplyIndex    int
	LastSnapshotIndex int

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
		//如果是旧的日志，直接跳过
		if kv.LastApplyIndex > msg.CommandIndex && msg.CommandValid {
			continue
		}
		//只能安装比上次快照大的index
		if kv.LastSnapshotIndex > msg.SnapshotIndex && msg.SnapshotValid {
			continue
		}
		if msg.CommandValid {
			DPrintf("Server %d receive msg %v", kv.me, msg)
			op := msg.Command.(Op)
			kv.mu.Lock()
			//泛型判断操作类型
			if op.Type == GetType {
				args := op.Cmd.(GetArgs)
				//如果操作已经被执行过
				if args.Seq <= kv.LastSeq[args.ClientID] {
					kv.mu.Unlock()
					continue
				}
				if _, ok := kv.DupGetTab[args.ClientID]; !ok {
					kv.DupGetTab[args.ClientID] = make(map[int64]GetReply)
				}
				kv.DupGetTab[args.ClientID][args.Seq] = GetReply{Value: kv.Data[args.Key], Err: OK}
				kv.LastSeq[args.ClientID] = args.Seq
				DPrintf("Server %d get 成功 key:%v value:%v ", kv.me, args.Key, kv.Data[args.Key])
			} else if op.Type == PutType {
				args := op.Cmd.(PutAppendArgs)
				//如果操作已经被执行过
				if args.Seq <= kv.LastSeq[args.ClientID] {
					kv.mu.Unlock()
					continue
				}
				kv.Data[args.Key] = args.Value
				kv.LastSeq[args.ClientID] = args.Seq
				DPrintf("Server %d put 成功 key:%v value:%v ", kv.me, args.Key, args.Value)
			} else if op.Type == AppendType {
				args := op.Cmd.(PutAppendArgs)
				if args.Seq <= kv.LastSeq[args.ClientID] {
					kv.mu.Unlock()
					continue
				}
				if _, ok := kv.DupGetTab[args.ClientID]; !ok {
					kv.DupGetTab[args.ClientID] = make(map[int64]GetReply)
				}
				kv.Data[args.Key] += args.Value
				DPrintf("Server %d Append 成功 key:%v value:%v ", kv.me, args.Key, args.Value)
				kv.LastSeq[args.ClientID] = args.Seq
			}
			kv.LastApplyIndex = msg.CommandIndex
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			//Snapshot
			kv.mu.Lock()
			DPrintf("Server %v 接收到快照 %v", kv.me, msg.SnapshotIndex)
			kv.readPersist(msg.Snapshot)
			kv.mu.Unlock()
		}

	}
}

// 简单垃圾回收
func (kv *KVServer) GC() {
	for {
		//STW !!!
		time.Sleep(200 * time.Millisecond)
		kv.mu.Lock()
		for client, seq := range kv.DupGetTab {
			for s, _ := range seq {
				if kv.LastSeq[client]-200 > s {
					delete(kv.DupGetTab[client], s)
				}
			}
		}
		kv.mu.Unlock()
	}

}

func (kv *KVServer) SnapshotRaft() {
	if kv.maxraftstate == -1 {
		return
	}
	for {
		time.Sleep(200 * time.Millisecond)
		RaftSize := kv.rf.Persister.RaftStateSize()
		if RaftSize > kv.maxraftstate-500 {
			kv.mu.Lock()
			if kv.LastApplyIndex <= kv.LastSnapshotIndex {
				kv.mu.Unlock()
				continue
			}
			kv.rf.Snapshot(kv.LastApplyIndex, kv.persist())
			kv.LastSnapshotIndex = kv.LastApplyIndex
			DPrintf("重点Server %v Snapshot at index %v", kv.me, kv.LastApplyIndex)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) persist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Data)
	e.Encode(kv.LastSeq)
	e.Encode(kv.LastApplyIndex)
	e.Encode(kv.LastSnapshotIndex)
	return w.Bytes()
}

func (kv *KVServer) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.Data)
	d.Decode(&kv.LastSeq)
	d.Decode(&kv.LastApplyIndex)
	d.Decode(&kv.LastSnapshotIndex)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK
	cmd := Op{Type: GetType, Cmd: *args}
	kv.mu.RLock()
	if rpl, ok := kv.DupGetTab[args.ClientID][args.Seq]; ok {
		reply.Err = OK
		reply.Value = rpl.Value
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Leader = int64(kv.me)

	for {
		select {
		case <-time.After(200 * time.Millisecond):
			reply.Err = ErrAgreement
			return
		default:
			kv.mu.RLock()
			if rpl, ok := kv.DupGetTab[args.ClientID][args.Seq]; ok {
				reply.Err = OK
				reply.Value = rpl.Value
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
	kv.mu.RLock()
	if kv.LastSeq[args.ClientID] >= args.Seq {
		kv.mu.RUnlock()
		reply.Err = OK
		return
	}
	kv.mu.RUnlock()
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
		case <-time.After(200 * time.Millisecond):
			reply.Err = ErrAgreement
			return
		default:
			kv.mu.RLock()
			if kv.LastSeq[args.ClientID] >= args.Seq {
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
	kv.Data = make(map[string]string)
	kv.DupGetTab = make(map[string]map[int64]GetReply)
	kv.LastSeq = make(map[string]int64)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readPersist(persister.ReadSnapshot())

	go kv.ExectuteOp()
	go kv.GC()
	go kv.SnapshotRaft()

	// You may need initialization code here.

	return kv
}

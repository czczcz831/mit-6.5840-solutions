package shardkv

import (
	"bytes"
	"log"
	"time"

	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type     int
	Cmd      interface{}
	ClientID int
	Seq      int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mapShard     map[string]string
	killed       bool

	// Your definitions here.
	DupGetTab map[string]map[int64]GetReply
	LastSeq   map[string]int64 //上一次应用的Seq,这个是给clerk用的，GID里不允许使用这个
	// NextSeq           map[int]int64    //gid -> Seq 发送到对方gid的Seq
	LastApplyIndex    int
	LastSnapshotIndex int
	OldConfig         shardctrler.Config
	Config            shardctrler.Config
	NewConfig         shardctrler.Config
	ShardsState       [shardctrler.NShards]int

	//ShardControllerClient
	ck *shardctrler.Clerk

	//Arguments As Handoff Client
	LastLeader map[int]int //gid->leader
}

func (kv *ShardKV) ExectuteOp() {
	for !kv.killed {
		msg := <-kv.applyCh
		index := msg.CommandIndex
		kv.mu.Lock()
		//如果是旧的日志，直接跳过
		if kv.LastApplyIndex > msg.CommandIndex && msg.CommandValid {
			DPrintf("被跳过")
			kv.mu.Unlock()
			continue
		}
		//只能安装比上次快照大的index
		if kv.LastSnapshotIndex > msg.SnapshotIndex && msg.SnapshotValid {
			kv.mu.Unlock()
			continue
		}
		if msg.CommandValid {
			// DPrintf("Server %d receive msg %v", kv.me, msg)
			op := msg.Command.(Op)
			//泛型判断操作类型
			if op.Type == GetType {
				args := op.Cmd.(GetArgs)
				//如果操作已经被执行过
				if args.Seq <= kv.LastSeq[args.ClientID] {
					kv.mu.Unlock()
					continue
				}
				if has := kv.hasKey(args.Key); !has {
					kv.mu.Unlock()
					continue
				}
				if _, ok := kv.DupGetTab[args.ClientID]; !ok {
					kv.DupGetTab[args.ClientID] = make(map[int64]GetReply)
				}

				kv.DupGetTab[args.ClientID][args.Seq] = GetReply{Value: kv.mapShard[args.Key], Err: OK}
				kv.LastSeq[args.ClientID] = args.Seq
				DPrintf("%v 集群 config %v", kv.gid, kv.Config)
				DPrintf("%v 的Server %d get 成功 key %vshard:%v value:%v index:%v ", kv.gid, kv.me, args.Key, key2shard(args.Key), kv.mapShard[args.Key], index)
			} else if op.Type == PutType {
				args := op.Cmd.(PutAppendArgs)
				DPrintf("%v 的Server %d 收到了put请求 k:%v v:%v ", kv.gid, kv.me, args.Key, args.Value)
				//如果操作已经被执行过或者正在等待配置
				if args.Seq <= kv.LastSeq[args.ClientID] {
					kv.mu.Unlock()
					continue
				}
				if has := kv.hasKey(args.Key); !has {
					kv.mu.Unlock()
					continue
				}

				kv.mapShard[args.Key] = args.Value
				kv.LastSeq[args.ClientID] = args.Seq
				DPrintf("%v 的Server %d put 成功 key:%v value:%v index: %v", kv.gid, kv.me, args.Key, args.Value, index)
			} else if op.Type == AppendType {
				args := op.Cmd.(PutAppendArgs)
				DPrintf("%v 的Server %d 收到了append请求 k:%v v:%v ", kv.gid, kv.me, args.Key, args.Value)
				if args.Seq <= kv.LastSeq[args.ClientID] {
					kv.mu.Unlock()
					continue
				}
				if has := kv.hasKey(args.Key); !has {
					kv.mu.Unlock()
					continue
				}

				if _, ok := kv.DupGetTab[args.ClientID]; !ok {
					kv.DupGetTab[args.ClientID] = make(map[int64]GetReply)
				}
				kv.mapShard[args.Key] += args.Value
				kv.LastSeq[args.ClientID] = args.Seq
				DPrintf("%v的Server %d Append %v 成功 key:%v value:%v index:%v  ", kv.gid, kv.me, args.Value, args.Key, kv.mapShard[args.Key], index)
			} else if op.Type == StartConfigType { //让整个集群都进入配置更新阶段
				startArgs := op.Cmd.(StartConfigArgs)

				//如果配置更新
				if startArgs.NewConfig.Num == kv.Config.Num+1 {
					DPrintf("%v 的Server %v 开始更新配置 %v", kv.gid, kv.me, startArgs.NewConfig)
					kv.NewConfig = startArgs.NewConfig
					kv.makeShardsState(kv.NewConfig)
					if kv.OldConfig.Num != 0 || kv.Config.Num != 0 {
						kv.OldConfig = kv.Config
					}
					kv.Config = kv.NewConfig
				}

			} else if op.Type == NeedOpType {
				args := op.Cmd.(NeedOp)
				if kv.Config.Num == args.Reply.ConfigNum && kv.ShardsState[args.Reply.Shard] == ShardNeed {
					DPrintf("%v 的Server %v 完成接收 %v", kv.gid, kv.me, args.Reply.Shard)
					tMap := make(map[string]string)
					copyMap(args.Reply.ShardData, tMap)
					for k, v := range tMap {
						kv.mapShard[k] = v
					}
					kv.ShardsState[args.Reply.Shard] = ShardHas

					// 将shard前任所有者的dupgettab中较新的记录应用到自己的dupgetab，用于防止shard迁移后重复执行某些指令导致结果错误
					// for client, seqs := range args.Reply.DupGetTab {
					// 	for seq, rpl := range seqs {
					// 		if _, ok := kv.DupGetTab[client]; !ok {
					// 			kv.DupGetTab[client] = make(map[int64]GetReply)
					// 		}
					// 		if kv.LastSeq[client] < seq {
					// 			kv.DupGetTab[client][seq] = rpl
					// 		}
					// 	}
					// }

				}

			} else if op.Type == SendOpType {
				args := op.Cmd.(SendOp)
				//应用map
				if kv.Config.Num == args.ConfigNum && kv.ShardsState[args.Shard] == ShardSend {
					kv.ShardsState[args.Shard] = ShardNoHas
					DPrintf("%v 的Server %v 完成发送! %v", kv.gid, kv.me, args.Shard)
					//清除shardData 数据
					for k, _ := range kv.mapShard {
						if key2shard(k) == args.Shard {
							delete(kv.mapShard, k)
						}
					}
				}

			}
			kv.LastApplyIndex = msg.CommandIndex
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			//Snapshot
			DPrintf("%v 的Server %v 接收到快照 %v", kv.gid, kv.me, msg.SnapshotIndex)
			kv.readPersist(msg.Snapshot)
			kv.mu.Unlock()
		}

	}
}

func (kv *ShardKV) persist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.mapShard)
	e.Encode(kv.LastSeq)
	e.Encode(kv.LastApplyIndex)
	e.Encode(kv.LastSnapshotIndex)
	e.Encode(kv.Config)
	e.Encode(kv.NewConfig)
	e.Encode(kv.OldConfig)
	e.Encode(kv.ShardsState)
	DPrintf("%v 的Server %v 保存 %v", kv.gid, kv.me, kv.mapShard)
	return w.Bytes()
}

func (kv *ShardKV) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.mapShard)
	d.Decode(&kv.LastSeq)
	d.Decode(&kv.LastApplyIndex)
	d.Decode(&kv.LastSnapshotIndex)
	d.Decode(&kv.Config)
	d.Decode(&kv.NewConfig)
	d.Decode(&kv.OldConfig)
	d.Decode(&kv.ShardsState)
	DPrintf("%v 的Server %v 恢复 %v last: %v ", kv.gid, kv.me, kv.mapShard, kv.LastSnapshotIndex)
}

func (kv *ShardKV) updateConfig() {
	for !kv.killed {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(80 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		if !kv.isUpdatingConfig() { // 说明此时已经在更新
			kv.mu.Unlock()
			time.Sleep(80 * time.Millisecond)
			continue
		}
		config := kv.ck.Query(-1)
		if config.Num == kv.Config.Num {
			kv.mu.Unlock()
			time.Sleep(80 * time.Millisecond)
			continue
		}
		if config.Num > kv.Config.Num+1 {
			config = kv.ck.Query(kv.Config.Num + 1)
		}
		DPrintf("%v的领导 %v 开始领导转移", kv.gid, kv.me)
		DPrintf("Config %v", kv.Config)
		DPrintf("newConfig %v", config)
		startArgs := StartConfigArgs{
			NewConfig: config,
		}
		kv.mu.Unlock()
		kv.rf.Start(Op{Type: StartConfigType, Cmd: startArgs})
		time.Sleep(80 * time.Millisecond)
	}
}

func (kv *ShardKV) makeShardsState(newConfig shardctrler.Config) {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.ShardsState[i] == ShardHas && newConfig.Shards[i] != kv.gid {
			kv.ShardsState[i] = ShardSend
		}
		if kv.ShardsState[i] == ShardNoHas && newConfig.Shards[i] == kv.gid {
			if newConfig.Num == 1 {
				kv.ShardsState[i] = ShardHas
			} else {
				kv.ShardsState[i] = ShardNeed
			}

		}
	}
}

// 保证鲁棒性，无论在什么状态，只要检测到ShardsState不对，就去尝试发起请求，当集齐到所有请求以后
func (kv *ShardKV) updateNeedShards() {
	for !kv.killed {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(80 * time.Millisecond)
			continue
		}

		//开始获取流程，获取流程是不会改变KV状态的
		//

		rplCh := make(chan RequestMapReply, shardctrler.NShards)

		kv.mu.Lock()
		tasks := 0
		for i, state := range kv.ShardsState {
			if state == ShardNeed {
				tasks++
				go kv.SendRequestMap(kv.OldConfig.Shards[i], i, rplCh)
				DPrintf("%v 的Server %v 开始请求 %v", kv.gid, kv.me, i)
				// DPrintf("OldConfig %v", kv.OldConfig)
				// DPrintf("CurConfig %v", kv.Config)
				// DPrintf("NewConfig %v", kv.NewConfig)
			}
		}
		kv.mu.Unlock()
		for cnt := 0; cnt != tasks; {
			rpl := <-rplCh
			if rpl.Err == OK {
				//拷贝临时Map
				//共识Need
				kv.rf.Start(Op{
					Type: NeedOpType,
					Cmd: NeedOp{
						Reply: rpl,
					},
				})
			}
			cnt++
			// Add a non-empty statement inside the critical section
			_ = cnt
		}

		time.Sleep(80 * time.Millisecond)
	}

}
func (kv *ShardKV) updateSendShards() {
	for !kv.killed {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(80 * time.Millisecond)
			continue
		}

		//开始获取流程，获取流程是不会改变KV状态的
		//

		rplCh := make(chan ReceiveMapReply, shardctrler.NShards)

		kv.mu.Lock()
		tasks := 0
		for i, state := range kv.ShardsState {
			if state == ShardSend {
				tasks++
				curGID := kv.Config.Shards[i]
				go kv.SendReceiveMap(curGID, i, rplCh)
				DPrintf("%v 的Server %v 开始主动发送 %v", kv.gid, kv.me, i)
			}
		}
		kv.mu.Unlock()
		for cnt := 0; cnt != tasks; {
			rpl := <-rplCh
			if rpl.Err == OK && rpl.Receive {
				//拷贝临时Map
				//共识Send
				DPrintf("%v 的Server %v 完成发送 %v,尝试共识", kv.gid, kv.me, rpl.Shard)
				sendOp := SendOp{
					ConfigNum: kv.Config.Num,
					Shard:     rpl.Shard,
				}
				kv.rf.Start(Op{
					Type: SendOpType,
					Cmd:  sendOp,
				})
			}
			cnt++
		}

		time.Sleep(80 * time.Millisecond)
	}

}

func (kv *ShardKV) SendReceiveMap(gid int, shard int, rplCh chan ReceiveMapReply) {
	reply := &ReceiveMapReply{}
	curServers := kv.Config.Groups[gid]
	curConfigNum := kv.Config.Num
	args := ReceiveMapArgs{
		Shard:     shard,
		ConfigNum: curConfigNum, //curConfigNum
	}
	for si := 0; si < len(curServers); si++ {
		// DPrintf(" %v 的 server %v 尝试寻找 %v Config: %v", kv.gid, kv.me, gid, kv.Config)
		reply = &ReceiveMapReply{}
		reply.Err = ""
		reply.Shard = shard
		srv := kv.make_end(curServers[si])
		ok := srv.Call("ShardKV.ReceiveMap", &args, &reply)

		if !ok || (ok && reply.Err == ErrWrongLeader) {
			continue
		}
		if ok && (reply.Err == ErrStaleConfig) {
			DPrintf("Stale Config %v", kv.Config)
			break
		}
		if ok && reply.Err == OK && !reply.Receive { //没收到
			break
		}
		//not ok or wrong leader
		// servers := kv.Config.Groups[gid]
		if ok && reply.Err == OK && reply.Receive {
			break
		}

	}

	// kv.NextSeq[gid]++
	// kv.LastLeader[gid] = leader
	rplCh <- *reply
}

func (kv *ShardKV) ReceiveMap(args *ReceiveMapArgs, reply *ReceiveMapReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.Config.Num < args.ConfigNum {
		DPrintf("%v gid 收到主动发送时 是过期的配置", kv.gid)
		reply.Err = ErrStaleConfig
		return
	}

	if kv.Config.Num > args.ConfigNum {
		reply.Err = OK
		reply.Receive = true
		return

	}

	if kv.ShardsState[args.Shard] == ShardHas {
		reply.Err = OK
		reply.Receive = true
		return
	}
	reply.Receive = false
	DPrintf("%v 没有收到", kv.gid)
	reply.Err = OK
	return
}

func copyMap(src map[string]string, dst map[string]string) {
	for k, v := range src {
		dst[k] = v
	}
}

func copyDupGetTab(src map[string]map[int64]GetReply, dst map[string]map[int64]GetReply) {

	for k, v := range src {
		dst[k] = make(map[int64]GetReply)
		for kk, vv := range v {
			dst[k][kk] = vv
		}
	}
}

func (kv *ShardKV) SendRequestMap(oldGID int, shard int, rplCh chan RequestMapReply) {
	reply := &RequestMapReply{}
	servers := kv.OldConfig.Groups[oldGID]
	curConfigNum := kv.Config.Num
	args := RequestMapArgs{
		Shard:     shard,
		ConfigNum: curConfigNum, //curConfigNum
	}
	for si := 0; si < len(servers); si++ {
		// DPrintf(" %v 的 server %v 尝试寻找 %v Config: %v", kv.gid, kv.me, gid, kv.Config)
		reply = &RequestMapReply{}
		reply.Err = ""
		srv := kv.make_end(servers[si])
		ok := srv.Call("ShardKV.RequestMap", &args, &reply)

		if !ok || (ok && reply.Err == ErrWrongLeader) {
			DPrintf("%v 的 %v 没有响应", oldGID, servers[si])
			continue
		}
		if ok && (reply.Err == ErrStaleConfig) {
			DPrintf("Stale Config %v", kv.Config)
			break
		}
		//not ok or wrong leader
		// servers := kv.Config.Groups[gid]
		if ok && reply.Err == OK {
			break
		}

	}

	// kv.NextSeq[gid]++
	// kv.LastLeader[gid] = leader
	rplCh <- *reply

}

func (kv *ShardKV) RequestMap(args *RequestMapArgs, reply *RequestMapReply) {
	reply.ConfigNum = args.ConfigNum
	DPrintf("%v 的 %v 收到了RequestMap Shard : %v", kv.gid, kv.me, args.Shard)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		DPrintf("Wrong Leader %v ", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if args.ConfigNum > kv.Config.Num {
		reply.Err = ErrStaleConfig
		DPrintf("%v 是过期的配置 %v ", kv.gid, kv.Config)
		kv.mu.Unlock()
		return
	}

	if args.ConfigNum < kv.Config.Num {
		DPrintf("请求方已经获得数据")
		return
	}
	//中断请求，防止传旧数据
	reply.Err = OK
	reply.Shard = args.Shard
	reply.ShardData = make(map[string]string)
	for k, v := range kv.mapShard {
		shard := key2shard(k)
		if shard == args.Shard {
			DPrintf("%v 的Server %v 发送 key: %v value : %v", kv.gid, kv.me, k, v)
			reply.ShardData[k] = v
		}
	}

	reply.DupGetTab = make(map[string]map[int64]GetReply)
	copyDupGetTab(kv.DupGetTab, reply.DupGetTab)

	kv.mu.Unlock()
}

func (kv *ShardKV) commitEmptyLog() {
	for !kv.killed {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(80 * time.Millisecond)
			continue
		}

		if !kv.rf.CheckCurrentTermLog() {
			emptyOp := Op{Type: EmptyOp}
			kv.rf.Start(emptyOp)
		}
		time.Sleep(80 * time.Millisecond)
	}
}

// 检查raft是否有当前term的日志

// 简单垃圾回收
func (kv *ShardKV) GC() {
	for !kv.killed {
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

func (kv *ShardKV) SnapshotRaft() {
	if kv.maxraftstate == -1 {
		return
	}
	for !kv.killed {
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
			// DPrintf("%v 的重点Server %v Snapshot at index %v", kv.gid, kv.me, kv.LastApplyIndex)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK
	cmd := Op{Type: GetType, Cmd: *args}
	kv.mu.Lock()

	if rpl, ok := kv.DupGetTab[args.ClientID][args.Seq]; ok {
		reply.Err = OK
		reply.Value = rpl.Value
		kv.mu.Unlock()
		return
	}

	if !kv.hasKey(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Leader = int64(kv.me)

	for !kv.killed {
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
	// raft.DPrintf("%v receive Get %v\n", kv.me, args)
	// reply.Err = OK
	// cmd := Op{Type: GetType, Cmd: args}
	// _, _, ok := kv.rf.Start(cmd)
	// if !ok {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }

	// timer := time.NewTimer(1000 * time.Millisecond)
	// defer timer.Stop()
	// select {
	// case <-timer.C:
	// 	reply.Err = ErrAgreement
	// case msg := <-kv.applyCh:
	// 	if msg.Command == cmd {
	// 		kv.mu.Lock()
	// 		reply.Value = kv.mapShard[args.Key]
	// 		kv.mu.Unlock()
	// 	}
	// }
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK
	cmd := Op{}
	cmd.Cmd = *args
	//说明是很久以前的请求，让client重新发送
	kv.mu.Lock()

	if kv.LastSeq[args.ClientID] >= args.Seq {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	//如果不是自己的shard
	if !kv.hasKey(args.Key) {
		reply.Err = ErrWrongGroup
		DPrintf("ErrWrongGroup Key : %v Value: %v", args.Key, args.Value)
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

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
	DPrintf("%v的Leader %v 发起共识 putappend k:%v v:%v", kv.gid, kv.me, args.Key, args.Value)
	for !kv.killed {
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

// func (kv *ShardKV) checkShard(key string) bool {
// 	latestConfig := kv.ck.Query(-1)
// 	shard := key2shard(key)
// 	if latestConfig.Num > kv.Config.Num {
// 		return false
// 	}
// 	if latestConfig.Shards[shard] == kv.gid && kv.Config.Shards[shard] == kv.gid {
// 		return true
// 	}
// 	return false
// }

func (kv *ShardKV) hasKey(key string) bool {
	shard := key2shard(key)
	return kv.ShardsState[shard] == ShardHas
}

func (kv *ShardKV) isUpdatingConfig() bool {
	for _, v := range kv.ShardsState {
		if v != ShardHas && v != ShardNoHas {
			return false
		}
	}
	return true
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killed = true
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendReply{})
	labgob.Register(RequestMapArgs{})
	labgob.Register(RequestMapReply{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(StartConfigArgs{})
	labgob.Register(SendOp{})
	labgob.Register(NeedOp{})
	labgob.Register(ReceiveMapArgs{})
	labgob.Register(ReceiveMapReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.ck = shardctrler.MakeClerk(kv.ctrlers)

	kv.mapShard = make(map[string]string)
	kv.DupGetTab = make(map[string]map[int64]GetReply)
	kv.LastSeq = make(map[string]int64)
	// kv.NextSeq = make(map[int]int64)
	kv.LastLeader = make(map[int]int)
	kv.ShardsState = [shardctrler.NShards]int{}
	for i := 0; i < shardctrler.NShards; i++ {
		kv.ShardsState[i] = ShardNoHas
	}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//read persist
	kv.readPersist(persister.ReadSnapshot())

	go kv.ExectuteOp()
	go kv.GC()
	go kv.SnapshotRaft()
	go kv.updateConfig()
	go kv.updateNeedShards()
	go kv.updateSendShards()
	go kv.commitEmptyLog()

	return kv
}

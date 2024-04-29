package shardctrler

import (
	"bytes"
	"log"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	DupGetTab         map[string]map[int64]QueryReply
	LastSeq           map[string]int64
	LastApplyIndex    int
	LastSnapshotIndex int

	persister    *raft.Persister
	maxraftstate int

	configs []Config // indexed by config num
}

func (sc *ShardCtrler) ExectuteOp() {
	for {
		msg := <-sc.applyCh
		//如果是旧的日志，直接跳过
		sc.mu.Lock()
		if sc.LastApplyIndex > msg.CommandIndex && msg.CommandValid {
			sc.mu.Unlock()
			continue
		}
		//只能安装比上次快照大的index
		if sc.LastSnapshotIndex > msg.SnapshotIndex && msg.SnapshotValid {
			sc.mu.Unlock()
			continue
		}
		sc.mu.Unlock()
		if msg.CommandValid {
			// DPrintf("Server %d receive msg %v", sc.me, msg)
			op := msg.Command.(Op)
			sc.mu.Lock()
			//泛型判断操作类型
			if op.Type == QueryType {
				args := op.Cmd.(QueryArgs)
				//如果操作已经被执行过
				if args.Seq <= sc.LastSeq[args.ClientID] {
					sc.mu.Unlock()
					continue
				}
				//Query逻辑
				if _, ok := sc.DupGetTab[args.ClientID]; !ok {
					sc.DupGetTab[args.ClientID] = make(map[int64]QueryReply)
				}
				if args.Num != -1 {
					sc.DupGetTab[args.ClientID][args.Seq] = QueryReply{
						WrongLeader: false,
						Err:         "",
						Config:      sc.configs[args.Num],
						Leader:      0,
					}
				} else { //如果是-1返回最新的config
					sc.DupGetTab[args.ClientID][args.Seq] = QueryReply{
						WrongLeader: false,
						Err:         "",
						Config:      sc.configs[len(sc.configs)-1],
						Leader:      0,
					}
				}
				sc.LastSeq[args.ClientID] = args.Seq
			} else if op.Type == JoinType {
				args := op.Cmd.(JoinArgs)
				//如果操作已经被执行过
				if args.Seq <= sc.LastSeq[args.ClientID] {
					sc.mu.Unlock()
					continue
				}
				//Join逻辑
				//创建新config，负载均衡Shards
				newConfig := sc.configs[len(sc.configs)-1].DeepCopy()
				newConfig.Num++
				//将新来的群组加入到新config中
				for gid, newServers := range args.Servers {
					for _, ns := range newServers {
						if _, ok := newConfig.Groups[gid]; !ok {
							newConfig.Groups[gid] = []string{}
						}
						newConfig.Groups[gid] = append(newConfig.Groups[gid], ns)
					}
				}
				//负载均衡Shards
				sc.BalanceNShards(&newConfig)
				sc.configs = append(sc.configs, newConfig)
				sc.LastSeq[args.ClientID] = args.Seq
				DPrintf("Server %v Join %v", sc.me, args.Servers)

			} else if op.Type == LeaveType {
				args := op.Cmd.(LeaveArgs)
				if args.Seq <= sc.LastSeq[args.ClientID] {
					sc.mu.Unlock()
					continue
				}
				//Leave逻辑
				newConfig := sc.configs[len(sc.configs)-1].DeepCopy()
				newConfig.Num++
				//将要删除的群组从新config中删除
				for _, gid := range args.GIDs {
					delete(newConfig.Groups, gid)
				}
				//负载均衡Shards
				sc.BalanceNShards(&newConfig)
				sc.configs = append(sc.configs, newConfig)
				sc.LastSeq[args.ClientID] = args.Seq
				DPrintf("Server %v Leave %v", sc.me, args.GIDs)
			} else if op.Type == MoveType {
				args := op.Cmd.(MoveArgs)
				if args.Seq <= sc.LastSeq[args.ClientID] {
					sc.mu.Unlock()
					continue
				}
				//Move逻辑
				//应该是下发到对应的GID再进行Move
				//下发config后如果发现自己负责的shard不是自己的gid，就要把数据转移
				newConfig := sc.configs[len(sc.configs)-1].DeepCopy()
				newConfig.Num++
				newConfig.Shards[args.Shard] = args.GID
				sc.configs = append(sc.configs, newConfig)
				sc.LastSeq[args.ClientID] = args.Seq
				DPrintf("Server %v Move %v to %v", sc.me, args.Shard, args.GID)
			}
			sc.LastApplyIndex = msg.CommandIndex
			sc.mu.Unlock()
		} else if msg.SnapshotValid {
			//Snapshot
			sc.mu.Lock()
			DPrintf("Server %v 接收到快照 %v", sc.me, msg.SnapshotIndex)
			sc.readPersist(msg.Snapshot)
			sc.mu.Unlock()
		}

	}
}

func (cfg *Config) DeepCopy() Config {
	newConfig := Config{}
	newConfig.Num = cfg.Num
	newConfig.Groups = make(map[int][]string)
	copy(newConfig.Shards[:], cfg.Shards[:])
	for gid, servers := range cfg.Groups {
		newConfig.Groups[gid] = servers
	}
	return newConfig
}

func (sc *ShardCtrler) BalanceNShards(conf *Config) {
	if len(conf.Groups) == 0 {
		return
	}
	//每次都按照同一种顺序分配GID
	// DPrintf("%v 负载均衡前: %v", sc.me, conf.Shards)
	gids := make([]int, 0, len(conf.Groups))
	for gid := range conf.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	gidNum := len(gids)
	avgShard := NShards / gidNum
	offset := 0
	//简单的负载均衡
	//按从小到大的顺序给gid分配avgShard数量的shard
	for i, gid := range gids {
		for j := 0; j < avgShard; j++ {
			offset = (i*avgShard + j) % NShards
			//说明这个shard已经分配过了,要发生把数据从老Shard转到新Shard
			//下发config后如果发现自己负责的shard不是自己的gid，就要把数据转移
			conf.Shards[offset] = gid
		}
	}
	//剩余的shard分配平均分配
	for offset < NShards-1 {
		for _, gid := range gids {
			offset++
			conf.Shards[offset] = gid
			if offset == NShards-1 {
				break
			}
		}
	}

	// DPrintf("%v 负载均衡后: %v", sc.me, conf.Shards)

}

type Op struct {
	// Your data here.
	Type     int
	Cmd      interface{}
	ClientID int
	Seq      int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Err = OK
	cmd := Op{}
	cmd.Cmd = *args
	cmd.Type = JoinType
	//说明是很久以前的请求，让client重新发送
	sc.mu.RLock()
	if sc.LastSeq[args.ClientID] >= args.Seq {
		sc.mu.RUnlock()
		reply.Err = OK
		return
	}
	sc.mu.RUnlock()
	_, _, IsLeader := sc.rf.Start(cmd)
	if !IsLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	reply.Leader = int64(sc.me)

	for {
		select {
		case <-time.After(200 * time.Millisecond):
			reply.Err = ErrAgreement
			return
		default:
			sc.mu.RLock()
			if sc.LastSeq[args.ClientID] >= args.Seq {
				reply.Err = OK
				sc.mu.RUnlock()
				return
			}
			sc.mu.RUnlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Err = OK
	cmd := Op{}
	cmd.Cmd = *args
	cmd.Type = LeaveType
	//说明是很久以前的请求，让client重新发送
	sc.mu.RLock()
	if sc.LastSeq[args.ClientID] >= args.Seq {
		sc.mu.RUnlock()
		reply.Err = OK
		return
	}
	sc.mu.RUnlock()
	_, _, IsLeader := sc.rf.Start(cmd)
	if !IsLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	reply.Leader = int64(sc.me)

	for {
		select {
		case <-time.After(200 * time.Millisecond):
			reply.Err = ErrAgreement
			return
		default:
			sc.mu.RLock()
			if sc.LastSeq[args.ClientID] >= args.Seq {
				reply.Err = OK
				sc.mu.RUnlock()
				return
			}
			sc.mu.RUnlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Err = OK
	cmd := Op{}
	cmd.Cmd = *args
	cmd.Type = MoveType
	//说明是很久以前的请求，让client重新发送
	sc.mu.RLock()
	if sc.LastSeq[args.ClientID] >= args.Seq {
		sc.mu.RUnlock()
		reply.Err = OK
		return
	}
	sc.mu.RUnlock()
	_, _, IsLeader := sc.rf.Start(cmd)
	if !IsLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	reply.Leader = int64(sc.me)

	for {
		select {
		case <-time.After(200 * time.Millisecond):
			reply.Err = ErrAgreement
			return
		default:
			sc.mu.RLock()
			if sc.LastSeq[args.ClientID] >= args.Seq {
				reply.Err = OK
				sc.mu.RUnlock()
				return
			}
			sc.mu.RUnlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.Err = OK
	cmd := Op{}
	cmd.Cmd = *args
	cmd.Type = QueryType

	sc.mu.RLock()
	if rpl, ok := sc.DupGetTab[args.ClientID][args.Seq]; ok {
		reply.Err = OK
		reply.Config = rpl.Config
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	_, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	reply.Leader = int64(sc.me)

	for {
		select {
		case <-time.After(200 * time.Millisecond):
			reply.Err = ErrAgreement
			return
		default:
			sc.mu.RLock()
			if rpl, ok := sc.DupGetTab[args.ClientID][args.Seq]; ok {
				reply.Err = OK
				reply.Config = rpl.Config
				sc.mu.RUnlock()
				return
			}
			sc.mu.RUnlock()
			time.Sleep(10 * time.Millisecond)
		}
	}

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) persist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.LastSeq)
	e.Encode(sc.LastApplyIndex)
	e.Encode(sc.LastSnapshotIndex)
	return w.Bytes()
}

func (sc *ShardCtrler) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&sc.configs)
	d.Decode(&sc.LastSeq)
	d.Decode(&sc.LastApplyIndex)
	d.Decode(&sc.LastSnapshotIndex)
}

func (sc *ShardCtrler) SnapshotRaft() {
	if sc.maxraftstate == -1 {
		return
	}
	for {
		time.Sleep(200 * time.Millisecond)
		RaftSize := sc.rf.Persister.RaftStateSize()
		if RaftSize > sc.maxraftstate-500 {
			sc.mu.Lock()
			if sc.LastApplyIndex <= sc.LastSnapshotIndex {
				sc.mu.Unlock()
				continue
			}
			sc.rf.Snapshot(sc.LastApplyIndex, sc.persist())
			sc.LastSnapshotIndex = sc.LastApplyIndex
			DPrintf("重点Server %v Snapshot at index %v", sc.me, sc.LastApplyIndex)
			sc.mu.Unlock()
		}
	}
}

// 简单垃圾回收
func (sc *ShardCtrler) GC() {
	for {
		//STW !!!
		time.Sleep(200 * time.Millisecond)
		sc.mu.Lock()
		for client, seq := range sc.DupGetTab {
			for s, _ := range seq {
				if sc.LastSeq[client]-200 > s {
					delete(sc.DupGetTab[client], s)
				}
			}
		}
		sc.mu.Unlock()
	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0
	sc.DupGetTab = make(map[string]map[int64]QueryReply)
	sc.LastSeq = make(map[string]int64)

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.maxraftstate = 1000
	sc.readPersist(persister.ReadSnapshot())
	go sc.ExectuteOp()
	go sc.GC()
	go sc.SnapshotRaft()

	// Your code here.

	return sc
}

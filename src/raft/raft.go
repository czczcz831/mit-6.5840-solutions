package raft

//BUG找出为什么再接收到lowTerm后leader还会更新term继续发送append
//可能原因 更新term前发送Append的时候，包发了很久才到达，这个时候会被reject，reject包又花了很久才回来，可能这个时候leader已经回归正常状态，不该在退回到follower
//BUG: votedFor更新有问题，看0日志
//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"context"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	hbTicker  *time.Ticker

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh           chan ApplyMsg
	applyChBuffer     chan ApplyMsg
	currentStatus     int // 1 follower 2 candidate 3 leader
	appendReceiveChan chan AppendArg
	tickerChan        chan time.Duration

	//Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	//Volatile state on all servers
	commitIndex       int
	lastApplied       int
	lastIncludedIndex int
	lastIncludedTerm  int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term int
	Cmd  interface{}
}

type AppendRpl struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type AppendArg struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func (rf *Raft) apply2StateMachine(CommitIdx int) {
	if rf.log[CommitIdx-rf.lastIncludedIndex].Term != rf.currentTerm {
		return
	}
	rf.commitIndex = CommitIdx
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		alyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i-rf.lastIncludedIndex].Cmd,
			CommandIndex: i,
		}
		DPrintf("Server %d apply Index:%v to state machine %v matchIdx: %v", rf.me, i, rf.log[i-rf.lastIncludedIndex].Cmd, rf.matchIndex)
		//applyCH会阻塞，而且最好保证只有一个goroutine提交
		// rf.mu.Unlock()
		rf.applyChBuffer <- alyMsg
		// rf.mu.Lock()
	}
	rf.lastApplied = rf.commitIndex
	rf.persist(rf.Persister.ReadSnapshot())
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		for data := range rf.applyChBuffer {
			rf.applyCh <- data
		}
	}
}

func (rf *Raft) LeaderHeartBeatProducer() {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.currentStatus != 3 {
			rf.mu.Unlock()
			break
		}
		// DPrintf("Leader %v log %v Commit: %v Term: %v", rf.me, rf.log, rf.commitIndex, rf.currentTerm)
		DPrintf("Leader %v   Commit: %v", rf.me, rf.commitIndex)
		rf.LeaderSendLog()
		rf.mu.Unlock()
		<-rf.hbTicker.C
		rf.hbTicker.Reset(50 * time.Millisecond)
	}
}

func (rf *Raft) AppendListener() {

	lastAppendTime := time.Now()
	isElecting := false
	ctx, cancel := context.WithCancel(context.Background())
	for rf.killed() == false {
		select {
		case msg := <-rf.appendReceiveChan:
			if isElecting {
				rf.mu.Lock()
				if msg.Term >= rf.currentTerm {
					//如果当前正在选举，终止选举状态
					cancel()
					DPrintf("Server %d cancel election 1", rf.me)
					//重新初始化ctx和cancel
					ctx, cancel = context.WithCancel(context.Background())
					isElecting = false
				} //否则reject msg
				rf.mu.Unlock()
			} else {
				lastAppendTime = time.Now()
				//下面这行导致了小概率bug,更改了votedFor导致出现了脑裂进而导致同一个term和同一个index日志不同
				// rf.mu.Lock()
				// rf.RefreshTerm(msg.Term)
				// rf.mu.Unlock()
			}

		case electionTimeout := <-rf.tickerChan: // check election timeout
			if isElecting {
				//如果当前正在选举，终止选举状态
				cancel()
				DPrintf("Server %d cancel election 2", rf.me)
				//重新初始化ctx和cancel
				ctx, cancel = context.WithCancel(context.Background())
				isElecting = false
			}
			rf.mu.Lock()

			//如果已经选举为leader
			if rf.currentStatus == 3 {
				rf.mu.Unlock()
				lastAppendTime = time.Now()
				isElecting = false
				continue
			}
			rf.mu.Unlock()

			if time.Since(lastAppendTime) > electionTimeout {
				lastAppendTime = time.Now()
				DPrintf("Server %d start election \n", rf.me)
				go rf.startElection(ctx)
				isElecting = true
			}
		}
	}
}

func (rf *Raft) RefreshTerm(term int) {
	if term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = term
	}
	rf.currentStatus = 1
	rf.persist(rf.Persister.ReadSnapshot())
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	// Your code here (2A).
	isleader := rf.currentStatus == 3

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapShot []byte) {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftState := w.Bytes()
	rf.Persister.Save(raftState, snapShot)
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("!!! Decode error")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.mu.Unlock()
	}

	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index-rf.lastIncludedIndex >= len(rf.log) {
		return
	}
	rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex].Term
	rf.log = rf.log[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index
	rf.persist(snapshot)
	DPrintf("Server %d snapshot Index %v", rf.me, rf.lastIncludedIndex)

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v 收到来自%v的snapshot", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {
		return
	}
	rf.RefreshTerm(args.Term)
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.SnapShot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.applyCh <- applyMsg
	//下面这段傻逼代码害我在lab3 de了几个小时的BUG,不用这段代码也行后面的日志全部删除
	// If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	// if args.LastIncludedIndex < len(rf.log)-1+rf.lastIncludedIndex && rf.log[args.LastIncludedIndex-rf.lastIncludedIndex].Term == args.LastIncludedTerm {
	// 	rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
	// 	DPrintf("%v 保留了lastIncludedIndex之后的log %v ", rf.me, rf.log)
	// 	rf.persist(args.SnapShot)
	// 	return
	// }
	DPrintf("%v 应用snapshot并丢弃了所有log ", rf.me)
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: rf.lastIncludedTerm, Cmd: nil}
	rf.persist(args.SnapShot)
}

func (rf *Raft) SendInstallSnapshot(server int, installArgs *InstallSnapshotArgs) bool {

	reply := &InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", installArgs, reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//处理回复时必须保证自从发送AppendEntries到收到回复期间，自己的状态没有发生变化,应付乱序情况
	if rf.currentTerm != installArgs.Term || rf.currentStatus != 3 {
		return false
	}
	if reply.Term > rf.currentTerm {
		DPrintf("%v 发现新任期 %v, 降级为follower CASE3", rf.me, reply.Term)
		rf.RefreshTerm(reply.Term)
		return false
	}
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	rf.matchIndex[server] = rf.lastIncludedIndex
	return true
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Voter       int
	//DEBUG
	Reason string
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapShot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//通知requestVote更新时间

	reply.Term = rf.currentTerm
	reply.Voter = rf.me
	reply.VoteGranted = true

	if args.Term > rf.currentTerm {
		//收到新任期的candidate
		rf.RefreshTerm(args.Term)
	}

	if rf.votedFor != -1 || args.Term < rf.currentTerm {
		if rf.votedFor != -1 {
			// DPrintf("REJECTION: Server %d reject vote request from %d because already vote for %d\n", rf.me, args.CandidateId, rf.votedFor)
			reply.Reason = "Already vote for " + strconv.Itoa(rf.votedFor)
		} else {
			// DPrintf("REJECTION: Server %d reject vote request from %d because term %d < %d \n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
			reply.Reason = "Term " + strconv.Itoa(args.Term) + " < " + strconv.Itoa(rf.currentTerm)
		}
		reply.VoteGranted = false
		rf.persist(rf.Persister.ReadSnapshot())
		return
	}
	//TODO: persist currentTerm and votedFor
	//if not at least up-to-date
	if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
		reply.Reason = "Term Not up-to-date "
		reply.VoteGranted = false
		rf.persist(rf.Persister.ReadSnapshot())
		return
	}
	//如果term相同但log不够长
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1+rf.lastIncludedIndex {
		reply.Reason = "Term same but qLog Index Not up-to-date"
		reply.VoteGranted = false
		rf.persist(rf.Persister.ReadSnapshot())
		return
	}
	// DPrintf("Server %v vote yes with log %v", rf.me, rf.log)
	rf.votedFor = args.CandidateId
	//Reset to follower
	rf.currentStatus = 1
	rf.persist(rf.Persister.ReadSnapshot())
}

func (rf *Raft) AppendEntries(args *AppendArg, reply *AppendRpl) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1
	//开始处理AppendEntries
	//接收的term小于当前的term,reject
	if args.Term < rf.currentTerm {
		DPrintf("Server %d reject append entries from %d :  low Term %v ,args %v ", rf.me, args.LeaderId, args.Term, args)
		reply.Success = false
		return
	}
	rf.appendReceiveChan <- *args

	if args.Term > rf.currentTerm {
		reply.Success = false
		DPrintf("%v 发现新任期leader %v ,转换为follower CASE 1", rf.me, args.LeaderId)
		rf.RefreshTerm(args.Term)
		return
	}

	if args.PrevLogIndex < rf.lastIncludedIndex { //说明已经Snapshot提交了
		reply.Success = true
		DPrintf("Server %d reject append entries from %d : log not enough\n", rf.me, args.LeaderId)
		return
	}

	//prevLogIndex和prevLogTerm不匹配,reject
	if len(rf.log)-1+rf.lastIncludedIndex < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = len(rf.log) + rf.lastIncludedIndex
		DPrintf("%v Commit: %v Server %d reject append entries from %d : log not enough\n", rf.log, rf.commitIndex, rf.me, args.LeaderId)
		return
	}

	if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.Success = false
		//找出conflictIndex
		//如果Follower在RPC参数中的prevLogIndex处的日志的term与prevLogTerm产生冲突，则将冲突处的term值和该term的第一条日志的index返回给Leader节点；若Leader节点包含该term的日志，则从该term在Leader日志中的最后一条日志处开始同步，否则从返回的index处开始同步。
		//这个优化特别重要，因为如果follower的log很长，leader的log很短，那么leader会从follower的log的最后一条开始同步，这样会很慢,lab2C Unrealiable网络环境下会产生很多错误日志，无法在规定时间内达成Agreement
		// 如果是因为prevLog.Term不匹配，记follower.prevLog.Term为conflictTerm。
		// 如果leader.log找不到Term为conflictTerm的日志，则下一次从follower.log中conflictTerm的第一个log的位置开始同步日志。
		// 如果leader.log找到了Term为conflictTerm的日志，则下一次从leader.log中conflictTerm的最后一个log的下一个位置开始同步日志。
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		// for i := 1; i <= args.PrevLogIndex; i++ {
		// 	if rf.log[i-rf.lastIncludedIndex].Term == reply.ConflictTerm {
		// 		reply.ConflictIndex = i
		// 		break
		// 	}
		// }
		for i := rf.lastIncludedIndex + 1; i <= args.PrevLogIndex-rf.lastIncludedIndex; i++ {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		DPrintf("Args : %v Server %d reject append entries from %d : log term not match left:%v right:%v ConflictIndex:%v PrevLogIndex:%v", args, rf.me, args.LeaderId, rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term, args.PrevLogTerm, reply.ConflictIndex, args.PrevLogIndex)
		return
	}
	//开始log replication
	if len(args.Entries) > 0 {
		// DPrintf("My len: %v  Server %d trying apply append entries %v from %d BeforeLog:%v FollowerCommit:%v PrevLogIndex: %v ", len(rf.log)-1+rf.lastIncludedIndex, rf.me, args.Entries, args.LeaderId, rf.log, rf.commitIndex, args.PrevLogIndex)
		rf.log = rf.log[:args.PrevLogIndex-rf.lastIncludedIndex+1]
		rf.log = append(rf.log, args.Entries...)
		// DPrintf("Server %d append entries %v from %d\n", rf.me, args.Entries, args.LeaderId)
		// DPrintf("Success Args %v Server %d append entries from %d succeed now log : %v", args, rf.me, args.LeaderId, rf.log)
	} else {
		// DPrintf("%v 收到心跳", rf.me)
	}

	if args.LeaderCommit > rf.commitIndex {
		//说明Leader已经commit了，follower跟进commit并应用到state machine

		if args.LeaderCommit > len(rf.log)-1+rf.lastIncludedIndex {
			// DPrintf("CASE 1 Server %d commitIndex %d rf.log: %v", rf.me, rf.commitIndex, rf.log)
			rf.apply2StateMachine(len(rf.log) - 1 + rf.lastIncludedIndex)
		} else {
			// DPrintf("CASE 2 Server %d commitIndex %d rf.log: %v", rf.me, args.LeaderCommit, rf.log)
			rf.apply2StateMachine(args.LeaderCommit)
		}
	}
	rf.persist(rf.Persister.ReadSnapshot())
	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyChan *chan RequestVoteReply) bool {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}
	if reply.Term > 0 {
		DPrintf("Server %d get vote reply %+v\n", rf.me, *reply)
		*replyChan <- *reply
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendArg, logEnd int) bool {
	reply := AppendRpl{}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//处理回复时必须保证自从发送AppendEntries到收到回复期间，自己的状态没有发生变化,应付乱序情况
	if rf.currentTerm != args.Term || rf.currentStatus != 3 {
		return false
	}

	if reply.Term > rf.currentTerm {
		DPrintf("%v 发现新任期 %v, 降级为follower CASE2", rf.me, reply.Term)
		//提前UNlock,因为里面要lock
		rf.RefreshTerm(reply.Term)
		return ok
	}
	if len(args.Entries) == 0 {
		//说明是心跳包
		return ok
	}
	if rf.currentStatus != 3 {
		return ok
	}
	if reply.Success {
		//更新NextIndex
		rf.nextIndex[server] = logEnd + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.checkMatchIndexAndCommit(rf.matchIndex[server])
	} else {
		//如果不是term不匹配
		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
			if rf.nextIndex[server] > len(rf.log)+rf.lastIncludedIndex {
				rf.nextIndex[server] = len(rf.log) + rf.lastIncludedIndex
			}
			if rf.nextIndex[server] < 1 {
				rf.nextIndex[server] = 1
			}

		} else { //如果是term不匹配
			conflictIndex := -1
			// for i := args.PrevLogIndex; i > 0; i-- {
			// 	if rf.log[i-rf.lastIncludedIndex].Term == reply.ConflictTerm {
			// 		conflictIndex = i
			// 		break
			// 	}
			// }
			// if conflictIndex != -1 {
			// 	rf.nextIndex[server] = conflictIndex + 1
			// } else {
			// 	rf.nextIndex[server] = reply.ConflictIndex
			// }
			for i := args.PrevLogIndex - rf.lastIncludedIndex; i > 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					conflictIndex = i
					break
				}
			}
			if conflictIndex != -1 {
				rf.nextIndex[server] = conflictIndex + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}

		}

		DPrintf("Leader %v 调整 %v 的失败reply", rf.me, server)

	}

	return ok
}

func (rf *Raft) checkMatchIndexAndCommit(index int) {

	if index < rf.commitIndex {
		return
	}

	if rf.currentStatus != 3 {
		return
	}

	if rf.log[index-rf.lastIncludedIndex].Term != rf.currentTerm {
		return
	}
	cnt := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] >= index {
			cnt++
		}
	}
	if cnt*2 > len(rf.peers) && rf.log[index-rf.lastIncludedIndex].Term == rf.currentTerm {
		// DPrintf("%v 将log replication到了majority,Index %v 应用到state machine", rf.me, rf.commitIndex)
		rf.apply2StateMachine(index)
		return
	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentStatus != 3 {
		return index, term, false
	}
	term = rf.currentTerm

	lg := LogEntry{
		Term: term,
		Cmd:  command,
	}

	rf.log = append(rf.log, lg)
	index = len(rf.log) - 1 + rf.lastIncludedIndex
	rf.persist(rf.Persister.ReadSnapshot())
	if rf.hbTicker != nil {
		rf.hbTicker.Reset(10 * time.Millisecond)
	}
	//给所有follower发送AppendEntries

	// Your code here (2B).

	return index, term, isLeader
}
func (rf *Raft) CheckCurrentTermLog() bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	latestLog := rf.log[len(rf.log)-1]
	if rf.currentTerm == latestLog.Term {
		return true
	}

	return false
}

func (rf *Raft) LeaderSendLog() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		//根据nextIndex动态构造
		// DPrintf("NextIndex: %v lastIncludedIndex:%v", rf.nextIndex, rf.lastIncludedIndex)

		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			//发送snapshot
			installArgs := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				SnapShot:          rf.Persister.ReadSnapshot(),
			}
			go rf.SendInstallSnapshot(i, installArgs)
			DPrintf("Leader %v 没有缺失的日志，发送snapshot给%v", rf.me, i)
			continue
		}

		appArg := AppendArg{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1, //len-1是刚添加的log
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1-rf.lastIncludedIndex].Term,
			Entries:      rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:],
			LeaderCommit: rf.commitIndex,
		}

		go rf.sendAppendEntries(i, &appArg, len(rf.log)-1+rf.lastIncludedIndex)
	}

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// electionTimeout := time.Duration(200+rand.Int63()%300) * time.Millisecond
		electionTimeout := time.Duration(800+rand.Int63()%400) * time.Millisecond
		time.Sleep(electionTimeout)
		rf.tickerChan <- electionTimeout
		// DPrintf("Server %d is %d with term %d ETO : %v\n", rf.me, rf.currentStatus, rf.currentTerm, electionTimeout)
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}

}

func (rf *Raft) startElection(ctx context.Context) {
	rf.mu.Lock()
	serverNum := len(rf.peers)
	//Start an election
	rf.currentStatus = 2
	rf.currentTerm++
	rf.votedFor = rf.me
	voteChan := make(chan RequestVoteReply, serverNum)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1 + rf.lastIncludedIndex,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()
	for i := 0; i < serverNum; i++ {
		if i != rf.me {
			go rf.sendRequestVote(i, args, &voteChan)
		}
	}

	voteYes := 1
	cnt := 1
	for {
		select {
		case <-ctx.Done():
			DPrintf("Server %d stop election\n cnt: %d", rf.me, cnt)
			return
		case rpl := <-voteChan:
			rf.mu.Lock()
			if rf.currentTerm != args.Term {
				rf.mu.Unlock()
				continue
			}
			DPrintf("Server %d with term %d get vote reply %+v\n", rf.me, rf.currentTerm, rpl)
			if rpl.Term > rf.currentTerm {
				DPrintf("Server %d 从candidate降级为follower\n", rf.me)
				rf.RefreshTerm(rpl.Term)
				rf.mu.Unlock()
				return
			}
			if rpl.VoteGranted {
				voteYes++
				if voteYes*2 > serverNum {
					DPrintf("Server %d become leader with term %d \n", rf.me, rf.currentTerm)
					//初始化Leader状态
					logLen := len(rf.log) + rf.lastIncludedIndex
					for i := range rf.nextIndex {
						rf.nextIndex[i] = logLen
						rf.matchIndex[i] = 0
					}
					rf.currentStatus = 3
					rf.mu.Unlock()
					rf.hbTicker = time.NewTicker(50 * time.Millisecond)
					go rf.LeaderHeartBeatProducer()
					return
				}
				rf.mu.Unlock()
			} else {
				cnt++
				if cnt*2 > serverNum {
					rf.currentStatus = 1
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}

	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.Persister = persister
	rf.currentStatus = 1
	rf.me = me
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.tickerChan = make(chan time.Duration)
	rf.appendReceiveChan = make(chan AppendArg, 100)
	rf.log = append(rf.log, LogEntry{Term: 0, Cmd: nil})
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.applyChBuffer = make(chan ApplyMsg, 10000)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.AppendListener()
	go rf.applier()
	return rf
}

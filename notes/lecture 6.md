Title : RAFT 2

#Paper Note
1.Snapshotting
log不能无限制的增长，必须想办法丢弃已经过时的log
Snapshot 有last included index和last included term 还有最后的state machine状态，
两个last变量是为了快照后第一个entry consistency check,在完成快照后，会删除在last included index前的快照和log

2.Client Interaction
Client会随机抽一个server，如果不是leader就发送leader地址给client,client每个command都有个serial number,如果leader crush, client timeout,resend the command,如果serial num已经被执行了，就
respond immdiately.

Leader在新上任后，会commit一个no-ops log,必须得到majority的认同后才会commit,保证只读操作不会读到stale data。



#Leader ELection:
1.Majortiy
2.at-least up to date 

当leader被选举后，会尝试往follower append,如果follower发现nextIndex、PT和PI不同后，会
拒绝leader,leader收到no后会将nextIndex--,PI,PT--，把上一个信息再发给follower,这样一直
重复直到follower回答YES
快照每个independently保存，在新follower加入集群或者意外导致follower落后太多的时候，leader会给
follower发snapshot

Leader为每个followerw维护一个matchIndex,当刚被选举时，全部悲观初始化为0，悲观认为每个follower
都没有拥有任何log, 初始化为0,

Erasing log entries
当leader把entry复制到大多数以后依然不能提交，必须在leader has commited one entry in its own term后才能提交之前的term,可以看figure 8

Persistence
服务器在rejoin后要replay log,然后voted for ,log[],current term要持久化

Service revoery
两种策略： 1.replay log 2.Snapshot(持久化)
 
 Using Raft:
 Service与Raft通过apply channel进行通信

 Linearizability(线性一致性)：把并发的同一时间的以先后顺序构建好顺序
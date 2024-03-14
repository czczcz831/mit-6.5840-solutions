##Paper note
1.Paxo和Raft都是分布式共识算法

##Pattern: 单点故障
Coordinator,Master,Storage Server => Avoid split brain
不能replicate 服务提供者，不然会出现脑裂问题,只能有一个服务提供者(leader)

##Majority
如果要保证f个服务器容错运行，至少需 2f+1 个服务器才能保证leader获得majority从而
保证服务正常运行。(Majority包括停机的和不停机的)

##Overview
1.每一个Raft Server都有KV instance,leader知道已经replicate majority log entryies 到
大多数服务器上，这个时候就会commit,然后发送到go channel给kv instance

2.replicate必须到硬盘上后才会ack

##Why logs?
1.重传(Retransmission)
2.Same Order
3.持久化 persistent

##Log entry
一个log entry包含 command,leader's term,log index

##Election
  
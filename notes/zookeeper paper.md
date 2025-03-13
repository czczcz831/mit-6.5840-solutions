# PaperNote
Zookeeper是一个分布式协调服务，它不实现功能，只提供协调服务api,只作为协调kernel，Zookeeper更像是一个没有锁服务的Chubby,zookeeper有FIFO和wait-free的属性,Zookeeper的pipeline流水线架构保证了异步FIFO。
为了保证线性化，ZK搞了一个 leader-based的广播模型 - Zab。
Zookeeper使用watch机制及时通知client更新缓存
这篇论文主要讨论 Coordination Kernel,Coordination recipe和experience.
## Service Overview
存储结构式 分层命名空间,像 `/usr/tmp`这样
Znode指在ZK内存中client操控的数据节点，Znode有两种类型,Regular和Ephemeral(会话关闭会自动删除),可以 用sequential让新创建的znode的n永远不会小于父节点的n，。
Client在发送读操作时可以 设置一个 `watch` Flag,只在当前会话有效，在资源改变后会通知client,会话事件（例如连接丢失事件）也会发送到 watch 回调，以便客户端知道 watch 事件可能会延迟。
ZK主要用来存储元数据，而不是通用文件数据 
## Client API
每个update方法都要求版本与预期一致，否则返回版本错误
## ZK Guarantees
ZK 保证Linearizable writes 线性写入和FIFO Client Order。
A线性一致性指一个线程client只能有一个outstanding(发送但未完成的任务),但ZK的线性一致性可以让client有多个outstanding操作.
Leader通过设置一个ready znode来保证自己的初始化完成，开始时删除ready znode,如果初始化完成之前die了或者还在初始化的时候,不会让其他节点读到未完全修改的config,只有当初始化完成ready被Leader设置其他节点才会读取config。

## Example of 原始应用
跟ZK没关系，只是展现在ZK上构建程序的应用。
羊群效应: 多个线程阻塞只有一个能争抢到分布式锁,当锁释放后会唤醒所有watch该锁的client。
### ZK分布式锁中解决羊群效应的解决办法:
### 锁的获取（Lock）

1. 客户端通过创建一个临时顺序节点（`EPHEMERAL|SEQUENTIAL`）来请求锁，节点路径以`l+"/lock-"`开始，其中`l`是锁节点。
2. 客户端调用`getChildren(l, false)`来获取锁节点下的所有子节点，不设置观察者（`watcher`）。
3. 如果客户端创建的节点在步骤2中获取的子节点列表中序号最小，那么它获得锁，流程结束。
4. 如果不是，找到序号刚好在该客户端节点之前的节点`p`。
5. 对节点`p`设置观察者，等待它被删除。一旦`p`被删除，客户端被唤醒。
6. 客户端返回步骤2，重新检查自己是否是序号最小的节点。

### 锁的释放（Unlock）

- 客户端通过删除其在获取锁时创建的节点来释放锁。

这种机制的关键在于使用了`EPHEMERAL|SEQUENTIAL`标志创建节点，这保证了每个锁请求都有一个唯一的、顺序的标识。如果客户端的节点是当前所有请求中序号最小的，那么它获得锁。如果不是，客户端就监听前一个节点的删除事件，这样当这个节点被删除时，只有一个客户端（即下一个序号的节点的持有者）被唤醒，而不是所有等待的客户端。

这种方法的优点包括：

1. 删除一个节点只会唤醒一个客户端，避免了羊群效应。
2. 没有轮询或超时机制。
3. 通过观察ZooKeeper的数据结构，可以直观地了解锁竞争的情况，进行锁的手动干预和调试。


### 读写锁伪代码
```python
Write Lock 1 n = create(l + “/write-”, EPHEMERAL|SEQUENTIAL) 
2 C = getChildren(l, false) 
3 if n is lowest znode in C, exit
4 p = znode in C ordered just before n
5 if exists(p, true) wait for event
6 goto 2 

Read Lock 
1 n = create(l + “/read-”, EPHEMERAL|SEQUENTIAL)
2 C = getChildren(l, false) 
3 if no write znodes lower than n in C, exit
4 p = write znode in C ordered just before n 
5 if exists(p, true) wait for event
6 goto 3
```
读锁是共享的，只要等待倒第二个写锁被释放就行了
### Double Barriers
作用是让节点同步开始计算，当屏障中的节点数达到阈值时同时开启所有线程的计算。

## ZK应用
Yahoo的Fetching Service 爬虫，主要用来管理元数据和Leader Election.
Katta分布式检索，用ZK跟踪Master和slave的状态，master failover Leader eleciton,配置中心。
Yahoo Message Broke （YMB）发布-订阅工具（消息队列），YMB用ZK元数据管理，容错检查。
![Pasted image 20240321081553](https://picture-bed-1251725997.cos.ap-beijing.myqcloud.com/blog/Pasted%20image%2020240321081553.png)
ZK流程图。

## ZK实现 
ZK读直接读内存数据库，写要达成共识和持久化到硬盘才写,每个znode默认最大1MB。
写请求到leader,读请求任何都可以。
### 读请求处理器
写请求只有请求与当前状态版本号匹配才会成功生成事务，如果处理时遇到错误则会生成 `errorTX`错误事务

### 原子广播
ZK用ZAB共识协议。
当跟新状态时，因为所有change depend on 版本号和之前的状态变更，ZAB 保证Leader会按顺序发送 更改状态，当前的leader也会有先前Leader做的所有更改。
ZAB用TCP保证顺序,
- **领导者选举**：Zab选举出的领导者同时也是ZooKeeper的领导者，这样创建事务的过程也是提出事务的过程。
- **日志追踪提议**：使用日志作为内存数据库的预写日志（write-ahead log），这样就不需要将消息写入磁盘两次
版本号保证了幂等，
#### 1. 客户端请求

当客户端发起一个写操作（如创建、删除节点或更新节点数据）时，这个操作会被封装成一个事务请求发送给ZooKeeper集群。

#### 2. 选举Leader

ZooKeeper集群中的服务器节点通过选举协议选出一个领导者（Leader），其他的服务器节点作为跟随者（Follower）或观察者（Observer）。所有的写请求都由Leader处理，以确保事务的顺序。

#### 3. 事务提案

Leader节点接收到写请求后，会生成一个事务提案（proposal），并将其广播给所有的Follower节点。这个提案包含了执行请求所需的所有信息，比如操作类型、数据等。

#### 4. 事务确认

Follower节点接收到事务提案后，会在本地预执行该事务，并在成功执行后向Leader发送一个投票（acknowledgment）表示确认。Leader节点等待直到收到超过半数节点的确认。

#### 5. 提交事务

一旦Leader节点收到了足够数量的确认，它就会广播一个提交（commit）消息给所有Follower节点，指示它们正式地在本地应用这个事务。这确保了所有活着的节点都应用了相同的事务序列。

#### 6. 响应客户端

事务一旦被成功提交，Leader节点就会向发起请求的客户端返回操作的结果。

#### 7. 日志和快照

所有的事务都会被记录在每个服务器节点的事务日志中以保证持久性。此外，为了优化性能和减少启动恢复时间，ZooKeeper会定期生成全系统状态的快照，并且可以在此基础上应用日志来恢复系统状态。


### Repicated Database
Zab创建模糊快照的时候不会锁住Zookeeper,ZAB会DFS 数据树然后如果在DFS时发生数据变更也不怕，因为更新数据是幂等的，所以重放snaoshot后的重复log也不怕。

### 客户端 服务器交互
处理写请求时，ZK会发送清除对应watch的notificaiton,按顺序处理写请求，不异步处理读写请求，server本地处理notification,只有与client交互的server会处理

读请求会在local产生一个 zxid 对应server见过的最后一个事务id,定义了读请求相对于写请求的部分顺序,读请求效率非常高，因为直接去memory里读并且ZK设计也是读多写少的场景.
这样有个缺点读可能会读到过时数据，为了保证这一点，用`sync`的FIFO顺序保证了在sync签发时能看到发送之前的所有变更。
sync实现原理，
#### 同步操作的队列处理

当执行同步操作时，ZooKeeper将该操作放在领导者和执行同步调用的服务器（即跟随者或follower）之间请求队列的末尾。这样做的目的是确保当同步操作被处理时，所有之前的事务都已经被处理，因此跟随者能够看到最新的系统状态。

#### 领导者身份的验证

为了这种机制有效，跟随者必须确保当前的领导者仍然是有效的领导者。这是通过检查待处理事务的存在来验证的：

- 如果有待提交的事务，这意味着领导者仍在有效地协调集群，跟随者就没有理由怀疑领导者的地位。
- 如果待处理的队列为空，为了维持领导者的身份并允许同步操作之后的正常运行，领导者需要发起一个空事务（null transaction）。这个空事务被提交后，同步操作就会在该事务之后被执行。

即使心跳已经包括了最近见过的zxid, Server给client的响应也包括 zxid,这样保证了client连到新服务器时可以比较zxid来确保在server catch up 之前client不会与他建立会话,为了保证 durability，能够保证client找到一个有最新view的server,因为majority已经replicate了。

client也会与server保持心跳，维持一个timeout,如果client在一段时间后没有发送命令，server就会断开与client的对话，client只能重新建立连接，client 库保证了断开会话后立马切换到与新server的新会话.

## Lecture Note
![Pasted image 20240321190351](https://picture-bed-1251725997.cos.ap-beijing.myqcloud.com/blog/Pasted%20image%2020240321190351.png)
 Zookeeper不保证强一致性，因为client在任何节点直接读，zxid是无论哪个节点上次写入的zxid,有两种情况
 1.如果follower还没有当前的zxid,就会等待zxid的log被leader同步后在return给client
 2.如果其他客户端写入了新状态，但该follower还没有更新，那就会读到过时的数据，不满足强一致性,ZK允许这种情况发生
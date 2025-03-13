# PaperNote
## 简要
链式复制时一个协调故障损坏存储服务器的方法，目的是以牺牲强一致性为代价支持大规模存储高吞吐和高可用的能力

## 介绍
本文章讨论的是一种介于数据库系统和文件系统的一种存储系统，大规模存储服务面临的一个挑战是在发生故障更改配置文件的同时保持高吞吐高可用， 保证查询和更新单个obj以顺序执行，更新后结果保证后续查询可见。
许多人认为强一致和高吞吐高可用冲突，大规模存储系统链式复制同时保证支持高吞吐高可用和强一致性。

## 接口
Service 为client的每一个请求单独生成一个回复更好，因为这允许丢包和超时重发。
- query(objId,opts)
- update(obiid,newVal,opts)
![](https://oss.czczcz.xyz/blog/Pasted%2520image%252020240327150257.png)
状态 
`hist objid`一个已更新操作的序列
`pending objid`待处理序列


请求示意图
T1 请求到达，加入待处理集合
T2 请求被忽略，从待处理集合中去除。在T2过渡期有些请求会被ignored.
T3 处理请求，从待处理集合减去，如果是读取请求则根据历史处理过的更新`Hist`去返回查询结果，如果是更新请求，就将`Hist`中对应的值更改，并回复更改后的结果。

客户端的视角是无法区分丢失的请求和被服务端忽略的请求的，所以服务器故障时对客户端来讲看上去是正常的，所以存储服务短暂低频停机是可接受的。
在CR中，短暂停机故障比添加删除新主机短得多，所以在故障和恢复时client依然会以最小代价的中断继续进行。大部分replicate-management protocal此时阻碍请求或者牺牲一致性。

## CR协议
t servers ,CR可以支持t-1台服务器fail而不牺牲可用性。
![Pasted image 20240327152103](https://oss.czczcz.xyz/blog/Pasted%20image%2020240327152103.png)
链表头部的第一个服务器被叫做`head`,最后一个服务器叫`tail`，流程大致如下：
- Reply Generation: `tail`生成并发送
- Query Processing: 读请求被转发到`tail` 并且返回`tail` 副本的结果。
- Update Processing : 更新请求被转发到`head`然后原子更新副本，通过可靠的FIFO顺序一直到更新到`tail`，由`tail` 回复。
強一致性由`tail`按顺序连续处理query和update保障。
只有`head`涉及计算，t-1的servers都只用写入`head`发来的副本。
### CR协议细节
- `Hist` 序列被存储在`tail`中。
- `Pending`表示任一服务器接收到的client请求但还没被`tail`处理。

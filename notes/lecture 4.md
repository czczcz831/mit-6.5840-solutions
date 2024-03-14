#Primary-Backup Replication

##备份方案
1.state transfer
2.replicated state machine(VM论文)

##Challenge
1.Primar有没有fail: 防止脑裂，两个primary
2.让primary in sync with backup,得让主备无缝切换,client无感
3.fail over

##两种方法:
1:State transfer:每隔一段时间checkpoint,通过传输数据来保持同步
2.RSM: 实时更新,(发送一摸一样的操作)

##执行什么级别的复制（replicate）
1.application-level (file append,write)
2.machine level(vm论文),任何程序都可以，transparent,用虚拟机(纯硬件也可以)

##VMFT
1.transparent replication
2.整个容错系统像单机一样

##Overview

hypervisor = virtual machine monitor 

test-set当primary和backup中间network partion，两个同时去存储服务器上设置flag,第一个设置为1的能够继续live,另一个commit suicide

##Divergence source(差异来源)
non-deterministic: interrupt,timer intertupt

logging channel只会发送不确定性指令的结果
所有指令通过锁步保证，只是logging channel会发送不确定的结果

##Election:
term和majority保证了不会出现脑裂问题，因为老leader term太旧了不会对新的term的server造成影响

##Chanllenge:
split-vote 通过随机选取elction timeout解决
在lab中用1s就差不多
follower需要在stable storage中存储自己投票给谁（voteFor）还有当前的term，不然重启后就会投两次票

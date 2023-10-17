
https://rocketmq.apache.org/zh/docs/domainModel/09subscription

GAMELOFT9

消息一致性:
A -> B/C  一个成功, 一个失败


kafka 功能比较单一, 存在丢数据可能性
RabbitMQ 消息堆积影响性能



namesever, broker(主从)
broker 将自己的信息注册到 namesever, Producer/Consumer 从 namesever 获取 Broker 信息

发送消息方式
同步发送 等待 MQ 响应 (Producer 内部会尝试进行重试) producer.send(msg), 阻塞
异步发送  发送消息到 MQ, 同时携带一个回调函数, 后续就不管了, MQ 接受到这个消息后, 回调这个函数 producer.send(msg, new SendCallback())
单向发送, 只推送消息到 MQ, 不需要确认消失是否到 MQ, producer.sendOneway(msg)

接受消息
消费者主动到 Broker 拉消息 consumer.fetchSubscribeMessageQuery("Topic")
得到对应 tocpic 下的 MessageQueue, 死循环 MessageQueue, 获取里面的消息
consumer.pullBlockIfNotFound(MessageQueue, subExpression, offset, maxNums) （DefaultLitePullCOnsumer 替代过期方法）

Broker 主动推消息到消费者 (本质还是消费者拉)， consumer.registerMessageListener(new MessageListenerConcurrenly())





顺序消息
producer.send(msg, new MessageQueueSelector()) 生产端指定消息推送到哪个 MessageQueue, 为顺序性提供条件
consumer.registerMessageListener(new MessageListenerQrderly())
优先把同一个队列中的消息获取完


广播消息
上面的情况，一个消息只会由一个消费者消费
同一个消息，可以多个消费者进行消费 consumer.setMessageModel(broadcast)

延迟消息
msg.setDelyTimeLevel(3), 先发到系统内部自己维护的一个 schedule_topic_xxx 的队列,

批量消息
producer.send(List<Message>) 有消息大小限制
producer.send(ListSplitter<Message>) 内部提供的类，会自定计算大小

过滤消息
tag 的使用   
consumer.subscribt("topic", MessageSelector.bySql("TAGS is not null and a is not null and a between 0 and 3")) 指定消息的过滤条件，同时消息里面有 a 这个属性 (msg.putUserProperty("a", "11"))  SQL92 语法

事务消息
事务消息只和生产者有关, 只保证了生产端的正常, 但是下游的消费失败等, 不受事务控制
```java
TransactionMQProducer producer
        
// executeLocalTransaction 不需要走事务        
// checkLocalTransaction        
        
// executeLocalTransaction 方法决定消息是否提交
//  return COMMIT_MESSAGE 事务提交, ROLLBACK_MESSAGE 事务回滚       
// return UNKNOW 走到 checkLocalTransaction 再次判断        
producer.setTransactionListener(new TransacationListner())
producer.setMessageInTransaction
```

流程
1. 生产者发送消息，这个消息会变为 half 消息 (这个消息这时下游不可见, 放到了一个 RMQ_SYS_TRANS_HALF_TOPIC 的 Topic 中)
2. Broker 回复 half 消息,
3. 生产者直行自己的本地事务(也就是自己的逻辑，比如入库什么的)
4. 返回自己本地事务的处理状态(commit, rollback, unknown)
   4.1 commit, 将消息发送给下游服务
   4.3 rollback, 丢弃消息
   4.4 unknown, 过一段时间回查生产者的本地事务的状态 (checkLocalTransaction)
   4.5 生产者可以去检查自己本地事务的执行情况, 在向 Broker 返回一个 commit/rollback/unknown
   4.6 如果还是 unknown, 继续回查本地的事务, 最多尝试 15 次, 最终进行丢弃

demo
下单 -> 支付 -> 下游

下单前先发一个 mq, 下单(订单入库), 如果直接下单, 再发 MQ (可能存在 RocketMQ 挂了, 导致消息发送失败)
下完单后，进行支付, 支付成功(支付也进行了异步处理), 才推送下游, 这时可以先返回 unknown, 后面 Broker 进行回查，
再去查询支付状态, 确定 commit/rollback, 保证最终消失是否推送

简单粗暴，
同步发送 + 多次重试，也可以实现事务

acl
Topic 权限配置

Broker 主从，主挂了, 从无法升级为主，所以非高可用的

4.5.0 以后的真高可用方式 Dledger
接管 Broker 的 Commit Log 消息存储
从集群中选举 Master 节点
完成 master 节点往 slave 节点的消息同步


消息存储

MQ 收到消息, 给生产者一个 ack, 然后将消息存储起来
MQ push 一条消息给消费者后, 等待消费者的 ack 响应, 同时将消息标记为已消费
如果没有标记为已消费, 不断尝试推送 (16次)
MQ 定期删除一些过期的消息

顺序写, 零拷贝技术加速文件读写 (用户态, 内核态)
mmap sendfile 2 种零拷贝技术

mmap 将文件读取到内核态缓冲区, 用户缓冲区是跟内核缓冲区共享一块映射数据的(可以理解为持有了内核态缓冲区中的一个 File 文件)
这样就减少了一次从内核态读取数据到用户态的 IO， mmap 减少了一次拷贝
(缺点, 文件不能太大, 只能映射 1.5- 2G, 所以 RocketMQ 单个 commit log 1g)

sendfile 去除了用户态,  offset 和 length
将文件的 offset、length 等数据拷贝到内核态，这些数据在内核态
传递给另一方 (Socket), Socket 通过这些数据读取文件
这样避免了文件在在各种态直接的拷贝
适用于大文件

CommitLog (Producer 发送的消息, Broker 会以最快的速度写入到这个文件, 所有的消息都写入到同一个文件, 所以可以看成是无序的)
IndexFile(消息索引文件)
ConsumerQueue (一个队列消息的消费进度)

ConsumerQueue 每一个 Node 内容 commitLog offset + size + message tag hash code 快速定位到 commit log 对应的位置 (message tag hash code 用于 tag 过滤)

IndexFile 除 tag 以外的辅助过滤, key hash, timestamp, commitLog offset, nextIndexOffset

topic 里面的读队列/写队列 ?


消息主从复制

同步复制
Producer 发消息到 Master Broker, Master Broker 同步给 Slave Broker,
再响应 Producer

异步复制
Producer 发消息到 Master Broker, Master Broker 立即响应 Producer, 然后再同步给
Slave Broker

消息刷盘

同步刷盘,
收到消息， 先写入内存的 page cache, 立即通知刷盘线程刷盘，
刷盘线程刷完成立即唤醒等待的线程, 线程返回消息写成功状态

异步刷盘
收到消息, 写入内存的 page cache, 返回成功,
后面内存的消息积累到一定的程度, 统一触发写磁盘动作


负载均衡
Producer 默认轮询, Topic 中的队列轮询发送
Consumer
广播: 每个 Consumer 都推送
集群：订阅实现的, 将同一个 Topic 下的队列分配给不同的 Consumer (Consumer 订阅队列)
每个 Consumer 订阅几个队列, 达到负载的效果
订阅的情况通过 AllocateMessageQueueStrategy 决定

消息重试 (给每一个消费组创建一个重试队列, 消费失败, 先进入这个队列)
1. 返回 null
2. 抛出异常
3. 返回 Action.ReconsumeLater

重试 16 次进入死信队列 %DLQ%+消费者组名
里面存放了所有消息, 不区分 Topic, 最多保存 3 天，超过删除

RocketMQ 至少消费一次, 刚好一次(最好的情况)


### 

消息类型
org.apache.rocketmq.common.attribute.TopicMessageType
未指明
普通
先进先出
事务


org.apache.rocketmq.remoting.netty.RemotingCodeDistributionHandler.channelRead

org.apache.rocketmq.remoting.protocol.RequestCode


###


**CommitLog**
默认 1 G
文件名长度为 20 位，左边补零 (00000000000000000000), 代表偏移量的起始位, （不满足, 部分移到下一个文件？）

ConsumeQueue
可以看成是基于 CommitLog 的索引文件
定长设计 20 个字节为一个条目, 8 字节的 CommitLog 物理偏移量 + 4 字节的消息长度 + 8 字节 tag hashcode
一个文件 30W 条目，约等于 5.72G
文件名 {topic}-{queueId}

IndexFile
可以通过 key 或时间区间来查询消息
单个 IndexFile 文件大小约为 400M，一个 IndexFile 可以保存 2000W 个索引，IndexFile 的底层存储设计类似 JDK 的 HashMap 数据结构

Kafka 每个 Topic 的每个 partition 对应一个文件，顺序写入，定时刷盘。但一旦单个 Broker 的 Topic 过多，顺序写将退化为随机写.
单个 Broker 所有 Topic 在同一个 CommitLog 中顺序写，是能够保证严格顺序写。RocketMQ 读取消息需要从 ConsumeQueue 中拿到消息实际物理偏移再去 CommitLog 读取消息内容，会造成随机读取


Page Cache
OS 对文件的缓存，用于加速对文件的读写,程序对文件进行顺序读写的速度几乎接近于内存的读写速度.

将一部分的内存用作 Page Cache。
对于数据的写入，OS 会先写入至 Cache 内，随后通过异步的方式由 pdflush 内核线程将 Cache 内的数据刷盘至物理磁盘上
对于数据的读取，如果一次读取文件时出现未命中 Page Cache 的情况，OS 从物理磁盘上访问读取文件的同时，会顺序对其他相邻块的数据文件进行预读取

mmap, sendFile (https://juejin.cn/post/6844903842472001550)

将磁盘上的物理文件直接映射到用户态的内存地址中，减少了传统 IO 将磁盘文件数据在操作系统内核地址空间的缓冲区和用户应用程序地址空间的缓冲区之间来回进行拷贝的性能开销


Broker
https://pic4.zhimg.com/80/v2-589b1bdf24a5845afa89ef8d9018a79f_1440w.webp

消息限制大小 256kb

###

4.
•返回自己本地事务的处理状态(commit, rollback, unknown)
4.1 commit，将消息发送给下游服务
4.3
polLback， 丢弃消息
4.4unknown，过一段时间回查生产者的本地事务的状态 〔checkLocalTransacti
4.5 生产者可以去检查自己本地事务的执行情况，在向 Broker 返回一个 commit/
今4.6 如果还是 unknown，继续回查本地的事务，最多尝试 15 次，最终进行丢弃


-demo
白下单
-＞支付一＞下游
口下单前先发一个mq，下单(订单入库），如果直接下单，再发MQ 〔可能存在 Rocke
下完单后，进行支付，支付成功(支付也进行了异步处理），才推送下游，这时可以先
今再去查询支付状态，确定commit/ro11back，保证最终消失是否推送
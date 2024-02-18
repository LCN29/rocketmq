# Topic

Broker 将自己的 Topic 信息注册到 NameSrv, Topic 存储在 NameSrv

Producer 从 NameSrv 获取 Topic 的路由信息，找到 broker, 发送消息到 Broker 

Consumer 从 NameSrv 获取 Topic 路由信息，找到 broker，然后从 broker 拉取消息，进行消费



## 为什么不把路由信息存储在 NameSrv 上
客户端只随机与 NameSrv 的其中一个建立长连接。

直接在 NameSrv 注册路由信息的话，那么 NameSrv 势必需要与其它的 NameSrv 保持通讯，这样才能使得所有的结点的路由信息保持完整和一致。
但是 NameSrv 的设计是无状态, 节点无任何信息同步。 





消息 ---> CommitLog
分配到对应的对象  consumequeue/Topic 名称/队列编号/文件 --> 消息的偏移量
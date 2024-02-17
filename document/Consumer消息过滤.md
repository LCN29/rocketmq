


> 1. subscribe(String topic, String subExpression) // 根据 tag 过滤
> 2. subscribe(final String topic, final MessageSelector messageSelector) // 根据选择器过滤
> 3. subscribe(String topic, String fullClassName, String filterClassSource) // 根据自定义的 class 过滤


方式一:
通过 tag 属性进行过滤，可以用 || 连接多个 tag

方式二:
MessageSelector 现在有 2 种实现方式
> 1. MessageSelector.byTag 是可以根据 tag 表达式进行过滤，实际上同按 tag 过滤
> 2. MessageSelector.bySql 可以用 SQL 表达式实现消息过滤，broker 要将 enablePropertyFilter 设置为 true, push 模式，起作用

pull




方式三:
按用户定义的类过滤

这个比较复杂, 需要远程在开启一个 FilterServer 的服务 (启动脚本在 {ROCKETMQ_HOME}/bin/startfsrv.sh)

实现一个类, 实现 MessageFilter 接口, 重写 match 方法, 返回 true/false
在 Consumer 启动时，加载这个实现类, 通过 MixAll.file2String 将文件的内容转为字符串
通过 subscribe 方法, 会将这个过滤类, 上传到 FilterServer 服务中

原理
> Broker 所在的服务器会启动多个 FilterServer 进程
> 消费者在订阅消息主题时会上传一个自定义的消息过滤实现类，FilterServer 加载并实例化
> Consumer 向 FilterServer 发送消息拉取请求，FilterServer 接收到消费者消息拉取请求后，FilterServer 将消息拉取请求转发给 Broker, 
> Broker 返回消息后，在 FilterServer 端执行消息过滤逻辑，然后返回符合订阅信息的消息给消息消费者进行消费 

注：4.3.0 中去掉这种方式，原因是不完善。但相关的 API 还没删掉，以后可能会重新实现


https://miludeer.github.io/2019/06/16/source-note-rocket-mq-features-filter/
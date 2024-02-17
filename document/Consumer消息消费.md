

相对 Broker 而言
Push 模式下由 Broker 服务主动将消息推送给消费者
Pull 模式下就是由消费者主动到 Broker 端拉取消息  --> 消费者可以根据自己的消费能力来控制拉取消息，灵活性比较高

源码实现层面
Push 模式和 Pull 模式在 RocketMQ 的实现层面都是 Pull 模式, 减少网络开销以及提高消息的推送率同时满足所有 pull 模式的好处。

通过 Pull 实现 Push 的核心思想就是**长轮询机制**，
当 Broker 接收到 Consumer 的 Pull 请求，判断如果没有对应的消息，不用直接给 Consumer 响应，而是将这个 Pull 请求缓存起来，
当 Producer 发送消息过来时，增加一个步骤去检查是否有对应的已缓存的 Pull 请求，如果有就及时将请求从缓存中拉取出来并将消息通知给 Consumer。




Product  ---> Broker

Message --> CommitLog   +  MessageQueue + IndexFile

MessageQueue  -->  存储目录 ${user.home}/store/consumequeue/${topic}/${queueId}/偏移量


## Message Tag 过滤

Message 格式 CommitLog Offset (8 byte) + MessageSize (4 byte) + Message Tag HashCode (8 byte)
CommitLog 在 CommitLog 文件的位置
MessageSize 文件的大小
Message Tag HashCode, Message Tag 计算出来后的 hash 值, 固定大小, 用于初步的查询

过滤过程:
Broker 初步过滤:
     先遍历 ConsumeQueue, 如果存储的 Message Tag 与订阅的 Message Tag 的 HashCode 不符合则跳过继续比对下一个, 符合则传输给 Consumer

Consumer 精确过滤:
   收到消息后, 通过 MessageTag 精确匹配, 而不是 HashCode

好处:
过滤过程中不会访问 Commit Log 数据，可以保证堆积情况下也能高效过滤
即使存在 Hash 冲突，也可以在 Consumer 端进行修正，保证万无一失


## 高级过滤 Filter Server 高版本已经去掉

> 1. 和 Broker 同一台机器上启动若干个 FilterServer 服务
> 2. Consumer 启动后, 会向 FilterServer 上传一个自定义的消息过滤实现类 MessageFilter
> 3. Consumer 向 FilterServer 发送消息拉取请求，FilterServer 接收到消费者消息拉取请求后，FilterServer 将消息拉取请求转发给 Broker, Broker 返回消息后，在 FilterServer 端执行消息过滤逻辑，然后返回符合订阅信息的消息给消息消费者进行消费

弊端: 安全问题, 过滤类可能导致服务端的安全问题











https://blog.csdn.net/weixin_45817985/article/details/134953878
https://miludeer.github.io/2019/06/16/source-note-rocket-mq-features-filter/
https://blog.csdn.net/prestigeding/article/details/79255328
https://blog.csdn.net/GAMEloft9/article/details/99854427?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522169718720016800211563902%2522%252C%2522scm%2522%253A%252220140713.130102334.pc%255Fblog.%2522%257D&request_id=169718720016800211563902&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~blog~first_rank_ecpm_v1~rank_v31_ecpm-6-99854427-null-null.nonecase&utm_term=RocketMQ&spm=1018.2226.3001.4450



allow pasting
var article_content=document.getElementById("article_content");
article_content.removeAttribute("style");

var follow_text=document.getElementsByClassName('follow-text')[0];
follow_text.parentElement.parentElement.removeChild(follow_text.parentElement);

var hide_article_box=document.getElementsByClassName(' hide-article-box')[0];
hide_article_box.parentElement.removeChild(hide_article_box);
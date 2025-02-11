CommitLog 文件名: 第一条消息的物理偏移量

CommitLog 中消息的格式:

msg total len：4 个字节, 消息总长度
msg magic: 4 个字节, 魔法值, 标记当前数据是一条消息
msg CRC: 4 个字节, 消息内容的 CRC 值
queue id: 4 个字节,消息所在的队列 id
msg flag: 4 个字节, 消息标记位
queue offset: 8个自己, 消息在队列的偏移量

physical offset: 8 个字节, 消息在 CommitLog 文件中的物理偏移量
sys flag: 4 个字节, 系统标记位
msg born timestamp: 8 个字节, 消息产生时的时间戳
born inet4: 4/6 个字节, 消息发送者的 IP 地址, ipv4 占用 4 个字节, ipv6 占用 16 byte
born port: 4 个字节, 消息发送者的端口

msg store timestamp: 8 个字节, 消息存储时的时间戳
broker inet4: 4/6 个字节, 消息存储的 broker 的 IP 地址, ipv4 占用 4 个字节, ipv6 占用 16 byte
broker port: 4 个字节, 消息存储的 broker 的端口
reconsume times: 4 个字节, 消息重试次数
transaction offset: 8 个字节, 事务消息的偏移量

body len: 4 个字节, 消息体长度
body: 消息体的内容, 不定长, 最大不超过 4MB, 消息体内容可能为空

topic name len: 1 个字节, topic 名称长度
topic name: 1 - 127 个字节, topic 名称

prop len: 2 个字节, 额外配置项的长度, 可为 0
prop content: 0 - 32767 个字节 额外配置项的内容, 可为空
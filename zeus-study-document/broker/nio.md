

RednaxelaFX


DirectByteBuffer
HeapByteBuffer

MQ 参数建议
https://blog.csdn.net/dmjxsy/article/details/129494752


# MappedByteBuffer
https://juejin.cn/post/7350977538276343817#heading-0
https://lishoubo.github.io/2017/09/27/MappedByteBuffer%E7%9A%84%E4%B8%80%E7%82%B9%E4%BC%98%E5%8C%96/


FileChannel 读取数据
> 切换到内核态
> 查看读取文件对应的 page cache 是否包含需要的数据, 将数据拷贝到 DirectByteBuffer 中, 并返回，切换用户态
> 从磁盘加载对应的数据到 page cache 中, 再将数据拷贝到 DirectByteBuffer 中, 并返回，切换用户态

2次切换, 1/2次拷贝


FileChannel 写入数据
> 切换到内核态
> 将 DirectByteBuffer 中的数据拷贝到 page cache 中, 并返回，切换用户态
> 后续操作系统会将 page cache 中的数据写入磁盘, 触发第二次拷贝

2次切换, 2次拷贝


MappedByteBuffer 对象的创建, 本身是需要使用到底层的 mmap 系统调用的, 所以本身的创建需要 2 次上下文切换

同时 MappedByteBuffer 刚创建时, 只是进程地址空间中的一段虚拟内存, 即还没有映射物理内存, 也就是对应的 Page Cache 是空的

MappedByteBuffer 读取数据
MappedByteBuffer 可以看作是 Page Cache 的引用

对 MappedByteBuffer 的读写操作, 实际上是对 Page Cache 的读写操作

读取数据
> 读取 MappedByteBuffer, 发现 Page Cache 中有数据, 直接返回
> 读取 MappedByteBuffer, 发现 Page Cache 中没有数据, 从磁盘加载数据到 Page Cache 中, 再返回, 2 次上下文切换，1 次拷贝


4M

MappedByteBuffer VS FileChannel

MappedByteBuffer 不会有缺页中断，
FileChannel 不会触发磁盘 IO 都是直接对 page cache 进行读写

读/写
32M --> MappedByteBuffer
64M --> FileChannel


MappedByteBuffer 缺页中断
FileChannel 会触发磁盘 IO

读
2k --> MappedByteBuffer
4k --> FileChannel

写入数据

512M --> MappedByteBuffer
512+ M --> FileChannel

但别忘了 MappedByteBuffer 是需要进程页表支持的，在实际访问内存的过程中会遇到页表竞争以及 TLB shootdown 等问题。
还有就是 MappedByteBuffer 刚刚被映射出来的时候，其在进程页表中对应的各级页表以及页目录可能都是空的。
所以缺页中断这里需要做的一件非常重要的事情就是补齐完善 MappedByteBuffer 在进程页表中对应的各级页目录表和页表，并在页表项中将 page cache 映射起来，最后还要刷新 TLB 等硬件缓存。


所以 MappedByteBuffer 的缺页中断要比 FileChannel 的系统调用开销要大



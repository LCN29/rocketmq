FileChannel 和 MappedByteBuffer 写数据的比较

## 数据写入系统的过程

屏蔽掉一些细节, 我们可以将数据写入文件的过程可以简单的分为 2 个阶段
> 1. 应用从用户态切换到内核态
> 2. 将数据写入磁盘

在这个简单的过程中, 实际还有一个缓存存在(Page Cache), 也就是说, 数据写入磁盘的过程可以分为 3 个阶段
> 1. 应用从用户态切换到内核态
> 2. 将数据写入 Page Cache
> 3. 将 Page Cache 中的数据写入磁盘

用户程序只需要完成 1, 2 步操作即可, 第 3 步由操作系统自己处理, 用户不需要关心 (当前, 操作系统也提供了对应的函数,
让用户操作这个缓存数据强制写入磁盘)

Page Cache 存在的意义:
将数据写入磁盘是一个非常耗时的操作, 为了提高性能, 操作系统引入了 Page Cache (文件的一个缓存), 将数据先写入 Page Cache,
然后由操作系统自己决定什么时候将 Page Cache 中的数据写入磁盘, 达到一个提高性能的目的。

## Java 的 NIO 中的写数据方式

在 Java 的 NIO 中, 向一个文件里面写数据常见的方式有 2 种

* 方式一: 使用 FileChannel.write(ByteBuffer) 方法, 这时数据还是在系统内存中, 可以通过 FileChannel.force() 方法将数据强制刷到磁盘

```java
private static void writeByFileChannel() throws Exception {

    // 创建一个堆外缓冲区
    // 下面的分析通过堆外缓冲区来说明, 因为这样比较简单, 使用堆内缓冲区的方式类似，只是要绕一步，后面说明
    byte[] bytes = "hello world".getBytes();
    // ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
    buffer.put(bytes);
    buffer.flip();

    FileChannel fileChannel = new RandomAccessFile(new File("./test.txt"), "rw").getChannel();

    // 写入数据到缓存
    fileChannel.write(buffer);
    // 强制将缓存数据刷新到磁盘
    fileChannel.force(false);
    fileChannel.close();
}
```

* 方式二: 使用 MappedByteBuffer.put(ByteBuffer) 方法, 同样可以通过 MappedByteBuffer.force() 方法将数据强制刷到磁盘

```java
private static void writeByMappedByteBuffer() throws IOException {

    // 创建一个堆外缓冲区
    // 下面的分析通过堆外缓冲区来说明, 因为这样比较简单, 使用堆内缓冲区的方式类似，只是要绕一步，后面说明
    byte[] bytes = "hello world".getBytes();
    // ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
    buffer.put(bytes);
    buffer.flip();

    FileChannel fileChannel = new RandomAccessFile(new File("./test.txt"), "rw").getChannel();
    MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, bytes.length);

    // 写入数据到缓存
    mappedByteBuffer.put(buffer);
    // 强制刷新到磁盘
    mappedByteBuffer.force();
    fileChannel.close();
}
```

通过 2 者的比较, 可以提出一些问题, 比如
> 1. MappedByteBuffer 的创建需要借助 FileChannel, 那么 2 者有什么关系
> 2. 2 种方式在实现上有什么区别
> 3. 2 种方式在性能上有什么区别, 使用上怎么选择呢

## MappedByteBuffer 和 FileChannel 的关系

[官方](https://docs.oracle.com/javase/7/docs/api/java/nio/channels/FileChannel.html#map(java.nio.channels.FileChannel.MapMode,%20long,%20long)
的说法(看 FileChannel 的 map 方法描述处)

```log
A mapping, once established, is not dependent upon the file channel that was used to create it. Closing the channel, in particular, has no effect upon the validity of the mapping.
```

简单说: MappedByteBuffer 的创建需要依靠 FileChannel 外, 2 者就没有任何关系了。

## 2 种方式在实现上有什么区别

### FileChannel.write

FileChannel 可以看作是一个开辟了一个通道, 在 Java 应用和文件系统之间建立了一个连接, 通过这个通道对文件进行读写。
图示

FileChannel.write(ByteBuffer) 方法, 本质是调用了底层的 vfs_write 函数, 将内存中的数据写入到 Page Cache。
图示

数据加载到 PageCache 所以
FileChannel.write(ByteBuffer) 的过程如下
> 1. 应用从用户态切换到内核态
> 2. 将 ByteBuffer 中的数据拷贝到 Page Cache 中 (CPU 拷贝), 因为这个 Page Cache 修改了, 就会变为脏页 (dirty page)
> 3. 从内核态切换到用户态
> 4. 后续操作系统会将变为脏页的 Page Cache 中的数据写入磁盘 (DMA 拷贝)

这个过程中, 跟用户进程耗时比较大的操作, 主要涉及到 2 次上下文切换 + 1 次 CPU 拷贝, DMA 拷贝由操作系统决定, 不考虑

### MappedByteBuffer.put

MappedByteBuffer 可以看作是一个内存区域, 会将文件的一部分内容(取决于声明 MappedByteBuffer 时的大小)映射到这个内存区域,
对这个内存区域的操作, 间接操作映射到这个区域的文件内容。

MappedByteBuffer 的创建, 本身是需要使用到底层的 mmap 函数, 而 mmap 系统调用的实现如下
调用 mmap 函数, 会在进程的地址空间中开辟一段虚拟内存, 和文件的对应内容建立映射关系, 这是 mmap 函数就结束了
但是对文件直接的操作, 是一个很慢的操作, 所以, 在对数据的读写操作时, 实际上还是对 Page Cache 的读写操作，
所以操作系统中, 有一个叫做**进程页表**的设计, 可以看作一个虚拟内存和 Page Cache 的映射关系。

MappedByteBuffer.put 的过程如下
> 1. 将 ByteBuffer 中的数据写入到 MappedByteBuffer 时, 实际就是直接写入到 Page Cache, 同样会变为脏页
> 2. 后续操作系统会将脏页的 Page Cache 中的数据写入磁盘 (DMA 拷贝)

读写 MappedByteBuffer 本质上就是读写内核态的 Page Cache, 因此没有上下文切换，也没有拷贝 (DMA 拷贝不考虑)。

看起来 MappedByteBuffer 几乎没有任何开销, 但是因为 mmap 的设计, 需要**进程页表**, 会有别的开销点
> 缺页中断, 建立 Page Cache 和虚拟内存的映射关系
> 在实际访问内存的过程中会遇到页表竞争以及 TLB shootdown 等问题

所以向通过 mmap 获取到的虚拟内存, 读写数据时
> 1. 通过虚拟地址到进程页表中查找对应的 Page Cache, 找到就返回数据或者写入数据
> 2. 如果没有找到, 会触发缺页中断 (page fault), 从磁盘加载数据到 Page Cache 中, 修改进程页表完成映射, 再返回数据或者写入数据

## 参考

[MappedByteBuffer VS FileChannel：从内核层面对比两者的性能差异](https://juejin.cn/post/7350977538276343817)  
[MappedByteBuffer的一点优化](https://lishoubo.github.io/2017/09/27/MappedByteBuffer%E7%9A%84%E4%B8%80%E7%82%B9%E4%BC%98%E5%8C%96/)  
[「抄底 Android 内存优化 4」 — 图解 mmap 原理](https://blog.csdn.net/qq_23191031/article/details/108227587)  

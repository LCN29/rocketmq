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

FileChannel.write(ByteBuffer) 方法, 本质是调用了底层的 vfs_write 系统调用, 将数据写入到 Page Cache。

### MappedByteBuffer.put

MappedByteBuffer 可以看作是一个内存区域, 会将文件的一部分内容(取决于声明 MappedByteBuffer 时的大小)映射到这个内存区域,
对这个内存区域的操作, 间接操作映射到这个区域的文件内容。

MappedByteBuffer 的创建, 本身是需要使用到底层的 mmap 系统调用的, 而 mmap 系统调用的实现如下

## 参考

[MappedByteBuffer VS FileChannel：从内核层面对比两者的性能差异](https://juejin.cn/post/7350977538276343817)  
[MappedByteBuffer的一点优化](https://lishoubo.github.io/2017/09/27/MappedByteBuffer%E7%9A%84%E4%B8%80%E7%82%B9%E4%BC%98%E5%8C%96/)  
[「抄底 Android 内存优化 4」 — 图解 mmap 原理](https://blog.csdn.net/qq_23191031/article/details/108227587)  

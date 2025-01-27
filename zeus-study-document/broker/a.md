FileChannel 和 MappedByteBuffer 写数据的比较

## 数据写入系统的过程

屏蔽掉一些细节, 我们可以将数据写入文件的过程可以简单的分为 2 个阶段
![Alt ''数据写入系统的过程''](./images/数据写入系统的过程.png)

> 1. 应用从用户态切换到内核态
> 2. 将数据写入磁盘

在这个简单的过程中, 实际还有一个缓存存在(Page Cache), 也就是说, 数据写入磁盘的过程可以分为 3 个阶段
![Alt ''数据写入系统的过程''](./images/数据写入系统的过程2.png)

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
private static void writeByFileChannel()throws Exception{

        // 创建一个堆外缓冲区
        // 下面的分析通过堆外缓冲区来说明, 因为这样比较简单, 使用堆内缓冲区的方式类似，只是要绕一步，后面说明
        byte[]bytes="hello world".getBytes();
        // ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        ByteBuffer buffer=ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        buffer.flip();

        FileChannel fileChannel=new RandomAccessFile(new File("./test.txt"),"rw").getChannel();

        // 写入数据到缓存
        fileChannel.write(buffer);
        // 强制将缓存数据刷新到磁盘
        fileChannel.force(false);
        fileChannel.close();
        }
```

* 方式二: 使用 MappedByteBuffer.put(ByteBuffer) 方法, 同样可以通过 MappedByteBuffer.force() 方法将数据强制刷到磁盘

```java
private static void writeByMappedByteBuffer()throws IOException{

        // 创建一个堆外缓冲区
        // 下面的分析通过堆外缓冲区来说明, 因为这样比较简单, 使用堆内缓冲区的方式类似，只是要绕一步，后面说明
        byte[]bytes="hello world".getBytes();
        // ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        ByteBuffer buffer=ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        buffer.flip();

        FileChannel fileChannel=new RandomAccessFile(new File("./test.txt"),"rw").getChannel();
        MappedByteBuffer mappedByteBuffer=fileChannel.map(FileChannel.MapMode.READ_WRITE,0,bytes.length);

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

FileChannel.write(ByteBuffer) 的过程如下
> 1. 应用从用户态切换到内核态
> 2. 将 ByteBuffer 中的数据拷贝到 Page Cache 中 (CPU 拷贝, 默认这时文件的数据已经存在 Page Cache), 因为这个 Page Cache
     修改了, 就会变为脏页 (dirty page)
> 3. 从内核态切换到用户态
> 4. 后续操作系统会将变为脏页的 Page Cache 中的数据写入磁盘 (DMA 拷贝)

这个过程中, 跟用户进程耗时比较大的操作, 主要涉及到 2 次上下文切换 + 1 次 CPU 拷贝, DMA 拷贝由操作系统决定, 不考虑

从图中可以看出, FileChannel 的写入数据的消耗主要是
> 1. 2 次上下文切换
> 2. 1 次数据拷贝 (CPU 拷贝, 既将 ByteBuffer 的数据拷贝到 Page Cache 中)

### MappedByteBuffer.put

MappedByteBuffer 可以看作是一个内存区域, 会将文件的一部分内容(取决于声明 MappedByteBuffer 时的大小)映射到这个内存区域,
对这个内存区域的操作, 间接操作映射到这个区域的文件内容。

MappedByteBuffer 的创建, 本身是需要使用到底层的 mmap 函数, 而 mmap 系统调用的实现如下
调用 mmap 函数, 会在进程的地址空间中开辟一段虚拟内存, 和文件的对应内容建立映射关系, 这是 mmap 函数就结束了。

但是对文件直接的操作, 是一个很慢的操作, 所以, 在对数据的读写操作时, 实际上还是对 Page Cache 的读写操作，
所以操作系统中, 有一个叫做**进程页表**的设计, 可以看作一个虚拟内存和 Page Cache 的映射关系。

写数据时, 先从进程页表中获取对应的 Page Cache, 然后将数据写入到对应的虚拟地址, 就相当于是直接读写 Page Cache 了。

MappedByteBuffer.put(ByteBuffer) 的过程如下
> 1. 补齐完善进程页表映射关系, 将 Page Cache (默认这时文件的数据已经存在 Page Cache) 映射到进程的虚拟地址空间
> 2. 将 ByteBuffer 中的数据写入到虚拟地址, 也就是写入到 Page Cache 中, 同样会变为脏页

读写 MappedByteBuffer 本质上就是读写内核态的 Page Cache, 因此没有上下文切换，也没有拷贝 (DMA 拷贝不考虑)。
看起来 MappedByteBuffer 几乎没有任何开销, 但是因为 mmap 的设计, 需要**进程页表**, 会有别的开销点
> 缺页中断, 文件的映射部分 Page Cache 不存在, 需要先加载到 Page Cache, 然后建立映射关系, 即**进程页表**
> 在实际访问内存的过程中会遇到页表竞争以及 TLB shootdown 等问题

## 2 种方式在性能上有什么区别, 使用上怎么选择呢

情况一:
对于 FileChannel, 文件数据已经存在于 Page Cache  
对于 MappedByteBuffer, 缺页中断处理完成了

结论:
MappedByteBuffer 适合小数据量的读写, 其本身的写性能随着数据量的增大和逐级降低
FileChannel 适合大数据量的读写, 其本身的写性能随着数据量的增大而逐级提高

情况二:
对于 FileChannel, 文件数据不存在于 Page Cache
对于 MappedByteBuffer, 缺页中断处理未完成

MappedByteBuffer 适合小数据量的读写, 其本身的写性能随着数据量的增大和逐级降低
FileChannel 适合大数据量的读写, 其本身的写性能随着数据量的先增大然后降低(在降低后的情况, 在同样数据量下, 还是比
MappedByteBuffer 要快)

## 堆内内存和堆外内存的写影响

当我们调用 FileChannel 的 read/write 方法, 传入的 ByteBuffer, 可以是一个堆内内存 (HeapByteBuffer),
也可以是一个堆外内存 (DirectByteBuffer)。
在传入的是一个堆内内存时, JVM 会向将这个堆内内存拷贝到一个临时的堆外内存, 再执行写流程。

```java
public class IOUtil {

    static int read(FileDescriptor fd, ByteBuffer dst, long position, NativeDispatcher nd) throws IOException {
        // 如果我们传入的 dst 是 DirectBuffer，那么直接进行文件的读取
        // 将文件内容读取到 dst 中
        if (dst instanceof DirectBuffer) return readIntoNativeBuffer(fd, dst, position, nd);

        // 如果我们传入的 dst 是一个 HeapBuffer，那么这里就需要创建一个临时的 DirectBuffer
        // 在调用 native 方法底层利用 read  or write 系统调用进行文件读写的时候
        // 传入的只能是 DirectBuffer
        ByteBuffer bb = Util.getTemporaryDirectBuffer(dst.remaining());
        try {
            // 底层通过 read 系统调用将文件内容拷贝到临时 DirectBuffer 中
            int n = readIntoNativeBuffer(fd, bb, position, nd);
            if (n > 0)
                // 将临时 DirectBuffer 中的文件内容在拷贝到 HeapBuffer 中返回
                dst.put(bb);
            return n;
        }
    }

    static int write(FileDescriptor fd, ByteBuffer src, long position, NativeDispatcher nd) throws IOException {
        // 如果传入的 src 是 DirectBuffer，那么直接将 DirectBuffer 中的内容拷贝到文件 page cache 中
        if (src instanceof DirectBuffer) return writeFromNativeBuffer(fd, src, position, nd);

        // 如果传入的 src 是 HeapBuffer，那么这里需要首先创建一个临时的 DirectBuffer
        ByteBuffer bb = Util.getTemporaryDirectBuffer(rem);
        try {
            // 首先将 HeapBuffer 中的待写入内容拷贝到临时的 DirectBuffer 中
            // 随后通过 write 系统调用将临时 DirectBuffer 中的内容写入到文件 page cache 中
            int n = writeFromNativeBuffer(fd, bb, position, nd);
            return n;
        }
    }
}
```

* HeapBuffer 为什么需要先转为 DirectBuffer ?

HeapByteBuffer 和 DirectByteBuffer 从本质上来说均是 JVM 进程地址空间内的一段虚拟内存。
FileChannel 的 read/write 方法, 最终会调用到 native 方法, 也就是 C++ 写的方法, 入参是一个引用地址。
而 HeapByteBuffer 是位于 JVM 堆中的内存，那么它必然会受到 GC 的管理, 传入到 native 方法的地址, 然后触发了 GC, 可能会移动存活的对象,
导致
HeapByteBuffer 在 GC 之后它背后的内存地址可能已经发生了变化, 最终导致 native 方法中的地址指向了错误的内存地址,
这样就会导致数据的错误读写。  
先将 HeapByteBuffer 转为 DirectByteBuffer, 变为堆外内存, 就是为了避免这个问题。

* 将 HeapByteBuffer 的内容拷贝到 DirectByteBuffer 的过程如果发生了 GC，HeapByteBuffer 背后引用内存的地址发生了变化,
  DirectByteBuffer 的内容会错误吗 ?

事实上, 这个拷贝的过程中是不会发生 GC 的，因为 HeapByteBuffer 到 DirectByteBuffer 的拷贝是通过 Unsafe#copyMemory 实现的,
copyMemory 被 JVM 实现为一个 intrinsic 方法，中间是没有 safepoint 的，执行 copyMemory 的线程由于不在 safepoint
中，所以在拷贝的过程中是不会发生 GC 的。

## 参考

[MappedByteBuffer VS FileChannel：从内核层面对比两者的性能差异](https://juejin.cn/post/7350977538276343817)  
[MappedByteBuffer的一点优化](https://lishoubo.github.io/2017/09/27/MappedByteBuffer%E7%9A%84%E4%B8%80%E7%82%B9%E4%BC%98%E5%8C%96/)  
[「抄底 Android 内存优化 4」 — 图解 mmap 原理](https://blog.csdn.net/qq_23191031/article/details/108227587)  

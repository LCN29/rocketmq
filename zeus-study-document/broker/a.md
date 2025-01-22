FileChannel 和 MappedByteBuffer 写数据的比较

## FileChannel 和 MappedByteBuffer 写数据的方式

在 Java 的 NIO 中, 向一个文件里面写数据常见的方式有 2 种
> 1. 使用 FileChannel.write(ByteBuffer) 方法, 这时数据还是在系统内存中, 可以通过 FileChannel.force() 方法将数据强制刷到磁盘
> 2. 使用 MappedByteBuffer.put(ByteBuffer) 方法, 同样可以通过 MappedByteBuffer.force() 方法将数据强制刷到磁盘

### FileChannel.write(ByteBuffer)

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

    // 写入数据
    fileChannel.write(buffer);
    // 强制刷新到磁盘
    fileChannel.force(false);
    fileChannel.close();
}
```

### MappedByteBuffer.put(ByteBuffer)

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

## 参考

[MappedByteBuffer VS FileChannel：从内核层面对比两者的性能差异](https://juejin.cn/post/7350977538276343817)
[MappedByteBuffer的一点优化](https://lishoubo.github.io/2017/09/27/MappedByteBuffer%E7%9A%84%E4%B8%80%E7%82%B9%E4%BC%98%E5%8C%96/)

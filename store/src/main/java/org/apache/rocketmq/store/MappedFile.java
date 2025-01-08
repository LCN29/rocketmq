/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.CommitLog.PutMessageContext;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 同步刷盘:
 * 数据先写入 ByteBuffer，由 CommitRealTime 线程定时 200ms 提交到 fileChannel 内存，再由 FlushRealTime 线程定时 500ms 刷 fileChannel 落盘
 * 异步刷盘:
 * 直接将数据写入到 MappedByteBuffer
 */
public class MappedFile extends ReferenceResource {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 操作系统页大小 4K
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;

    /**
     * 所有 MappedFile 实例已使用字节总数
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 所有 MappedFile 个数
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * 当前 MappedFile 对象当前写指针, 下次写数据从此开始写入
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * 当前 MappedFile 对象提交的指针
     * 指针之前的数据已提交到 fileChannel, committedPosition 到 wrotePosition 之间的数据是还未提交到 fileChannel 的
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * 当前 MappedFile 对象刷写到磁盘的指针
     * 指针之前的数据已落盘, committedPosition 到 flushedPosition 之间的数据是还未落盘的
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    /**
     * 文件总大小
     */
    protected int fileSize;

    /**
     * 文件通道
     */
    protected FileChannel fileChannel;

    /**
     * NIO 内存映射技术:
     * 将文件系统中的文件映射到内存中, 实现对文件的操作转换对内存地址的操作 (这个内存对象, 在代码里面就是 ByteBuffer)
     * 可以看 TransientStorePool 类的 init 方法, 里面有跟详细说明
     * <p>
     * ByteBuffer 缓冲池, 将最近的 5 个 CommitLog 加载到这个缓冲池中, 一个 CommitLog 文件就是一个 ByteBuffer
     * <p>
     * 开启了 transientStorePoolEnable, 同时主节点 + 异步刷盘, 这个对象才有值,
     */
    protected TransientStorePool transientStorePool = null;

    /**
     * transientStorePool 池中的第一个 ByteBuffer 对象
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * 文件名称
     */
    private String fileName;

    /**
     * 当前文件起始的写入字节数
     * <p>
     * RocketMQ 的设计:
     * 一个文件的名字就是当前文件存储的第一条消息的偏移量,
     * 通过这个偏移量 + 一个文件大小, 就可以获取到下一个文件的名字
     */
    private long fileFromOffset;

    /**
     * 文件对象
     */
    private File file;

    /**
     * 实际就是操作系统的 PageCache (文件内容加载到内存中的对象)
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 最近一次存储时间戳
     */
    private volatile long storeTimestamp = 0;

    /**
     * 文件第一次创建
     */
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            if (dirName.contains(MessageStoreConfig.MULTI_PATH_SPLITTER)) {
                String[] dirs = dirName.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
                for (String dir : dirs) {
                    createDirIfNotExist(dir);
                }
            } else {
                createDirIfNotExist(dirName);
            }
        }
    }

    private static void createDirIfNotExist(String dirName) {
        File f = new File(dirName);
        if (!f.exists()) {
            boolean result = f.mkdirs();
            log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) return;
        // 发射 ByteBuffer 的 attachment 方法, 获取到 ByteBuffer
        // 再通过 ByteBuffer 的 cleaner 方法获取到 sun.misc.Cleaner 对象
        // 最后通过 Cleaner 的 clean 方法释放 ByteBuffer 对象
        // 达到清除 ByteBuffer 对象的目的
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method method = method(target, methodName, args);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }
        // 通过反射获取 ByteBuffer 的 attachment 方法
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null) return buffer;
        else return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        // commitLog 的文件名就是当前文件开始的消息偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        // 确保文件目录没有问题
        ensureDirOK(this.file.getParent());

        try {
            // 文件通道
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 通过 FileChannel 创建 MappedByteBuffer, 将文件映射到内存中, 也就是使用 mmap 技术
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            // 全局记录用了多少文件映射内存
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            // 全局记录文件映射内存的文件个数
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 向 MappedFile 中追加单条消息
     *
     * @param msg               消息
     * @param cb                具体文件对象的写入消息实现函数
     * @param putMessageContext 上下文
     * @return 追加结果
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb, PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    /**
     * 向 MappedFile 中追加批量消息
     *
     * @param messageExtBatch   批量消息
     * @param cb                具体文件对象的写入消息实现函数
     * @param putMessageContext 上下文
     * @return 追加结果
     */
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb, PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    /**
     * 向 MappedFile 中追加消息
     *
     * @param messageExt        消息
     * @param cb                具体文件对象的写入消息实现函数
     * @param putMessageContext 上下文
     * @return 追加结果
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb, PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;
        // 当前文件写入的指针
        int currentPos = this.wrotePosition.get();
        // 写入的指针位置还没达到文件的大小, 即文件还有空间可以写入
        // 一般来说在写入过程中会控制文件剩余空间不够本次写入数据, 会自动创建新文件继续写入, 所以这里的写入位置应该永远小于文件末尾位置才对
        if (currentPos < this.fileSize) {
            // 获取当前写入的 ByteBuffer 对象, 如果开启了 transientStorePoolEnable + 主节点 + 异步刷盘, 那么就是从池中获取的 ByteBuffer 对象, 否则就是 MappedByteBuffer
            // ByteBuffer.slice() 方法创建一个新的 ByteBuffer 对象, 与原始 ByteBuffer 共享数据元素, 但是会重置自己的 pos, lim, mark 属性 (可以简单的看出是拷贝了一个新的对象)
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBrokerInner) {
                // AppendMessageCallback 现在只有一个实现 DefaultAppendMessageCallback
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBrokerInner) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBatch) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 将内存中的数据强制同步到操作系统的磁盘缓存中, 并确保数据在磁盘上持久化
     *
     * @param flushLeastPages 刷新的页数, 会按照 OS_PAGE_SIZE 为一页进行分配处理, 为 0 就正常的偏移量进行处理
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {

        // 计算当前内存累计的脏页是否到了最低脏页刷盘的阈值 flushLeastPages
        if (this.isAbleToFlush(flushLeastPages)) {
            // 当前 MappedFile 是否还被至少一个线程持有者
            // hold 是 ReferenceResource 里面的方法, 会记录持有这个文件的线程数, 大于等于 1, 就返回 true
            if (this.hold()) {

                // 获取最新的位置, 写位置或者提交位置, 由是否开启了 堆外缓冲池功能 决定
                int value = getReadPosition();
                try {
                    // We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        // 只将数据刷入到磁盘, 不修改文件的元数据(比如文件的修改时间)
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                // 更新最新的刷盘位置
                this.flushedPosition.set(value);
                // 当前文件的引用数减 1
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     *
     * @param commitLeastPages
     * @return
     */
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            // 是否可以提交
            // 如果 commitLeastPages 大于 0, 会计算写指标到提交指标直接的偏差 除以 每页的大小, 得到当前的偏差满足的页数, 如果没达到对应的页数, 就返回 false
            // 小于等于 0, 那么只要写偏差大于提交偏差就返回 true
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        // committedPosition 就是提交的位置等于文件的大小了, 说明文件已经写满了, 释放当前文件对 writeBuffer 的引用, 给后续的 MappedFile 使用
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }
        // 返回提交的位置
        return this.committedPosition.get();
    }

    protected void commit0() {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > 0) {
            try {
                // 将上一次提交的位置到当前写入的位置的数据提交到 FileChannel 中, 也就是系统内存中, 这时数据未必会真正的写入到磁盘中
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否可以刷盘
     *
     * @param flushLeastPages 刷盘的页数
     * @return 是否可以刷盘
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        // 获取刷盘位置
        int flush = this.flushedPosition.get();
        // 获取写入位置
        int write = getReadPosition();

        // 写入位置已经到了文件末尾, 说明文件已经写满了, 需要刷盘
        if (this.isFull()) {
            return true;
        }

        // 如果刷盘页数大于 0, 那么就计算写入位置和刷盘位置之间的偏差, 除以每页的大小, 得到当前的偏差满足的页数, 如果没达到对应的页数, 就返回 false
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }
        // 如果刷盘页数小于等于 0, 那么只要写入位置大于刷盘位置就返回 true
        return write > flush;
    }

    /**
     * 是否可以提交
     *
     * @param commitLeastPages 提交的页数
     * @return 是否可以提交
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        // 获取提交位置
        int commit = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: " + this.fileFromOffset);
            }
        } else {
            log.warn(
                    "selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info(
                        "delete file[REF:" + this.getRefCount() + "] " + this.fileName + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:" + this.getFlushedPosition() + ", " + UtilAll.computeElapsedTimeMilliseconds(
                                beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn(
                    "destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 具有有效数据的最大位置
     *
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        // writeBuffer 不为空, 就返回提交位置, 否则返回写入位置
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 文件预热, 简单理解就是在文件的每 4K (也就是一页) 处写入一个 0, 提前预热整个文件
     * 一个文件的预热的时间是一个比较耗时的过程, 但是预热后的文件的写入性能更好
     * <p>
     * 例子
     * 向一个没有预热的文件写入 1G 的数据, 可能需要 300ms
     * 但是如果预热后, 再写入 1G 的数据, 可能只需要 270ms, 但是前面需要花 100ms 的时间去预热
     *
     * @param type
     * @param pages
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            // OS_PAGE_SIZE = 1024 * 4
            // 简单理解就是在文件的每 4K (也就是一页) 处写入一个 0
            // 原理: mmap 只是将磁盘文件映射到程序的虚拟内存地址中, 并没有分配真正的物理内存页,
            // 提前写入一个 0, 强制分配一个物理内存页, 也就是预热
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}", this.getFileName(),
                    System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /**
     * 文件加锁
     */
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret,
                    System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret,
                    System.currentTimeMillis() - beginTime);
        }
    }

    /**
     * 文件解锁
     */
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret,
                System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}

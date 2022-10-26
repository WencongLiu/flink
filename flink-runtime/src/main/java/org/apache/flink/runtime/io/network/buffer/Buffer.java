/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Wrapper for pooled {@link MemorySegment} instances with reference counting.
 *
 * <p>This is similar to Netty's <tt>ByteBuf</tt> with some extensions and restricted to the methods
 * our use cases outside Netty handling use.
 *
 * In particular, we use two different indexes for read
 * and write operations, i.e. the <tt>reader</tt> and <tt>writer</tt> index (size of written data),
 * which specify three regions inside the memory segment:
 *
 * <pre>
 *     readerIndex writerIndex maxCapacity
 *     +-------------------+----------------+----------------+
 *     | discardable bytes | readable bytes | writable bytes |
 *     +-------------------+----------------+----------------+
 *     |                   |                |                |
 *     0      <=      readerIndex  <=  writerIndex   <=  max capacity
 * </pre>
 * <p>Our non-Netty usages of this <tt>Buffer</tt> class either rely on the underlying {@link
 * #getMemorySegment()} directly, or on {@link ByteBuffer} wrappers of this buffer which do not
 * modify either index, so the indices need to be updated manually via {@link #setReaderIndex(int)}
 * and {@link #setSize(int)}.
 */
public interface Buffer {
    /**
     * Returns whether this buffer represents a buffer or an event.
     *
     * @return <tt>true</tt> if this is a real buffer, <tt>false</tt> if this is an event
     */

    boolean isBuffer();

    /**
     * Returns the underlying memory segment. This method is dangerous since it ignores
     *
     * 忽略了读的限制 以及 分片的限制 ？
     * read only protections and omits slices.
     *
     * Use it only along the {@link #getMemorySegmentOffset()}.
     *
     * <p>This method will be removed in the future. For writing use {@link BufferBuilder}.
     *
     * @return the memory segment backing this buffer
     */
    @Deprecated
    MemorySegment getMemorySegment();

    /**
     * This method will be removed in the future. For writing use {@link BufferBuilder}.
     *
     * @return the offset where this (potential slice) {@link Buffer}'s data start in the underlying
     *     memory segment.
     */
    @Deprecated
    int getMemorySegmentOffset();

    /**
     * Gets the buffer's recycler.
     *
     * @return buffer recycler
     */
    BufferRecycler getRecycler();

    /**
     * Releases this buffer once, i.e. reduces the reference count and recycles the buffer if the
     * reference count reaches <tt>0</tt>.
     *
     * @see #retainBuffer()
     */
    void recycleBuffer();

    /**
     * Returns whether this buffer has been recycled or not.
     *
     * @return <tt>true</tt> if already recycled, <tt>false</tt> otherwise
     */
    boolean isRecycled();

    /**
     * Retains this buffer for further use, increasing the reference counter by <tt>1</tt>.
     *
     * @return <tt>this</tt> instance (for chained calls)
     * @see #recycleBuffer()
     */
    Buffer retainBuffer();

    /**
     * Returns a read-only slice of this buffer's readable bytes, i.e. between {@link
     * #getReaderIndex()} and {@link #getSize()}.
     *
     * <p>Reader and writer indices as well as markers are not shared. Reference counters are shared
     * but the slice is not {@link #retainBuffer() retained} automatically.
     *
     * @return a read-only sliced buffer
     */
    Buffer readOnlySlice();

    /**
     * Returns a read-only slice of this buffer.
     *
     * <p>Reader and writer indices as well as markers are not shared. Reference counters are shared
     * but the slice is not {@link #retainBuffer() retained} automatically.
     *
     * @param index the index to start from
     * @param length the length of the slice
     * @return a read-only sliced buffer
     */
    // 从这个index开始截取length长度的 Slice
    Buffer readOnlySlice(int index, int length);

    /**
     * Returns the maximum size of the buffer, i.e. the capacity of the underlying {@link
     * MemorySegment}.
     *
     * @return size of the buffer
     */
    // 获取buffer最大的size
    int getMaxCapacity();

    /**
     * Returns the <tt>reader index</tt> of this buffer.
     *
     * <p>This is where readable (unconsumed) bytes start in the backing memory segment.
     *
     * @return reader index (from 0 (inclusive) to the size of the backing {@link MemorySegment}
     *     (inclusive))
     */
    // 返回buffer的reader index 意思是可读起点
    int getReaderIndex();

    /**
     * Sets the <tt>reader index</tt> of this buffer.
     *
     * @throws IndexOutOfBoundsException if the index is less than <tt>0</tt> or greater than {@link
     *     #getSize()}
     */
    // 设置buffer的 可读起点 我尼玛 这还是可以设置的
    void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException;

    /**
     * Returns the size of the written data, i.e. the <tt>writer index</tt>, of this buffer.
     *
     * <p>This is where writable bytes start in the backing memory segment.
     *
     * @return writer index (from 0 (inclusive) to the size of the backing {@link MemorySegment}
     *     (inclusive))
     */
    // 返回写入数据的大小，即为buffer的writer index
    int getSize();

    /**
     * Sets the size of the written data, i.e. the <tt>writer index</tt>, of this buffer.
     *
     * @throws IndexOutOfBoundsException if the index is less than {@link #getReaderIndex()} or
     *     greater than {@link #getMaxCapacity()}
     */
    // 设置了 written data的大小，即为buffer的 writer index
    void setSize(int writerIndex);

    /**
     * Returns the number of readable bytes (same as <tt>{@link #getSize()} - {@link
     * #getReaderIndex()}</tt>).
     */
    // 获取可读的byte数量
    int readableBytes();

    // 返回一个ByteBuffer共享底层相同的字节数组
    ByteBuffer getNioBufferReadable();
    ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException;

    // 为什么要把Netty的ByteBufAllocator放进来
    void setAllocator(ByteBufAllocator allocator);

    // 直接转为一个ByteBuf
    ByteBuf asByteBuf();

    // 获取/设置压缩状态
    boolean isCompressed();
    void setCompressed(boolean isCompressed);

    // 获取/设置ByteBuffer的数据类型
    DataType getDataType();
    void setDataType(DataType dataType);


    // 获取当前Buffer的引用数量
    // retainBuffer 增加引用
    // recycleBuffer 减少引用
    int refCnt();

    // 一个工具方法
    default String toDebugString(boolean includeHash) {
        // 生成一个PrettyString
        StringBuilder prettyString =
                new StringBuilder("Buffer{cnt=")
                        .append(refCnt())
                        .append(", size=")
                        .append(getSize());
        // PrettyString 中是否包含 hash
        if (includeHash) {
            byte[] bytes = new byte[getSize()];
            readOnlySlice().asByteBuf().readBytes(bytes);
            prettyString.append(", hash=").append(Arrays.hashCode(bytes));
        }
        return prettyString.append("}").toString();
    }

    /**
     * Used to identify the type of data contained in the {@link Buffer} so that we can get the
     * information without deserializing the serialized data.
     *
     * <p>Notes: Currently, one byte is used to serialize the ordinal of {@link DataType} in {@link
     * org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse}, so the maximum number
     * of supported data types is 128.
     */

    // 注意 DataType 和 Buffer 数据类型强绑定

    enum DataType {
        /** {@link #NONE} indicates that there is no buffer. */
        NONE(false, false, false, false, false),

        /** {@link #DATA_BUFFER} indicates that this buffer represents a non-event data buffer. */
        DATA_BUFFER(true, false, false, false, false),

        /**
         * {@link #EVENT_BUFFER} indicates that this buffer represents serialized data of an event.
         * Note that this type can be further divided into more fine-grained event types like {@link
         * #ALIGNED_CHECKPOINT_BARRIER} and etc.
         */
        // 这个EVENT BUFFER 类型 还可以被拆分成多个小类型
        EVENT_BUFFER(false, true, false, false, false),

        /** Same as EVENT_BUFFER, but the event has been prioritized (e.g. it skipped buffers). */
        PRIORITIZED_EVENT_BUFFER(false, true, false, true, false),

        // 是一个 barrier 相关的 DataType
        /**
         * {@link #ALIGNED_CHECKPOINT_BARRIER} indicates that this buffer represents a serialized
         * checkpoint barrier of aligned exactly-once checkpoint mode.
         */
        ALIGNED_CHECKPOINT_BARRIER(false, true, true, false, false),

        /**
         * {@link #TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER} indicates that this buffer represents a
         * serialized checkpoint barrier of aligned exactly-once checkpoint mode, that can be
         * time-out'ed to an unaligned checkpoint barrier.
         */
        // 同时拥有超时机制
        TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER(false, true, true, false, true),

        /**
         * Indicates that this subpartition state is fully recovered (emitted). Further data can be
         * consumed after unblocking.
         * 为什么这个buffer还可以标识这些内容..
         */
        RECOVERY_COMPLETION(false, true, true, false, false);

        /**
         * 注意 优先Buffer和Announce Buffer
         */

        // 是不是buffer
        private final boolean isBuffer;
        // 是不是event
        private final boolean isEvent;
        // 是不是block上游
        private final boolean isBlockingUpstream;
        // 是不是优先buffer
        private final boolean hasPriority;
        /**
         * If buffer (currently only Events are supported in that case) requires announcement, it's
         * arrival in the {@link
         * org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel} will be
         * announced, by a special announcement message. Announcement messages are {@link
         * #PRIORITIZED_EVENT_BUFFER} processed out of order. It allows readers of the input to
         * react sooner on arrival of such Events, before it will be able to be processed normally.
         */
        // 是否需要 announce
        private final boolean requiresAnnouncement;

        DataType(
                boolean isBuffer,
                boolean isEvent,
                boolean isBlockingUpstream,
                boolean hasPriority,
                boolean requiresAnnouncement) {
            checkState(
                    !(requiresAnnouncement && hasPriority),
                    "DataType [%s] has both priority and requires announcement, which is not supported "
                            + "and doesn't make sense. There should be no need for announcing priority events, which are always "
                            + "overtaking in-flight data.",
                    this);
            this.isBuffer = isBuffer;
            this.isEvent = isEvent;
            this.isBlockingUpstream = isBlockingUpstream;
            this.hasPriority = hasPriority;
            this.requiresAnnouncement = requiresAnnouncement;
        }

        public boolean isBuffer() {
            return isBuffer;
        }

        public boolean isEvent() {
            return isEvent;
        }

        public boolean hasPriority() {
            return hasPriority;
        }

        public boolean isBlockingUpstream() {
            return isBlockingUpstream;
        }

        public boolean requiresAnnouncement() {
            return requiresAnnouncement;
        }

        // 内部的Event，也可以持有DataTupe类型
        public static DataType getDataType(AbstractEvent event, boolean hasPriority) {
            if (hasPriority) {
                return PRIORITIZED_EVENT_BUFFER;
            } else if (event instanceof EndOfChannelStateEvent) {
                return RECOVERY_COMPLETION;
            } else if (!(event instanceof CheckpointBarrier)) {
                return EVENT_BUFFER;
            }
            CheckpointBarrier barrier = (CheckpointBarrier) event;
            if (barrier.getCheckpointOptions().needsAlignment()) {
                if (barrier.getCheckpointOptions().isTimeoutable()) {
                    return TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER;
                } else {
                    return ALIGNED_CHECKPOINT_BARRIER;
                }
            } else {
                return EVENT_BUFFER;
            }
        }
    }
}

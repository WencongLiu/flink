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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A single subpartition of a {@link ResultPartition} instance. */
// 赶紧看一下RSP的实现
public abstract class ResultSubpartition {

    /** The info of the subpartition to identify it globally within a task. */
    // 一个RS的Info信息
    protected final ResultSubpartitionInfo subpartitionInfo;

    /** The parent partition this subpartition belongs to. */
    // 持有一个RSP的归属 是 RP
    protected final ResultPartition parent;

    // - Statistics ----------------------------------------------------------

    public ResultSubpartition(int index, ResultPartition parent) {
        this.parent = parent;
        this.subpartitionInfo = new ResultSubpartitionInfo(parent.getPartitionIndex(), index);
    }

    // 一个RS的Info信息
    public ResultSubpartitionInfo getSubpartitionInfo() {
        return subpartitionInfo;
    }

    /** Gets the total numbers of buffers (data buffers plus events). */
    // 获取所有的buffer数量？
    protected abstract long getTotalNumberOfBuffersUnsafe();

    // 获取所有的byte数量？
    protected abstract long getTotalNumberOfBytesUnsafe();

    // 获取subpartition的索引
    public int getSubPartitionIndex() {
        return subpartitionInfo.getSubPartitionIdx();
    }

    /** Notifies the parent partition about a consumed {@link ResultSubpartitionView}. */
    // 如果这个RSP已经被消费 那么就会调用这个方法
    protected void onConsumedSubpartition() {
        parent.onConsumedSubpartition(getSubPartitionIndex());
    }

    // 分配barrier信息
    public abstract void alignedBarrierTimeout(long checkpointId) throws IOException;

    // 把Checkpoint取消掉？
    public abstract void abortCheckpoint(long checkpointId, CheckpointException cause);

    @VisibleForTesting
    // 这暂时不知道干啥的
    public final int add(BufferConsumer bufferConsumer) throws IOException {
        return add(bufferConsumer, 0);
    }

    /**
     * Adds the given buffer.
     *
     * <p>The request may be executed synchronously, or asynchronously, depending on the
     * implementation.
     *
     * overtaken 赶超 超过
     * <p><strong>IMPORTANT:</strong> Before adding new {@link BufferConsumer} previously added must
     * be in finished state. Because of the performance reasons, this is only enforced during the
     * data reading. Priority events can be added while the previous buffer consumer is still open,
     * in which case the open buffer consumer is overtaken.
     *
     * @param bufferConsumer the buffer to add (transferring ownership to this writer)
     * @param partialRecordLength the length of bytes to skip in order to start with a complete
     *     record, from position index 0 of the underlying {@cite MemorySegment}.
     * @return the preferable buffer size for this subpartition or -1 if the add operation fails.
     * @throws IOException thrown in case of errors while adding the buffer
     */
    // 这个方法暂时不知道干啥的..
    public abstract int add(BufferConsumer bufferConsumer, int partialRecordLength)
            throws IOException;
    // 这个是 flush
    public abstract void flush();
    // 这个是finish 应该是可以标识结束把..
    public abstract void finish() throws IOException;
    // 这个是release 不知道和finish的区别
    public abstract void release() throws IOException;
    // 暂时不清楚为什么要创建View
    public abstract ResultSubpartitionView createReadView(
            BufferAvailabilityListener availabilityListener) throws IOException;
    // 标识是否release
    public abstract boolean isReleased();

    /** Gets the number of non-event buffers in this subpartition. */
    // 获取非event buffer，什么情况..
    abstract int getBuffersInBacklogUnsafe();

    // 意思是这个方法不要去干预任务和网络线程
    /**
     * Makes a best effort to get the current size of the queue. This method must not acquire locks
     * or interfere with the task and network threads in any way.
     */
    public abstract int unsynchronizedGetNumberOfQueuedBuffers();

    // 获取当前队列的大小
    /** Get the current size of the queue. */
    public abstract int getNumberOfQueuedBuffers();

    // 这个就不清楚是 什么样的bufferSize了
    public abstract void bufferSize(int desirableNewBufferSize);

    // ------------------------------------------------------------------------

    /**
     * A combination of a {@link Buffer} and the backlog length indicating how many non-event
     * buffers are available in the subpartition.
     * 反正意思就是 Buffer对象 和 backlog length 的结合体
     * 能够indicate 在subpartition内 还有多少非event buffer是可用的
     */
    public static final class BufferAndBacklog {
        // 怎么tmd有个 buffer 呢
        private final Buffer buffer;
        // 我擦 为什么还有个 buffersInBacklog 数值呢
        private final int buffersInBacklog;
        // 怎么TMD还有一个 DataType
        private final Buffer.DataType nextDataType;
        // 还有一个int类型的 sequenceNumber
        // 为什么还有一个这个 我靠
        private final int sequenceNumber;

        // 这他吗也只是接收了这几个参数 并没有什么用
        // 也只是说 可以初始化这几个构造参数
        public BufferAndBacklog(
                Buffer buffer,
                int buffersInBacklog,
                Buffer.DataType nextDataType,
                int sequenceNumber) {
            this.buffer = checkNotNull(buffer);
            this.buffersInBacklog = buffersInBacklog;
            this.nextDataType = checkNotNull(nextDataType);
            this.sequenceNumber = sequenceNumber;
        }

        public Buffer buffer() {
            return buffer;
        }

        public boolean isDataAvailable() {
            return nextDataType != Buffer.DataType.NONE;
        }

        public int buffersInBacklog() {
            return buffersInBacklog;
        }

        public boolean isEventAvailable() {
            return nextDataType.isEvent();
        }

        public Buffer.DataType getNextDataType() {
            return nextDataType;
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        public static BufferAndBacklog fromBufferAndLookahead(
                Buffer current, Buffer.DataType nextDataType, int backlog, int sequenceNumber) {
            return new BufferAndBacklog(current, backlog, nextDataType, sequenceNumber);
        }
    }
}

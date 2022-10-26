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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An input channel consumes a single {@link ResultSubpartitionView}.
 *
 * <p>For each channel, the consumption life cycle is as follows:
 *
 * <ol>
 *   <li>{@link #requestSubpartition()}
 *   <li>{@link #getNextBuffer()}
 *   <li>{@link #releaseAllResources()}
 * </ol>
 */
// 就是一个消费行为呗 很简单

public abstract class InputChannel {
    /** The info of the input channel to identify it globally within a task. */
    // NND 自己也有一个Index
    protected final InputChannelInfo channelInfo;

    /** The parent partition of the subpartition consumed by this channel. */
    // 它还记着 RPID 呢
    protected final ResultPartitionID partitionId;

    /** The index of the subpartition consumed by this channel. */
    // 上游消费的 index
    protected final int consumedSubpartitionIndex;

    // 反向饮用一个 SIG
    protected final SingleInputGate inputGate;

    // - Asynchronous error notification --------------------------------------
    // 我靠 原子引用
    private final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

    // 补偿 回退 抵扣
    // - Partition request backoff --------------------------------------------

    /**
     * 没看懂这几个什么意思
     */
    /** The initial backoff (in ms). */
    protected final int initialBackoff;
    /** The maximum backoff (in ms). */
    protected final int maxBackoff;
    /** The current backoff (in ms). */
    private int currentBackoff;

    // 计算InputGate的 numBytes 和 numBuffers
    protected final Counter numBytesIn;
    protected final Counter numBuffersIn;


    protected InputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            int consumedSubpartitionIndex,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn) {

        checkArgument(channelIndex >= 0);

        int initial = initialBackoff;
        int max = maxBackoff;

        checkArgument(initial >= 0 && initial <= max);

        this.inputGate = checkNotNull(inputGate);
        this.channelInfo = new InputChannelInfo(inputGate.getGateIndex(), channelIndex);
        this.partitionId = checkNotNull(partitionId);

        checkArgument(consumedSubpartitionIndex >= 0);
        this.consumedSubpartitionIndex = consumedSubpartitionIndex;

        this.initialBackoff = initial;
        this.maxBackoff = max;
        this.currentBackoff = initial == 0 ? -1 : 0;

        this.numBytesIn = numBytesIn;
        this.numBuffersIn = numBuffersIn;
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------


    public int getChannelIndex() {
        return channelInfo.getInputChannelIdx();
    }
    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }
    public ResultPartitionID getPartitionId() {
        return partitionId;
    }
    public int getConsumedSubpartitionIndex() {
        return consumedSubpartitionIndex;
    }

    /**
     * After sending a {@link org.apache.flink.runtime.io.network.api.CheckpointBarrier} of
     * exactly-once mode, the upstream will be blocked and become unavailable. This method tries to
     * unblock the corresponding upstream and resume data consumption.
     */
    // 似乎就是在解锁上游
    public abstract void resumeConsumption() throws IOException;

    /**
     * When received {@link EndOfData} from one channel, it need to acknowledge after this event get
     * processed.
     */
    // 自己提供出去哟还能过来确认？
    public abstract void acknowledgeAllRecordsProcessed() throws IOException;

    /**
     * Notifies the owning {@link SingleInputGate} that this channel became non-empty.
     *
     * <p>This is guaranteed to be called only when a Buffer was added to a previously empty input
     * channel. The notion of empty is atomically consistent with the flag {@link
     * BufferAndAvailability#moreAvailable()} when polling the next buffer from this channel.
     *
     * <p><b>Note:</b> When the input channel observes an exception, this method is called
     * regardless of whether the channel was empty before. That ensures that the parent InputGate
     * will always be notified about the exception.
     */
    // 如果这个inputChannel内有数据 就应当直接notify
    protected void notifyChannelNonEmpty() {
        inputGate.notifyChannelNonEmpty(this);
    }

    public void notifyPriorityEvent(int priorityBufferNumber) { inputGate.notifyPriorityEvent(this, priorityBufferNumber);}


    // 这个就看不太懂了
    protected void notifyBufferAvailable(int numAvailableBuffers) throws IOException {}

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------
    // 这里的Consume我理解就是对外暴露 消费上游的数据

    /**
     * Requests the subpartition specified by {@link #partitionId} and {@link
     * #consumedSubpartitionIndex}.
     */
    // 请求上游RSP
    abstract void requestSubpartition() throws IOException, InterruptedException;

    /**
     * Returns the next buffer from the consumed subpartition or {@code Optional.empty()} if there
     * is no data to return.
     */
    // 最核心的方法
    abstract Optional<BufferAndAvailability> getNextBuffer() throws IOException, InterruptedException;

    /**
     * Called by task thread when checkpointing is started (e.g., any input channel received
     * barrier).
     */
    // 通知所有RemoteInputChannel 已经开始做CK了
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {}

    /** Called by task thread on cancel/complete to clean-up temporary data. */
    // 通知所有RemoteInputChannel CK已经做完了
    public void checkpointStopped(long checkpointId) {}

    public void convertToPriorityEvent(int sequenceNumber) throws IOException {}

    // ------------------------------------------------------------------------
    // Task events
    // ------------------------------------------------------------------------

    /**
     * Sends a {@link TaskEvent} back to the task producing the consumed result partition.
     *
     * <p><strong>Important</strong>: The producing task has to be running to receive backwards
     * events. This means that the result type needs to be pipelined and the task logic has to
     * ensure that the producer will wait for all backwards events. Otherwise, this will lead to an
     * Exception at runtime.
     */
    // 我靠 反向发送Event
    abstract void sendTaskEvent(TaskEvent event) throws IOException;

    // ------------------------------------------------------------------------
    // Life cycle
    // ------------------------------------------------------------------------

    abstract boolean isReleased();

    /** Releases all resources of the channel. */
    abstract void releaseAllResources() throws IOException;

    abstract void announceBufferSize(int newBufferSize);

    abstract int getBuffersInUseCount();

    // ------------------------------------------------------------------------
    // Error notification
    // ------------------------------------------------------------------------

    /**
     * Checks for an error and rethrows it if one was reported.
     *
     * <p>Note: Any {@link PartitionException} instances should not be transformed and make sure
     * they are always visible in task failure cause.
     */
    protected void checkError() throws IOException {
        final Throwable t = cause.get();

        if (t != null) {
            if (t instanceof CancelTaskException) {
                throw (CancelTaskException) t;
            }
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException(t);
            }
        }
    }

    /**
     * Atomically sets an error for this channel and notifies the input gate about available data to
     * trigger querying this channel by the task thread.
     */
    protected void setError(Throwable cause) {
        if (this.cause.compareAndSet(null, checkNotNull(cause))) {
            // Notify the input gate.
            notifyChannelNonEmpty();
        }
    }

    // ------------------------------------------------------------------------
    // Partition request exponential backoff
    // ------------------------------------------------------------------------

    /** Returns the current backoff in ms. */
    protected int getCurrentBackoff() {
        return currentBackoff <= 0 ? 0 : currentBackoff;
    }

    /**
     * Increases the current backoff and returns whether the operation was successful.
     *
     * @return <code>true</code>, iff the operation was successful. Otherwise, <code>false</code>.
     */
    protected boolean increaseBackoff() {
        // Backoff is disabled
        if (currentBackoff < 0) {
            return false;
        }

        // This is the first time backing off
        if (currentBackoff == 0) {
            currentBackoff = initialBackoff;

            return true;
        }

        // Continue backing off
        else if (currentBackoff < maxBackoff) {
            currentBackoff = Math.min(currentBackoff * 2, maxBackoff);

            return true;
        }

        // Reached maximum backoff
        return false;
    }

    // ------------------------------------------------------------------------
    // Metric related method
    // ------------------------------------------------------------------------

    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    public long unsynchronizedGetSizeOfQueuedBuffers() {
        return 0;
    }

    // ------------------------------------------------------------------------

    /**
     * A combination of a {@link Buffer} and a flag indicating availability of further buffers, and
     * the backlog length indicating how many non-event buffers are available in the subpartition.
     */
    public static final class BufferAndAvailability {

        private final Buffer buffer;
        private final Buffer.DataType nextDataType;
        private final int buffersInBacklog;
        private final int sequenceNumber;

        public BufferAndAvailability(
                Buffer buffer,
                Buffer.DataType nextDataType,
                int buffersInBacklog,
                int sequenceNumber) {
            this.buffer = checkNotNull(buffer);
            this.nextDataType = checkNotNull(nextDataType);
            this.buffersInBacklog = buffersInBacklog;
            this.sequenceNumber = sequenceNumber;
        }

        public Buffer buffer() {
            return buffer;
        }

        public boolean moreAvailable() {
            return nextDataType != Buffer.DataType.NONE;
        }

        public boolean morePriorityEvents() {
            return nextDataType.hasPriority();
        }

        public int buffersInBacklog() {
            return buffersInBacklog;
        }

        public boolean hasPriority() {
            return buffer.getDataType().hasPriority();
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public String toString() {
            return "BufferAndAvailability{"
                    + "buffer="
                    + buffer
                    + ", nextDataType="
                    + nextDataType
                    + ", buffersInBacklog="
                    + buffersInBacklog
                    + ", sequenceNumber="
                    + sequenceNumber
                    + '}';
        }
    }

    void setup() throws IOException {}
}

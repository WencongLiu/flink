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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * 一个Task对应一个RP
 * A result partition for data produced by a single task.
 * IntermediateResultPartition 的 运行时实现
 * <p>This class is the runtime part of a logical {@link IntermediateResultPartition}. Essentially,
 * a result partition is a collection of {@link Buffer} instances.
 * 意思就是一个RP的实现就是 IntermediateResultPartition 的运行时实现
 *
 * buffer 在多个RSP的实例或者是在一个聚合体内 被组织
 * The buffers are organized in one or more {@link ResultSubpartition} instances or in a joint structure which further partition the
 * data depending on the number of consuming tasks and the data {@link DistributionPattern}.
 *
 * Task在消费RP的前提下，需要去请求其中一个subpartition
 * 远程请求: RemoteInputChannel
 * 本地请求: LocalInputChannel
 * <p>Tasks, which consume a result partition have to request one of its subpartitions. The request
 * happens either remotely (see {@link RemoteInputChannel}) or locally (see {@link LocalInputChannel})
 *
 * <h2>Life-cycle</h2>
 * RP的生命周期 即为 生产 消费 释放
 *
 * <p>The life-cycle of each result partition has three (possibly overlapping) phases:
 *
 * <ol>
 *   <li><strong>Produce</strong>:
 *   <li><strong>Consume</strong>:
 *   <li><strong>Release</strong>:
 * </ol>
 *
 * <h2>Buffer management</h2>
 *
 * <h2>State management</h2>
 */
public abstract class ResultPartition implements ResultPartitionWriter {

    protected static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);

    // 有一个名字
    private final String owningTaskName;

    // 天呐 还有一个partitionIndex
    private final int partitionIndex;

    // 还有一个RPID ResultPartitionID
    protected final ResultPartitionID partitionId;

    // RP的类型约束了具体的实现类是什么
    /** Type of this partition. Defines the concrete subpartition implementation to use. */
    protected final ResultPartitionType partitionType;

    // 这个Manager 持有了 所有RP 的引用
    protected final ResultPartitionManager partitionManager;

    // 这个是 RSP 数量
    protected final int numSubpartitions;
    // 这个是 Target KG的数量
    private final int numTargetKeyGroups;

    // - Runtime state --------------------------------------------------------

    // 判断是不是 released
    private final AtomicBoolean isReleased = new AtomicBoolean();
    // 还持有了一个bufferPool的引用
    protected BufferPool bufferPool;
    // 判断是不是 Finished
    private boolean isFinished;
    // 一个Throwable类型 的 cause的引用
    private volatile Throwable cause;
    // TODO 这里为什么会有一个bufferPoolFactory
    private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    /** Used to compress buffer to reduce IO. */
    // BufferCompressor 可以用来压缩buffer
    @Nullable protected final BufferCompressor bufferCompressor;
    // Counter1
    protected Counter numBytesOut = new SimpleCounter();
    // Counter2
    protected Counter numBuffersOut = new SimpleCounter();

    /**
     * The difference with {@link #numBytesOut} : numBytesProduced represents the number of bytes
     * actually produced, and numBytesOut represents the number of bytes sent to downstream tasks.
     * In unicast scenarios, these two values should be equal. In broadcast scenarios, numBytesOut
     * should be (N * numBytesProduced), where N refers to the number of subpartitions.
     */
    // Counter3
    protected Counter numBytesProduced = new SimpleCounter();

    public ResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            // RPM 传过来了
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            // Factory 也传过来了
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        this.owningTaskName = checkNotNull(owningTaskName);
        Preconditions.checkArgument(0 <= partitionIndex, "The partition index must be positive.");
        // 这里的RP index是全局的
        this.partitionIndex = partitionIndex;
        this.partitionId = checkNotNull(partitionId);
        this.partitionType = checkNotNull(partitionType);
        this.numSubpartitions = numSubpartitions;
        this.numTargetKeyGroups = numTargetKeyGroups;
        this.partitionManager = checkNotNull(partitionManager);
        this.bufferCompressor = bufferCompressor;
        this.bufferPoolFactory = bufferPoolFactory;
    }

    /**
     *  每个RP会持有一个 BP 的引用
     * Registers a buffer pool with this result partition.
     *
     * <p>There is one pool for each result partition, which is shared by all its sub partitions.
     *
     * <p>The pool is registered with the partition *after* it as been constructed in order to
     * conform to the life-cycle of task registrations in the {@link TaskExecutor}.
     */
    @Override
    // new 之后然后调用setup
    // setup 之前 RPM中不会维持 RP的存在
    public void setup() throws IOException {
        checkState(
                this.bufferPool == null,
                "Bug in result partition setup logic: Already registered buffer pool.");

        this.bufferPool = checkNotNull(bufferPoolFactory.get());
        setupInternal();
        partitionManager.registerResultPartition(this);
    }

    /** Do the subclass's own setup operation. */
    protected abstract void setupInternal() throws IOException;

    // 获取 TaskName
    public String getOwningTaskName() {
        return owningTaskName;
    }

    @Override
    // 获取 ResultPartitionID
    public ResultPartitionID getPartitionId() {
        return partitionId;
    }

    // 获取 partitionIndex
    public int getPartitionIndex() {
        return partitionIndex;
    }

    @Override
    // 获取Subpartition数量
    public int getNumberOfSubpartitions() {
        return numSubpartitions;
    }

    // 获取bufferPool
    public BufferPool getBufferPool() {
        return bufferPool;
    }

    // 获取所有RSP数量
    /** Returns the total number of queued buffers of all subpartitions. */
    public abstract int getNumberOfQueuedBuffers();

    // 获取所有byte数量
    /** Returns the total size in bytes of queued buffers of all subpartitions. */
    public abstract long getSizeOfQueuedBuffersUnsafe();

    // 获取特定RSP的buffer数量
    /** Returns the number of queued buffers of the given target subpartition. */
    public abstract int getNumberOfQueuedBuffers(int targetSubpartition);

    // 设置buffer透支数量
    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        this.bufferPool.setMaxOverdraftBuffersPerGate(maxOverdraftBuffersPerGate);
    }

    /**
     * Returns the type of this result partition.
     *
     * @return result partition type
     */
    public ResultPartitionType getPartitionType() {
        return partitionType;
    }

    // ------------------------------------------------------------------------

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        throw new UnsupportedOperationException();
    }

    /**
     * The subpartition notifies that the corresponding downstream task have processed all the user
     * records.
     *
     * @see EndOfData
     * @param subpartition The index of the subpartition sending the notification.
     */
    // 暂时不知道谁去调用它
    public void onSubpartitionAllDataProcessed(int subpartition) {}

    /**
     * Finishes the result partition.
     *
     * <p>After this operation, it is not possible to add further data to the result partition.
     *
     * <p>For BLOCKING results, this will trigger the deployment of consuming tasks.
     */
    @Override
    public void finish() throws IOException {
        checkInProduceState();

        isFinished = true;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    public void release() {
        release(null);
    }

    @Override
    public void release(Throwable cause) {
        if (isReleased.compareAndSet(false, true)) {
            LOG.debug("{}: Releasing {}.", owningTaskName, this);

            // Set the error cause
            if (cause != null) {
                this.cause = cause;
            }

            releaseInternal();
        }
    }

    /** Releases all produced data including both those stored in memory and persisted on disk. */
    protected abstract void releaseInternal();

    private void closeBufferPool() {
        if (bufferPool != null) {
            bufferPool.lazyDestroy();
        }
    }

    @Override
    public void close() {
        closeBufferPool();
    }

    @Override
    public void fail(@Nullable Throwable throwable) {
        // the task canceler thread will call this method to early release the output buffer pool
        closeBufferPool();
        partitionManager.releasePartition(partitionId, throwable);
    }

    public Throwable getFailureCause() {
        return cause;
    }

    @Override
    public int getNumTargetKeyGroups() {
        return numTargetKeyGroups;
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        numBytesOut = metrics.getNumBytesOutCounter();
        numBuffersOut = metrics.getNumBuffersOutCounter();
        metrics.registerNumBytesProducedCounterForPartition(
                partitionId.getPartitionId(), numBytesProduced);
    }

    /**
     * Whether this partition is released.
     *
     * <p>A partition is released when each subpartition is either consumed and communication is
     * closed by consumer or failed. A partition is also released if task is cancelled.
     */
    @Override
    public boolean isReleased() {
        return isReleased.get();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return bufferPool.getAvailableFuture();
    }

    @Override
    public String toString() {
        return "ResultPartition "
                + partitionId.toString()
                + " ["
                + partitionType
                + ", "
                + numSubpartitions
                + " subpartitions]";
    }

    // ------------------------------------------------------------------------

    // 一个RSP的通知
    /** Notification when a subpartition is released. */
    void onConsumedSubpartition(int subpartitionIndex) {

        if (isReleased.get()) {
            return;
        }

        LOG.debug(
                "{}: Received release notification for subpartition {}.", this, subpartitionIndex);
    }

    // ------------------------------------------------------------------------

    protected void checkInProduceState() throws IllegalStateException {
        checkState(!isFinished, "Partition already finished.");
    }

    // 获取RPM
    @VisibleForTesting
    public ResultPartitionManager getPartitionManager() {
        return partitionManager;
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    // 判断一个BUffer是否可以被压缩
    protected boolean canBeCompressed(Buffer buffer) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }
}

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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * joint structure 一种联合结构是什么情况..
 *
 * A {@link ResultPartition} which writes buffers directly to {@link ResultSubpartition}s. This is
 * in contrast to implementations where records are written to a joint structure, from which the
 * subpartitions draw the data after the write phase is finished, for example the sort-based
 * partitioning.
 *
 * <p>To avoid confusion: On the read side, all subpartitions return buffers (and backlog) to be
 * transported through the network.
 */
public abstract class BufferWritingResultPartition extends ResultPartition {

    /** The subpartitions of this partition. At least one. */
    // 记录所有的subpartition
    protected final ResultSubpartition[] subpartitions;

    /**
     * For non-broadcast mode, each subpartition maintains a separate BufferBuilder which might be
     * null.
     */
    // 非Broadcast 每个subpartition都关联一个独立的BufferBuilder
    private final BufferBuilder[] unicastBufferBuilders;

    // Broadcast 所有subpartition共享一个BufferBuilder
    /** For broadcast mode, a single BufferBuilder is shared by all subpartitions. */
    private BufferBuilder broadcastBufferBuilder;

    // TimerGauge 是一个计时器
    private TimerGauge hardBackPressuredTimeMsPerSecond = new TimerGauge();

    // 记录written的Bytes数量
    private long totalWrittenBytes;

    // 肯定要初始化 subpartitions
    public BufferWritingResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            ResultSubpartition[] subpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                subpartitions.length,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);
        // 根据subpartitions对自身属性进行赋值
        this.subpartitions = checkNotNull(subpartitions);
        this.unicastBufferBuilders = new BufferBuilder[subpartitions.length];
    }

    @Override
    protected void setupInternal() throws IOException {
        // Buffer pool 没有充足的buffer
        // 需要保证MS数量大于RSP数量
        checkState(
                bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
                "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for"
                        + " this result partition.");
    }

    // 获取队列中buffer的数量
    @Override
    public int getNumberOfQueuedBuffers() {
        int totalBuffers = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return totalBuffers;
    }

    // 获取RP中所有RSP缓存buffer的总byte数量
    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        long totalNumberOfBytes = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalNumberOfBytes += Math.max(0, subpartition.getTotalNumberOfBytesUnsafe());
        }

        return totalWrittenBytes - totalNumberOfBytes;
    }

    // 获取特定RSP缓存buffer的byte数量
    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
        return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
    }

    // flush某个RSP
    protected void flushSubpartition(int targetSubpartition, boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilder(targetSubpartition);
        }

        subpartitions[targetSubpartition].flush();
    }

    // flush所有的RSP
    protected void flushAllSubpartitions(boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilders();
        }

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.flush();
        }
    }


    // 拿到record对应ByteBuffer的引用，以及 targetSubpartition 的编号

    /**
     * 注意! 这里的ByteBuffer 包裹了一个 Record 全部的数据
     *
     * @param record
     * @param targetSubpartition
     * @throws IOException
     */


    @Override
    // 重中之重: 一个完成的序列化之后的ByteBuffer如何下发给RP的RSP
    // 注意，一个完整的record会被单独放到一个RSP中
    // 这地方应该画一下逻辑图
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {

        totalWrittenBytes += record.remaining();

        // 申请一个BufferBuilder，1.把ByteBuffer中的数据放进去 2.将BufferBuilder放进RSP中
        BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);

        // BufferBuilder内数据大小为 32KB
        // 根据 Record 数据的大小
        // 可以划分为三种情况
        // 1. BufferBuilder内数据写满了，Record还有剩余
        // 2. BufferBuilder内数据写满了，Record也正好没有剩余了
        // 3. BufferBuilder内数据没有写满

        while (record.hasRemaining()) {
            // full buffer, partial record
            // 把当前最新的BufferBuilder给finish掉
            finishUnicastBufferBuilder(targetSubpartition);
            // 重复这个动作 申请一个BufferBuilder，1.把ByteBuffer中的数据放进去 2.将BufferBuilder放进RSP中
            // TODO 这里的 appendUnicastDataForNewRecord 和 appendUnicastDataForRecordContinuation 的逻辑几乎是一样的
            buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishUnicastBufferBuilder(targetSubpartition);
        }

        // partial buffer, full record
    }

    // 这个涉及到广播record的传输，和非广播比较类似
    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        totalWrittenBytes += ((long) record.remaining() * numSubpartitions);

        BufferBuilder buffer = appendBroadcastDataForNewRecord(record);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishBroadcastBufferBuilder();
            buffer = appendBroadcastDataForRecordContinuation(record);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishBroadcastBufferBuilder();
        }

        // partial buffer, full record
    }

    // 为什么要触发 这两个方法的调用
    //  finishBroadcastBufferBuilder();
    //  finishUnicastBufferBuilders();
    // 这涉及到了Event的broadcast
    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        checkInProduceState();
        // 先finish了这两个Builder
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        try (BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(event, isPriorityEvent)) {
            totalWrittenBytes += ((long) eventBufferConsumer.getWrittenBytes() * numSubpartitions);
            // 所有RSP都拿到这个BufferConsumer
            for (ResultSubpartition subpartition : subpartitions) {
                // Retain the buffer so that it can be recycled by each channel of targetPartition
                subpartition.add(eventBufferConsumer.copy(), 0);
            }
        }
    }

    // 这里牵扯到了BarrierTimeout的东西，还和checkpointId相关联
    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.alignedBarrierTimeout(checkpointId);
        }
    }

    // 将归属的SP都进行abort
    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.abortCheckpoint(checkpointId, cause);
        }
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        hardBackPressuredTimeMsPerSecond = metrics.getHardBackPressuredTimePerSecond();
    }

    //  为什么要创建ResultSubpartition View
    @Override
    public ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
        checkState(!isReleased(), "Partition released.");


        // 一个RSP对应一个RSV
        ResultSubpartition subpartition = subpartitions[subpartitionIndex];
        ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);

        LOG.debug("Created {}", readView);

        return readView;
    }

    @Override
    public void finish() throws IOException {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        for (ResultSubpartition subpartition : subpartitions) {
            // 看来重点还是RSP的finish
            subpartition.finish();
        }

        super.finish();
    }

    @Override
    protected void releaseInternal() {
        // Release all subpartitions
        for (ResultSubpartition subpartition : subpartitions) {
            try {
                subpartition.release();
            }
            // Catch this in order to ensure that release is called on all subpartitions
            catch (Throwable t) {
                LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        // We can not close these buffers in the release method because of the potential race
        // condition. This close method will be only called from the Task thread itself.
        if (broadcastBufferBuilder != null) {
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
        for (int i = 0; i < unicastBufferBuilders.length; ++i) {
            if (unicastBufferBuilders[i] != null) {
                unicastBufferBuilders[i].close();
                unicastBufferBuilders[i] = null;
            }
        }
        super.close();
    }

    // 很重要的逻辑
    private BufferBuilder appendUnicastDataForNewRecord(final ByteBuffer record, final int targetSubpartition) throws IOException {

        if (targetSubpartition < 0 || targetSubpartition > unicastBufferBuilders.length) {
            throw new ArrayIndexOutOfBoundsException(targetSubpartition);
        }

        // 获取到对应SP归属的BufferBuilder，最开始肯定是 null
        BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];

        // 没有就去申请
        if (buffer == null) {
            // 申请到一个Buffer，并且提供给 unicastBufferBuilders 持有
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            // 基于该Buffer创建BufferConsumer，然后放到RSP中
            addToSubpartition(buffer, targetSubpartition, 0, record.remaining());
        }

        buffer.appendAndCommit(record);

        return buffer;
    }

    // 意思是把BufferBuilder放进去 并且 标明 这个BufferBuilder中包含多少数据
    // 即为 minDesirableBufferSize
    // 但此时 BufferBuilder内的Buffer并没有被写入数据
    private void addToSubpartition(
            BufferBuilder buffer,
            int targetSubpartition,
            int partialRecordLength,
            int minDesirableBufferSize)
            throws IOException {
        // 将BufferBuilder 转为 BufferConsumer 放到对应subpartition中去
        int desirableBufferSize = subpartitions[targetSubpartition].add(buffer.createBufferConsumerFromBeginning(), partialRecordLength);
        resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
    }

    private void resizeBuffer(
            BufferBuilder buffer, int desirableBufferSize, int minDesirableBufferSize) {
        if (desirableBufferSize > 0) {
            // !! If some of partial data has written already to this buffer, the result size can
            // not be less than written value.
            buffer.trim(Math.max(minDesirableBufferSize, desirableBufferSize));
        }
    }

    private BufferBuilder appendUnicastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes, final int targetSubpartition)
            throws IOException {
        final BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes, partialRecordBytes);
        return buffer;
    }

    // 第一次 将bufferbuilder append 到所有 RSP 上
    // 剩余的次数 都是直接在这个 buffer builder上直接 appendAndCommit
    private BufferBuilder appendBroadcastDataForNewRecord(final ByteBuffer record)
            throws IOException {
        BufferBuilder buffer = broadcastBufferBuilder;

        if (buffer == null) {
            buffer = requestNewBroadcastBufferBuilder();
            createBroadcastBufferConsumers(buffer, 0, record.remaining());
        }

        buffer.appendAndCommit(record);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes) throws IOException {
        final BufferBuilder buffer = requestNewBroadcastBufferBuilder();
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);
        createBroadcastBufferConsumers(buffer, partialRecordBytes, partialRecordBytes);

        return buffer;
    }

    private void createBroadcastBufferConsumers(
            BufferBuilder buffer, int partialRecordBytes, int minDesirableBufferSize)
            throws IOException {
        try (final BufferConsumer consumer = buffer.createBufferConsumerFromBeginning()) {
            int desirableBufferSize = Integer.MAX_VALUE;
            for (ResultSubpartition subpartition : subpartitions) {
                int subPartitionBufferSize = subpartition.add(consumer.copy(), partialRecordBytes);
                desirableBufferSize =
                        subPartitionBufferSize > 0
                                ? Math.min(desirableBufferSize, subPartitionBufferSize)
                                : desirableBufferSize;
            }
            resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
        }
    }

    private BufferBuilder requestNewUnicastBufferBuilder(int targetSubpartition)
            throws IOException {
        checkInProduceState();
        ensureUnicastMode();
        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);
        unicastBufferBuilders[targetSubpartition] = bufferBuilder;

        return bufferBuilder;
    }

    private BufferBuilder requestNewBroadcastBufferBuilder() throws IOException {
        checkInProduceState();
        ensureBroadcastMode();

        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
        broadcastBufferBuilder = bufferBuilder;
        return bufferBuilder;
    }

    // 如果申不到buffer就会阻塞在这里
    private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition)
            throws IOException {
        BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
        if (bufferBuilder != null) {
            return bufferBuilder;
        }

        hardBackPressuredTimeMsPerSecond.markStart();
        try {
            bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
            hardBackPressuredTimeMsPerSecond.markEnd();
            return bufferBuilder;
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for buffer");
        }
    }

    private void finishUnicastBufferBuilder(int targetSubpartition) {
        final BufferBuilder bufferBuilder = unicastBufferBuilders[targetSubpartition];
        if (bufferBuilder != null) {
            int bytes = bufferBuilder.finish();
            numBytesProduced.inc(bytes);
            numBytesOut.inc(bytes);
            numBuffersOut.inc();
            unicastBufferBuilders[targetSubpartition] = null;
            bufferBuilder.close();
        }
    }

    private void finishUnicastBufferBuilders() {
        for (int channelIndex = 0; channelIndex < numSubpartitions; channelIndex++) {
            finishUnicastBufferBuilder(channelIndex);
        }
    }

    private void finishBroadcastBufferBuilder() {
        if (broadcastBufferBuilder != null) {
            int bytes = broadcastBufferBuilder.finish();
            numBytesProduced.inc(bytes);
            numBytesOut.inc(bytes * numSubpartitions);
            numBuffersOut.inc(numSubpartitions);
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
    }

    private void ensureUnicastMode() {
        finishBroadcastBufferBuilder();
    }

    private void ensureBroadcastMode() {
        finishUnicastBufferBuilders();
    }

    @VisibleForTesting
    public TimerGauge getHardBackPressuredTimeMsPerSecond() {
        return hardBackPressuredTimeMsPerSecond;
    }

    @VisibleForTesting
    public ResultSubpartition[] getAllPartitions() {
        return subpartitions;
    }
}

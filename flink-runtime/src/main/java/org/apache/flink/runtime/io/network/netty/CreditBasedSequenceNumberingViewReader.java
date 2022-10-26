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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;


// 基于信用机制的Subpartition Reader
/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound handler about
 * non-emptiness, similar to the {@link LocalInputChannel}.
 */

// 这种 Reader 一旦加入 PartitionRequestQueue，由 PRQ来操作 Reader
class CreditBasedSequenceNumberingViewReader
        implements BufferAvailabilityListener, NetworkSequenceViewReader {

    // requestLock
    private final Object requestLock = new Object();

    // Inputchannel 的 ID
    private final InputChannelID receiverId;

    // 反向引用 Partition请求队列
    private final PartitionRequestQueue requestQueue;

    // 初始值
    private final int initialCredit;

    // 每个Reader都持有了 一个 RSPV
    private volatile ResultSubpartitionView subpartitionView;

    /**
     * The status indicating whether this reader is already enqueued in the pipeline for
     * transferring data or not.
     *
     * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
     * thread only since there is no synchronisation.
     */
    // 判断这个reader是否已在 PartitionRequestQueue 中排队
    private boolean isRegisteredAsAvailable = false;

    /** The number of available buffers for holding data on the consumer side. */
    // 消费者这一侧可用的buffer数量
    private int numCreditsAvailable;

    // 意思就是这个reader被创建时会初始化一些信息
    CreditBasedSequenceNumberingViewReader(
            InputChannelID receiverId, int initialCredit, PartitionRequestQueue requestQueue) {
        checkArgument(initialCredit >= 0, "Must be non-negative.");
        // 将 receiverId 作为 唯一ID
        this.receiverId = receiverId;
        // 上游为下游预留的初始信用值
        // 初始信用值
        this.initialCredit = initialCredit;
        // 可以用的信用值额度
        this.numCreditsAvailable = initialCredit;
        // 持有一个Partition请求队列
        this.requestQueue = requestQueue;
    }

    // 下游过来注册的时候，创建一个reader，并向reader内 提供 RPID 和 subpartitionIndex
    @Override
    public void requestSubpartitionView(
            ResultPartitionProvider partitionProvider,
            ResultPartitionID resultPartitionId,
            int subPartitionIndex)
            throws IOException {

        synchronized (requestLock) {
            if (subpartitionView == null) {
                // This call can trigger a notification we have to
                // schedule a separate task at the event loop that will
                // start consuming this. Otherwise the reference to the
                // view cannot be available in getNextBuffer().

                // 创建了一个RSPV
                // RSPV本质上是在RSP和自己之间建立了一种联系
                this.subpartitionView = partitionProvider.createSubpartitionView(resultPartitionId, subPartitionIndex, this);
            } else {
                throw new IllegalStateException("Subpartition already requested");
            }
        }
        // 创建RSPV的时候 就notify了
        notifyDataAvailable();
    }

    // 意思是下游回来增加信用值，即为 消费者这一侧可用的buffer数量
    @Override
    public void addCredit(int creditDeltas) {
        numCreditsAvailable += creditDeltas;
    }

    // 对于Hybrid Shuffle 这个方法不会被调用
    @Override
    public void resumeConsumption() {
        if (initialCredit == 0) {
            // reset available credit if no exclusive buffer is available at the
            // consumer side for all floating buffers must have been released
            numCreditsAvailable = 0;
        }
        subpartitionView.resumeConsumption();
    }

    // PartitionRequestQueue 调用
    // 所有Record都被处理 然后仅仅调用 subpartitionView.acknowledgeAllDataProcessed()
    @Override
    public void acknowledgeAllRecordsProcessed() {
        subpartitionView.acknowledgeAllDataProcessed();
    }

    // PartitionRequestQueue 调用
    // 确认 CreditBasedSequenceNumberingViewReader 被放到 PRQ 中？ 是PRQ来调用的
    @Override
    public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
        this.isRegisteredAsAvailable = isRegisteredAvailable;
    }

    @Override
    // PartitionRequestQueue 调用
    //
    public boolean isRegisteredAsAvailable() {
        return isRegisteredAsAvailable;
    }

    /**
     * Returns true only if the next buffer is an event or the reader has both available credits and
     * buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getNextDataType(BufferAndBacklog)}, such
     *     that {@code getNextDataType(bufferAndBacklog) != NONE <=>
     *     AvailabilityWithBacklog#isAvailable()}!
     */

    // PartitionRequestQueue 调用
    //
    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog() {
        return subpartitionView.getAvailabilityAndBacklog(numCreditsAvailable);
    }

    /**
     * Returns the {@link org.apache.flink.runtime.io.network.buffer.Buffer.DataType} of the next
     * buffer in line.
     *
     * <p>Returns the next data type only if the next buffer is an event or the reader has both
     * available credits and buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getAvailabilityAndBacklog()}, such that
     *     {@code getNextDataType(bufferAndBacklog) != NONE <=>
     *     AvailabilityWithBacklog#isAvailable()}!
     * @param bufferAndBacklog current buffer and backlog including information about the next
     *     buffer
     * @return the next data type if the next buffer can be pulled immediately or {@link
     *     Buffer.DataType#NONE}
     */
    // 解析 BufferAndBacklog 的 类型
    private Buffer.DataType getNextDataType(BufferAndBacklog bufferAndBacklog) {
        final Buffer.DataType nextDataType = bufferAndBacklog.getNextDataType();
        if (numCreditsAvailable > 0 || nextDataType.isEvent()) {
            return nextDataType;
        }
        return Buffer.DataType.NONE;
    }

    // PartitionRequestQueue 调用
    //
    @Override
    public InputChannelID getReceiverId() {
        return receiverId;
    }

    // PartitionRequestQueue 调用
    // 间接调用 subpartitionView.notifyNewBufferSize(newBufferSize)
    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        subpartitionView.notifyNewBufferSize(newBufferSize);
    }

    @VisibleForTesting
    int getNumCreditsAvailable() {
        return numCreditsAvailable;
    }

    @VisibleForTesting
    ResultSubpartitionView.AvailabilityWithBacklog hasBuffersAvailable() {
        return subpartitionView.getAvailabilityAndBacklog(Integer.MAX_VALUE);
    }

    @Nullable
    @Override
    public BufferAndAvailability getNextBuffer() throws IOException {
        BufferAndBacklog next = subpartitionView.getNextBuffer();
        if (next != null) {
            if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
                throw new IllegalStateException("no credit available");
            }

            final Buffer.DataType nextDataType = getNextDataType(next);
            return new BufferAndAvailability(
                    next.buffer(), nextDataType, next.buffersInBacklog(), next.getSequenceNumber());
        } else {
            return null;
        }
    }

    @Override
    public boolean needAnnounceBacklog() {
        return initialCredit == 0 && numCreditsAvailable == 0;
    }

    @Override
    public boolean isReleased() {
        return subpartitionView.isReleased();
    }

    @Override
    public Throwable getFailureCause() {
        return subpartitionView.getFailureCause();
    }

    @Override
    public void releaseAllResources() throws IOException {
        subpartitionView.releaseAllResources();
    }

    @Override
    public void notifyDataAvailable() {
        requestQueue.notifyReaderNonEmpty(this);
    }

    @Override
    public void notifyPriorityEvent(int prioritySequenceNumber) {
        notifyDataAvailable();
    }

    @Override
    public String toString() {
        return "CreditBasedSequenceNumberingViewReader{"
                + "requestLock="
                + requestLock
                + ", receiverId="
                + receiverId
                + ", numCreditsAvailable="
                + numCreditsAvailable
                + ", isRegisteredAsAvailable="
                + isRegisteredAsAvailable
                + '}';
    }
}

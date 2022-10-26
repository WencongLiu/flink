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

import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** View over a pipelined in-memory only subpartition. */


public class PipelinedSubpartitionView implements ResultSubpartitionView {

    /** The subpartition this view belongs to. */
    // 关联到某个Subpartition
    private final PipelinedSubpartition parent;

    // 可以看到，BufferAvailabilityListener 的实现类 最终会关联给一个RSPV
    private final BufferAvailabilityListener availabilityListener;

    /** Flag indicating whether this view has been released. */
    // 一个原子释放flag
    final AtomicBoolean isReleased;

    public PipelinedSubpartitionView(
            PipelinedSubpartition parent, BufferAvailabilityListener listener) {
        this.parent = checkNotNull(parent);
        this.availabilityListener = checkNotNull(listener);
        this.isReleased = new AtomicBoolean();
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() {
        return parent.pollBuffer();
    }

    @Override
    // 会去notify
    public void notifyDataAvailable() {
        availabilityListener.notifyDataAvailable();
    }

    @Override
    // 会去notifyPriorityEvent
    public void notifyPriorityEvent(int priorityBufferNumber) {
        availabilityListener.notifyPriorityEvent(priorityBufferNumber);
    }

    @Override
    public void releaseAllResources() {
        if (isReleased.compareAndSet(false, true)) {
            // The view doesn't hold any resources and the parent cannot be restarted. Therefore,
            // it's OK to notify about consumption as well.
            parent.onConsumedSubpartition();
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased.get() || parent.isReleased();
    }

    @Override
    public void resumeConsumption() {
        parent.resumeConsumption();
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        parent.acknowledgeAllDataProcessed();
    }

    @Override
    // 感觉这个View啥也不干呀
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        return parent.getAvailabilityAndBacklog(numCreditsAvailable);
    }

    @Override
    // 获取FailureCause
    public Throwable getFailureCause() {
        Throwable cause = parent.getFailureCause();
        if (cause != null) {
            return new ProducerFailedException(cause);
        }
        return null;
    }

    @Override
    // 非锁获取队列中的buffer
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return parent.unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    // 获取RSP
    public int getNumberOfQueuedBuffers() {
        return parent.getNumberOfQueuedBuffers();
    }

    @Override
    // 触发新的BufferSize
    public void notifyNewBufferSize(int newBufferSize) {
        parent.bufferSize(newBufferSize);
    }

    @Override
    public String toString() {
        return String.format(
                "%s(index: %d) of ResultPartition %s",
                this.getClass().getSimpleName(),
                parent.getSubPartitionIndex(),
                parent.parent.getPartitionId());
    }
}

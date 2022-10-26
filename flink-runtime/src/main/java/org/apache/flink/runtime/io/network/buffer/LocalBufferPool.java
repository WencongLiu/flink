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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/**
 * A buffer pool used to manage a number of {@link Buffer} instances from the {@link
 * NetworkBufferPool}.
 *
 * <p>Buffer requests are mediated to the network buffer pool to ensure dead-lock free operation of
 * the network stack by limiting the number of buffers per local buffer pool. It also implements the
 * default mechanism for buffer recycling, which ensures that every buffer is ultimately returned to
 * the network buffer pool.
 *
 * <p>The size of this pool can be dynamically changed at runtime ({@link #setNumBuffers(int)}. It
 * will then lazily return the required number of buffers to the {@link NetworkBufferPool} to match
 * its new size.
 *
 * <p>Availability is defined as returning a segment on a subsequent {@link #requestBuffer()}/
 * {@link #requestBufferBuilder()} and heaving a non-blocking {@link
 * #requestBufferBuilderBlocking(int)}. In particular,
 *
 * <ul>
 *   <li>There is at least one {@link #availableMemorySegments}.
 *   <li>No subpartitions has reached {@link #maxBuffersPerChannel}.
 * </ul>
 *
 * <p>To ensure this contract, the implementation eagerly fetches additional memory segments from
 * {@link NetworkBufferPool} as long as it hasn't reached {@link #maxNumberOfMemorySegments} or one
 * subpartition reached the quota.
 */


class LocalBufferPool implements BufferPool {

    private static final Logger LOG = LoggerFactory.getLogger(LocalBufferPool.class);

    private static final int UNKNOWN_CHANNEL = -1;

    /** Global network buffer pool to get buffers from. */
    private final NetworkBufferPool networkBufferPool;

    // LBP最小MS数量
    /** The minimum number of required segments for this pool. */
    private final int numberOfRequiredMemorySegments;

    /**
     * The currently available memory segments. These are segments, which have been requested from
     * the network buffer pool and are currently not handed out as Buffer instances.
     *
     * <p><strong>BEWARE:</strong> Take special care with the interactions between this lock and
     * locks acquired before entering this class vs. locks being acquired during calls to external
     * code inside this class, e.g. with {@link
     * org.apache.flink.runtime.io.network.partition.consumer.BufferManager#bufferQueue} via the
     * {@link #registeredListeners} callback.
     */

    // 可用MS队列
    private final ArrayDeque<MemorySegment> availableMemorySegments =
            new ArrayDeque<MemorySegment>();

    /**
     * Buffer availability listeners, which need to be notified when a Buffer becomes available.
     * Listeners can only be registered at a time/state where no Buffer instance was available.
     */
    /**
     * 被注册的 Listener
     */
    private final ArrayDeque<BufferListener> registeredListeners = new ArrayDeque<>();

    /** Maximum number of network buffers to allocate. */
    // LBP最大MS数量
    private final int maxNumberOfMemorySegments;

    /** The current size of this pool. */
    @GuardedBy("availableMemorySegments")
    // LBP当前MS数量
    private int currentPoolSize;

    /**
     * Number of all memory segments, which have been requested from the network buffer pool and are
     * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available
     * segments).
     */
    @GuardedBy("availableMemorySegments")
    // 即 已经申请到的MS数量
    // numberOfRequestedMemorySegments
    // currentPoolSize
    // numberOfRequiredMemorySegments
    // 三个指标的大小关系？
    private int numberOfRequestedMemorySegments;

    // 每个Channel最大Buffer数量 ---> 为什么还有Channel概念
    private final int maxBuffersPerChannel;

    @GuardedBy("availableMemorySegments")
    // 每个RSP对应的Buffer数量
    private final int[] subpartitionBuffersCount;

    // 每个RSP对应的BufferRecycler
    private final BufferRecycler[] subpartitionBufferRecyclers;


    @GuardedBy("availableMemorySegments")
    // 不可用的RSP的计数
    private int unavailableSubpartitionsCount = 0;

    private int maxOverdraftBuffersPerGate;

    @GuardedBy("availableMemorySegments")
    private int numberOfRequestedOverdraftMemorySegments;

    @GuardedBy("availableMemorySegments")
    private boolean isDestroyed;

    @GuardedBy("availableMemorySegments")
    // LBP 对应一个AvailabilityHelper
    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    // 这个暂时没搞懂
    @GuardedBy("availableMemorySegments")
    private boolean requestingWhenAvailable;

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal number of
     * network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     */
    LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments)
            throws IOException {
        this(
                networkBufferPool,
                numberOfRequiredMemorySegments,
                Integer.MAX_VALUE,
                0,
                Integer.MAX_VALUE,
                0);
    }

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal and maximal
     * number of network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     * @param maxNumberOfMemorySegments maximum number of network buffers to allocate
     */
    LocalBufferPool(
            NetworkBufferPool networkBufferPool,
            int numberOfRequiredMemorySegments,
            int maxNumberOfMemorySegments)
            throws Exception {
        this(
                networkBufferPool,
                numberOfRequiredMemorySegments,
                maxNumberOfMemorySegments,
                0,
                Integer.MAX_VALUE,
                0);
    }

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> and <tt>bufferPoolOwner</tt>
     * with a minimal and maximal number of network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     * @param maxNumberOfMemorySegments maximum number of network buffers to allocate
     * @param numberOfSubpartitions number of subpartitions
     * @param maxBuffersPerChannel maximum number of buffers to use for each channel
     * @param maxOverdraftBuffersPerGate maximum number of overdraft buffers to use for each gate
     */


    // NetworkBufferPool + BufferPoolOwner + Buffers ==> LBP

    LocalBufferPool(
            NetworkBufferPool networkBufferPool,
            int numberOfRequiredMemorySegments,
            int maxNumberOfMemorySegments,
            int numberOfSubpartitions,
            int maxBuffersPerChannel,
            int maxOverdraftBuffersPerGate)
            throws IOException {
        checkArgument(
                numberOfRequiredMemorySegments > 0,
                "Required number of memory segments (%s) should be larger than 0.",
                numberOfRequiredMemorySegments);

        checkArgument(
                maxNumberOfMemorySegments >= numberOfRequiredMemorySegments,
                "Maximum number of memory segments (%s) should not be smaller than minimum (%s).",
                maxNumberOfMemorySegments,
                numberOfRequiredMemorySegments);

        LOG.debug(
                "Using a local buffer pool with {}-{} buffers",
                numberOfRequiredMemorySegments,
                maxNumberOfMemorySegments);

        this.networkBufferPool = networkBufferPool;
        this.maxNumberOfMemorySegments = maxNumberOfMemorySegments;

        // numberOfRequiredMemorySegments ==> 设置为保证的和当前的大小

        this.numberOfRequiredMemorySegments = numberOfRequiredMemorySegments;
        this.currentPoolSize = numberOfRequiredMemorySegments;



        if (numberOfSubpartitions > 0) {
            checkArgument(
                    maxBuffersPerChannel > 0,
                    "Maximum number of buffers for each channel (%s) should be larger than 0.",
                    maxBuffersPerChannel);
            checkArgument(
                    maxOverdraftBuffersPerGate >= 0,
                    "Maximum number of overdraft buffers for each gate (%s) should not be less than 0.",
                    maxOverdraftBuffersPerGate);
        }

        this.subpartitionBuffersCount = new int[numberOfSubpartitions];
        subpartitionBufferRecyclers = new BufferRecycler[numberOfSubpartitions];
        // 觉得好奇怪.. 这里拆成RSPBufferRecycler有什么意义 ？
        for (int i = 0; i < subpartitionBufferRecyclers.length; i++) {
            subpartitionBufferRecyclers[i] = new SubpartitionBufferRecycler(i, this);
        }

        this.maxBuffersPerChannel = maxBuffersPerChannel;
        this.maxOverdraftBuffersPerGate = maxOverdraftBuffersPerGate;

        // Lock is only taken, because #checkAvailability asserts it. It's a small penalty for
        // thread safety.
        synchronized (this.availableMemorySegments) {
            if (checkAvailability()) {
                availabilityHelper.resetAvailable();
            }

            checkConsistentAvailability();
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    // SingleInputGate调用
    // 用来留存内部的MS
    // 在这个方法中 numberOfRequestedMemorySegments 并没有发生变化..
    @Override
    public void reserveSegments(int numberOfSegmentsToReserve) throws IOException {
        checkArgument(
                numberOfSegmentsToReserve <= numberOfRequiredMemorySegments,
                "Can not reserve more segments than number of required segments.");

        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            checkDestroyed();

            if (numberOfRequestedMemorySegments < numberOfSegmentsToReserve) {
                availableMemorySegments.addAll(
                        networkBufferPool.requestPooledMemorySegmentsBlocking(numberOfSegmentsToReserve - numberOfRequestedMemorySegments));
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            }

        }
        mayNotifyAvailable(toNotify);
    }

    /**
     * isDestroyed
     */
    @Override
    public boolean isDestroyed() {
        synchronized (availableMemorySegments) {
            return isDestroyed;
        }
    }

    @Override
    public int getNumberOfRequiredMemorySegments() {
        return numberOfRequiredMemorySegments;
    }

    @Override
    public int getMaxNumberOfMemorySegments() {
        return maxNumberOfMemorySegments;
    }

    /**
     * @return the same value as {@link #getMaxNumberOfMemorySegments()} for bounded pools. For
     *     unbounded pools it returns an approximation based upon {@link
     *     #getNumberOfRequiredMemorySegments()}
     */
    // 感觉这个无法理解..
    public int getNumberOfRequestedMemorySegments() {
        if (maxNumberOfMemorySegments < NetworkBufferPool.UNBOUNDED_POOL_SIZE) {
            return maxNumberOfMemorySegments;
        } else {
            return getNumberOfRequiredMemorySegments() * 2;
        }
    }

    @VisibleForTesting
    public int getNumberOfRequestedOverdraftMemorySegments() {
        return numberOfRequestedOverdraftMemorySegments;
    }

    @Override
    public int getNumberOfAvailableMemorySegments() {
        synchronized (availableMemorySegments) {
            return availableMemorySegments.size();
        }
    }

    @Override
    public int getNumBuffers() {
        synchronized (availableMemorySegments) {
            return currentPoolSize;
        }
    }

    @Override
    public int bestEffortGetNumOfUsedBuffers() {
        return Math.max(
                0,
                numberOfRequestedMemorySegments
                        + numberOfRequestedOverdraftMemorySegments
                        - availableMemorySegments.size());
    }

    @Override
    public Buffer requestBuffer() {
        return toBuffer(requestMemorySegment());
    }

    @Override
    public BufferBuilder requestBufferBuilder() {
        return toBufferBuilder(requestMemorySegment(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
    }

    @Override
    public BufferBuilder requestBufferBuilder(int targetChannel) {
        return toBufferBuilder(requestMemorySegment(targetChannel), targetChannel);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
        return toBufferBuilder(requestMemorySegmentBlocking(), UNKNOWN_CHANNEL);
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking() throws InterruptedException {
        return requestMemorySegmentBlocking(UNKNOWN_CHANNEL);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking(int targetChannel)
            throws InterruptedException {
        return toBufferBuilder(requestMemorySegmentBlocking(targetChannel), targetChannel);
    }

    private Buffer toBuffer(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }
        return new NetworkBuffer(memorySegment, this);
    }

    private BufferBuilder toBufferBuilder(MemorySegment memorySegment, int targetChannel) {
        if (memorySegment == null) {
            return null;
        }

        if (targetChannel == UNKNOWN_CHANNEL) {
            return new BufferBuilder(memorySegment, this);
        } else {
            return new BufferBuilder(memorySegment, subpartitionBufferRecyclers[targetChannel]);
        }
    }

    private MemorySegment requestMemorySegmentBlocking(int targetChannel)
            throws InterruptedException {
        MemorySegment segment;
        while ((segment = requestMemorySegment(targetChannel)) == null) {
            try {
                // wait until available
                getAvailableFuture().get();
            } catch (ExecutionException e) {
                LOG.error("The available future is completed exceptionally.", e);
                ExceptionUtils.rethrow(e);
            }
        }
        return segment;
    }

    @Nullable
    private MemorySegment requestMemorySegment(int targetChannel) {
        MemorySegment segment;
        synchronized (availableMemorySegments) {
            checkDestroyed();

            if (availableMemorySegments.isEmpty()) {
                segment = requestOverdraftMemorySegmentFromGlobal();
            } else {
                segment = availableMemorySegments.poll();
            }

            if (segment == null) {
                return null;
            }

            if (targetChannel != UNKNOWN_CHANNEL) {
                if (++subpartitionBuffersCount[targetChannel] == maxBuffersPerChannel) {
                    unavailableSubpartitionsCount++;
                }
            }

            if (!checkAvailability()) {
                availabilityHelper.resetUnavailable();
            }

            checkConsistentAvailability();
        }
        return segment;
    }

    private void checkDestroyed() {
        if (isDestroyed) {
            throw new CancelTaskException("Buffer pool has already been destroyed.");
        }
    }

    @Override
    public MemorySegment requestMemorySegment() {
        return requestMemorySegment(UNKNOWN_CHANNEL);
    }

    private boolean requestMemorySegmentFromGlobal() {
        assert Thread.holdsLock(availableMemorySegments);

        if (isRequestedSizeReached()) {
            return false;
        }

        checkState(
                !isDestroyed,
                "Destroyed buffer pools should never acquire segments - this will lead to buffer leaks.");

        MemorySegment segment = networkBufferPool.requestPooledMemorySegment();
        if (segment != null) {
            availableMemorySegments.add(segment);
            numberOfRequestedMemorySegments++;
            return true;
        }
        return false;
    }

    private MemorySegment requestOverdraftMemorySegmentFromGlobal() {
        assert Thread.holdsLock(availableMemorySegments);

        if (numberOfRequestedOverdraftMemorySegments >= maxOverdraftBuffersPerGate) {
            return null;
        }

        checkState(
                !isDestroyed,
                "Destroyed buffer pools should never acquire segments - this will lead to buffer leaks.");

        MemorySegment segment = networkBufferPool.requestPooledMemorySegment();
        if (segment != null) {
            numberOfRequestedOverdraftMemorySegments++;
        }
        return segment;
    }

    /**
     * Tries to obtain a buffer from global pool as soon as one pool is available. Note that
     * multiple {@link LocalBufferPool}s might wait on the future of the global pool, hence this
     * method double-check if a new buffer is really needed at the time it becomes available.
     */
    private void requestMemorySegmentFromGlobalWhenAvailable() {
        assert Thread.holdsLock(availableMemorySegments);

        if (requestingWhenAvailable) {
            return;
        }
        requestingWhenAvailable = true;

        assertNoException(
                networkBufferPool.getAvailableFuture().thenRun(this::onGlobalPoolAvailable));
    }

    private void onGlobalPoolAvailable() {
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            requestingWhenAvailable = false;
            if (isDestroyed || availabilityHelper.isApproximatelyAvailable()) {
                // there is currently no benefit to obtain buffer from global; give other pools
                // precedent
                return;
            }

            // Check availability and potentially request the memory segment. The call may also
            // result in invoking
            // #requestMemorySegmentFromGlobalWhenAvailable again if no segment could be fetched
            // because of
            // concurrent requests from different LocalBufferPools.
            if (checkAvailability()) {
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            }
        }
        mayNotifyAvailable(toNotify);
    }

    private boolean shouldBeAvailable() {
        assert Thread.holdsLock(availableMemorySegments);

        return !availableMemorySegments.isEmpty()
                && unavailableSubpartitionsCount == 0
                && numberOfRequestedOverdraftMemorySegments == 0;
    }

    // 重要!
    // 检查可用性的标准是什么？
    // 1.1 availableMemorySegments为空
    // --------------> 2.1 已申请的MS数量大于等于当前大小 --> 说明不能再申请了，那么真的就是用完了
    // --------------> 2.2 已申请的MS数量小于当前大小
    //
    // 1.2 availableMemorySegments不为空
    // --------------> 根据 再次判断是否为空以及其他属性 来返回 True/False
    private boolean checkAvailability() {
        assert Thread.holdsLock(availableMemorySegments);
        if (!availableMemorySegments.isEmpty()) {
            return shouldBeAvailable();
        }
        // 2. availableMemorySegments为空并且已经已申请的MS数量大于等于当前大小
        if (isRequestedSizeReached()) {
            return false;
        }

        // There aren't availableMemorySegments and we continue to request new memory segment.
        if (!requestMemorySegmentFromGlobal()) {
            requestMemorySegmentFromGlobalWhenAvailable();
        }
        return shouldBeAvailable();
    }

    private void checkConsistentAvailability() {
        assert Thread.holdsLock(availableMemorySegments);

        final boolean shouldBeAvailable = shouldBeAvailable();
        checkState(
                availabilityHelper.isApproximatelyAvailable() == shouldBeAvailable,
                "Inconsistent availability: expected " + shouldBeAvailable);
    }

    @Override
    public void recycle(MemorySegment segment) {
        recycle(segment, UNKNOWN_CHANNEL);
    }

    private void recycle(MemorySegment segment, int channel) {
        BufferListener listener;
        CompletableFuture<?> toNotify = null;
        do {
            synchronized (availableMemorySegments) {
                if (channel != UNKNOWN_CHANNEL) {
                    if (subpartitionBuffersCount[channel]-- == maxBuffersPerChannel) {
                        unavailableSubpartitionsCount--;
                    }
                }

                if (isDestroyed || hasExcessBuffers()) {
                    returnMemorySegment(segment);
                    return;
                } else {
                    listener = registeredListeners.poll();
                    if (listener == null) {
                        availableMemorySegments.add(segment);
                        if (!availabilityHelper.isApproximatelyAvailable() && shouldBeAvailable()) {
                            toNotify = availabilityHelper.getUnavailableToResetAvailable();
                        }
                        break;
                    }
                }

                checkConsistentAvailability();
            }
        } while (!fireBufferAvailableNotification(listener, segment));

        mayNotifyAvailable(toNotify);
    }

    private boolean fireBufferAvailableNotification(
            BufferListener listener, MemorySegment segment) {
        // We do not know which locks have been acquired before the recycle() or are needed in the
        // notification and which other threads also access them.
        // -> call notifyBufferAvailable() outside of the synchronized block to avoid a deadlock
        // (FLINK-9676)
        return listener.notifyBufferAvailable(new NetworkBuffer(segment, this));
    }

    /** Destroy is called after the produce or consume phase of a task finishes. */
    @Override
    public void lazyDestroy() {
        // NOTE: if you change this logic, be sure to update recycle() as well!
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            if (!isDestroyed) {
                MemorySegment segment;
                while ((segment = availableMemorySegments.poll()) != null) {
                    returnMemorySegment(segment);
                }

                BufferListener listener;
                while ((listener = registeredListeners.poll()) != null) {
                    listener.notifyBufferDestroyed();
                }

                if (!isAvailable()) {
                    toNotify = availabilityHelper.getAvailableFuture();
                }

                isDestroyed = true;
            }
        }

        mayNotifyAvailable(toNotify);

        networkBufferPool.destroyBufferPool(this);
    }

    @Override
    public boolean addBufferListener(BufferListener listener) {
        synchronized (availableMemorySegments) {
            if (!availableMemorySegments.isEmpty() || isDestroyed) {
                return false;
            }

            registeredListeners.add(listener);
            return true;
        }
    }

    @Override
    public void setNumBuffers(int numBuffers) {
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            checkArgument(
                    numBuffers >= numberOfRequiredMemorySegments,
                    "Buffer pool needs at least %s buffers, but tried to set to %s",
                    numberOfRequiredMemorySegments,
                    numBuffers);

            currentPoolSize = Math.min(numBuffers, maxNumberOfMemorySegments);

            returnExcessMemorySegments();

            if (isDestroyed) {
                // FLINK-19964: when two local buffer pools are released concurrently, one of them
                // gets buffers assigned
                // make sure that checkAvailability is not called as it would pro-actively acquire
                // one buffer from NetworkBufferPool
                return;
            }

            if (checkAvailability()) {
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            } else {
                availabilityHelper.resetUnavailable();
            }

            checkConsistentAvailability();
        }

        mayNotifyAvailable(toNotify);
    }

    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        this.maxOverdraftBuffersPerGate = maxOverdraftBuffersPerGate;
    }

    public int getMaxOverdraftBuffersPerGate() {
        return maxOverdraftBuffersPerGate;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    @Override
    public String toString() {
        synchronized (availableMemorySegments) {
            return String.format(
                    "[size: %d, required: %d, requested: %d, available: %d, max: %d, listeners: %d,"
                            + "subpartitions: %d, maxBuffersPerChannel: %d, destroyed: %s]",
                    currentPoolSize,
                    numberOfRequiredMemorySegments,
                    numberOfRequestedMemorySegments,
                    availableMemorySegments.size(),
                    maxNumberOfMemorySegments,
                    registeredListeners.size(),
                    subpartitionBuffersCount.length,
                    maxBuffersPerChannel,
                    isDestroyed);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Notifies the potential segment consumer of the new available segments by completing the
     * previous uncompleted future.
     */
    private void mayNotifyAvailable(@Nullable CompletableFuture<?> toNotify) {
        if (toNotify != null) {
            toNotify.complete(null);
        }
    }

    private void returnMemorySegment(MemorySegment segment) {
        assert Thread.holdsLock(availableMemorySegments);

        // When using the overdraft buffer, return the overdraft buffer first.
        if (numberOfRequestedOverdraftMemorySegments > 0) {
            numberOfRequestedOverdraftMemorySegments--;
        } else {
            numberOfRequestedMemorySegments--;
        }
        networkBufferPool.recyclePooledMemorySegment(segment);
    }

    private void returnExcessMemorySegments() {
        assert Thread.holdsLock(availableMemorySegments);

        while (hasExcessBuffers()) {
            MemorySegment segment = availableMemorySegments.poll();
            if (segment == null) {
                return;
            }

            returnMemorySegment(segment);
        }
    }

    private boolean hasExcessBuffers() {
        return numberOfRequestedOverdraftMemorySegments > 0
                || numberOfRequestedMemorySegments > currentPoolSize;
    }

    private boolean isRequestedSizeReached() {
        return numberOfRequestedMemorySegments >= currentPoolSize;
    }

    private static class SubpartitionBufferRecycler implements BufferRecycler {

        private int channel;
        private LocalBufferPool bufferPool;

        SubpartitionBufferRecycler(int channel, LocalBufferPool bufferPool) {
            this.channel = channel;
            this.bufferPool = bufferPool;
        }

        @Override
        public void recycle(MemorySegment memorySegment) {
            bufferPool.recycle(memorySegment, channel);
        }
    }
}

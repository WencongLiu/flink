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

import java.io.IOException;

/** A dynamically sized buffer pool. */
// BufferPool本身就是一个回收器, 同时还是一个BufferProvider
// 它本身将作为一个回收器 它的引用将传递给其中的 Buffer
public interface BufferPool extends BufferProvider, BufferRecycler {

    /**
     * Reserves the target number of segments to this pool. Will throw an exception if it can not
     * allocate enough segments.
     */
    // 在BufferPool内保存特定数量的MemorySegment
    // -- InputGate
    void reserveSegments(int numberOfSegmentsToReserve) throws IOException;

    /**
     * Destroys this buffer pool.
     *
     * <p>If not all buffers are available, they are recycled lazily as soon as they are recycled.
     */
    // 摧毁buffer Pool
    // --NBP
    // --RP
    // --IG
    void lazyDestroy();

    /** Checks whether this buffer pool has been destroyed. */
    @Override
    // 检查buffer Pool 是否已经被摧毁
    // --BM
    boolean isDestroyed();

    /** Returns the number of guaranteed (minimum number of) memory segments of this buffer pool. */
    // 最小MS数量，BP内MS数量不会小于这个数字
    int getNumberOfRequiredMemorySegments();

    /**
     * Returns the maximum number of memory segments this buffer pool should use.
     *
     * @return maximum number of memory segments to use or <tt>-1</tt> if unlimited
     */
    // 最大MS数量，BP内MS数量不会超过这个数字
    int getMaxNumberOfMemorySegments();

    /**
     * Returns the current size of this buffer pool.
     *
     * <p>The size of the buffer pool can change dynamically at runtime.
     */
    // 获取实际MS数量
    int getNumBuffers();

    /**
     * Sets the current size of this buffer pool.
     *
     * <p>The size needs to be greater or equal to the guaranteed number of memory segments.
     */
    // 设置实际MS数量
    void setNumBuffers(int numBuffers);


    // MS数量和可用Buffer数量之间，没有必然联系

    ////////////////////////////////////////////////////////////////////////////////////////////////

    /** Returns the number memory segments, which are currently held by this buffer pool. */
    // 获取BP内实际的Buffer数量
    // 木有人用
    int getNumberOfAvailableMemorySegments();

    /** Returns the number of used buffers of this buffer pool. */
    // 获取被使用的Buffer数量
    // 给监控使用的
    int bestEffortGetNumOfUsedBuffers();


    /** Sets the max overdraft buffer size of per gate. */
    void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate);

    /** Returns the max overdraft buffer size of per gate. */
    int getMaxOverdraftBuffersPerGate();
}

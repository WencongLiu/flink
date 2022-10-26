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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.PARTIAL_RECORD;

/** @param <T> The type of the record to be deserialized. */
public class SpillingAdaptiveSpanningRecordDeserializer<T extends IOReadableWritable>
        implements RecordDeserializer<T> {
    // 5MB 溢写数据
    public static final int DEFAULT_THRESHOLD_FOR_SPILLING = 5 * 1024 * 1024; // 5 MiBytes

    // 文件Buffer大小 2MB
    public static final int DEFAULT_FILE_BUFFER_SIZE = 2 * 1024 * 1024; // 2 MiBytes

    // 溢写数据阈值 100KB
    private static final int MIN_THRESHOLD_FOR_SPILLING = 100 * 1024; // 100 KiBytes

    // 文件buffer大小最小是 50KB
    private static final int MIN_FILE_BUFFER_SIZE = 50 * 1024; // 50 KiBytes

    // BYTES的LENGTH
    static final int LENGTH_BYTES = Integer.BYTES;

    // 非跨越包装器
    private final NonSpanningWrapper nonSpanningWrapper;

    // 跨越包装器
    private final SpanningWrapper spanningWrapper;

    // Buffer
    @Nullable private Buffer currentBuffer;

    public SpillingAdaptiveSpanningRecordDeserializer(String[] tmpDirectories) {
        this(tmpDirectories, DEFAULT_THRESHOLD_FOR_SPILLING, DEFAULT_FILE_BUFFER_SIZE);
    }
    // 在new的时候 就创建出了 SpillingAdaptiveSpanningRecordDeserializer
    public SpillingAdaptiveSpanningRecordDeserializer(
            String[] tmpDirectories, int thresholdForSpilling, int fileBufferSize) {
        nonSpanningWrapper = new NonSpanningWrapper();
        spanningWrapper =
                new SpanningWrapper(
                        tmpDirectories,
                        Math.max(thresholdForSpilling, MIN_THRESHOLD_FOR_SPILLING),
                        Math.max(fileBufferSize, MIN_FILE_BUFFER_SIZE));
    }

    // 它持续需要增加一个Buffer
    @Override
    public void setNextBuffer(Buffer buffer) throws IOException {
        // 将Buffer 设置为 currentBuffer
        currentBuffer = buffer;
        // 为什么要获取 offset呢
        int offset = buffer.getMemorySegmentOffset();
        MemorySegment segment = buffer.getMemorySegment();
        // byte 数量
        int numBytes = buffer.getSize();
        // 如果正在使用 spanning 那么就给 spanning 增加一个 MemorySegment
        if (spanningWrapper.getNumGatheredBytes() > 0) {
            spanningWrapper.addNextChunkFromMemorySegment(segment, offset, numBytes);
        }
        // 如果没有使用 spanning 那么就根据这个MS初始化一个 nonSpanning
        else {
            nonSpanningWrapper.initializeFromMemorySegment(segment, offset, numBytes + offset);
        }
    }

    // 不知道为什么要求获取 未消费的 Buffer 数据
    // 从逻辑上看，nonSpanning 和 spanning 只能各选一个使用
    @Override
    public CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException {
        return nonSpanningWrapper.hasRemaining()
                ? nonSpanningWrapper.getUnconsumedSegment()
                : spanningWrapper.getUnconsumedSegment();
    }

    // 传过来一个Target 返回序列化结果的状态
    @Override
    public DeserializationResult getNextRecord(T target) throws IOException {
        // always check the non-spanning wrapper first.
        // this should be the majority of the cases for small records
        // for large records, this portion of the work is very small in comparison anyways

        final DeserializationResult result = readNextRecord(target);
        if (result.isBufferConsumed()) {
            currentBuffer.recycleBuffer();
            currentBuffer = null;
        }
        return result;
    }

    // 持续的去把bytes数据反序列化给Target
    private DeserializationResult readNextRecord(T target) throws IOException {

        // 如果nonSpanning有数据 那就去读 nonSpanning 的
        if (nonSpanningWrapper.hasCompleteLength()) {
            return readNonSpanningRecord(target);

        } else if (nonSpanningWrapper.hasRemaining()) {
            nonSpanningWrapper.transferTo(spanningWrapper.lengthBuffer);
            return PARTIAL_RECORD;

        } else if (spanningWrapper.hasFullRecord()) {
            target.read(spanningWrapper.getInputView());
            spanningWrapper.transferLeftOverTo(nonSpanningWrapper);
            return nonSpanningWrapper.hasRemaining()
                    ? INTERMEDIATE_RECORD_FROM_BUFFER
                    : LAST_RECORD_FROM_BUFFER;

        } else {
            return PARTIAL_RECORD;
        }
    }

    private DeserializationResult readNonSpanningRecord(T target) throws IOException {
        // 读取record的长度
        int recordLen = nonSpanningWrapper.readInt();
        // 如果 nonSpanningWrapper 中剩余的字节长度 足够消费
        if (nonSpanningWrapper.canReadRecord(recordLen)) {
            // 那么就基于 nonSpanningWrapper 创建 T类型 的 target
            return nonSpanningWrapper.readInto(target);
        } else {
            spanningWrapper.transferFrom(nonSpanningWrapper, recordLen);
            return PARTIAL_RECORD;
        }
    }

    @Override
    public void clear() {
        if (currentBuffer != null && !currentBuffer.isRecycled()) {
            currentBuffer.recycleBuffer();
            currentBuffer = null;
        }
        nonSpanningWrapper.clear();
        spanningWrapper.clear();
    }
}

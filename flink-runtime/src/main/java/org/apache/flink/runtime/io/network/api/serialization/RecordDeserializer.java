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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;

/** Interface for turning sequences of memory segments into records. */
// 先看下反序列化器 为什么要定义这些行为
// 我理解反序列化 就是 byte数据 转为 类型化数据 的过程
public interface RecordDeserializer<T extends IOReadableWritable> {

    /** Status of the deserialization result. */
    enum DeserializationResult {
        // 没有被反序列化完毕
        PARTIAL_RECORD(false, true),

        // 一个Buffer里有多个结果是吧，但只消费了少量的结果
        INTERMEDIATE_RECORD_FROM_BUFFER(true, false),

        // 一个Buffer里的所有结果都被消费的状态
        LAST_RECORD_FROM_BUFFER(true, true);

        private final boolean isFullRecord;

        private final boolean isBufferConsumed;

        private DeserializationResult(boolean isFullRecord, boolean isBufferConsumed) {
            this.isFullRecord = isFullRecord;
            this.isBufferConsumed = isBufferConsumed;
        }

        public boolean isFullRecord() {
            return this.isFullRecord;
        }

        public boolean isBufferConsumed() {
            return this.isBufferConsumed;
        }
    }

    // 这个行为就让我很误解
    DeserializationResult getNextRecord(T target) throws IOException;

    // 这个又是什么意思 ？为什么要 set next buffer ?
    void setNextBuffer(Buffer buffer) throws IOException;

    // 直接清除掉吧
    void clear();

    /**
     * 获取没有被消费的buffer
     * Gets the unconsumed buffer which needs to be persisted in unaligned checkpoint scenario.
     *
     * <p>Note that the unconsumed buffer might be null if the whole buffer was already consumed
     * before and there are no partial length or data remained in the end of buffer.
     */
    // 非要用这个 CloseableIterator 就感觉很奇怪..
    CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException;
}

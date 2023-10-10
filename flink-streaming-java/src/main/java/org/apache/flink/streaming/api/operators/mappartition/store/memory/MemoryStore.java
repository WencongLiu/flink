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

package org.apache.flink.streaming.api.operators.mappartition.store.memory;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.streaming.api.operators.mappartition.MemorySegmentPool;
import org.apache.flink.streaming.api.operators.mappartition.store.Store;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** @param <T> */
public class MemoryStore<T> implements Store<T> {

    private final TypeSerializer<T> recordSerializer;

    private final ManagedMemoryOutputStream outputStream;

    private final DataOutputView outputView;

    private final MemorySegmentPool memorySegmentPool;

    private int recordCount = 0;

    public MemoryStore(TypeSerializer<T> recordSerializer, MemorySegmentPool memorySegmentPool) {
        this.recordSerializer = recordSerializer;
        this.outputStream = new ManagedMemoryOutputStream(memorySegmentPool);
        this.outputView = new DataOutputViewStreamWrapper(outputStream);
        this.memorySegmentPool = memorySegmentPool;
    }

    public boolean addRecord(T record) throws IOException {
        try {
            recordSerializer.serialize(record, outputView);
            recordCount++;
            return true;
        } catch (RuntimeException e) {
            if (e.getCause() instanceof MemoryAllocationException) {
                return false;
            }
            throw e;
        }
    }

    public Iterator<T> getRecordIterator() {
        return new MemoryStoreIterator<T>(
                recordCount, outputStream.getMemorySegments(), recordSerializer, memorySegmentPool);
    }

    /**
     * The iterator of memory store.
     *
     * @param <T>
     */
    private static class MemoryStoreIterator<T> implements Iterator<T> {
        private final TypeSerializer<T> serializer;

        private final DataInputView inputView;

        private final int totalCount;

        private int count;

        MemoryStoreIterator(
                int totalCount,
                List<MemorySegment> segments,
                TypeSerializer<T> serializer,
                MemorySegmentPool memorySegmentPool) {
            this.totalCount = totalCount;
            this.count = 0;
            this.inputView =
                    new DataInputViewStreamWrapper(
                            new ManagedMemoryInputStream(segments, memorySegmentPool));
            this.serializer = serializer;
        }

        @Override
        public boolean hasNext() {
            return count < totalCount;
        }

        @Override
        public T next() {
            T value;
            try {
                value = serializer.deserialize(inputView);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            count++;
            return value;
        }
    }
}

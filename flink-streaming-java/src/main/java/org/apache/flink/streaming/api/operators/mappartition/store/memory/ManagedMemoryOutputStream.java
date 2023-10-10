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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.streaming.api.operators.mappartition.MemorySegmentPool;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/** The outputstream of memory. */
public class ManagedMemoryOutputStream extends OutputStream {

    /** The pool to allocate memory segments from. */
    private final MemorySegmentPool segmentPool;

    /** The number of bytes in a memory segment. */
    private final int pageSize;

    /** The memory segments containing written bytes. */
    private final List<MemorySegment> segments = new ArrayList<>();

    /** The index of the segment that currently accepts written bytes. */
    private int segmentIndex;

    /** The number of bytes in the current segment that have been written. */
    private int segmentOffset;

    /** The number of bytes that have been written so far. */
    private long globalOffset;

    /** The number of bytes that have been allocated so far. */
    private long allocatedBytes;

    public ManagedMemoryOutputStream(MemorySegmentPool segmentPool) {
        this.segmentPool = segmentPool;
        this.pageSize = segmentPool.pageSize();
    }

    public long getPos() {
        return globalOffset;
    }

    public List<MemorySegment> getMemorySegments() {
        return segments;
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(@Nullable byte[] b, int off, int len) throws IOException {
        try {
            ensureCapacity(globalOffset + len);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }

        while (len > 0) {
            int currentLen = Math.min(len, pageSize - segmentOffset);
            segments.get(segmentIndex).put(segmentOffset, b, off, currentLen);
            segmentOffset += currentLen;
            globalOffset += currentLen;
            if (segmentOffset >= pageSize) {
                segmentIndex++;
                segmentOffset = 0;
            }
            off += currentLen;
            len -= currentLen;
        }
    }

    private void ensureCapacity(long capacity) throws MemoryAllocationException {
        if (allocatedBytes >= capacity) {
            return;
        }

        int required =
                (int) (capacity % pageSize == 0 ? capacity / pageSize : capacity / pageSize + 1)
                        - segments.size();

        List<MemorySegment> allocatedSegments = new ArrayList<>();
        for (int i = 0; i < required; i++) {
            MemorySegment memorySegment = segmentPool.nextSegment();
            if (memorySegment == null) {
                segmentPool.returnAll(allocatedSegments);
                throw new MemoryAllocationException();
            }
            allocatedSegments.add(memorySegment);
        }

        segments.addAll(allocatedSegments);
        allocatedBytes += (long) allocatedSegments.size() * pageSize;
    }
}

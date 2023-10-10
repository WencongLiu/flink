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
import org.apache.flink.streaming.api.operators.mappartition.MemorySegmentPool;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

/** The input stream of managed memory. */
public class ManagedMemoryInputStream extends InputStream {

    /** The memory segments to read bytes from. */
    private final List<MemorySegment> segments;

    /** The index of the segment that is currently being read. */
    private int segmentIndex;

    /** The number of bytes that have been read from the current segment so far. */
    private int segmentOffset;

    private MemorySegmentPool memorySegmentPool;

    public ManagedMemoryInputStream(
            List<MemorySegment> segments, MemorySegmentPool memorySegmentPool) {
        this.segments = segments;
        this.segmentIndex = 0;
        this.segmentOffset = 0;
        this.memorySegmentPool = memorySegmentPool;
    }

    @Override
    public int read() {
        int ret = segments.get(segmentIndex).get(segmentOffset) & 0xff;
        segmentOffset += 1;
        if (segmentOffset >= segments.get(segmentIndex).size()) {
            memorySegmentPool.returnAll(Collections.singletonList(segments.get(segmentIndex)));
            segmentIndex++;
            segmentOffset = 0;
        }
        return ret;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        int readLen = 0;

        while (len > 0 && segmentIndex < segments.size()) {
            int currentLen = Math.min(segments.get(segmentIndex).size() - segmentOffset, len);
            segments.get(segmentIndex).get(segmentOffset, b, off, currentLen);
            segmentOffset += currentLen;
            if (segmentOffset >= segments.get(segmentIndex).size()) {
                memorySegmentPool.returnAll(Collections.singletonList(segments.get(segmentIndex)));
                segmentIndex++;
                segmentOffset = 0;
            }

            readLen += currentLen;
            off += currentLen;
            len -= currentLen;
        }

        return readLen;
    }
}

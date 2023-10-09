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

package org.apache.flink.streaming.api.operators.mappartition;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Records the data received and replayed them when required. */
public class RecordCacheWriter<T> {

    /** A soft limit on the max allowed size of a single segment. */
    static final long MAX_SEGMENT_SIZE = 1L << 30; // 1GB

    /** The tool to serialize received records into bytes. */
    private final TypeSerializer<T> serializer;

    /** The file system that contains the cache files. */
    private final FileSystem fileSystem;

    /** A generator to generate paths of cache files. */
    private final SupplierWithException<Path, IOException> pathGenerator;

    /** An optional pool that provide memory segments to hold cached records in memory. */
    @Nullable private final MemorySegmentPool segmentPool;

    /** The segments that contain previously added records. */
    private final List<Segment> finishedSegments;

    /** The current writer for new records. */
    @Nullable private SegmentWriter<T> currentSegmentWriter;

    public RecordCacheWriter(
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator,
            @Nullable MemorySegmentPool segmentPool)
            throws IOException {
        this.fileSystem = fileSystem;
        this.pathGenerator = pathGenerator;
        this.segmentPool = segmentPool;
        this.serializer = serializer;
        this.finishedSegments = new ArrayList<>();
        this.currentSegmentWriter = createSegmentWriter();
    }

    public void addRecord(T record) throws IOException {
        if (!currentSegmentWriter.addRecord(record)) {
            currentSegmentWriter.finish().ifPresent(finishedSegments::add);
            currentSegmentWriter = new FileSegmentWriter<>(serializer, pathGenerator.get());
            Preconditions.checkState(currentSegmentWriter.addRecord(record));
        }
    }

    /**
     * Flushes all added records to segments and returns a list of segments containing all cached
     * records.
     */
    public List<Segment> getSegments() throws IOException {
        finishCurrentSegmentIfExists();
        return finishedSegments;
    }

    private void finishCurrentSegmentIfExists() throws IOException {
        if (currentSegmentWriter == null) {
            return;
        }

        currentSegmentWriter.finish().ifPresent(finishedSegments::add);
        currentSegmentWriter = createSegmentWriter();
    }

    /** Removes all previously added records. */
    public void clear() throws IOException {
        finishCurrentSegmentIfExists();
        for (Segment segment : finishedSegments) {
            if (segment.getFsSize() > 0) {
                fileSystem.delete(segment.getPath(), false);
            }
            if (!segment.getCache().isEmpty()) {
                segmentPool.returnAll(segment.getCache());
            }
        }
        finishedSegments.clear();
    }

    private SegmentWriter<T> createSegmentWriter() throws IOException {
        if (segmentPool != null) {
            try {
                return new MemorySegmentWriter<>(serializer, pathGenerator.get(), segmentPool, 0L);
            } catch (MemoryAllocationException ignored) {
                // ignore MemoryAllocationException and create FileSegmentWriter instead.
            }
        }
        return new FileSegmentWriter<>(serializer, pathGenerator.get());
    }
}

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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** Reads the cached data from a list of segments. */
public class RecordIterator<T> implements Iterator<T> {

    /** The tool to deserialize bytes into records. */
    private final TypeSerializer<T> serializer;

    /** The segments where to read the records from. */
    private final List<Segment> segments;

    /** The current reader for next records. */
    @Nullable private SegmentReader<T> currentSegmentReader;

    public RecordIterator(TypeSerializer<T> serializer, List<Segment> segments) {
        this.serializer = serializer;
        this.segments = segments;
        try {
            this.currentSegmentReader = new MemorySegmentReader<>(serializer, segments.get(0), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return currentSegmentReader != null && currentSegmentReader.hasNext();
    }

    @Override
    public T next() {
        try {
            T record = currentSegmentReader.next();
            if (!currentSegmentReader.hasNext()) {
                currentSegmentReader.close();
                if (segments.size() > 1) {
                    currentSegmentReader = new FileSegmentReader<>(serializer, segments.get(1), 0);
                } else {
                    currentSegmentReader = null;
                }
            }
            return record;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

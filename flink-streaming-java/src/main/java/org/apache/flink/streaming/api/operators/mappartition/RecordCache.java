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
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** */
public class RecordCache<T> {

    /** The tool to serialize/deserialize records. */
    private final TypeSerializer<T> recordSerializer;

    /** The data cache writer for the received records. */
    private final RecordCacheWriter<T> recordCacheWriter;

    public RecordCache(
            TypeSerializer<T> recordSerializer,
            StreamTask<?, ?> containingTask,
            StreamingRuntimeContext runtimeContext,
            OperatorID operatorID) {
        this.recordSerializer = recordSerializer;

        MemorySegmentPool segmentPool = null;
        double fraction =
                containingTask
                        .getConfiguration()
                        .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.OPERATOR,
                                runtimeContext.getTaskManagerRuntimeInfo().getConfiguration(),
                                runtimeContext.getUserCodeClassLoader());
        if (fraction > 0) {
            MemoryManager memoryManager = containingTask.getEnvironment().getMemoryManager();
            segmentPool =
                    new LazyMemorySegmentPool(
                            containingTask,
                            memoryManager,
                            memoryManager.computeNumberOfPages(fraction));
        }

        Path basePath =
                getDataCachePath(
                        containingTask
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        try {
            this.recordCacheWriter =
                    new RecordCacheWriter<>(
                            recordSerializer,
                            basePath.getFileSystem(),
                            createDataCacheFileGenerator(basePath, operatorID),
                            segmentPool);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterable<T> getRecordIterator() throws Exception {
        List<Segment> segments = recordCacheWriter.getSegments();
        return () -> new RecordIterator<>(recordSerializer, segments);
    }

    public void addRecord(T t) throws Exception {
        recordCacheWriter.addRecord(t);
    }

    private SupplierWithException<Path, IOException> createDataCacheFileGenerator(
            Path basePath, OperatorID operatorId) {
        return () ->
                new Path(
                        String.format(
                                "%s/%s-%s-%s",
                                basePath.toString(), "cache", operatorId, UUID.randomUUID()));
    }

    private static Path getDataCachePath(String[] localSpillPaths) {
        Random random = new Random();
        final String localSpillPath = localSpillPaths[random.nextInt(localSpillPaths.length)];
        String pathStr = Paths.get(localSpillPath).toUri().toString();
        return new Path(pathStr);
    }
}

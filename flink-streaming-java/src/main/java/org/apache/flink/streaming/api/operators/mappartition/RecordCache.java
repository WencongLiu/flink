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
import org.apache.flink.streaming.api.operators.mappartition.store.Store;
import org.apache.flink.streaming.api.operators.mappartition.store.disk.DiskStore;
import org.apache.flink.streaming.api.operators.mappartition.store.memory.MemoryStore;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

/** */
public class RecordCache<T> {

    private final Store<T> memoryStore;
    private final Store<T> diskStore;

    private boolean shouldWriteToMemory = true;

    public RecordCache(
            TypeSerializer<T> recordSerializer,
            StreamTask<?, ?> containingTask,
            StreamingRuntimeContext runtimeContext,
            OperatorID operatorID) {

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
        // Initialize the memory store.
        this.memoryStore = new MemoryStore<T>(recordSerializer, segmentPool);
        // Initialize the disk store.
        this.diskStore =
                new DiskStore<T>(
                        recordSerializer,
                        getSpillPath(
                                containingTask
                                        .getEnvironment()
                                        .getIOManager()
                                        .getSpillingDirectoriesPaths(),
                                operatorID));
    }

    public Iterable<T> getRecordIterator() {
        return () ->
                new RecordIterator<T>(
                        Arrays.asList(
                                memoryStore.getRecordIterator(), diskStore.getRecordIterator()));
    }

    public void addRecord(T t) throws Exception {
        diskStore.addRecord(t);
        //if (shouldWriteToMemory) {
        //    shouldWriteToMemory = memoryStore.addRecord(t);
        //} else {
        //    diskStore.addRecord(t);
        //}
    }

    private Path getSpillPath(String[] localSpillPaths, OperatorID operatorId) {
        Random random = new Random();
        final String localSpillPath = localSpillPaths[random.nextInt(localSpillPaths.length)];
        String pathStr = Paths.get(localSpillPath).toUri().toString();
        return new Path(
                String.format("%s/%s-%s-%s", pathStr, "cache", operatorId, UUID.randomUUID()));
    }
}

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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.ArrayList;
import java.util.List;

/** The {@link MapPartitionOperator} is used to process all records in each partition. */
public class MapPartitionOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, MapPartitionFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private final TypeInformation<IN> inputType;

    private final List<IN> allRecords = new ArrayList<>();

    private final MapPartitionFunction<IN, OUT> function;

    private long lastWatermarkTimestamp = Long.MIN_VALUE;

    public MapPartitionOperator(
            TypeInformation<IN> inputType, MapPartitionFunction<IN, OUT> function) {
        super(function);
        this.inputType = checkInputType(inputType);
        this.function = function;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        ClassLoader userCodeClassLoader = containingTask.getUserCodeClassLoader();
        double managedMemoryFraction =
                config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        containingTask.getEnvironment().getTaskConfiguration(),
                        userCodeClassLoader);
        MemoryManager memoryManager = containingTask.getEnvironment().getMemoryManager();
        IOManager ioManager = containingTask.getEnvironment().getIOManager();
        List<MemorySegment> memory;
        try {
            memory =
                    memoryManager.allocatePages(
                            this, memoryManager.computeNumberOfPages(managedMemoryFraction));
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
        System.out.println();
    }

    @Override
    public void endInput() throws Exception {
        TimestampedCollector<OUT> outputCollector = new TimestampedCollector<>(output);
        function.mapPartition(allRecords, outputCollector);
        Watermark watermark = new Watermark(lastWatermarkTimestamp);
        if (getTimeServiceManager().isPresent()) {
            getTimeServiceManager().get().advanceWatermark(watermark);
        }
        outputCollector.emitWatermark(watermark);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        allRecords.add(element.getValue());
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (lastWatermarkTimestamp > watermark.getTimestamp()) {
            throw new RuntimeException("Invalid watermark");
        }
        lastWatermarkTimestamp = watermark.getTimestamp();
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder()
                .setOutputOnEOF(true)
                .setInternalSorterSupported(true)
                .build();
    }

    private TypeInformation<IN> checkInputType(TypeInformation<IN> inputType) {
        if (inputType instanceof MissingTypeInfo) {
            MissingTypeInfo typeInfo = (MissingTypeInfo) inputType;
            throw new InvalidTypesException(
                    "The return type of function '"
                            + typeInfo.getFunctionName()
                            + "' could not be determined automatically, due to type erasure. "
                            + "You can give type information hints by using the returns(...) method on the result of "
                            + "the transformation call, or by letting your function implement the 'ResultTypeQueryable' "
                            + "interface.",
                    typeInfo.getTypeException());
        }
        return inputType;
    }
}

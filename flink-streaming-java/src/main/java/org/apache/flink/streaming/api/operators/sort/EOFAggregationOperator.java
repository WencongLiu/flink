/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.runtime.util.NonReusingKeyGroupedIterator;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.MutableObjectIterator;

/**
 * This operator would replace the regular Aggregate process when the WindowAssigner is
 * EndOfStreamWindows.
 */
@Internal
public class EOFAggregationOperator<IN, KEY, ACC, OUT>
        extends AbstractUdfStreamOperator<OUT, AggregateFunction<IN, ACC, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private PushSorter<Tuple2<byte[], StreamRecord<IN>>> sorter;
    TypeSerializer<Tuple2<byte[], StreamRecord<IN>>> keyAndValueSerializer;
    TypeComparator<Tuple2<byte[], StreamRecord<IN>>> comparator;
    private KeySelector<IN, KEY> keySelector;
    private TypeSerializer<KEY> keySerializer;
    private DataOutputSerializer dataOutputSerializer;
    private long lastWatermarkTimestamp = Long.MIN_VALUE;

    public EOFAggregationOperator(AggregateFunction<IN, ACC, OUT> userFunction) {
        super(userFunction);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        ClassLoader userCodeClassLoader = containingTask.getUserCodeClassLoader();
        MemoryManager memoryManager = containingTask.getEnvironment().getMemoryManager();
        IOManager ioManager = containingTask.getEnvironment().getIOManager();

        keySelector = (KeySelector<IN, KEY>) config.getStatePartitioner(0, userCodeClassLoader);
        keySerializer = config.getStateKeySerializer(userCodeClassLoader);
        int keyLength = keySerializer.getLength();

        TypeSerializer<IN> typeSerializerA = config.getTypeSerializerIn(0, userCodeClassLoader);
        keyAndValueSerializer = new KeyAndValueSerializer<>(typeSerializerA, keyLength);

        if (keyLength > 0) {
            dataOutputSerializer = new DataOutputSerializer(keyLength);
            comparator = new FixedLengthByteKeyComparator<>(keyLength);
        } else {
            dataOutputSerializer = new DataOutputSerializer(64);
            comparator = new VariableLengthByteKeyComparator<>();
        }

        ExecutionConfig executionConfig = containingTask.getEnvironment().getExecutionConfig();

        double managedMemoryFraction =
                config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        containingTask.getEnvironment().getTaskConfiguration(),
                        userCodeClassLoader);

        Configuration jobConfiguration = containingTask.getEnvironment().getJobConfiguration();

        try {
            sorter =
                    ExternalSorter.newBuilder(
                                    memoryManager,
                                    containingTask,
                                    keyAndValueSerializer,
                                    comparator,
                                    executionConfig)
                            .memoryFraction(managedMemoryFraction)
                            .enableSpilling(
                                    ioManager,
                                    jobConfiguration.get(AlgorithmOptions.SORT_SPILLING_THRESHOLD))
                            .maxNumFileHandles(
                                    jobConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN))
                            .objectReuse(executionConfig.isObjectReuseEnabled())
                            .largeRecords(
                                    jobConfiguration.get(
                                            AlgorithmOptions.USE_LARGE_RECORDS_HANDLER))
                            .build();
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void endInput() throws Exception {
        sorter.finishReading();
        MutableObjectIterator<Tuple2<byte[], StreamRecord<IN>>> iterator = sorter.getIterator();
        TimestampedCollector<OUT> outputCollector = new TimestampedCollector<>(output);
        NonReusingKeyGroupedIterator<Tuple2<byte[], StreamRecord<IN>>> keyGroupedIterator =
                new NonReusingKeyGroupedIterator<>(iterator, comparator);

        while (keyGroupedIterator.nextKey()) {
            ACC acc = userFunction.createAccumulator();
            NonReusingKeyGroupedIterator<Tuple2<byte[], StreamRecord<IN>>>.ValuesIterator records =
                    keyGroupedIterator.getValues();
            for (Tuple2<byte[], StreamRecord<IN>> record : records) {
                acc = userFunction.add(record.f1.getValue(), acc);
            }
            outputCollector.collect(userFunction.getResult(acc));
        }

        Watermark watermark = new Watermark(lastWatermarkTimestamp);
        if (getTimeServiceManager().isPresent()) {
            getTimeServiceManager().get().advanceWatermark(watermark);
        }
        output.emitWatermark(watermark);
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (lastWatermarkTimestamp > watermark.getTimestamp()) {
            throw new RuntimeException("Invalid watermark");
        }
        lastWatermarkTimestamp = watermark.getTimestamp();
    }

    @Override
    public void close() throws Exception {
        super.close();
        sorter.close();
    }

    @Override
    public void processElement(StreamRecord<IN> streamRecord) throws Exception {
        KEY key = keySelector.getKey(streamRecord.getValue());
        keySerializer.serialize(key, dataOutputSerializer);
        byte[] serializedKey = dataOutputSerializer.getCopyOfBuffer();
        dataOutputSerializer.clear();
        sorter.writeRecord(Tuple2.of(serializedKey, streamRecord));
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder()
                .setOutputOnEOF(true)
                .setInternalSorterSupported(true)
                .build();
    }
}

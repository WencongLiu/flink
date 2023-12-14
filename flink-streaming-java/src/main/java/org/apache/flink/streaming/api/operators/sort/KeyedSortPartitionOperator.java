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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.MutableObjectIterator;

/**
 * The {@link KeyedSortPartitionOperator} sorts records of a partition on {@link KeyedStream}. It
 * ensures that all records with the same key are sorted in a user-defined order.
 *
 * @param <INPUT_TYPE> The type of input record.
 * @param <SORT_TYPE> The type used to sort the records.
 * @param <KEY_TYPE> The type of record key, which has already been defined in {@link KeyedStream}.
 */
@SuppressWarnings("unchecked")
public class KeyedSortPartitionOperator<INPUT_TYPE, SORT_TYPE, KEY_TYPE>
        extends AbstractSortPartitionOperator<INPUT_TYPE, SORT_TYPE> {

    private PushSorter<Tuple2<byte[], StreamRecord<SORT_TYPE>>> sorter;
    TypeSerializer<Tuple2<byte[], StreamRecord<SORT_TYPE>>> sortTypeSerializer;
    TypeComparator<Tuple2<byte[], StreamRecord<SORT_TYPE>>> sortTypeComparator;
    private KeySelector<INPUT_TYPE, KEY_TYPE> recordKeySelector;
    private TypeSerializer<KEY_TYPE> recordKeySerializer;
    private DataOutputSerializer recordKeyBuffer;
    private long lastWatermarkTimestamp = Long.MIN_VALUE;

    public KeyedSortPartitionOperator(TypeInformation<INPUT_TYPE> inputType, Order sortOrder) {
        super(inputType, sortOrder);
    }

    public KeyedSortPartitionOperator(
            TypeInformation<INPUT_TYPE> inputType, int sortPositionField, Order sortOrder) {
        super(inputType, sortPositionField, sortOrder);
    }

    public KeyedSortPartitionOperator(
            TypeInformation<INPUT_TYPE> inputType, String sortField, Order sortOrder) {
        super(inputType, sortField, sortOrder);
    }

    public KeyedSortPartitionOperator(
            TypeInformation<INPUT_TYPE> inputType,
            KeySelector<INPUT_TYPE, ?> sortkeySelector,
            Order sortOrder) {
        super(inputType, sortkeySelector, sortOrder);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<INPUT_TYPE>> output) {
        super.setup(containingTask, config, output);
        ClassLoader userCodeClassLoader = containingTask.getUserCodeClassLoader();
        recordKeySelector =
                (KeySelector<INPUT_TYPE, KEY_TYPE>)
                        config.getStatePartitioner(0, userCodeClassLoader);

        // 1. Create the serializer of sort type.
        recordKeySerializer = config.getStateKeySerializer(userCodeClassLoader);
        int keyLength = recordKeySerializer.getLength();
        if (sortFieldSelector == null) {
            sortTypeSerializer =
                    new KeyAndValueSerializer<>(
                            config.getTypeSerializerIn(0, userCodeClassLoader), keyLength);
        } else {
            sortTypeSerializer =
                    new KeyAndValueSerializer<>(
                            sortType.createSerializer(getExecutionConfig()), keyLength);
        }

        // 2. Create the comparator of sort type.
        if (keyLength > 0) {
            recordKeyBuffer = new DataOutputSerializer(keyLength);
            sortTypeComparator =
                    new FixedLengthByteKeyAndValueComparator<>(keyLength, getSortTypeComparator());
        } else {
            recordKeyBuffer = new DataOutputSerializer(64);
            sortTypeComparator =
                    new VariableLengthByteKeyAndValueComparator<>(getSortTypeComparator());
        }

        // 3. Create the sorter.
        sorter =
                getSorter(
                        sortTypeSerializer,
                        sortTypeComparator,
                        containingTask,
                        containingTask.getEnvironment().getMemoryManager(),
                        containingTask.getEnvironment().getExecutionConfig(),
                        containingTask.getEnvironment().getIOManager());
    }

    @Override
    public void endInput() throws Exception {
        sorter.finishReading();
        MutableObjectIterator<Tuple2<byte[], StreamRecord<SORT_TYPE>>> iterator =
                sorter.getIterator();
        TimestampedCollector<INPUT_TYPE> outputCollector = new TimestampedCollector<>(output);
        Tuple2<byte[], StreamRecord<SORT_TYPE>> record;
        while ((record = iterator.next()) != null) {
            if (sortFieldSelector != null) {
                outputCollector.collect(((Tuple2<?, INPUT_TYPE>) record.f1.getValue()).f1);
            } else {
                outputCollector.collect((INPUT_TYPE) record.f1.getValue());
            }
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
    public void processElement(StreamRecord<INPUT_TYPE> streamRecord) throws Exception {
        KEY_TYPE key = recordKeySelector.getKey(streamRecord.getValue());
        recordKeySerializer.serialize(key, recordKeyBuffer);
        byte[] serializedKey = recordKeyBuffer.getCopyOfBuffer();
        recordKeyBuffer.clear();
        if (sortFieldSelector != null) {
            sorter.writeRecord(
                    Tuple2.of(
                            serializedKey,
                            (StreamRecord<SORT_TYPE>)
                                    new StreamRecord<>(
                                            Tuple2.of(
                                                    sortFieldSelector.getKey(
                                                            streamRecord.getValue()),
                                                    streamRecord.getValue()))));
        } else {
            sorter.writeRecord(Tuple2.of(serializedKey, (StreamRecord<SORT_TYPE>) streamRecord));
        }
    }
}

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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.MutableObjectIterator;

/**
 * The {@link NonKeyedSortPartitionOperator} sorts records of a partition on non-keyed data stream.
 * It ensures that all records within the same task are sorted in a user-defined order.
 *
 * @param <INPUT_TYPE> The type of input record.
 * @param <SORT_TYPE> The type used to sort the records.
 */
@SuppressWarnings("unchecked")
public class NonKeyedSortPartitionOperator<INPUT_TYPE, SORT_TYPE>
        extends AbstractSortPartitionOperator<INPUT_TYPE, SORT_TYPE> {

    private long lastWatermarkTimestamp = Long.MIN_VALUE;

    private PushSorter<SORT_TYPE> sorter;

    public NonKeyedSortPartitionOperator(TypeInformation<INPUT_TYPE> inputType, Order sortOrder) {
        super(inputType, sortOrder);
    }

    public NonKeyedSortPartitionOperator(
            TypeInformation<INPUT_TYPE> inputType, int sortPositionField, Order sortOrder) {
        super(inputType, sortPositionField, sortOrder);
    }

    public NonKeyedSortPartitionOperator(
            TypeInformation<INPUT_TYPE> inputType, String sortField, Order sortOrder) {
        super(inputType, sortField, sortOrder);
    }

    public <K> NonKeyedSortPartitionOperator(
            TypeInformation<INPUT_TYPE> inputType,
            KeySelector<INPUT_TYPE, K> sortKeySelector,
            Order sortOrder) {
        super(inputType, sortKeySelector, sortOrder);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<INPUT_TYPE>> output) {
        super.setup(containingTask, config, output);

        // 1. Create the serializer of sort type.
        TypeSerializer<SORT_TYPE> sortTypeSerializer;
        ExecutionConfig executionConfig = containingTask.getEnvironment().getExecutionConfig();
        if (sortFieldSelector != null) {
            sortTypeSerializer = sortType.createSerializer(executionConfig);
        } else {
            sortTypeSerializer =
                    (TypeSerializer<SORT_TYPE>) inputType.createSerializer(executionConfig);
        }

        // 2. Create the comparator of sort type.
        TypeComparator<SORT_TYPE> sortTypeComparator = getSortTypeComparator();

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
        MutableObjectIterator<SORT_TYPE> dataIterator = sorter.getIterator();
        TimestampedCollector<INPUT_TYPE> outputCollector = new TimestampedCollector<>(output);
        SORT_TYPE record = dataIterator.next();
        while (record != null) {
            if (sortFieldSelector != null) {
                outputCollector.collect(((Tuple2<?, INPUT_TYPE>) record).f1);
            } else {
                outputCollector.collect((INPUT_TYPE) record);
            }
            record = dataIterator.next();
        }
        Watermark watermark = new Watermark(lastWatermarkTimestamp);
        if (getTimeServiceManager().isPresent()) {
            getTimeServiceManager().get().advanceWatermark(watermark);
        }
        output.emitWatermark(watermark);
    }

    @Override
    public void processElement(StreamRecord<INPUT_TYPE> element) throws Exception {
        if (sortFieldSelector != null) {
            sorter.writeRecord(
                    (SORT_TYPE)
                            Tuple2.of(
                                    sortFieldSelector.getKey(element.getValue()),
                                    element.getValue()));
        } else {
            sorter.writeRecord((SORT_TYPE) element.getValue());
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (lastWatermarkTimestamp > watermark.getTimestamp()) {
            throw new RuntimeException("Invalid watermark");
        }
        lastWatermarkTimestamp = watermark.getTimestamp();
    }
}

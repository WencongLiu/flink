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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * The {@link PartitionReduceOperator} is used to apply the reduce transformation on all records in
 * each partition.
 */
public class PartitionReduceOperator<IN> extends AbstractUdfStreamOperator<IN, ReduceFunction<IN>>
        implements OneInputStreamOperator<IN, IN>, BoundedOneInput {

    private final ReduceFunction<IN> reduceFunction;

    private IN currentRecord = null;

    private long lastWatermarkTimestamp = Long.MIN_VALUE;

    public PartitionReduceOperator(ReduceFunction<IN> reduceFunction) {
        super(reduceFunction);
        this.reduceFunction = reduceFunction;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<IN>> output) {
        super.setup(containingTask, config, output);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (currentRecord == null) {
            currentRecord = element.getValue();
        } else {
            currentRecord = reduceFunction.reduce(currentRecord, element.getValue());
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (lastWatermarkTimestamp > watermark.getTimestamp()) {
            throw new RuntimeException("Invalid watermark");
        }
        lastWatermarkTimestamp = watermark.getTimestamp();
    }

    @Override
    public void endInput() throws Exception {
        TimestampedCollector<IN> outputCollector = new TimestampedCollector<>(output);
        outputCollector.collect(currentRecord);
        Watermark watermark = new Watermark(lastWatermarkTimestamp);
        if (getTimeServiceManager().isPresent()) {
            getTimeServiceManager().get().advanceWatermark(watermark);
        }
        outputCollector.emitWatermark(watermark);
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder()
                .setOutputOnEOF(true)
                .setInternalSorterSupported(true)
                .build();
    }
}

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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link PartitionAggregateOperator} is used to apply the aggregate transformation on all
 * records of each partition.
 */
@Internal
public class PartitionAggregateOperator<IN, ACC, OUT>
        extends AbstractUdfStreamOperator<OUT, AggregateFunction<IN, ACC, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

    private ACC currentAccumulator = null;

    private long lastWatermarkTimestamp = Long.MIN_VALUE;

    public PartitionAggregateOperator(AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        super(aggregateFunction);
        this.aggregateFunction = aggregateFunction;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        this.currentAccumulator = aggregateFunction.createAccumulator();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        checkNotNull(currentAccumulator);
        aggregateFunction.add(element.getValue(), currentAccumulator);
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
        TimestampedCollector<OUT> outputCollector = new TimestampedCollector<>(output);
        outputCollector.collect(aggregateFunction.getResult(currentAccumulator));
        Watermark watermark = new Watermark(lastWatermarkTimestamp);
        if (getTimeServiceManager().isPresent()) {
            getTimeServiceManager().get().advanceWatermark(watermark);
        }
        outputCollector.emitWatermark(watermark);
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder().setOutputOnlyAfterEndOfStream(true).build();
    }
}

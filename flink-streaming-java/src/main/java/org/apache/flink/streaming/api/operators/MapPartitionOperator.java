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
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;

/**
 * The {@link MapPartitionOperator} is used to process all records in each partition on non-keyed
 * stream.
 */
@Internal
public class MapPartitionOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, MapPartitionFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private final MapPartitionFunction<IN, OUT> function;

    private long lastWatermarkTimestamp = Long.MIN_VALUE;

    private InternalAsyncIterator<IN> iterator;

    public MapPartitionOperator(MapPartitionFunction<IN, OUT> function) {
        super(function);
        this.function = function;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        this.iterator = new InternalAsyncIteratorImpl<>();
        this.iterator.registerUDF(
                iterator -> {
                    TimestampedCollector<OUT> outputCollector = new TimestampedCollector<>(output);
                    try {
                        function.mapPartition(() -> iterator, outputCollector);
                    } catch (Exception e) {
                        ExceptionUtils.rethrow(e);
                    }
                });
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        iterator.addRecord(element.getValue());
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
        iterator.close();
        Watermark watermark = new Watermark(lastWatermarkTimestamp);
        if (getTimeServiceManager().isPresent()) {
            getTimeServiceManager().get().advanceWatermark(watermark);
        }
        output.emitWatermark(watermark);
        output.close();
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder().setOutputOnlyAfterEndOfStream(true).build();
    }
}

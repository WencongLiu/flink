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

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.mappartition2.DefaultInternalAsyncProcesser;
import org.apache.flink.streaming.api.operators.mappartition2.InternalAsyncProcesser;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;

/** The {@link MapPartitionOperator2} is used to process all records in each partition. */
public class MapPartitionOperator2<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, MapPartitionFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private final MapPartitionFunction<IN, OUT> function;

    private long lastWatermarkTimestamp = Long.MIN_VALUE;

    private InternalAsyncProcesser<IN> processer;

    public MapPartitionOperator2(MapPartitionFunction<IN, OUT> function) {
        super(function);
        this.function = function;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        this.processer = new DefaultInternalAsyncProcesser<>();
        this.processer.registerUDF(
                iterable -> {
                    TimestampedCollector<OUT> outputCollector = new TimestampedCollector<>(output);
                    Watermark watermark = null;
                    try {
                        function.mapPartition(iterable, outputCollector);
                        watermark = new Watermark(lastWatermarkTimestamp);
                        if (getTimeServiceManager().isPresent()) {
                            getTimeServiceManager().get().advanceWatermark(watermark);
                        }
                    } catch (Exception e) {
                        ExceptionUtils.rethrow(e);
                    }
                    outputCollector.emitWatermark(watermark);
                });
    }

    @Override
    public void endInput() throws Exception {
        processer.endOfInput();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        processer.processRecordAsync(element.getValue());
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
}

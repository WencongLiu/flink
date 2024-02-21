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

package org.apache.flink.streaming.util;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Collection;

/**
 * {@link TestRecordValueAndWatermarkOutput} is a test collector to collect both the value of {@link
 * StreamRecord} and {@link Watermark}.
 */
public class TestRecordValueAndWatermarkOutput<T> implements Output<StreamRecord<T>> {

    private final Collection<T> outputs;

    private final Collection<Watermark> watermarks;

    public TestRecordValueAndWatermarkOutput() {
        this.outputs = new ArrayList<>();
        this.watermarks = new ArrayList<>();
    }

    public TestRecordValueAndWatermarkOutput(
            Collection<T> outputs, Collection<Watermark> watermarks) {
        this.outputs = outputs;
        this.watermarks = watermarks;
    }

    @Override
    public void collect(StreamRecord<T> record) {
        outputs.add(record.getValue());
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        throw new UnsupportedOperationException("Side output is not supported");
    }

    @Override
    public void emitWatermark(Watermark mark) {
        watermarks.add(mark);
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        throw new RuntimeException("WatermarkStatus is not supported");
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        throw new RuntimeException();
    }

    @Override
    public void emitRecordAttributes(RecordAttributes recordAttributes) {
        throw new RuntimeException("RecordAttributes is not supported");
    }

    @Override
    public void close() {}
}

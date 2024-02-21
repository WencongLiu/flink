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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.streaming.util.TestRecordValueAndWatermarkOutput;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link PartitionAggregateOperator}. */
class PartitionAggregateOperatorTest {

    /** The test environment. */
    private Environment environment;

    /** The test stream task. */
    private StreamTask<?, ?> containingTask;

    /** The test stream config. */
    private StreamConfig config;

    @BeforeEach
    void before() throws Exception {
        environment = MockEnvironment.builder().build();
        containingTask =
                new StreamTask<Object, StreamOperator<Object>>(environment) {
                    @Override
                    protected void init() {}
                };
        config = new MockStreamConfig(new Configuration(), 1);
    }

    @Test
    void testSetup() {
        PartitionAggregateOperator<Integer, TestAccumulator, String> partitionAggregateOperator =
                createPartitionAggregateOperator();
        TestRecordValueAndWatermarkOutput<String> output =
                new TestRecordValueAndWatermarkOutput<>();
        assertDoesNotThrow(() -> partitionAggregateOperator.setup(containingTask, config, output));
    }

    @Test
    void testProcessElementAndWatermark() throws Exception {
        PartitionAggregateOperator<Integer, TestAccumulator, String> partitionAggregateOperator =
                createPartitionAggregateOperator();
        List<String> integerOutputList = new ArrayList<>();
        List<Watermark> watermarkList = new ArrayList<>();
        TestRecordValueAndWatermarkOutput<String> output =
                new TestRecordValueAndWatermarkOutput<>(integerOutputList, watermarkList);
        long timestamp = new Random().nextLong();
        Watermark testWatermark = new Watermark(timestamp);
        partitionAggregateOperator.setup(containingTask, config, output);
        partitionAggregateOperator.processElement(new StreamRecord<>(1));
        partitionAggregateOperator.processElement(new StreamRecord<>(1));
        partitionAggregateOperator.processElement(new StreamRecord<>(1));
        partitionAggregateOperator.processWatermark(testWatermark);
        partitionAggregateOperator.endInput();
        assertThat(integerOutputList.size()).isOne();
        assertEquals(integerOutputList.get(0), "303");
        assertThat(watermarkList.size()).isOne();
        assertEquals(watermarkList.get(0).getTimestamp(), timestamp);
    }

    private PartitionAggregateOperator<Integer, TestAccumulator, String>
            createPartitionAggregateOperator() {
        return new PartitionAggregateOperator<>(
                new AggregateFunction<Integer, TestAccumulator, String>() {
                    @Override
                    public TestAccumulator createAccumulator() {
                        return new TestAccumulator();
                    }

                    @Override
                    public TestAccumulator add(Integer value, TestAccumulator accumulator) {
                        accumulator.addNumber(value);
                        return accumulator;
                    }

                    @Override
                    public String getResult(TestAccumulator accumulator) {
                        return accumulator.getResult();
                    }

                    @Override
                    public TestAccumulator merge(TestAccumulator a, TestAccumulator b) {
                        return null;
                    }
                });
    }

    /** The test accumulator. */
    public static class TestAccumulator {
        private Integer result = 0;

        public void addNumber(Integer number) {
            result = result + number + 100;
        }

        public String getResult() {
            return String.valueOf(result);
        }
    }
}

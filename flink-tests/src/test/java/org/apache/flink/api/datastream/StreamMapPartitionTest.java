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

package org.apache.flink.api.datastream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.RepeatedTest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** */
public class StreamMapPartitionTest {

    @RepeatedTest(10)
    public void testDataStreamMapPartition() throws Exception {
        long startTime = System.currentTimeMillis();
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> source =
                executionEnvironment.fromCollection(new OneThousandSource(), String.class);
        source.setParallelism(1);
        SingleOutputStreamOperator<String> mapStream =
                source.rebalance()
                        .map(
                                new MapFunction<String, String>() {
                                    @Override
                                    public String map(String value) throws Exception {
                                        return value;
                                    }
                                });
        mapStream.setParallelism(10);
        mapStream
                .fullWindowPartition()
                .mapPartition(
                        new RichMapPartitionFunction<String, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<String> values, Collector<String> out) {
                                StringBuilder builder = new StringBuilder();
                                builder.append("Current Subtask ID: ")
                                        .append(getRuntimeContext().getIndexOfThisSubtask());
                                int number = 0;
                                for (String value : values) {
                                    ++number;
                                }
                                builder.append(" ").append(number).append(" records.");
                                out.collect(builder.toString());
                            }
                        })
                .print();
        executionEnvironment.execute();
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        double seconds = executionTime / 1000.0;
        System.out.println("testDataStreamMapPartition：" + seconds + " second");
    }

    @RepeatedTest(10)
    public void testDataSetMapPartition() throws Exception {
        long startTime = System.currentTimeMillis();
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source =
                executionEnvironment.fromCollection(new OneThousandSource(), String.class);
        source.setParallelism(1);
        MapOperator<String, String> mapStream =
                source
                        .map(
                                new MapFunction<String, String>() {
                                    @Override
                                    public String map(String value) throws Exception {
                                        return value;
                                    }
                                });
        mapStream.setParallelism(10);
        mapStream
                .mapPartition(
                        new RichMapPartitionFunction<String, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<String> values, Collector<String> out) {
                                StringBuilder builder = new StringBuilder();
                                builder.append("Current Subtask ID: ")
                                        .append(getRuntimeContext().getIndexOfThisSubtask());
                                int number = 0;
                                for (String value : values) {
                                    ++number;
                                }
                                builder.append(" ").append(number).append(" records.");
                                out.collect(builder.toString());
                            }
                        })
                .print();
        //executionEnvironment.execute();
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        double seconds = executionTime / 1000.0;
        System.out.println("testDataSetMapPartition：" + seconds + " second");
    }

    static class OneThousandSource implements Iterator<String>, Serializable {
        private static final int TOTAL_NUMBER = 1000000;

        private int currentPosition = 0;

        private final List<Integer> allElements = new ArrayList<>();

        public OneThousandSource() {
            for (int index = 0; index < TOTAL_NUMBER; ++index) {
                allElements.add(index);
            }
        }

        @Override
        public boolean hasNext() {
            return currentPosition < TOTAL_NUMBER;
        }

        @Override
        public String next() {
            return String.valueOf(allElements.get(currentPosition++));
        }
    }
}

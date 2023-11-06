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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** */
public class StreamMapPartitionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> source =
                executionEnvironment.fromCollection(new OneThousandSource(), String.class);
        source.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream =
                source.rebalance()
                        .map(
                                new RichMapFunction<String, Tuple2<String, Integer>>() {
                                    @Override
                                    public Tuple2<String, Integer> map(String string) {
                                        return Tuple2.of(
                                                String.valueOf(
                                                        getRuntimeContext()
                                                                .getIndexOfThisSubtask()),
                                                Integer.valueOf(string));
                                    }
                                });
        mapStream.setParallelism(10);
        mapStream
                .fullWindowPartition()
                .mapPartition(
                        new RichMapPartitionFunction<Tuple2<String, Integer>, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<Tuple2<String, Integer>> values,
                                    Collector<String> out) {
                                StringBuilder builder = new StringBuilder();
                                builder.append("Current Subtask ID: ")
                                        .append(getRuntimeContext().getIndexOfThisSubtask());
                                int number = 0;
                                for (Tuple2<String, Integer> value : values) {
                                    ++number;
                                }
                                builder.append(" ").append(number).append(" records.");
                                System.out.println(builder);
                            }
                        });
        executionEnvironment.execute();
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

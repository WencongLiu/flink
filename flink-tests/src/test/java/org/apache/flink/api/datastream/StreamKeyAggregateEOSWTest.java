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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EndOfStreamWindows;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** */
public class StreamKeyAggregateEOSWTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        executionEnvironment.setStateBackend(new EmbeddedRocksDBStateBackend());
        DataStreamSource<String> source =
                executionEnvironment.fromCollection(new OneThousandSource(), String.class);
        source.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream =
                source.rescale()
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
        SingleOutputStreamOperator<String> aggregate = mapStream
                .keyBy(
                        new KeySelector<Tuple2<String, Integer>, String>() {

                            @Override
                            public String getKey(Tuple2<String, Integer> value) throws Exception {
                                return value.f0;
                            }
                        })
                .window(EndOfStreamWindows.get())
                .aggregate(
                        new AggregateFunction<
                                Tuple2<String, Integer>, List<Tuple2<String, Integer>>, String>() {
                            @Override
                            public List<Tuple2<String, Integer>> createAccumulator() {
                                return new ArrayList<>();
                            }

                            @Override
                            public List<Tuple2<String, Integer>> add(
                                    Tuple2<String, Integer> value,
                                    List<Tuple2<String, Integer>> accumulator) {
                                accumulator.add(value);
                                return accumulator;
                            }

                            @Override
                            public String getResult(List<Tuple2<String, Integer>> accumulator) {
                                return "Key " + accumulator.get(0).f0 + ", Value " + accumulator.size();
                            }

                            @Override
                            public List<Tuple2<String, Integer>> merge(
                                    List<Tuple2<String, Integer>> a,
                                    List<Tuple2<String, Integer>> b) {
                                return null;
                            }
                        });
        aggregate.setParallelism(1);
        DataStreamSink<String> print = aggregate.print();
        print.setParallelism(1);
        // apply.setParallelism(10);
        // apply.print();
        executionEnvironment.execute();
    }

    static class OneThousandSource implements Iterator<String>, Serializable {

        private int currentPosition = 0;

        private final List<Integer> allElements = new ArrayList<>();

        public OneThousandSource() {
            for (int index = 0; index < 20; ++index) {
                allElements.add(index);
            }
        }

        @Override
        public boolean hasNext() {
            return currentPosition < 20;
        }

        @Override
        public String next() {
            return String.valueOf(allElements.get(currentPosition++));
        }
    }
}

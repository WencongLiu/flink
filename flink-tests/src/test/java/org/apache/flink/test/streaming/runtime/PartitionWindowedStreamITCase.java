/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.PartitionWindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Integration tests for {@link PartitionWindowedStream}. */
public class PartitionWindowedStreamITCase {

    private static List<String> testResults;

    @Before
    public void setup() {
        testResults = new ArrayList<>();
    }

    @Test
    public void testMapPartition() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source =
                env.fromElements("Test", "Test", "Test", "Test", "Test", "Test");
        source.map(v -> v)
                .setParallelism(2)
                .fullWindowPartition()
                .mapPartition(
                        new MapPartitionFunction<String, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<String> values, Collector<String> out) {
                                StringBuilder sb = new StringBuilder();
                                for (String value : values) {
                                    sb.append(value);
                                }
                                out.collect(sb.toString());
                            }
                        })
                .addSink(new ResultSink());

        env.execute().getAllAccumulatorResults();
        expectInAnyOrder("TestTestTest", "TestTestTest");
    }

    @Test
    public void testSortPartitionOfTupleElementsAscending() throws Exception {
        sortPartitionOfTupleElementsInOrder(Order.ASCENDING);
        expectInAnyOrder("013", "013");
    }

    @Test
    public void testSortPartitionOfTupleElementsDescending() throws Exception {
        sortPartitionOfTupleElementsInOrder(Order.DESCENDING);
        expectInAnyOrder("310", "310");
    }

    @Test
    public void testSortPartitionOfPojoElementsAscending() throws Exception {
        sortPartitionOfPojoElementsInOrder(Order.ASCENDING);
        expectInAnyOrder("013", "013");
    }

    @Test
    public void testSortPartitionOfPojoElementsDescending() throws Exception {
        sortPartitionOfPojoElementsInOrder(Order.DESCENDING);
        expectInAnyOrder("310", "310");
    }

    @Test
    public void testSortPartitionByKeySelectorAscending() throws Exception {
        sortPartitionByKeySelectorInOrder(Order.ASCENDING);
        expectInAnyOrder("013", "013");
    }

    @Test
    public void testSortPartitionByKeySelectorDescending() throws Exception {
        sortPartitionByKeySelectorInOrder(Order.DESCENDING);
        expectInAnyOrder("310", "310");
    }

    @Test
    public void testReduce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 1, 1, 1, 998, 998);
        source.map(v -> v)
                .setParallelism(2)
                .fullWindowPartition()
                .reduce(
                        new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        })
                .map(
                        new MapFunction<Integer, String>() {
                            @Override
                            public String map(Integer value) throws Exception {
                                return String.valueOf(value);
                            }
                        })
                .addSink(new ResultSink());

        env.execute().getAllAccumulatorResults();
        expectInAnyOrder("1000", "1000");
    }

    @Test
    public void testAggregate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 1, 2, 2, 3, 3);
        source.map(v -> v)
                .setParallelism(2)
                .fullWindowPartition()
                .aggregate(
                        new AggregateFunction<Integer, TestAccumulator, String>() {
                            @Override
                            public TestAccumulator createAccumulator() {
                                return new TestAccumulator();
                            }

                            @Override
                            public TestAccumulator add(Integer value, TestAccumulator accumulator) {
                                accumulator.addTestField(value);
                                return accumulator;
                            }

                            @Override
                            public String getResult(TestAccumulator accumulator) {
                                return accumulator.getTestField();
                            }

                            @Override
                            public TestAccumulator merge(TestAccumulator a, TestAccumulator b) {
                                throw new RuntimeException();
                            }
                        })
                .addSink(new ResultSink());

        env.execute().getAllAccumulatorResults();
        expectInAnyOrder("94", "94");
    }

    private void sortPartitionOfTupleElementsInOrder(Order order) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> source =
                env.fromElements(
                        Tuple2.of("Test", 0),
                        Tuple2.of("Test", 0),
                        Tuple2.of("Test", 3),
                        Tuple2.of("Test", 3),
                        Tuple2.of("Test", 1),
                        Tuple2.of("Test", 1));
        source.rebalance()
                .map(
                        new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(Tuple2<String, Integer> value)
                                    throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .fullWindowPartition()
                .sortPartition(1, order)
                .fullWindowPartition()
                .mapPartition(
                        new MapPartitionFunction<Tuple2<String, Integer>, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<Tuple2<String, Integer>> values,
                                    Collector<String> out) {
                                StringBuilder sb = new StringBuilder();
                                for (Tuple2<String, Integer> value : values) {
                                    sb.append(value.f1);
                                }
                                out.collect(sb.toString());
                            }
                        })
                .addSink(new ResultSink());
        env.execute();
    }

    private void sortPartitionOfPojoElementsInOrder(Order order) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TestPojo> source =
                env.fromElements(
                        new TestPojo(0),
                        new TestPojo(0),
                        new TestPojo(3),
                        new TestPojo(3),
                        new TestPojo(1),
                        new TestPojo(1));
        source.rebalance()
                .map(
                        new MapFunction<TestPojo, TestPojo>() {
                            @Override
                            public TestPojo map(TestPojo value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .fullWindowPartition()
                .sortPartition("value", order)
                .fullWindowPartition()
                .mapPartition(
                        new MapPartitionFunction<TestPojo, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<TestPojo> values, Collector<String> out) {
                                StringBuilder sb = new StringBuilder();
                                for (TestPojo value : values) {
                                    sb.append(value.getValue());
                                }
                                out.collect(sb.toString());
                            }
                        })
                .addSink(new ResultSink());
        env.execute();
    }

    private void sortPartitionByKeySelectorInOrder(Order order) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TestPojo> source =
                env.fromElements(
                        new TestPojo(0),
                        new TestPojo(0),
                        new TestPojo(3),
                        new TestPojo(3),
                        new TestPojo(1),
                        new TestPojo(1));
        source.rebalance()
                .map(
                        new MapFunction<TestPojo, TestPojo>() {
                            @Override
                            public TestPojo map(TestPojo value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .fullWindowPartition()
                .sortPartition(
                        new KeySelector<TestPojo, Integer>() {
                            @Override
                            public Integer getKey(TestPojo value) throws Exception {
                                return value.getValue();
                            }
                        },
                        order)
                .fullWindowPartition()
                .mapPartition(
                        new MapPartitionFunction<TestPojo, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<TestPojo> values, Collector<String> out) {
                                StringBuilder sb = new StringBuilder();
                                for (TestPojo value : values) {
                                    sb.append(value.getValue());
                                }
                                out.collect(sb.toString());
                            }
                        })
                .addSink(new ResultSink());
        env.execute();
    }

    private static void expectInAnyOrder(String... expected) {
        List<String> listExpected = Lists.newArrayList(expected);
        Collections.sort(listExpected);
        Collections.sort(testResults);
        Assert.assertEquals(listExpected, testResults);
    }

    private static class ResultSink implements SinkFunction<String> {
        @Override
        public void invoke(String value, Context context) throws Exception {
            testResults.add(value);
        }
    }

    /** The test pojo. */
    public static class TestPojo {
        private Integer value;

        public TestPojo() {}

        public TestPojo(Integer value) {
            this.value = value;
        }

        public Integer getValue() {
            return value;
        }

        public void setValue(Integer value) {
            this.value = value;
        }
    }

    /** The test accumulator. */
    public static class TestAccumulator {
        private Integer testField = 100;

        private void addTestField(Integer number) {
            testField = testField - number;
        }

        public String getTestField() {
            return String.valueOf(testField);
        }
    }
}

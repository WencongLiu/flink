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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import org.junit.Test;

/** Integration tests for {@link NonKeyedPartitionWindowedStreamITCase}. */
public class NonKeyedPartitionWindowedStreamITCase extends PartitionWindowedStreamITCaseBase {

    @Test
    public void testMapPartition() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source =
                env.fromData("Test", "Test", "Test", "Test", "Test", "Test");
        CloseableIterator<String> resultIterator =
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
                        .executeAndCollect();
        expectInAnyOrder(resultIterator, "TestTestTest", "TestTestTest");
    }

    @Test
    public void testSortPartitionOfTupleElementsAscending() throws Exception {
        expectInAnyOrder(sortPartitionOfTupleElementsInOrder(Order.ASCENDING), "013", "013");
    }

    @Test
    public void testSortPartitionOfTupleElementsDescending() throws Exception {
        expectInAnyOrder(sortPartitionOfTupleElementsInOrder(Order.DESCENDING), "310", "310");
    }

    @Test
    public void testSortPartitionOfPojoElementsAscending() throws Exception {
        expectInAnyOrder(sortPartitionOfPojoElementsInOrder(Order.ASCENDING), "013", "013");
    }

    @Test
    public void testSortPartitionOfPojoElementsDescending() throws Exception {
        expectInAnyOrder(sortPartitionOfPojoElementsInOrder(Order.DESCENDING), "310", "310");
    }

    @Test
    public void testSortPartitionByKeySelectorAscending() throws Exception {
        expectInAnyOrder(sortPartitionByKeySelectorInOrder(Order.ASCENDING), "013", "013");
    }

    @Test
    public void testSortPartitionByKeySelectorDescending() throws Exception {
        expectInAnyOrder(sortPartitionByKeySelectorInOrder(Order.DESCENDING), "310", "310");
    }

    @Test
    public void testReduce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromData(1, 1, 1, 1, 998, 998);
        CloseableIterator<String> resultIterator =
                source.map(v -> v)
                        .setParallelism(2)
                        .fullWindowPartition()
                        .reduce(
                                new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer value1, Integer value2)
                                            throws Exception {
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
                        .executeAndCollect();
        expectInAnyOrder(resultIterator, "1000", "1000");
    }

    @Test
    public void testAggregate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromData(1, 1, 2, 2, 3, 3);
        CloseableIterator<String> resultIterator =
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
                                    public TestAccumulator add(
                                            Integer value, TestAccumulator accumulator) {
                                        accumulator.addTestField(value);
                                        return accumulator;
                                    }

                                    @Override
                                    public String getResult(TestAccumulator accumulator) {
                                        return accumulator.getTestField();
                                    }

                                    @Override
                                    public TestAccumulator merge(
                                            TestAccumulator a, TestAccumulator b) {
                                        throw new RuntimeException();
                                    }
                                })
                        .executeAndCollect();
        expectInAnyOrder(resultIterator, "94", "94");
    }

    private CloseableIterator<String> sortPartitionOfTupleElementsInOrder(Order order)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> source =
                env.fromData(
                        Tuple2.of("Test", 0),
                        Tuple2.of("Test", 0),
                        Tuple2.of("Test", 3),
                        Tuple2.of("Test", 3),
                        Tuple2.of("Test", 1),
                        Tuple2.of("Test", 1));
        return source.rebalance()
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
                .executeAndCollect();
    }

    private CloseableIterator<String> sortPartitionOfPojoElementsInOrder(Order order)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TestPojo> source =
                env.fromData(
                        new TestPojo(0),
                        new TestPojo(0),
                        new TestPojo(3),
                        new TestPojo(3),
                        new TestPojo(1),
                        new TestPojo(1));
        return source.rebalance()
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
                .executeAndCollect();
    }

    private CloseableIterator<String> sortPartitionByKeySelectorInOrder(Order order)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TestPojo> source =
                env.fromData(
                        new TestPojo("KEY", 0),
                        new TestPojo("KEY", 0),
                        new TestPojo("KEY", 3),
                        new TestPojo("KEY", 3),
                        new TestPojo("KEY", 1),
                        new TestPojo("KEY", 1));
        return source.rebalance()
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
                .executeAndCollect();
    }
}

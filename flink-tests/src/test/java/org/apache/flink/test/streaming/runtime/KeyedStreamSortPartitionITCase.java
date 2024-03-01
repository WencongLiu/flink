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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Integration tests for sort partition API in {@link KeyedStream}. */
public class KeyedStreamSortPartitionITCase {

    @Test
    public void testSortPartitionOfTupleElementsAscending() throws Exception {
        expectInAnyOrder(
                sortPartitionOfTupleElementsInOrder(Order.ASCENDING),
                "0 1 3 7 ",
                "0 1 79 100 ",
                "8 55 66 77 ");
    }

    @Test
    public void testSortPartitionOfTupleElementsDescending() throws Exception {
        expectInAnyOrder(
                sortPartitionOfTupleElementsInOrder(Order.DESCENDING),
                "7 3 1 0 ",
                "100 79 1 0 ",
                "77 66 55 8 ");
    }

    @Test
    public void testSortPartitionOfPojoElementsAscending() throws Exception {
        expectInAnyOrder(
                sortPartitionOfPojoElementsInOrder(Order.ASCENDING),
                "0 1 3 7 ",
                "0 1 79 100 ",
                "8 55 66 77 ");
    }

    @Test
    public void testSortPartitionOfPojoElementsDescending() throws Exception {
        expectInAnyOrder(
                sortPartitionOfPojoElementsInOrder(Order.DESCENDING),
                "7 3 1 0 ",
                "100 79 1 0 ",
                "77 66 55 8 ");
    }

    @Test
    public void testSortPartitionByKeySelectorAscending() throws Exception {
        expectInAnyOrder(
                sortPartitionByKeySelectorInOrder(Order.ASCENDING),
                "0 1 3 7 ",
                "0 1 79 100 ",
                "8 55 66 77 ");
    }

    @Test
    public void testSortPartitionByKeySelectorDescending() throws Exception {
        expectInAnyOrder(
                sortPartitionByKeySelectorInOrder(Order.DESCENDING),
                "7 3 1 0 ",
                "100 79 1 0 ",
                "77 66 55 8 ");
    }

    private CloseableIterator<String> sortPartitionOfTupleElementsInOrder(Order order)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> source =
                env.fromData(
                        Tuple2.of("key1", 0),
                        Tuple2.of("key1", 7),
                        Tuple2.of("key1", 3),
                        Tuple2.of("key1", 1),
                        Tuple2.of("key2", 1),
                        Tuple2.of("key2", 100),
                        Tuple2.of("key2", 0),
                        Tuple2.of("key2", 79),
                        Tuple2.of("key3", 77),
                        Tuple2.of("key3", 66),
                        Tuple2.of("key3", 55),
                        Tuple2.of("key3", 8));
        return source.map(
                        new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(Tuple2<String, Integer> value)
                                    throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .keyBy(
                        new KeySelector<Tuple2<String, Integer>, String>() {
                            @Override
                            public String getKey(Tuple2<String, Integer> value) throws Exception {
                                return value.f0;
                            }
                        })
                .sortPartition(1, order)
                .fullWindowPartition()
                .mapPartition(
                        new MapPartitionFunction<Tuple2<String, Integer>, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<Tuple2<String, Integer>> values,
                                    Collector<String> out) {
                                StringBuilder sb = new StringBuilder();
                                String preKey = null;
                                for (Tuple2<String, Integer> value : values) {
                                    if (preKey != null && !preKey.equals(value.f0)) {
                                        out.collect(sb.toString());
                                        sb = new StringBuilder();
                                    }
                                    sb.append(value.f1);
                                    sb.append(" ");
                                    preKey = value.f0;
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
                        new TestPojo("key1", 0),
                        new TestPojo("key1", 7),
                        new TestPojo("key1", 3),
                        new TestPojo("key1", 1),
                        new TestPojo("key2", 1),
                        new TestPojo("key2", 100),
                        new TestPojo("key2", 0),
                        new TestPojo("key2", 79),
                        new TestPojo("key3", 77),
                        new TestPojo("key3", 66),
                        new TestPojo("key3", 55),
                        new TestPojo("key3", 8));
        return source.map(
                        new MapFunction<TestPojo, TestPojo>() {
                            @Override
                            public TestPojo map(TestPojo value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .keyBy(
                        new KeySelector<TestPojo, String>() {
                            @Override
                            public String getKey(TestPojo value) throws Exception {
                                return value.getKey();
                            }
                        })
                .sortPartition("value", order)
                .fullWindowPartition()
                .mapPartition(
                        new MapPartitionFunction<TestPojo, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<TestPojo> values, Collector<String> out) {
                                StringBuilder sb = new StringBuilder();
                                String preKey = null;
                                for (TestPojo value : values) {
                                    if (preKey != null && !preKey.equals(value.getKey())) {
                                        out.collect(sb.toString());
                                        sb = new StringBuilder();
                                    }
                                    sb.append(value.getValue());
                                    sb.append(" ");
                                    preKey = value.getKey();
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
                        new TestPojo("key1", 0),
                        new TestPojo("key1", 7),
                        new TestPojo("key1", 3),
                        new TestPojo("key1", 1),
                        new TestPojo("key2", 1),
                        new TestPojo("key2", 100),
                        new TestPojo("key2", 0),
                        new TestPojo("key2", 79),
                        new TestPojo("key3", 77),
                        new TestPojo("key3", 66),
                        new TestPojo("key3", 55),
                        new TestPojo("key3", 8));
        return source.map(
                        new MapFunction<TestPojo, TestPojo>() {
                            @Override
                            public TestPojo map(TestPojo value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .keyBy(
                        new KeySelector<TestPojo, String>() {
                            @Override
                            public String getKey(TestPojo value) throws Exception {
                                return value.getKey();
                            }
                        })
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
                                String preKey = null;
                                for (TestPojo value : values) {
                                    if (preKey != null && !preKey.equals(value.getKey())) {
                                        out.collect(sb.toString());
                                        sb = new StringBuilder();
                                    }
                                    sb.append(value.getValue());
                                    sb.append(" ");
                                    preKey = value.getKey();
                                }
                                out.collect(sb.toString());
                            }
                        })
                .executeAndCollect();
    }

    private void expectInAnyOrder(CloseableIterator<String> resultIterator, String... expected) {
        List<String> listExpected = Lists.newArrayList(expected);
        List<String> testResults = Lists.newArrayList(resultIterator);
        Collections.sort(listExpected);
        Collections.sort(testResults);
        assertEquals(listExpected, testResults);
    }

    /** The test pojo. */
    public static class TestPojo {

        public String key;

        public Integer value;

        public TestPojo() {}

        public TestPojo(Integer value) {
            this.value = value;
        }

        public TestPojo(String key, Integer value) {
            this.key = key;
            this.value = value;
        }

        public Integer getValue() {
            return value;
        }

        public void setValue(Integer value) {
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }
}

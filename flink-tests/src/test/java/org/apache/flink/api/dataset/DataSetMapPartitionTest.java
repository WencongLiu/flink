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

package org.apache.flink.api.dataset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Test. */
public class DataSetMapPartitionTest {

    public static void main(String[] args) throws Exception {
        final org.apache.flink.api.java.ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = env.fromCollection(new OneThousandSource(), String.class);
        MapOperator<String, Integer> map = source.map(
                new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.valueOf(value);
                    }
                });
        map.setParallelism(10);
        MapPartitionOperator<Integer, Integer> mapPartition = map.mapPartition(new MapPartitionFunction<Integer, Integer>() {
            @Override
            public void mapPartition(Iterable<Integer> iterator, Collector<Integer> collector) throws Exception {
                int sum = 0;
                for (int i : iterator) {
                    sum += i;
                }
                collector.collect(sum);
            }
        });
        mapPartition.setParallelism(20);
        mapPartition.print();
    }

    private static class OneThousandSource implements Iterator<String>, Serializable {

        private int currentPosition = 0;

        private final List<Integer> allElements = new ArrayList<>();

        public OneThousandSource() {
            for (int index = 0; index < 1000; ++index) {
                allElements.add(1);
            }
        }

        @Override
        public boolean hasNext() {
            return currentPosition < 1000;
        }

        @Override
        public String next() {
            return String.valueOf(allElements.get(currentPosition++));
        }
    }

    private static class BlackHoleOutputFormat extends FileOutputFormat<Tuple2<String, Integer>> {

        @Override
        public void open(InitializationContext context) {}

        @Override
        public void writeRecord(Tuple2<String, Integer> stringIntegerTuple2) {}
    }
}

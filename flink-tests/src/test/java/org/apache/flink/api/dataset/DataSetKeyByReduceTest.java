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
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.ExecutionEnvironment;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

/** Test. */
public class DataSetKeyByReduceTest {

    public static final String TEMP_PATH = "./test";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        final org.apache.flink.api.java.ExecutionEnvironment env =
                org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = env.fromCollection(new TupleStringSource(10000), String.class);
        MapOperator<String, Tuple2<String, Integer>> map =
                source.map(
                        new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String s) throws Exception {
                                String[] words = s.split(" ");
                                return Tuple2.of(words[0], Integer.valueOf(words[1]));
                            }
                        });
        map.setParallelism(10);
        AggregateOperator<Tuple2<String, Integer>> aggregate = map.groupBy(0).aggregate(SUM, 1);
        aggregate.setParallelism(20);
        DataSink<Tuple2<String, Integer>> write =
                aggregate.write(new BlackHoleOutputFormat(), TEMP_PATH);
        write.setParallelism(30);
        env.execute("AggregateGroup");
    }

    private static class TupleStringSource implements Iterator<String>, Serializable {

        private static final long DEFAULT_NUMBER = 10000L;

        private final Random dataGenerator = new Random();

        private long totalNumber = DEFAULT_NUMBER;

        private long currentPosition = 0L;

        public TupleStringSource() {}

        public TupleStringSource(long totalNumber) {
            this.totalNumber = totalNumber;
        }

        @Override
        public boolean hasNext() {
            return currentPosition <= totalNumber;
        }

        @Override
        public String next() {
            currentPosition++;
            return dataGenerator.nextInt(100) + " " + dataGenerator.nextInt(50);
        }
    }

    private static class BlackHoleOutputFormat extends FileOutputFormat<Tuple2<String, Integer>> {

        @Override
        public void open(InitializationContext context) {}

        @Override
        public void writeRecord(Tuple2<String, Integer> stringIntegerTuple2) {}
    }
}

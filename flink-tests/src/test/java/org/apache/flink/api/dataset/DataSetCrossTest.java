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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** DataStream Cross. */
public class DataSetCrossTest {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source1 = env.fromCollection(new JoinSource(), String.class);
        MapOperator<String, Tuple2<String, Integer>> map1 =
                source1.map(
                        new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String value) throws Exception {
                                String[] words = value.split(" ");
                                return Tuple2.of(words[0], Integer.valueOf(words[1]));
                            }
                        });
        map1.setParallelism(10);

        DataSource<String> source2 = env.fromCollection(new JoinSource(), String.class);
        MapOperator<String, Tuple2<String, Integer>> map2 =
                source2.map(
                        new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String value) throws Exception {
                                String[] words = value.split(" ");
                                return Tuple2.of(words[0], Integer.valueOf(words[1]));
                            }
                        });
        map2.setParallelism(12);

        CrossOperator<Tuple2<String, Integer>, Tuple2<String, Integer>, String> cross =
                map1.cross(map2)
                        .with(
                                new CrossFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String>() {
                                    @Override
                                    public String cross(
                                            Tuple2<String, Integer> data1,
                                            Tuple2<String, Integer> data2) {
                                        return data1.toString() + " " + data2.toString();
                                    }
                                });

        cross.setParallelism(20);
        cross.print();
    }

    private static class JoinSource implements Iterator<String>, Serializable {

        private int currentPosition = 0;

        private final List<String> allElements = new ArrayList<>();

        public JoinSource() {
            allElements.add("a");
            allElements.add("b");
            allElements.add("c");
            allElements.add("d");
            allElements.add("e");
            allElements.add("f");
            allElements.add("g");
            allElements.add("h");
            allElements.add("i");
            allElements.add("j");
            allElements.add("k");
            allElements.add("l");
            allElements.add("m");
            allElements.add("n");
        }

        @Override
        public boolean hasNext() {
            return currentPosition < 14;
        }

        @Override
        public String next() {
            return allElements.get(currentPosition++) + " " + 100;
        }
    }
}

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
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/** */
public class StreamSortPartitionPojoTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        executionEnvironment.setStateBackend(new EmbeddedRocksDBStateBackend());
        DataStreamSource<String> source =
                executionEnvironment.fromCollection(new OneThousandSource(), String.class);
        source.setParallelism(1);
        SingleOutputStreamOperator<MyFriend> mapStream =
                source.rescale()
                        .map(
                                new RichMapFunction<String, MyFriend>() {
                                    @Override
                                    public MyFriend map(String string) {
                                        return new MyFriend(Integer.parseInt(string));
                                    }
                                });
        mapStream.setParallelism(1);
        DataStream<MyFriend> sortResult =
                mapStream.fullWindowPartition().sortPartition("age", Order.ASCENDING);
        sortResult
                .fullWindowPartition()
                .mapPartition(
                        new RichMapPartitionFunction<MyFriend, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<MyFriend> friends, Collector<String> out) {
                                StringBuilder builder = new StringBuilder();
                                builder.append("Current Subtask ID: ")
                                        .append(getRuntimeContext().getIndexOfThisSubtask())
                                        .append(" ");
                                for (MyFriend friend : friends) {
                                    builder.append(friend.getAge()).append(" ");
                                }
                                System.out.println(builder);
                            }
                        });
        executionEnvironment.execute();
    }

    public static class MyFriend {
        Integer age;

        public MyFriend() {}

        public MyFriend(Integer age) {
            this.age = age;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }

    static class OneThousandSource implements Iterator<String>, Serializable {

        private int currentPosition = 0;

        private final List<Integer> allElements = new ArrayList<>();

        public OneThousandSource() {
            for (int index = 0; index < 20; ++index) {
                allElements.add(new Random().nextInt(20));
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

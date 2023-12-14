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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.sort.KeyedSortPartitionOperator;
import org.apache.flink.streaming.api.windowing.assigners.EndOfStreamWindows;

/**
 * {@link KeyedPartitionWindowedStream} represents a data stream that collects all records with the
 * same key separately into a full window.
 */
@Internal
public class KeyedPartitionWindowedStream<T, KEY> implements PartitionWindowedStream<T> {

    private final StreamExecutionEnvironment environment;

    private final KeyedStream<T, KEY> input;

    public KeyedPartitionWindowedStream(
            StreamExecutionEnvironment environment, KeyedStream<T, KEY> input) {
        this.environment = environment;
        this.input = input;
    }

    @Override
    public <R> SingleOutputStreamOperator<R> mapPartition(
            MapPartitionFunction<T, R> mapPartitionFunction) {
        if (mapPartitionFunction == null) {
            throw new NullPointerException("The map partition function must not be null.");
        }
        return input.window(EndOfStreamWindows.get()).apply(mapPartitionFunction);
    }

    @Override
    public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> reduceFunction) {
        if (reduceFunction == null) {
            throw new IllegalArgumentException("The reduce function must not be null.");
        }
        reduceFunction = environment.clean(reduceFunction);
        return input.window(EndOfStreamWindows.get()).reduce(reduceFunction);
    }

    @Override
    public <ACC, R> SingleOutputStreamOperator<R> aggregate(
            AggregateFunction<T, ACC, R> aggregateFunction) {
        if (aggregateFunction == null) {
            throw new IllegalArgumentException("The aggregate function must not be null.");
        }
        aggregateFunction = environment.clean(aggregateFunction);
        return input.window(EndOfStreamWindows.get()).aggregate(aggregateFunction);
    }

    @Override
    public SingleOutputStreamOperator<T> sortPartition(int field, Order order) {
        if (order == null) {
            throw new IllegalArgumentException("The order must not be null.");
        }
        KeyedSortPartitionOperator<T, T, KEY> operator =
                new KeyedSortPartitionOperator<>(input.getType(), field, order);
        final String opName = "KeyedSortPartition";
        return input.transform(opName, input.getType(), operator)
                .setFixedParallelism(input.getParallelism());
    }

    @Override
    public SingleOutputStreamOperator<T> sortPartition(String field, Order order) {
        if (field == null) {
            throw new IllegalArgumentException("The field must not be null.");
        }
        if (order == null) {
            throw new IllegalArgumentException("The order must not be null.");
        }
        KeyedSortPartitionOperator<T, T, KEY> operator =
                new KeyedSortPartitionOperator<>(input.getType(), field, order);
        final String opName = "KeyedSortPartition";
        return input.transform(opName, input.getType(), operator)
                .setFixedParallelism(input.getParallelism());
    }

    @Override
    public <K> SingleOutputStreamOperator<T> sortPartition(
            KeySelector<T, K> keySelector, Order order) {
        if (keySelector == null) {
            throw new IllegalArgumentException("The key selector must not be null.");
        }
        if (order == null) {
            throw new IllegalArgumentException("The order must not be null.");
        }
        KeyedSortPartitionOperator<T, Tuple2<?, T>, KEY> operator =
                new KeyedSortPartitionOperator<>(
                        input.getType(), environment.clean(keySelector), order);
        final String opName = "KeyedSortPartition";
        return input.transform(opName, input.getType(), operator)
                .setFixedParallelism(input.getParallelism());
    }
}

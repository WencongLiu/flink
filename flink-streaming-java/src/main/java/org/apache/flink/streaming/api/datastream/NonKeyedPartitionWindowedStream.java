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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.MapPartitionOperator;
import org.apache.flink.streaming.api.operators.PartitionAggregateOperator;
import org.apache.flink.streaming.api.operators.PartitionReduceOperator;
import org.apache.flink.streaming.api.operators.sort.NonKeyedSortPartitionOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

/**
 * {@link NonKeyedPartitionWindowedStream} represents a data stream that collects all records of
 * each subtask separately into a full window.
 */
@Internal
public class NonKeyedPartitionWindowedStream<T> implements PartitionWindowedStream<T> {

    private final StreamExecutionEnvironment environment;

    private final DataStream<T> input;

    public NonKeyedPartitionWindowedStream(
            StreamExecutionEnvironment environment, DataStream<T> input) {
        this.environment = environment;
        this.input = input;
    }

    @Override
    public <R> SingleOutputStreamOperator<R> mapPartition(
            MapPartitionFunction<T, R> mapPartitionFunction) {
        if (mapPartitionFunction == null) {
            throw new NullPointerException("The map partition function must not be null.");
        }
        mapPartitionFunction = environment.clean(mapPartitionFunction);
        String opName = "MapPartition";
        TypeInformation<R> resultType =
                TypeExtractor.getMapPartitionReturnTypes(
                        mapPartitionFunction, input.getType(), opName, true);
        return input.setConnectionType(new ForwardPartitioner<>())
                .transform(opName, resultType, new MapPartitionOperator<>(mapPartitionFunction))
                .setFixedParallelism(input.getParallelism());
    }

    public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> reduceFunction) {
        if (reduceFunction == null) {
            throw new IllegalArgumentException("The reduce function must not be null.");
        }
        reduceFunction = environment.clean(reduceFunction);
        String opName = "PartitionReduce";
        return input.setConnectionType(new ForwardPartitioner<>())
                .transform(
                        opName,
                        input.getTransformation().getOutputType(),
                        new PartitionReduceOperator<>(reduceFunction))
                .setFixedParallelism(input.getParallelism());
    }

    public <ACC, R> SingleOutputStreamOperator<R> aggregate(
            AggregateFunction<T, ACC, R> aggregateFunction) {
        if (aggregateFunction == null) {
            throw new IllegalArgumentException("The aggregate function must not be null.");
        }
        aggregateFunction = environment.clean(aggregateFunction);
        String opName = "PartitionAggregate";
        TypeInformation<R> resultType =
                TypeExtractor.getAggregateFunctionReturnType(
                        aggregateFunction, input.getType(), opName, true);
        return input.setConnectionType(new ForwardPartitioner<>())
                .transform(opName, resultType, new PartitionAggregateOperator<>(aggregateFunction))
                .setFixedParallelism(input.getParallelism());
    }

    public SingleOutputStreamOperator<T> sortPartition(int field, Order order) {
        if (order == null) {
            throw new IllegalArgumentException("The order must not be null.");
        }
        NonKeyedSortPartitionOperator<T, T> operator =
                new NonKeyedSortPartitionOperator<>(input.getType(), field, order);
        final String opName = "SortPartition";
        return input.setConnectionType(new ForwardPartitioner<>())
                .transform(opName, input.getType(), operator)
                .setFixedParallelism(input.getParallelism());
    }

    public SingleOutputStreamOperator<T> sortPartition(String field, Order order) {
        if (field == null) {
            throw new IllegalArgumentException("The field must not be null.");
        }
        if (order == null) {
            throw new IllegalArgumentException("The order must not be null.");
        }
        NonKeyedSortPartitionOperator<T, T> operator =
                new NonKeyedSortPartitionOperator<>(input.getType(), field, order);
        final String opName = "SortPartition";
        return input.setConnectionType(new ForwardPartitioner<>())
                .transform(opName, input.getType(), operator)
                .setFixedParallelism(input.getParallelism());
    }

    public <K> SingleOutputStreamOperator<T> sortPartition(
            KeySelector<T, K> keySelector, Order order) {
        if (keySelector == null) {
            throw new IllegalArgumentException("The key selector must not be null.");
        }
        if (order == null) {
            throw new IllegalArgumentException("The order must not be null.");
        }
        NonKeyedSortPartitionOperator<T, Tuple2<?, T>> operator =
                new NonKeyedSortPartitionOperator<>(
                        input.getType(), environment.clean(keySelector), order);
        final String opName = "SortPartition";
        return input.setConnectionType(new ForwardPartitioner<>())
                .transform(opName, input.getType(), operator)
                .setFixedParallelism(input.getParallelism());
    }
}

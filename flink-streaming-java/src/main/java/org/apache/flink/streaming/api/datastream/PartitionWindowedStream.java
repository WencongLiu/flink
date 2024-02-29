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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.MapPartitionOperator;
import org.apache.flink.streaming.api.operators.PartitionAggregateOperator;
import org.apache.flink.streaming.api.operators.PartitionReduceOperator;
import org.apache.flink.streaming.api.operators.sortpartition.SortPartitionOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import static org.apache.flink.streaming.api.operators.sortpartition.SortPartitionOperator.DEFAULT_MANAGE_MEMORY_WEIGHT;

/**
 * {@link PartitionWindowedStream} represents a data stream that collects records of each partition
 * into a separate full window from non-keyed stream. Each partition only contains all records of a
 * subtask. The window emission will be triggered at the end of inputs.
 *
 * @param <T> The type of the elements in this stream.
 */
@PublicEvolving
public class PartitionWindowedStream<T> {

    private final StreamExecutionEnvironment environment;

    private final DataStream<T> input;

    public PartitionWindowedStream(StreamExecutionEnvironment environment, DataStream<T> input) {
        this.environment = environment;
        this.input = input;
    }

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
                .setParallelism(input.getParallelism());
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
                .setParallelism(input.getParallelism());
    }

    /**
     * Applies the given aggregate function to the records of the window. The aggregate function is
     * called for each element, aggregating values incrementally in the window.
     *
     * @param aggregateFunction The aggregation function.
     * @return The resulting data stream.
     * @param <ACC> The type of the AggregateFunction's accumulator.
     * @param <R> The type of the elements in the resulting stream, equal to the AggregateFunction's
     *     result type.
     */
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
                .setParallelism(input.getParallelism());
    }

    /**
     * Sorts the records of the window on the specified field in the specified order. The type of
     * records must be {@link Tuple}.
     *
     * @param field The field index on which records is sorted.
     * @param order The order in which records is sorted.
     * @return The resulting data stream with sorted records in each partition.
     */
    public SingleOutputStreamOperator<T> sortPartition(int field, Order order) {
        if (order == null) {
            throw new IllegalArgumentException("The order must not be null.");
        }
        if (field < 0) {
            throw new IllegalArgumentException("The field mustn't be less than zero.");
        }
        SortPartitionOperator<T> operator =
                new SortPartitionOperator<>(input.getType(), field, order);
        final String opName = "SortPartition";
        SingleOutputStreamOperator<T> result =
                input.setConnectionType(new ForwardPartitioner<>())
                        .transform(opName, input.getType(), operator)
                        .setParallelism(input.getParallelism());
        result.getTransformation()
                .declareManagedMemoryUseCaseAtOperatorScope(
                        ManagedMemoryUseCase.OPERATOR, DEFAULT_MANAGE_MEMORY_WEIGHT);
        return result;
    }

    /**
     * Sorts the records of the window on the specified field in the specified order. The type of
     * records must be Flink POJO {@link PojoTypeInfo}. A type is considered a Flink POJO type, if
     * it fulfills the conditions below.
     *
     * <ul>
     *   <li>It is a public class, and standalone (not a non-static inner class).
     *   <li>It has a public no-argument constructor.
     *   <li>All non-static, non-transient fields in the class (and all superclasses) are either
     *       public (and non-final) or have a public getter and a setter method that follows the
     *       Java beans naming conventions for getters and setters.
     *   <li>It is a fixed-length, null-aware composite type with non-deterministic field order.
     *       Every field can be null independent of the field's type.
     * </ul>
     *
     * @param field The field expression referring to the field on which records is sorted.
     * @param order The order in which records is sorted.
     * @return The resulting data stream with sorted records in each partition.
     */
    public SingleOutputStreamOperator<T> sortPartition(String field, Order order) {
        if (field == null) {
            throw new IllegalArgumentException("The field must not be null.");
        }
        if (order == null) {
            throw new IllegalArgumentException("The order must not be null.");
        }
        SortPartitionOperator<T> operator =
                new SortPartitionOperator<>(input.getType(), field, order);
        final String opName = "SortPartition";
        SingleOutputStreamOperator<T> result =
                input.setConnectionType(new ForwardPartitioner<>())
                        .transform(opName, input.getType(), operator)
                        .setParallelism(input.getParallelism());
        result.getTransformation()
                .declareManagedMemoryUseCaseAtOperatorScope(
                        ManagedMemoryUseCase.OPERATOR, DEFAULT_MANAGE_MEMORY_WEIGHT);
        return result;
    }

    /**
     * Sorts the records of the window on the extracted key in the specified order.
     *
     * @param keySelector The KeySelector function which extracts the key values from records.
     * @param order The order in which records is sorted.
     * @return The resulting data stream with sorted records in each partition.
     */
    public <K> SingleOutputStreamOperator<T> sortPartition(
            KeySelector<T, K> keySelector, Order order) {
        if (keySelector == null) {
            throw new IllegalArgumentException("The key selector must not be null.");
        }
        if (order == null) {
            throw new IllegalArgumentException("The order must not be null.");
        }
        SortPartitionOperator<T> operator =
                new SortPartitionOperator<>(input.getType(), environment.clean(keySelector), order);
        final String opName = "SortPartition";
        SingleOutputStreamOperator<T> result =
                input.setConnectionType(new ForwardPartitioner<>())
                        .transform(opName, input.getType(), operator)
                        .setParallelism(input.getParallelism());
        result.getTransformation()
                .declareManagedMemoryUseCaseAtOperatorScope(
                        ManagedMemoryUseCase.OPERATOR, DEFAULT_MANAGE_MEMORY_WEIGHT);
        return result;
    }
}

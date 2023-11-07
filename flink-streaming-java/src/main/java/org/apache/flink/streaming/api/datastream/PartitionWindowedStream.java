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
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.MapPartitionOperator;
import org.apache.flink.streaming.api.operators.PartitionAggregateOperator;
import org.apache.flink.streaming.api.operators.PartitionReduceOperator;
import org.apache.flink.streaming.api.operators.SortPartitionOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import java.util.Iterator;

/**
 * {@link PartitionWindowedStream} represents a data stream that collects all records of each
 * subtask into a separate full window. Window emission will be triggered at the end of inputs.
 *
 * @param <T> The type of the elements in this data stream.
 */
@PublicEvolving
public class PartitionWindowedStream<T> extends DataStream<T> {

    public PartitionWindowedStream(
            StreamExecutionEnvironment environment, Transformation<T> transformation) {
        super(environment, transformation);
    }

    /**
     * Process the records of the window by {@link MapPartitionFunction}. The records will be
     * available in the given {@link Iterator} function parameter.
     *
     * @param mapPartitionFunction The {@link MapPartitionFunction} that is called for the full
     *     records in the window.
     * @return The resulting data stream.
     * @param <R> The type of the elements in the resulting stream, equal to the
     *     MapPartitionFunction's result type.
     */
    public <R> DataStream<R> mapPartition(MapPartitionFunction<T, R> mapPartitionFunction) {
        if (mapPartitionFunction == null) {
            throw new NullPointerException("MapPartition function must not be null.");
        }
        mapPartitionFunction = getExecutionEnvironment().clean(mapPartitionFunction);
        String opName = "MapPartition";
        TypeInformation<R> resultType =
                TypeExtractor.getMapPartitionReturnTypes(
                        mapPartitionFunction, getType(), opName, true);
        this.setConnectionType(new ForwardPartitioner<>());
        return this.transform(opName, resultType, new MapPartitionOperator<>(mapPartitionFunction))
                .setParallelism(this.transformation.getParallelism());
    }

    /**
     * Sorts the records of the window on the specified field in the specified order. The type of
     * records must be {@link Tuple}.
     *
     * @param field The field index on which records is sorted.
     * @param order The order in which records is sorted.
     * @return The resulting data stream with sorted records in each subtask.
     */
    public DataStream<T> sortPartition(int field, Order order) {
        SortPartitionOperator<T> operator = new SortPartitionOperator<>(getType(), field, order);
        final String opName = "SortPartition";
        this.setConnectionType(new ForwardPartitioner<>());
        SingleOutputStreamOperator<T> resultStream =
                this.transform(opName, getType(), operator)
                        .setParallelism(this.transformation.getParallelism());
        setManagedMemoryWeight(resultStream, 100);
        return resultStream;
    }

    /**
     * Sorts the records of the window on the specified field in the specified order. The type of
     * records must be {@link Tuple} or POJO. The type of records must be {@link Tuple} or POJO
     * class. The POJO class must be public and have getter and setter methods for each field. It
     * mustn't implement any interfaces and extend any * classes.
     *
     * @param field The field expression referring to the field on which records is sorted.
     * @param order The order in which records is sorted.
     * @return The resulting data stream with sorted records in each subtask.
     */
    public DataStream<T> sortPartition(String field, Order order) {
        SortPartitionOperator<T> operator = new SortPartitionOperator<>(getType(), field, order);
        final String opName = "SortPartition";
        this.setConnectionType(new ForwardPartitioner<>());
        SingleOutputStreamOperator<T> resultStream =
                this.transform(opName, getType(), operator)
                        .setParallelism(this.transformation.getParallelism());
        setManagedMemoryWeight(resultStream, 100);
        return resultStream;
    }

    /**
     * Sorts the records of the window on the extracted key in the specified order.
     *
     * @param keySelector The KeySelector function which extracts the key values from records.
     * @param order The order in which records is sorted.
     * @return The resulting data stream with sorted records in each subtask.
     */
    public <K> DataStream<T> sortPartition(KeySelector<T, K> keySelector, Order order) {
        SortPartitionOperator<T> operator =
                new SortPartitionOperator<>(getType(), clean(keySelector), order);
        final String opName = "SortPartition";
        this.setConnectionType(new ForwardPartitioner<>());
        SingleOutputStreamOperator<T> resultStream =
                this.transform(opName, getType(), operator)
                        .setParallelism(transformation.getParallelism());
        setManagedMemoryWeight(resultStream, 100);
        return resultStream;
    }

    /**
     * Applies a reduce transformation on the records of the window. The {@link ReduceFunction} will
     * be called for every record in the window.
     *
     * @param reduceFunction The reduce function.
     * @return The resulting data stream.
     */
    public DataStream<T> reduce(ReduceFunction<T> reduceFunction) {
        if (reduceFunction == null) {
            throw new NullPointerException("ReduceFunction function must not be null.");
        }
        reduceFunction = getExecutionEnvironment().clean(reduceFunction);
        String opName = "PartitionReduce";
        this.setConnectionType(new ForwardPartitioner<>());
        return this.transform(
                        opName,
                        getTransformation().getOutputType(),
                        new PartitionReduceOperator<>(reduceFunction))
                .setParallelism(this.transformation.getParallelism());
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
    public <ACC, R> DataStream<R> aggregate(AggregateFunction<T, ACC, R> aggregateFunction) {
        if (aggregateFunction == null) {
            throw new NullPointerException("ReduceFunction function must not be null.");
        }
        aggregateFunction = getExecutionEnvironment().clean(aggregateFunction);
        String opName = "PartitionAggregate";
        this.setConnectionType(new ForwardPartitioner<>());
        TypeInformation<R> resultType =
                TypeExtractor.getAggregateFunctionReturnType(
                        aggregateFunction, getType(), opName, true);
        return this.transform(
                        opName, resultType, new PartitionAggregateOperator<>(aggregateFunction))
                .setParallelism(this.transformation.getParallelism());
    }
}

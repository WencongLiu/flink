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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;

import java.util.Iterator;

/**
 * {@link PartitionWindowedStream} represents a data stream that collects all records of each
 * partition separately into a full window. Window emission will be triggered at the end of inputs.
 * For non-keyed DataStream, a partition contains all records of a subtask. For {@link KeyedStream},
 * a partition contains all records of a key.
 *
 * @param <T> The type of the elements in this stream.
 */
@PublicEvolving
public interface PartitionWindowedStream<T> {

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
    <R> DataStream<R> mapPartition(MapPartitionFunction<T, R> mapPartitionFunction);

    /**
     * Sorts the records of the window on the specified field in the specified order. The type of
     * records must be {@link Tuple}.
     *
     * @param field The field index on which records is sorted.
     * @param order The order in which records is sorted.
     * @return The resulting data stream with sorted records in each subtask.
     */
    DataStream<T> sortPartition(int field, Order order);

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
    DataStream<T> sortPartition(String field, Order order);

    /**
     * Sorts the records of the window on the extracted key in the specified order.
     *
     * @param keySelector The KeySelector function which extracts the key values from records.
     * @param order The order in which records is sorted.
     * @return The resulting data stream with sorted records in each subtask.
     */
    <K> DataStream<T> sortPartition(KeySelector<T, K> keySelector, Order order);

    /**
     * Applies a reduce transformation on the records of the window. The {@link ReduceFunction} will
     * be called for every record in the window.
     *
     * @param reduceFunction The reduce function.
     * @return The resulting data stream.
     */
    DataStream<T> reduce(ReduceFunction<T> reduceFunction);

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
    <ACC, R> DataStream<R> aggregate(AggregateFunction<T, ACC, R> aggregateFunction);
}

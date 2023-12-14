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
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

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
    <R> SingleOutputStreamOperator<R> mapPartition(MapPartitionFunction<T, R> mapPartitionFunction);

    /**
     * Applies a reduce transformation on the records of the window. The {@link ReduceFunction} will
     * be called for every record in the window.
     *
     * @param reduceFunction The reduce function.
     * @return The resulting data stream.
     */
    SingleOutputStreamOperator<T> reduce(ReduceFunction<T> reduceFunction);
}

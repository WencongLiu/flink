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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.MapPartitionOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

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
}

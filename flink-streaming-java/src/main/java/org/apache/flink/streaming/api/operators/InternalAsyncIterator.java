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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapPartitionFunction;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * The {@link InternalAsyncIterator} is an iterator used in {@link MapPartitionOperator}. The task
 * main thread will add records to it. The user-defined function {@link MapPartitionFunction} will
 * use it as the input {@link Iterator} parameter and process records from it. It will cache the
 * context where last record is processed and continue to process from the context when the next
 * record is added.
 */
@Internal
public interface InternalAsyncIterator<IN> extends Iterator<IN> {

    /**
     * Register a user-defined function needed the {@link Iterator} as its input parameter to
     * process records.
     *
     * @param udf the user-defined function.
     */
    void registerUDF(Consumer<Iterator<IN>> udf);

    /** Add a record to the iterator. */
    void addRecord(IN record);

    /** Close the iterator. */
    void close();
}

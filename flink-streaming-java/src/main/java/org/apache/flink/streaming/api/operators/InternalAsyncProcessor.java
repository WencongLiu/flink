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

import java.util.function.Consumer;

/**
 * The {@link InternalAsyncProcessor} is invoked by task main thread in the operator. It's used to
 * process records asynchronously in the user-defined function with {@link Iterable} parameter. It
 * will store the context where last record is processed and continue to process from the context
 * when the next record arrives.
 */
public interface InternalAsyncProcessor<T> {

    /**
     * Register a user-defined function to the process records asynchronously. The udf will accept
     * an iterable as the input to process record by record.
     *
     * @param udf the user-defined function.
     */
    void registerUDF(Consumer<Iterable<T>> udf);

    /** Process a record asynchronously. */
    void processRecordAsync(T record);

    /** Close the processor. */
    void close();
}

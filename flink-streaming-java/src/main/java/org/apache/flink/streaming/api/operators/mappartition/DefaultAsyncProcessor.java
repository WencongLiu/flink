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

package org.apache.flink.streaming.api.operators.mappartition;

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/** The default implementation of {@link InternalAsyncProcessor}. */
public class DefaultAsyncProcessor<T>
        implements InternalAsyncProcessor<T>, Iterable<T>, Iterator<T> {

    private final Lock lock = new ReentrantLock();

    private final Condition producerCondition = lock.newCondition();

    private final Condition consumerCondition = lock.newCondition();

    private final ScheduledExecutorService asyncExecutor =
            Executors.newScheduledThreadPool(
                    1, new ExecutorThreadFactory("InternalAsyncProcessor"));

    private volatile boolean isEndOfInput = false;

    private volatile boolean isUDFEnded = false;

    private volatile T nextRecord = null;

    @Override
    public void registerUDF(Consumer<Iterable<T>> udf) {
        asyncExecutor.execute(
                () -> {
                    udf.accept(this);
                    runWithLock(
                            () -> {
                                isUDFEnded = true;
                                producerCondition.signal();
                            });
                });
    }

    @Override
    public void processRecordAsync(T record) {
        runWithLock(
                () -> {
                    if (isUDFEnded) {
                        return;
                    }
                    consumerCondition.signal();
                    if (nextRecord == null) {
                        nextRecord = record;
                    } else {
                        try {
                            producerCondition.await();
                        } catch (InterruptedException e) {
                            ExceptionUtils.rethrow(e);
                        }
                        if (isUDFEnded) {
                            return;
                        }
                        checkState(nextRecord == null);
                        nextRecord = record;
                    }
                });
    }

    @Override
    public void endOfInput() {
        runWithLock(
                () -> {
                    isEndOfInput = true;
                    consumerCondition.signal();
                });
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        AtomicBoolean hasNext = new AtomicBoolean(false);
        runWithLock(
                () -> {
                    if (nextRecord != null) {
                        hasNext.set(true);
                    } else if (isEndOfInput) {
                        hasNext.set(false);
                    } else {
                        try {
                            consumerCondition.await();
                        } catch (InterruptedException e) {
                            ExceptionUtils.rethrow(e);
                        }
                        hasNext.set(nextRecord != null);
                    }
                });
        return hasNext.get();
    }

    @Override
    public T next() {
        return supplyWithLock(
                () -> {
                    T record;
                    if (nextRecord != null) {
                        record = nextRecord;
                        nextRecord = null;
                        producerCondition.signal();
                        return record;
                    }
                    try {
                        consumerCondition.await();
                    } catch (InterruptedException e) {
                        ExceptionUtils.rethrow(e);
                    }
                    if(nextRecord == null){
                        checkState(isEndOfInput);
                        return null;
                    }
                    record = nextRecord;
                    nextRecord = null;
                    return record;
                });
    }

    private void runWithLock(Runnable runnable) {
        try {
            lock.lock();
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    private T supplyWithLock(Supplier<T> supplier) {
        T result;
        try {
            lock.lock();
            result = supplier.get();
        } finally {
            lock.unlock();
        }
        return result;
    }
}

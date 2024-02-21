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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/** The default implementation of {@link InternalAsyncIterator}. */
@Internal
public class InternalAsyncIteratorImpl<IN> implements InternalAsyncIterator<IN> {

    /**
     * Max number of caches.
     *
     * <p>The constant defines the maximum number of caches that can be created. Its value is set to
     * 100, which is considered sufficient for most parallel jobs. Each cache is a record and
     * occupies a minimal amount of memory so the value is not excessively large.
     */
    private static final int DEFAULT_MAX_CACHE_NUM = 100;

    /** The lock to ensure consistency between task main thread and udf executor. */
    private final Lock lock = new ReentrantLock();

    /** The condition of lock. */
    private final Condition condition = lock.newCondition();

    /** The task udf executor. */
    private final Executor udfExecutor =
            Executors.newFixedThreadPool(1, new ExecutorThreadFactory("TaskUDFExecutor"));

    /** The queue to store record caches. */
    private final Queue<IN> recordCaches = new LinkedList<>();

    /** The flag to represent the finished state of udf. */
    private volatile boolean isUDFFinished = false;

    /** The flag to represent the closed state of this iterator. */
    private volatile boolean isClosed = false;

    @Override
    public void registerUDF(Consumer<Iterator<IN>> udf) {
        checkState(!isClosed);
        udfExecutor.execute(
                () -> {
                    udf.accept(this);
                    runWithLock(
                            () -> {
                                isUDFFinished = true;
                                notifyCacheStatus();
                            });
                });
    }

    @Override
    public void addRecord(IN record) {
        runWithLock(
                () -> {
                    checkState(!isClosed);
                    if (isUDFFinished) {
                        return;
                    }
                    if (recordCaches.size() < DEFAULT_MAX_CACHE_NUM) {
                        recordCaches.add(record);
                        notifyCacheStatus();
                    } else {
                        waitToGetCacheStatus();
                        addRecord(record);
                    }
                });
    }

    @Override
    public void close() {
        runWithLock(
                () -> {
                    isClosed = true;
                    notifyCacheStatus();
                    if (!isUDFFinished) {
                        waitToGetCacheStatus();
                    }
                });
    }

    @Override
    public boolean hasNext() {
        return supplyWithLock(
                () -> {
                    if (recordCaches.size() > 0) {
                        return true;
                    } else if (isClosed) {
                        return false;
                    } else {
                        waitToGetCacheStatus();
                        return hasNext();
                    }
                });
    }

    @Override
    public IN next() {
        return supplyWithLock(
                () -> {
                    IN record;
                    if (recordCaches.size() > 0) {
                        record = recordCaches.poll();
                        if (!isClosed) {
                            notifyCacheStatus();
                        }
                        return record;
                    }
                    waitToGetCacheStatus();
                    if (recordCaches.size() == 0) {
                        checkState(isClosed);
                        return null;
                    }
                    return recordCaches.poll();
                });
    }

    /**
     * Notify task main thread or udf executor the latest status of record caches, including
     * following status:
     *
     * <p>1. The record caches have records.
     *
     * <p>2. The record caches are able to store more records,
     *
     * <p>3. There will be no more record to be stored in record caches.
     */
    private void notifyCacheStatus() {
        condition.signal();
    }

    /** Wait until the latest status of record caches is notified. */
    private void waitToGetCacheStatus() {
        try {
            condition.await();
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    private void runWithLock(Runnable runnable) {
        try {
            lock.lock();
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    private <ANY> ANY supplyWithLock(Supplier<ANY> supplier) {
        ANY result;
        try {
            lock.lock();
            result = supplier.get();
        } finally {
            lock.unlock();
        }
        return result;
    }
}

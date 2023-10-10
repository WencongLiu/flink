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

import java.util.Iterator;
import java.util.List;

/** Reads the cached data from a list of segments. */
public class RecordIterator<T> implements Iterator<T> {

    private final List<Iterator<T>> iteratorsList;

    private int iteratorIndex = 0;

    public RecordIterator(List<Iterator<T>> iteratorsList) {
        this.iteratorsList = iteratorsList;
    }

    @Override
    public boolean hasNext() {
        if (iteratorIndex >= iteratorsList.size()) {
            return false;
        }
        boolean hasNext = iteratorsList.get(iteratorIndex).hasNext();
        if(hasNext){
            return hasNext;
        }else {
            ++iteratorIndex;
            return hasNext();
        }
    }

    @Override
    public T next() {
        if (iteratorIndex >= iteratorsList.size()) {
            return null;
        }
        Iterator<T> currentIterator = iteratorsList.get(iteratorIndex);
        if(currentIterator.hasNext()){
            return currentIterator.next();
        }else {
            ++iteratorIndex;
            return next();
        }
    }
}

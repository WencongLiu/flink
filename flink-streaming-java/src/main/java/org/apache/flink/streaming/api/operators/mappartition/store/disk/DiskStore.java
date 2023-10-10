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

package org.apache.flink.streaming.api.operators.mappartition.store.disk;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.operators.mappartition.store.Store;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

/** @param <T> */
public class DiskStore<T> implements Store<T> {

    private final TypeSerializer<T> recordSerializer;

    private final OutputStream outputStream;

    private final DataOutputView outputView;

    private final Path path;

    private int recordCount = 0;

    public DiskStore(TypeSerializer<T> recordSerializer, Path path) {
        this.recordSerializer = recordSerializer;
        this.path = path;
        try {
            this.outputStream =
                    new BufferedOutputStream(
                            path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.outputView = new DataOutputViewStreamWrapper(outputStream);
    }

    public boolean addRecord(T record) throws IOException {
        recordSerializer.serialize(record, outputView);
        outputStream.flush();
        recordCount++;
        return true;
    }

    public Iterator<T> getRecordIterator() {
        return new FileStoreIterator<T>(recordCount, path, recordSerializer);
    }

    /**
     * The iterator of memory store.
     *
     * @param <T>
     */
    private static class FileStoreIterator<T> implements Iterator<T> {
        private final TypeSerializer<T> serializer;

        private final DataInputView inputView;

        private final int totalRecordCount;

        private int currentRecordIndex;

        FileStoreIterator(int totalRecordCount, Path path, TypeSerializer<T> recordSerializer) {
            this.totalRecordCount = totalRecordCount;
            this.currentRecordIndex = 0;
            BufferedInputStream bufferedInputStream;
            try {
                bufferedInputStream = new BufferedInputStream(path.getFileSystem().open(path));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.inputView = new DataInputViewStreamWrapper(bufferedInputStream);
            this.serializer = recordSerializer;
        }

        @Override
        public boolean hasNext() {
            return currentRecordIndex < totalRecordCount;
        }

        @Override
        public T next() {
            T value;
            try {
                value = serializer.deserialize(inputView);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            currentRecordIndex++;
            return value;
        }
    }
}

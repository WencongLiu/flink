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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Arrays;

/**
 * The {@link VariableLengthByteKeyAndValueComparator} is used to compare records according to both
 * the key value and record value. It uses binary format produced by the {@link
 * KeyAndValueSerializer}.
 *
 * <p>It assumes the length of record key is variable and serialized to be stored.
 */
final class VariableLengthByteKeyAndValueComparator<IN>
        extends TypeComparator<Tuple2<byte[], StreamRecord<IN>>> {
    private final TypeComparator<IN> valueComparator;
    private byte[] keyReference;
    private IN valueReference;

    public VariableLengthByteKeyAndValueComparator(TypeComparator<IN> valueComparator) {
        this.valueComparator = valueComparator;
    }

    @Override
    public int hash(Tuple2<byte[], StreamRecord<IN>> record) {
        return record.hashCode();
    }

    @Override
    public void setReference(Tuple2<byte[], StreamRecord<IN>> toCompare) {
        this.keyReference = toCompare.f0;
        this.valueReference = toCompare.f1.getValue();
    }

    @Override
    public boolean equalToReference(Tuple2<byte[], StreamRecord<IN>> candidate) {
        return Arrays.equals(keyReference, candidate.f0)
                && valueReference == candidate.f1.getValue();
    }

    @Override
    public int compareToReference(
            TypeComparator<Tuple2<byte[], StreamRecord<IN>>> referencedComparator) {
        byte[] otherKey =
                ((VariableLengthByteKeyAndValueComparator<IN>) referencedComparator).keyReference;
        IN otherValue =
                ((VariableLengthByteKeyAndValueComparator<IN>) referencedComparator).valueReference;
        int keyCmp = compare(otherKey, this.keyReference);
        if (keyCmp != 0) {
            return keyCmp;
        }
        return valueComparator.compare(this.valueReference, otherValue);
    }

    @Override
    public int compare(
            Tuple2<byte[], StreamRecord<IN>> first, Tuple2<byte[], StreamRecord<IN>> second) {
        int keyCmp = compare(first.f0, second.f0);
        if (keyCmp != 0) {
            return keyCmp;
        }
        return valueComparator.compare(first.f1.getValue(), second.f1.getValue());
    }

    private int compare(byte[] first, byte[] second) {
        int firstLength = first.length;
        int secondLength = second.length;
        int minLength = Math.min(firstLength, secondLength);
        for (int i = 0; i < minLength; i++) {
            int cmp = Byte.compare(first[i], second[i]);

            if (cmp != 0) {
                return cmp;
            }
        }

        return Integer.compare(firstLength, secondLength);
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        int firstLength = firstSource.readInt();
        int secondLength = secondSource.readInt();
        int minLength = Math.min(firstLength, secondLength);
        while (minLength-- > 0) {
            byte firstValue = firstSource.readByte();
            byte secondValue = secondSource.readByte();

            int cmp = Byte.compare(firstValue, secondValue);
            if (cmp != 0) {
                return cmp;
            }
        }
        int lengthCompare = Integer.compare(firstLength, secondLength);
        if (lengthCompare != 0) {
            return lengthCompare;
        } else {
            // skip the timestamp
            firstSource.readLong();
            secondSource.readLong();
            return valueComparator.compareSerialized(firstSource, secondSource);
        }
    }

    @Override
    public boolean supportsNormalizedKey() {
        return false;
    }

    @Override
    public int getNormalizeKeyLen() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return false;
    }

    @Override
    public void putNormalizedKey(
            Tuple2<byte[], StreamRecord<IN>> record,
            MemorySegment target,
            int offset,
            int numBytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean invertNormalizedKey() {
        return false;
    }

    @Override
    public TypeComparator<Tuple2<byte[], StreamRecord<IN>>> duplicate() {
        return new VariableLengthByteKeyAndValueComparator<>(this.valueComparator);
    }

    @Override
    public int extractKeys(Object record, Object[] target, int index) {
        target[index] = record;
        return 1;
    }

    @Override
    public TypeComparator<?>[] getFlatComparators() {
        return new TypeComparator[] {this};
    }

    @Override
    public boolean supportsSerializationWithKeyNormalization() {
        return false;
    }

    @Override
    public void writeWithKeyNormalization(
            Tuple2<byte[], StreamRecord<IN>> record, DataOutputView target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Tuple2<byte[], StreamRecord<IN>> readWithKeyDenormalization(
            Tuple2<byte[], StreamRecord<IN>> reuse, DataInputView source) {
        throw new UnsupportedOperationException();
    }
}

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

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.KeyFunctions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** The {@link SortPartitionOperator} is used to sort partitions. */
public class SortPartitionOperator<T> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<T, T>, BoundedOneInput {

    private final TypeInformation<T> inputType;

    private final List<T> allRecords = new ArrayList<>();

    private final Order sortOrder;

    private final int sortPositionField;

    private final String sortStringField;

    private final KeySelector<T, ?> keySelector;

    private long lastWatermarkTimestamp = Long.MIN_VALUE;

    private Ordering orderInformation;

    private TypeComparator<T> sortComparator;

    public SortPartitionOperator(
            TypeInformation<T> inputType, int sortPositionField, Order sortOrder) {
        this.inputType = checkInputType(inputType);
        ensureSortableKey(sortPositionField);
        this.sortPositionField = sortPositionField;
        this.sortStringField = null;
        this.keySelector = null;
        this.sortOrder = sortOrder;
    }

    public SortPartitionOperator(
            TypeInformation<T> inputType, String sortField, Order sortOrder) {
        this.inputType = checkInputType(inputType);
        ensureSortableKey(sortField);
        this.sortPositionField = -1;
        this.sortStringField = sortField;
        this.keySelector = null;
        this.sortOrder = sortOrder;
    }

    public <K> SortPartitionOperator(
            TypeInformation<T> inputType, KeySelector<T, K> keySelector, Order sortOrder) {
        this.inputType = checkInputType(inputType);
        ensureSortableKey(keySelector);
        this.sortPositionField = -1;
        this.sortStringField = null;
        this.keySelector = keySelector;
        this.sortOrder = sortOrder;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<T>> output) {
        super.setup(containingTask, config, output);
        // Initialize the order information.
        this.orderInformation = getOrderInformation();
        // Initialize the sort comparator.
        this.sortComparator = getSortComparator();
    }

    @Override
    public void endInput() throws Exception {
        TimestampedCollector<T> outputCollector = new TimestampedCollector<>(output);
        allRecords.sort(
                new Comparator<T>() {
                    @Override
                    public int compare(T o1, T o2) {
                        return sortComparator.compare(o1, o2);
                    }
                });
        for (T record : allRecords) {
            outputCollector.collect(record);
        }

        Watermark watermark = new Watermark(lastWatermarkTimestamp);
        if (getTimeServiceManager().isPresent()) {
            getTimeServiceManager().get().advanceWatermark(watermark);
        }
        outputCollector.emitWatermark(watermark);
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        allRecords.add(element.getValue());
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (lastWatermarkTimestamp > watermark.getTimestamp()) {
            throw new RuntimeException("Invalid watermark");
        }
        lastWatermarkTimestamp = watermark.getTimestamp();
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder()
                .setOutputOnEOF(true)
                .setInternalSorterSupported(true)
                .build();
    }

    private TypeComparator<T> getSortComparator() {
        int[] sortColumns = this.orderInformation.getFieldPositions();
        boolean[] sortOrderings = this.orderInformation.getFieldSortDirections();
        if (inputType instanceof CompositeType) {
            return ((CompositeType<T>) inputType)
                    .createComparator(sortColumns, sortOrderings, 0, getExecutionConfig());
        } else if (inputType instanceof AtomicType) {
            return ((AtomicType<T>) inputType)
                    .createComparator(sortOrderings[0], getExecutionConfig());
        } else {
            throw new UnsupportedOperationException("Sorting does not support type " + inputType);
        }
    }

    private <K> Ordering getOrderInformation() {
        int sortKeyPosition;
        if (sortPositionField != -1) {
            // The sort field is element in Tuple
            sortKeyPosition =
                    new Keys.ExpressionKeys<>(sortPositionField, inputType)
                            .computeLogicalKeyPositions()[0];
        } else if (sortStringField != null) {
            // The sort field is field in Pojo object
            sortKeyPosition =
                    new Keys.ExpressionKeys<>(sortStringField, inputType)
                            .computeLogicalKeyPositions()[0];
        } else {
            // The sort field is the selected key by KeySelector
            TypeInformation<?> keyType = TypeExtractor.getKeySelectorTypes(keySelector, inputType);
            Keys.SelectorFunctionKeys<T, K> keys =
                    new Keys.SelectorFunctionKeys<>(
                            (KeySelector<T, K>) keySelector,
                            inputType,
                            (TypeInformation<K>) keyType);
            TypeInformation<Tuple2<K, T>> typeInfoWithKey = KeyFunctions.createTypeWithKey(keys);
            Keys.ExpressionKeys<Tuple2<K, T>> newKey =
                    new Keys.ExpressionKeys<>(0, typeInfoWithKey);
            sortKeyPosition = newKey.computeLogicalKeyPositions()[0];
        }
        Ordering orderInformation = new Ordering();
        return orderInformation.appendOrdering(sortKeyPosition, null, sortOrder);
    }

    private void ensureSortableKey(int field) throws InvalidProgramException {
        if (!Keys.ExpressionKeys.isSortKey(field, inputType)) {
            throw new InvalidProgramException("Selected sort key is not a sortable type");
        }
    }

    private void ensureSortableKey(String field) throws InvalidProgramException {
        if (!Keys.ExpressionKeys.isSortKey(field, inputType)) {
            throw new InvalidProgramException("Selected sort key is not a sortable type");
        }
    }

    private <K> void ensureSortableKey(KeySelector<T, K> keySelector) {
        TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keySelector, inputType);
        Keys.SelectorFunctionKeys<T, K> sortKey =
                new Keys.SelectorFunctionKeys<>(keySelector, inputType, keyType);
        if (!sortKey.getKeyType().isSortKeyType()) {
            throw new InvalidProgramException("Selected sort key is not a sortable type");
        }
    }

    private TypeInformation<T> checkInputType(TypeInformation<T> inputType) {
        if (inputType instanceof MissingTypeInfo) {
            MissingTypeInfo typeInfo = (MissingTypeInfo) inputType;
            throw new InvalidTypesException(
                    "The return type of function '"
                            + typeInfo.getFunctionName()
                            + "' could not be determined automatically, due to type erasure. "
                            + "You can give type information hints by using the returns(...) method on the result of "
                            + "the transformation call, or by letting your function implement the 'ResultTypeQueryable' "
                            + "interface.",
                    typeInfo.getTypeException());
        }
        return inputType;
    }
}

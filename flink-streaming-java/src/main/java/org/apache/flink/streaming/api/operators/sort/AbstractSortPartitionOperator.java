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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * The {@link AbstractSortPartitionOperator} is the base class of sort partition operator.
 *
 * @param <INPUT_TYPE> The type of input record.
 * @param <SORT_TYPE> The type used to sort the records.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractSortPartitionOperator<INPUT_TYPE, SORT_TYPE>
        extends AbstractStreamOperator<INPUT_TYPE>
        implements OneInputStreamOperator<INPUT_TYPE, INPUT_TYPE>, BoundedOneInput {

    /** The type information of input records. */
    protected final TypeInformation<INPUT_TYPE> inputType;

    /** The type information used to sort the records. */
    protected final TypeInformation<SORT_TYPE> sortType;

    /** The order to sort records. */
    private final Order sortOrder;

    /** The string field to indicate the sort key for records with tuple or pojo type. */
    private final String stringSortField;

    /** The int field to indicate the sort key for records with tuple type. */
    private final int positionSortField;

    /** The selector to create the sort key for records. */
    protected final KeySelector<INPUT_TYPE, ?> sortFieldSelector;

    /** The sorter to sort records. */
    protected PushSorter<SORT_TYPE> sorter = null;

    public AbstractSortPartitionOperator(TypeInformation<INPUT_TYPE> inputType, Order sortOrder) {
        this.inputType = inputType;
        this.sortType = (TypeInformation<SORT_TYPE>) inputType;
        this.positionSortField = -1;
        this.stringSortField = null;
        this.sortFieldSelector = null;
        this.sortOrder = sortOrder;
    }

    public AbstractSortPartitionOperator(
            TypeInformation<INPUT_TYPE> inputType, int positionSortField, Order sortOrder) {
        this.inputType = inputType;
        ensureFieldSortable(positionSortField);
        this.sortType = (TypeInformation<SORT_TYPE>) inputType;
        this.positionSortField = positionSortField;
        this.stringSortField = null;
        this.sortFieldSelector = null;
        this.sortOrder = sortOrder;
    }

    public AbstractSortPartitionOperator(
            TypeInformation<INPUT_TYPE> inputType, String stringSortField, Order sortOrder) {
        this.inputType = inputType;
        ensureFieldSortable(stringSortField);
        this.sortType = (TypeInformation<SORT_TYPE>) inputType;
        this.positionSortField = -1;
        this.stringSortField = stringSortField;
        this.sortFieldSelector = null;
        this.sortOrder = sortOrder;
    }

    public <K> AbstractSortPartitionOperator(
            TypeInformation<INPUT_TYPE> inputType,
            KeySelector<INPUT_TYPE, K> sortFieldSelector,
            Order sortOrder) {
        this.inputType = inputType;
        ensureFieldSortable(sortFieldSelector);
        this.sortType =
                (TypeInformation<SORT_TYPE>)
                        Types.TUPLE(
                                TypeExtractor.getKeySelectorTypes(sortFieldSelector, inputType),
                                inputType);
        this.positionSortField = -1;
        this.stringSortField = null;
        this.sortFieldSelector = sortFieldSelector;
        this.sortOrder = sortOrder;
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder()
                .setOutputOnEOF(true)
                .setInternalSorterSupported(true)
                .build();
    }

    /**
     * Get the sorter.
     *
     * @return the sorter.
     */
    <T> PushSorter<T> getSorter(
            TypeSerializer<T> typeSerializer,
            TypeComparator<T> typeComparator,
            StreamTask<?, ?> streamTask,
            MemoryManager memoryManager,
            ExecutionConfig executionConfig,
            IOManager ioManager) {
        ClassLoader userCodeClassLoader = streamTask.getUserCodeClassLoader();
        Configuration jobConfiguration = streamTask.getEnvironment().getJobConfiguration();
        double managedMemoryFraction =
                config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        streamTask.getEnvironment().getTaskConfiguration(),
                        userCodeClassLoader);
        try {
            return ExternalSorter.newBuilder(
                            memoryManager,
                            streamTask,
                            typeSerializer,
                            typeComparator,
                            executionConfig)
                    .memoryFraction(managedMemoryFraction)
                    .enableSpilling(
                            ioManager,
                            jobConfiguration.get(AlgorithmOptions.SORT_SPILLING_THRESHOLD))
                    .maxNumFileHandles(jobConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN))
                    .objectReuse(executionConfig.isObjectReuseEnabled())
                    .largeRecords(jobConfiguration.get(AlgorithmOptions.USE_LARGE_RECORDS_HANDLER))
                    .build();
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the comparator for sort type.
     *
     * @return the comparator for sort type.
     */
    protected TypeComparator<SORT_TYPE> getSortTypeComparator() {
        // 1.Initialize the logical sort field index.
        int[] logicalSortFieldIndex = new int[1];
        if (positionSortField != -1) {
            logicalSortFieldIndex[0] =
                    new Keys.ExpressionKeys<>(positionSortField, inputType)
                            .computeLogicalKeyPositions()[0];
        } else if (stringSortField != null) {
            logicalSortFieldIndex[0] =
                    new Keys.ExpressionKeys<>(stringSortField, inputType)
                            .computeLogicalKeyPositions()[0];
        }

        // 2.Initialize the sort order.
        boolean[] sortOrderIndicator = new boolean[1];
        sortOrderIndicator[0] = this.sortOrder == Order.ASCENDING;

        // 3.Create the type comparator.
        if (sortType instanceof CompositeType) {
            return ((CompositeType<SORT_TYPE>) sortType)
                    .createComparator(
                            logicalSortFieldIndex, sortOrderIndicator, 0, getExecutionConfig());
        } else if (sortType instanceof AtomicType) {
            return ((AtomicType<SORT_TYPE>) sortType)
                    .createComparator(this.sortOrder == Order.ASCENDING, getExecutionConfig());
        } else {
            throw new UnsupportedOperationException(sortType + " doesn't support sorting.");
        }
    }

    private void ensureFieldSortable(int field) throws InvalidProgramException {
        if (!Keys.ExpressionKeys.isSortKey(field, inputType)) {
            throw new InvalidProgramException(
                    "The field " + field + " of input type " + inputType + " is not sortable.");
        }
    }

    private void ensureFieldSortable(String field) throws InvalidProgramException {
        if (!Keys.ExpressionKeys.isSortKey(field, inputType)) {
            throw new InvalidProgramException(
                    "The field " + field + " of input type " + inputType + " is not sortable.");
        }
    }

    private <K> void ensureFieldSortable(KeySelector<INPUT_TYPE, K> keySelector) {
        TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keySelector, inputType);
        Keys.SelectorFunctionKeys<INPUT_TYPE, K> sortKey =
                new Keys.SelectorFunctionKeys<>(keySelector, inputType, keyType);
        if (!sortKey.getKeyType().isSortKeyType()) {
            throw new InvalidProgramException("The key type " + keyType + " is not sortable.");
        }
    }
}

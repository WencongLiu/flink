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

package org.apache.flink.table.gateway.rest.message.parameters;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.utils.ParseTools;

/** {@link MessagePathParameter} that parse the {@link OperationHandle}. */
public class OperationHandleIdPathParameter extends MessagePathParameter<OperationHandle> {

    public static final String KEY = "operation_handle";

    public OperationHandleIdPathParameter() {
        super(KEY);
    }

    @Override
    protected OperationHandle convertFromString(String sessionHandleId) throws ConversionException {
        return ParseTools.parseStringToOperationHandler(sessionHandleId);
    }

    @Override
    protected String convertToString(OperationHandle operationHandle) {
        return operationHandle.getIdentifier().getFullID();
    }

    @Override
    public String getDescription() {
        return "The OperationHandle that identifies a operation.";
    }
}

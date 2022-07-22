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

package org.apache.flink.table.gateway.rest.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationType;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.message.MockedCreateOperationHeaders;
import org.apache.flink.table.gateway.rest.message.MockedCreateOperationResponseBody;
import org.apache.flink.table.gateway.rest.message.parameters.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.parameters.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Mocked handler for test. */
public class MockedCreateOperationHandler
        extends AbstractSqlGatewayRestHandler<
                EmptyRequestBody, MockedCreateOperationResponseBody, SessionMessageParameters> {

    public MockedCreateOperationHandler(
            SqlGatewayService service,
            Time timeout,
            Map<String, String> responseHeaders,
            MockedCreateOperationHeaders messageHeaders) {
        super(service, timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<MockedCreateOperationResponseBody> handleRequest(
            SqlGatewayRestAPIVersion version, @Nonnull HandlerRequest<EmptyRequestBody> request) {
        SessionHandle sessionHandle = request.getPathParameter(SessionHandleIdPathParameter.class);
        OperationHandle operationHandle =
                service.submitOperation(
                        sessionHandle,
                        OperationType.UNKNOWN,
                        () -> {
                            try {
                                TimeUnit.SECONDS.sleep(10);
                            } catch (InterruptedException ignored) {
                            }
                            return ResultSet.NOT_READY_RESULTS;
                        });

        return CompletableFuture.completedFuture(
                new MockedCreateOperationResponseBody(operationHandle.getIdentifier().getFullID()));
    }
}

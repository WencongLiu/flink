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

package org.apache.flink.table.gateway.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.ParseTools;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.MockedCreateOperationHeaders;
import org.apache.flink.table.gateway.rest.message.MockedCreateOperationResponseBody;
import org.apache.flink.table.gateway.rest.message.operation.CancelOperationHeaders;
import org.apache.flink.table.gateway.rest.message.operation.CloseOperationHeaders;
import org.apache.flink.table.gateway.rest.message.operation.GetOperationStatusHeaders;
import org.apache.flink.table.gateway.rest.message.operation.OperationStatusResponseBody;
import org.apache.flink.table.gateway.rest.message.parameters.OperationMessageParameters;
import org.apache.flink.table.gateway.rest.message.parameters.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.rest.utils.MockedSqlGatewayRestEndpoint;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.GATEWAY_ENDPOINT_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test basic logic of {@link SqlGatewayRestEndpoint} with handlers inherited from {@link
 * AbstractSqlGatewayRestHandler}.
 */
public class OperationCaseITTest extends RestITCaseBase {

    private final String sessionName = "test";
    private static final Map<String, String> properties = new HashMap<>();

    static {
        properties.put("k1", "v1");
        properties.put("k2", "v2");
        Configuration flinkConfig =
                new DelegatingConfiguration(context.getFlinkConfig(), GATEWAY_ENDPOINT_PREFIX);
        try {
            sqlGatewayRestEndpoint =
                    new MockedSqlGatewayRestEndpoint(flinkConfig, sqlGatewayService);
            sqlGatewayRestEndpoint.start();
            TimeUnit.SECONDS.sleep(3);
        } catch (Exception e) {
            throw new SqlGatewayException("Cannot create sql gateway.");
        }
        address = sqlGatewayRestEndpoint.getServerAddress();
    }

    OpenSessionHeaders openSessionHeaders = OpenSessionHeaders.getInstance();
    OpenSessionRequestBody openSessionRequestBody =
            new OpenSessionRequestBody(sessionName, properties);
    EmptyMessageParameters emptyParameters = EmptyMessageParameters.getInstance();

    MockedCreateOperationHeaders mockedCreateOperationHeaders =
            MockedCreateOperationHeaders.getInstance();
    EmptyRequestBody emptyRequestBody = EmptyRequestBody.getInstance();
    GetOperationStatusHeaders getOperationStatusHeaders = GetOperationStatusHeaders.getInstance();
    CancelOperationHeaders cancelOperationHeaders = CancelOperationHeaders.getInstance();
    CloseOperationHeaders closeOperationHeaders = CloseOperationHeaders.getInstance();

    @Test
    public void testWhenSubmitOperation() throws Exception {
        submitOperation();
    }

    @Test
    public void testOperationRelatedApis() throws Exception {
        List<String> ids;
        String status;
        // Get the RUNNING status when an operation is submitted.
        ids = submitOperation();
        status = getOperationStatus(ids);
        assertEquals(OperationStatus.RUNNING.toString(), status);
        // Get the CANCELED status when an operation is canceled.
        ids = submitOperation();
        status = cancelOperation(ids);
        assertEquals(OperationStatus.CANCELED.toString(), status);
        status = getOperationStatus(ids);
        assertEquals(OperationStatus.CANCELED.toString(), status);
        // Get the CLOSED status when an operation is closed.
        ids = submitOperation();
        status = closeOperation(ids);
        assertEquals(OperationStatus.CLOSED.toString(), status);
        SessionHandle sessionHandle = ParseTools.parseStringToSessinHandler(ids.get(0));
        OperationHandle operationHandle = ParseTools.parseStringToOperationHandler(ids.get(1));
        assertThrows(
                SqlGatewayException.class,
                () ->
                        sessionManager
                                .getSession(sessionHandle)
                                .getOperationManager()
                                .getOperation(operationHandle));
    }

    public List<String> submitOperation() throws Exception {
        CompletableFuture<OpenSessionResponseBody> response =
                sendRequest(openSessionHeaders, emptyParameters, openSessionRequestBody);
        String sessionHandleId = response.get().getSessionHandle();
        assertNotNull(sessionHandleId);
        SessionHandle sessionHandle = ParseTools.parseStringToSessinHandler(sessionHandleId);
        assertNotNull(
                sessionManager.getSession(ParseTools.parseStringToSessinHandler(sessionHandleId)));
        SessionMessageParameters sessionMessageParameters =
                new SessionMessageParameters(sessionHandle);
        CompletableFuture<MockedCreateOperationResponseBody> response2 =
                sendRequest(
                        mockedCreateOperationHeaders, sessionMessageParameters, emptyRequestBody);

        String operationHandleId = response2.get().getOperationHandle();
        assertNotNull(operationHandleId);
        return Arrays.asList(sessionHandleId, operationHandleId);
    }

    public String getOperationStatus(List<String> ids) throws Exception {
        String sessionId = ids.get(0);
        String operationId = ids.get(1);
        OperationMessageParameters operationMessageParameters =
                new OperationMessageParameters(
                        ParseTools.parseStringToSessinHandler(sessionId),
                        ParseTools.parseStringToOperationHandler(operationId));
        CompletableFuture<OperationStatusResponseBody> future =
                sendRequest(
                        getOperationStatusHeaders, operationMessageParameters, emptyRequestBody);
        return future.get().getStatus();
    }

    public String cancelOperation(List<String> ids) throws Exception {
        CompletableFuture<OperationStatusResponseBody> future =
                sendRequest(cancelOperationHeaders, getMessageParameters(ids), emptyRequestBody);
        return future.get().getStatus();
    }

    public String closeOperation(List<String> ids) throws Exception {
        CompletableFuture<OperationStatusResponseBody> future =
                sendRequest(closeOperationHeaders, getMessageParameters(ids), emptyRequestBody);
        return future.get().getStatus();
    }

    public OperationMessageParameters getMessageParameters(List<String> ids) throws Exception {
        String sessionId = ids.get(0);
        String operationId = ids.get(1);
        return new OperationMessageParameters(
                ParseTools.parseStringToSessinHandler(sessionId),
                ParseTools.parseStringToOperationHandler(operationId));
    }
}

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
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.ParseTools;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.parameters.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.session.CloseSessionHeaders;
import org.apache.flink.table.gateway.rest.message.session.CloseSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.GetSessionConfigHeaders;
import org.apache.flink.table.gateway.rest.message.session.GetSessionConfigResponseBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.TriggerSessionHeartbeatHeaders;
import org.apache.flink.table.gateway.service.session.Session;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.GATEWAY_ENDPOINT_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test basic logic of {@link SqlGatewayRestEndpoint} with handlers inherited from {@link
 * AbstractSqlGatewayRestHandler}.
 */
public class SessionCaseITTest extends RestITCaseBase {

    private final String sessionName = "test";
    private static final Map<String, String> properties = new HashMap<>();
    private static final int SESSION_NUMBER = 5;

    static {
        properties.put("k1", "v1");
        properties.put("k2", "v2");
        Configuration flinkConfig =
                new DelegatingConfiguration(context.getFlinkConfig(), GATEWAY_ENDPOINT_PREFIX);
        try {
            sqlGatewayRestEndpoint = new SqlGatewayRestEndpoint(flinkConfig, sqlGatewayService);
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

    CloseSessionHeaders closeSessionHeaders = CloseSessionHeaders.getInstance();
    EmptyRequestBody emptyRequestBody = EmptyRequestBody.getInstance();
    SessionMessageParameters sessionMessageParameters;

    @Test
    public void testWhenCreateMultiSessions() throws Exception {
        for (int num = 0; num < SESSION_NUMBER; ++num) {
            CompletableFuture<OpenSessionResponseBody> response =
                    sendRequest(openSessionHeaders, emptyParameters, openSessionRequestBody);
            String sessionHandleId = response.get().getSessionHandle();
            assertNotNull(sessionHandleId);
            assertNotNull(
                    sessionManager.getSession(
                            ParseTools.parseStringToSessinHandler(sessionHandleId)));
        }
    }

    @Test
    public void testWhenCreateAndCloseSessions() throws Exception {
        List<SessionHandle> sessionHandles = new ArrayList<>();
        for (int num = 0; num < SESSION_NUMBER; ++num) {
            CompletableFuture<OpenSessionResponseBody> response =
                    sendRequest(openSessionHeaders, emptyParameters, openSessionRequestBody);
            String sessionHandleId = response.get().getSessionHandle();
            assertNotNull(sessionHandleId);
            SessionHandle sessionHandle = ParseTools.parseStringToSessinHandler(sessionHandleId);
            assertNotNull(sessionManager.getSession(sessionHandle));
            sessionHandles.add(sessionHandle);
        }

        for (int num = 0; num < SESSION_NUMBER; ++num) {
            SessionHandle sessionHandle = sessionHandles.get(num);
            sessionMessageParameters = new SessionMessageParameters(sessionHandle);
            CompletableFuture<CloseSessionResponseBody> response =
                    sendRequest(closeSessionHeaders, sessionMessageParameters, emptyRequestBody);
            String status = response.get().getStatus();
            assertEquals(status, "CLOSED");
            assertThrows(SqlGatewayException.class, () -> sessionManager.getSession(sessionHandle));
        }
    }

    @Test
    public void testWhenGetSessionConfiguration() throws Exception {
        CompletableFuture<OpenSessionResponseBody> response =
                sendRequest(openSessionHeaders, emptyParameters, openSessionRequestBody);
        String sessionHandleId = response.get().getSessionHandle();
        assertNotNull(sessionHandleId);
        SessionHandle sessionHandle = ParseTools.parseStringToSessinHandler(sessionHandleId);
        assertNotNull(sessionManager.getSession(sessionHandle));

        sessionMessageParameters = new SessionMessageParameters(sessionHandle);
        CompletableFuture<GetSessionConfigResponseBody> future =
                sendRequest(
                        GetSessionConfigHeaders.getInstance(),
                        sessionMessageParameters,
                        emptyRequestBody);
        Map<String, String> getProperties = future.get().getProperties();
        for (String key : properties.keySet()) {
            assertEquals(properties.get(key), getProperties.get(key));
        }
    }

    @Test
    public void testTouchSession() throws Exception {
        CompletableFuture<OpenSessionResponseBody> response =
                sendRequest(openSessionHeaders, emptyParameters, openSessionRequestBody);
        String sessionHandleId = response.get().getSessionHandle();
        assertNotNull(sessionHandleId);
        SessionHandle sessionHandle = ParseTools.parseStringToSessinHandler(sessionHandleId);
        Session session = sessionManager.getSession(sessionHandle);
        assertNotNull(session);

        long lastAccessTime = session.getLastAccessTime();

        sessionMessageParameters = new SessionMessageParameters(sessionHandle);
        CompletableFuture<EmptyResponseBody> future =
                sendRequest(
                        TriggerSessionHeartbeatHeaders.getInstance(),
                        sessionMessageParameters,
                        emptyRequestBody);
        future.get();
        assertTrue(session.getLastAccessTime() > lastAccessTime);
    }
}

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
import org.apache.flink.table.gateway.rest.message.MockedHeaders;
import org.apache.flink.table.gateway.rest.message.MockedRequestBody;
import org.apache.flink.table.gateway.rest.message.MockedResponseBody;
import org.apache.flink.table.gateway.rest.utils.MockedSqlGatewayRestEndpoint;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.gateway.rest.message.MockedRequestBody.OPTION1;
import static org.apache.flink.table.gateway.rest.message.MockedRequestBody.OPTION2;
import static org.apache.flink.table.gateway.rest.message.MockedResponseBody.FAILED_FLAG;
import static org.apache.flink.table.gateway.rest.message.MockedResponseBody.SUCCEED_FLAG;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.GATEWAY_ENDPOINT_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Mocked rest case test. */
public class RestMockedCaseITTest extends RestITCaseBase {

    private static final int WATT_SERVICE_START_SECONDS = 3;

    @Test
    public void testGetLegalResultWhenSendMockedRequest() throws Exception {
        Configuration flinkConfig =
                new DelegatingConfiguration(context.getFlinkConfig(), GATEWAY_ENDPOINT_PREFIX);
        sqlGatewayRestEndpoint = new MockedSqlGatewayRestEndpoint(flinkConfig, sqlGatewayService);
        sqlGatewayRestEndpoint.start();
        TimeUnit.SECONDS.sleep(WATT_SERVICE_START_SECONDS);
        address = sqlGatewayRestEndpoint.getServerAddress();
        CompletableFuture<MockedResponseBody> mockedFuture1 =
                sendRequest(
                        MockedHeaders.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        new MockedRequestBody(OPTION1));
        assertEquals(mockedFuture1.get().getStatus(), SUCCEED_FLAG);
        CompletableFuture<MockedResponseBody> mockedFuture2 =
                sendRequest(
                        MockedHeaders.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        new MockedRequestBody(OPTION2));
        assertEquals(mockedFuture2.get().getStatus(), FAILED_FLAG);
    }
}

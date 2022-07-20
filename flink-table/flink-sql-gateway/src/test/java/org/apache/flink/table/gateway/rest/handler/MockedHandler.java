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
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.rest.message.MockedRequestBody;
import org.apache.flink.table.gateway.rest.message.MockedResponseBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.table.gateway.rest.message.MockedRequestBody.OPTION1;

/** Mocked handler for test. */
public class MockedHandler
        extends AbstractSqlGatewayRestHandler<
                MockedRequestBody, MockedResponseBody, EmptyMessageParameters> {

    public MockedHandler(
            SqlGatewayService service,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<MockedRequestBody, MockedResponseBody, EmptyMessageParameters>
                    messageHeaders) {
        super(service, timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<MockedResponseBody> handleRequest(
            SqlGatewayRestAPIVersion version, @Nonnull HandlerRequest<MockedRequestBody> request) {
        MockedRequestBody requestBody = request.getRequestBody();
        String mockedField = requestBody.getMockedField();
        assert mockedField != null;
        if (mockedField.equals(OPTION1)) {
            return CompletableFuture.completedFuture(
                    new MockedResponseBody(MockedResponseBody.SUCCEED_FLAG));
        } else {
            return CompletableFuture.completedFuture(
                    new MockedResponseBody(MockedResponseBody.FAILED_FLAG));
        }
    }
}

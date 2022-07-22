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

package org.apache.flink.table.gateway.rest.message;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.message.parameters.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.parameters.SessionMessageParameters;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Message headers for mock test. */
public class MockedCreateOperationHeaders
        implements SqlGatewayMessageHeaders<
                EmptyRequestBody, MockedCreateOperationResponseBody, SessionMessageParameters> {

    public static final String URL = "/create_operation/:" + SessionHandleIdPathParameter.KEY;
    private static final MockedCreateOperationHeaders INSTANCE = new MockedCreateOperationHeaders();

    private MockedCreateOperationHeaders() {}

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Class<MockedCreateOperationResponseBody> getResponseClass() {
        return MockedCreateOperationResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Mock for test";
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public SessionMessageParameters getUnresolvedMessageParameters() {
        return new SessionMessageParameters();
    }

    public static MockedCreateOperationHeaders getInstance() {
        return INSTANCE;
    }
}

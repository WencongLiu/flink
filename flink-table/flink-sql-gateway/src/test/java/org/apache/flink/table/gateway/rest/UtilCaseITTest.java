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
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.handler.utilities.GetInfoHandler;
import org.apache.flink.table.gateway.rest.message.utilities.GetApiVersionResponseBody;
import org.apache.flink.table.gateway.rest.message.utilities.GetApiVersonHeaders;
import org.apache.flink.table.gateway.rest.message.utilities.GetInfoHeaders;
import org.apache.flink.table.gateway.rest.message.utilities.GetInfoResponseBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.GATEWAY_ENDPOINT_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test basic logic of {@link SqlGatewayRestEndpoint} with handlers inherited from {@link
 * AbstractSqlGatewayRestHandler}.
 */
public class UtilCaseITTest extends RestITCaseBase {

    static {
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

    GetInfoHeaders getInfoHeaders = GetInfoHeaders.getInstance();
    EmptyRequestBody emptyRequestBody = EmptyRequestBody.getInstance();
    EmptyMessageParameters emptyParameters = EmptyMessageParameters.getInstance();

    GetApiVersonHeaders getApiVersonHeaders = GetApiVersonHeaders.getInstance();

    @Test
    public void testWhenGetInfoAndApiVersion() throws Exception {
        CompletableFuture<GetInfoResponseBody> response =
                sendRequest(getInfoHeaders, emptyParameters, emptyRequestBody);
        String productName = response.get().getProductName();
        String version = response.get().getProductVersion();
        assertEquals(GetInfoHandler.PRODUCT_NAME, productName);
        assertEquals(EnvironmentInformation.getVersion(), version);

        CompletableFuture<GetApiVersionResponseBody> response2 =
                sendRequest(getApiVersonHeaders, emptyParameters, emptyRequestBody);
        List<String> versions = response2.get().getVersions();
        assertEquals(Collections.singletonList(SqlGatewayRestAPIVersion.V1.toString()), versions);
    }
}

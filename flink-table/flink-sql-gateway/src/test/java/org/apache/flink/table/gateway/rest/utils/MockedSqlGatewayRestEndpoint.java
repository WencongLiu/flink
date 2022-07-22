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

package org.apache.flink.table.gateway.rest.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpoint;
import org.apache.flink.table.gateway.rest.handler.MockedCreateOperationHandler;
import org.apache.flink.table.gateway.rest.handler.MockedHandler;
import org.apache.flink.table.gateway.rest.handler.session.OpenSessionHandler;
import org.apache.flink.table.gateway.rest.message.MockedCreateOperationHeaders;
import org.apache.flink.table.gateway.rest.message.MockedHeaders;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionHeaders;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The mocked sql gateway rest endpoint for test. */
public class MockedSqlGatewayRestEndpoint extends SqlGatewayRestEndpoint {

    public MockedSqlGatewayRestEndpoint(
            ReadableConfig configuration, SqlGatewayService sqlGatewayService)
            throws IOException, ConfigurationException {
        super(configuration, sqlGatewayService);
    }

    @Override
    protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(
            CompletableFuture<String> localAddressFuture) {
        List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers =
                new ArrayList<>(32);
        MockedHandler mockedHandler =
                new MockedHandler(service, timeout, responseHeaders, MockedHeaders.getInstance());

        OpenSessionHandler openSessionHandler =
                new OpenSessionHandler(
                        service, timeout, responseHeaders, OpenSessionHeaders.getInstance());
        handlers.add(Tuple2.of(OpenSessionHeaders.getInstance(), openSessionHandler));

        MockedCreateOperationHandler mockedCreateOperationHandler =
                new MockedCreateOperationHandler(
                        service,
                        timeout,
                        responseHeaders,
                        MockedCreateOperationHeaders.getInstance());

        handlers.add(Tuple2.of(MockedHeaders.getInstance(), mockedHandler));
        handlers.add(
                Tuple2.of(
                        MockedCreateOperationHeaders.getInstance(), mockedCreateOperationHandler));

        addOperationRelatedHandlers(handlers);
        return handlers;
    }
}

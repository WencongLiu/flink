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

package org.apache.flink.table.gateway.rest.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/** Options to configure {@link org.apache.flink.table.gateway.rest.SqlGatewayRestEndpoint}. */
@PublicEvolving
public class SqlGatewayRestOptions {

    /** The key name of port option. */
    private static final String REST_PORT_KEY = "rest.port";

    /**
     * The prefix for all sql gateway options, which will be used in {@link DelegatingConfiguration}
     * during the initialization of sql gateway server.
     */
    public static final String GATEWAY_ENDPOINT_PREFIX = "sql-gateway.endpoint.";

    /** The address that should be used by clients to connect to the sql gateway server. */
    public static final ConfigOption<String> ADDRESS =
            key("rest.address")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The address that should be used by clients to connect to the sql gateway server.");

    /** The address that the sql gateway server binds itself to. */
    public static final ConfigOption<String> BIND_ADDRESS =
            key("rest.bind-address")
                    .stringType()
                    .defaultValue("0.0.0.0")
                    .withFallbackKeys(SqlGatewayRestOptions.ADDRESS.key())
                    .withDescription("The address that the sql gateway server binds itself.");

    /** The port range that the sql gateway server could bind itself to. */
    public static final ConfigOption<String> BIND_PORT =
            key("rest.bind-port")
                    .stringType()
                    .defaultValue("8083")
                    .withFallbackKeys(REST_PORT_KEY)
                    .withDescription(
                            "The port that the sql gateway server binds itself. Accepts a list of ports (“50100,50101”), ranges"
                                    + " (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid"
                                    + " collisions when multiple sql gateway servers are running on the same machine.");

    /**
     * The port that the client connects to and the sql gateway server binds to if {@link
     * #BIND_PORT} has not been specified.
     */
    public static final ConfigOption<Integer> PORT =
            key(REST_PORT_KEY)
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The port that the client connects to. If %s has not been specified, then the sql gateway server will bind to this port.",
                                            text(BIND_PORT.key()))
                                    .build());
}

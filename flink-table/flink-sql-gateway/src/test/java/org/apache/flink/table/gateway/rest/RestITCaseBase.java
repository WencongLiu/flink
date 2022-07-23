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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.session.SessionManager;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;

/** Base IT case test class. */
public class RestITCaseBase {

    public static final String CONF_FILE_NAME = "flink-conf.yaml";
    public static final String REST_ADDRESS_CONFIG_NAME = "sql-gateway.endpoint.rest.address";
    public static final String REST_BIND_ADDRESS_CONFIG_NAME =
            "sql-gateway.endpoint.rest.bind-address";
    public static final String REST_PORT_CONFIG_NAME = "sql-gateway.endpoint.rest.port";
    public static final String REST_BIND_PORT_CONFIG_NAME = "sql-gateway.endpoint.rest.bind-port";

    public static SqlGatewayRestEndpoint sqlGatewayRestEndpoint;
    public static SqlGatewayService sqlGatewayService;
    public static SessionManager sessionManager;
    public static TemporaryFolder temporaryFolder;
    public static InetSocketAddress address;
    public static DefaultContext context;

    public static RestClient restClient;
    public static ExecutorService executor;

    private static final int NUM_TMS = 2;
    private static final int NUM_SLOTS_PER_TM = 2;

    @BeforeAll
    public static void beforeClass() throws Exception {
        // prepare conf dir
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        File confFolder = temporaryFolder.newFolder("conf");
        File confYaml = new File(confFolder, CONF_FILE_NAME);
        if (!confYaml.createNewFile()) {
            throw new IOException("Can't create testing flink-conf.yaml file.");
        }

        Map<String, String> map = new HashMap<>(System.getenv());
        map.put(ENV_FLINK_CONF_DIR, confFolder.getAbsolutePath());
        CommonTestUtils.setEnv(map);
        Properties properties = new Properties();
        properties.setProperty(REST_ADDRESS_CONFIG_NAME, "127.0.0.1");
        properties.setProperty(REST_PORT_CONFIG_NAME, String.format("%s-%s", 8000, 9000));

        MiniClusterWithClientResource miniClusterWithClientResource = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(getMiniClusterConfig())
                        .setNumberTaskManagers(NUM_TMS)
                        .setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
                        .build());
        miniClusterWithClientResource.before();
        ClusterClient clusterClient = miniClusterWithClientResource.getClusterClient();
        Configuration flinkConfiguration = clusterClient.getFlinkConfiguration();
        flinkConfiguration.addAll(ConfigurationUtils.createConfiguration(properties));

        context = DefaultContext.load(flinkConfiguration.toMap());

        context = DefaultContext.load(ConfigurationUtils.createConfiguration(properties).toMap());
        sessionManager = new SessionManager(context);
        sessionManager.start();
        sqlGatewayService = new SqlGatewayServiceImpl(sessionManager);
        executor =
                Executors.newFixedThreadPool(
                        1, new ExecutorThreadFactory("rest-it-case-thread-pool"));
        restClient = new RestClient(new Configuration(), executor);
    }

    @AfterAll
    public static void afterClass() throws Exception {
        sqlGatewayRestEndpoint.stop();
        sessionManager.stop();
        restClient.shutdown(Time.seconds(5));
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
    }

    protected <
            M extends MessageHeaders<R, P, U>,
            U extends MessageParameters,
            R extends RequestBody,
            P extends ResponseBody>
    CompletableFuture<P> sendRequest(M messageHeaders, U messageParameters, R request)
            throws IOException {
        return restClient.sendRequest(
                address.getHostString(),
                address.getPort(),
                messageHeaders,
                messageParameters,
                request);
    }

    private static Configuration getMiniClusterConfig() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
        config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
        return config;
    }
}

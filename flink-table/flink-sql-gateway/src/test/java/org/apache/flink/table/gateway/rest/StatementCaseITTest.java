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
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.api.utils.ParseTools;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.message.parameters.FetchResultsTokenParameters;
import org.apache.flink.table.gateway.rest.message.parameters.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementHeaders;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.service.SqlGatewayServiceStatementITCase;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.utils.SqlScriptReader;
import org.apache.flink.table.gateway.service.utils.TestSqlStatement;

import org.apache.flink.shaded.guava30.com.google.common.io.PatternFilenameFilter;

import org.apache.calcite.util.Util;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.GATEWAY_ENDPOINT_PREFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Test statement related apis. */
@Execution(CONCURRENT)
public class StatementCaseITTest extends RestITCaseBase {

    private final String sessionName = "test";
    private static final Map<String, String> properties = new HashMap<>();
    private final Map<String, String> replaceVars = new HashMap<>();


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

    private static final SessionEnvironment defaultSessionEnvironment =
            SessionEnvironment.newBuilder()
                    .setSessionEndpointVersion(MockedEndpointVersion.V1)
                    .build();
    private static final SessionHandle sessionHandle =
            sqlGatewayService.openSession(defaultSessionEnvironment);

    ExecuteStatementHeaders executeStatementHeaders = ExecuteStatementHeaders.getInstance();
    SessionMessageParameters sessionMessageParameters =
            new SessionMessageParameters(sessionHandle);;

    FetchResultsHeaders fetchResultsHeaders = FetchResultsHeaders.getInstance();
    FetchResultsTokenParameters fetchResultsTokenParameters;

    @BeforeEach
    public void before(@TempDir Path temporaryFolder) throws IOException {
        // initialize new folders for every test, so the vars can be reused by every SQL scripts
        replaceVars.put(
                "$VAR_STREAMING_PATH",
                Files.createDirectory(temporaryFolder.resolve("streaming")).toFile().getPath());
        replaceVars.put(
                "$VAR_BATCH_PATH",
                Files.createDirectory(temporaryFolder.resolve("batch")).toFile().getPath());
    }

    @ParameterizedTest
    @MethodSource("queries")
    public void testGetCorrectResultWhenSubmittingSqlStatements(String sqlPath) throws Exception {
        String in = getInputFromPath(sqlPath);
        List<TestSqlStatement> testSqlStatements = SqlScriptReader.parseSqlScript(in);
        for (TestSqlStatement sqlStatement : testSqlStatements) {
            runSingleStatement(sqlStatement.getSql());
        }
    }

    private void runSingleStatement(String statement) throws Exception {
        ExecuteStatementRequestBody executeStatementRequestBody =
                new ExecuteStatementRequestBody(statement, 0L);
        CompletableFuture<ExecuteStatementResponseBody> response =
                sendRequest(
                        executeStatementHeaders,
                        sessionMessageParameters,
                        executeStatementRequestBody);
        ExecuteStatementResponseBody executeStatementResponseBody = response.get();
        String operationHandleString = executeStatementResponseBody.getOperationHandle();
        assertNotNull(operationHandleString);
        OperationHandle operationHandle =
                ParseTools.parseStringToOperationHandler(operationHandleString);
        assertDoesNotThrow(
                () ->
                        sessionManager
                                .getSession(sessionHandle)
                                .getOperationManager()
                                .getOperation(operationHandle));

        CommonTestUtils.waitUtil(
                () ->
                        sqlGatewayService
                                .getOperationInfo(sessionHandle, operationHandle)
                                .getStatus()
                                .isTerminalStatus(),
                Duration.ofSeconds(100),
                "Failed to wait operation finish.");

        OperationManager.Operation operation =
                sessionManager
                        .getSession(sessionHandle)
                        .getOperationManager()
                        .getOperation(operationHandle);

        ResultSet resultSet = null;
        Exception fetchResultException = null;
        try {
            resultSet = operation.fetchResults(0, Integer.MAX_VALUE);
        } catch (Exception e) {
            fetchResultException = e;
        }

        FetchResultsResponseBody fetchResultsResponseBody = fetchResults(operationHandle);
        System.out.println("Get the response.");
        if (fetchResultException != null) {
            // Check exception info
            assert fetchResultsResponseBody.getExceptionInfo() != null;
            assertEquals(
                    fetchResultException.getCause().getClass().getName(),
                    fetchResultsResponseBody.getExceptionInfo().getRootClassName());
            assertEquals(
                    fetchResultException.getMessage(),
                    fetchResultsResponseBody.getExceptionInfo().getMessage());
            assertEquals(
                    Arrays.toString(fetchResultException.getStackTrace()),
                    fetchResultsResponseBody.getExceptionInfo().getStack());
        }
        if (resultSet != null) {
            // Check result set
            assert fetchResultsResponseBody.getResults() != null;
            assertEquals(
                    resultSet.getResultSchema().toString(),
                    fetchResultsResponseBody.getResults().getResultSchema().toString());
            assertEquals(
                    resultSet.getResultType(),
                    fetchResultsResponseBody.getResults().getResultType());
            if (resultSet.getData() != null && resultSet.getData().size() > 0) {
                assertEquals(resultSet.getData(), fetchResultsResponseBody.getResults().getData());
            }
        }
    }

    public FetchResultsResponseBody fetchResults(OperationHandle operationHandle) throws Exception {
        CommonTestUtils.waitUtil(
                () ->
                        sqlGatewayService
                                .getOperationInfo(sessionHandle, operationHandle)
                                .getStatus()
                                .isTerminalStatus(),
                Duration.ofSeconds(100),
                "Failed to wait operation finish.");
        System.out.println("Operation finished");
        FetchResultsRequestBody fetchResultsRequestBody =
                new FetchResultsRequestBody(Integer.MAX_VALUE);
        fetchResultsTokenParameters =
                new FetchResultsTokenParameters(sessionHandle, operationHandle, 0L);
        CompletableFuture<FetchResultsResponseBody> response =
                sendRequest(
                        fetchResultsHeaders, fetchResultsTokenParameters, fetchResultsRequestBody);
        return response.get();
    }

    public static Stream<String> queries() throws Exception {
        String first = "sql/table.q";
        URL url = SqlGatewayServiceStatementITCase.class.getResource("/" + first);
        File firstFile = Paths.get(checkNotNull(url.toURI())).toFile();
        final int commonPrefixLength = firstFile.getAbsolutePath().length() - first.length();
        File dir = firstFile.getParentFile();
        final List<String> paths = new ArrayList<>();
        final FilenameFilter filter = new PatternFilenameFilter(".*\\.q$");
        for (File f : Util.first(dir.listFiles(filter), new File[0])) {
            paths.add(f.getAbsolutePath().substring(commonPrefixLength));
        }
        return paths.stream();
    }

    private String getInputFromPath(String sqlPath) throws IOException {
        URL url = SqlGatewayServiceStatementITCase.class.getResource("/" + sqlPath);

        // replace the placeholder with specified value if exists
        String[] keys = replaceVars.keySet().toArray(new String[0]);
        String[] values = Arrays.stream(keys).map(replaceVars::get).toArray(String[]::new);

        return StringUtils.replaceEach(
                IOUtils.toString(checkNotNull(url), StandardCharsets.UTF_8), keys, values);
    }
}

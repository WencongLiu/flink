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

package org.apache.flink.table.gateway.rest.handler.statement;

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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ExceptionInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.parameters.FetchResultsTokenParameters;
import org.apache.flink.table.gateway.rest.message.parameters.FetchResultsTokenPathParameter;
import org.apache.flink.table.gateway.rest.message.parameters.OperationHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.parameters.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to fetch results. */
public class FetchResultsHandler
        extends AbstractSqlGatewayRestHandler<
                FetchResultsRequestBody, FetchResultsResponseBody, FetchResultsTokenParameters> {

    public FetchResultsHandler(
            SqlGatewayService service,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            FetchResultsRequestBody,
                            FetchResultsResponseBody,
                            FetchResultsTokenParameters>
                    messageHeaders) {
        super(service, timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<FetchResultsResponseBody> handleRequest(
            SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<FetchResultsRequestBody> request) {
        System.out.println("actually get the result0");
        SessionHandle sessionHandle = request.getPathParameter(SessionHandleIdPathParameter.class);
        OperationHandle operationHandle =
                request.getPathParameter(OperationHandleIdPathParameter.class);
        Long token = request.getPathParameter(FetchResultsTokenPathParameter.class);
        Integer maxFetchSize = request.getRequestBody().getMaxFetchSize();
        if (maxFetchSize != null && maxFetchSize <= 0) {
            throw new SqlGatewayException("Max fetch size must be positive.");
        }
        maxFetchSize = maxFetchSize == null ? Integer.MAX_VALUE : maxFetchSize;

        Throwable fetchResultsException = null;

        ResultSet resultSet = null;
        String resultType = null;
        Long nextToken = 0L;
        try {
            System.out.println("actually get the result1");
            resultSet = service.fetchResults(sessionHandle, operationHandle, token, maxFetchSize);
            System.out.println("actually get the result2");
            nextToken = resultSet.getNextToken();
            resultType = resultSet.getResultType().toString();
        } catch (Exception e) {
            fetchResultsException = e.getCause();
        }

        String nextResultUri =
                FetchResultsHeaders.buildNextUri(
                        version.toString(),
                        sessionHandle.getIdentifier().getFullID(),
                        operationHandle.getIdentifier().getFullID(),
                        nextToken);

        ExceptionInfo exceptionInfo = null;
        if (fetchResultsException != null) {
            exceptionInfo =
                    new ExceptionInfo(
                            fetchResultsException.getCause().getClass().getName(),
                            fetchResultsException.getMessage(),
                            Arrays.toString(fetchResultsException.getStackTrace()));
        }

        return CompletableFuture.completedFuture(
                new FetchResultsResponseBody(resultSet, resultType, nextResultUri, exceptionInfo));
    }
}

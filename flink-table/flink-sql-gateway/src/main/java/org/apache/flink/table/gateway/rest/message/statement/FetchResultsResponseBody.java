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

package org.apache.flink.table.gateway.rest.message.statement;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.table.gateway.api.results.ExceptionInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** {@link ResponseBody} for execute a statement. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FetchResultsResponseBody implements ResponseBody {

    private static final String FIELD_RESULT_TYPE = "result_type";
    private static final String FIELD_RESULTS = "results";
    private static final String FIELD_NEXT_RESULT_URI = "next_result_uri";
    private static final String FIELD_EXCEPTION = "exception";

    @JsonProperty(FIELD_RESULTS)
    private final ResultSet results;

    @JsonProperty(FIELD_RESULT_TYPE)
    private final String resultType;

    @JsonProperty(FIELD_NEXT_RESULT_URI)
    private final String nextResultUri;

    @JsonProperty(FIELD_EXCEPTION)
    private final ExceptionInfo exceptionInfo;

    public FetchResultsResponseBody(
            @JsonProperty(FIELD_RESULTS) ResultSet results,
            @JsonProperty(FIELD_RESULT_TYPE) String resultType,
            @JsonProperty(FIELD_NEXT_RESULT_URI) String nextResultUri,
            @JsonProperty(FIELD_EXCEPTION) ExceptionInfo exceptionInfo) {
        this.results = results;
        this.resultType = resultType;
        this.nextResultUri = nextResultUri;
        this.exceptionInfo = exceptionInfo;
    }

    public ResultSet getResults() {
        return results;
    }

    public String getResultType() {
        return resultType;
    }

    public String getNextResultUri() {
        return nextResultUri;
    }

    public ExceptionInfo getExceptionInfo() {
        return exceptionInfo;
    }
}

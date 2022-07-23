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

package org.apache.flink.table.gateway.api.results;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** ExceptionInfo for SQL gateway service. */
public class ExceptionInfo {

    private static final String FIELD_EXCEPTION_ROOT_CLASS_NAME = "root";
    private static final String FIELD_EXCEPTION_MESSAGE = "message";
    private static final String FIELD_EXCEPTION_STACK = "exception_stack";

    @JsonProperty(FIELD_EXCEPTION_ROOT_CLASS_NAME)
    private String rootClassName;

    @JsonProperty(FIELD_EXCEPTION_MESSAGE)
    private String message;

    @JsonProperty(FIELD_EXCEPTION_STACK)
    private String stack;

    @JsonCreator
    public ExceptionInfo(
            @JsonProperty(FIELD_EXCEPTION_ROOT_CLASS_NAME) String rootClassName,
            @JsonProperty(FIELD_EXCEPTION_MESSAGE) String message,
            @JsonProperty(FIELD_EXCEPTION_STACK) String stack) {
        this.rootClassName =
                Preconditions.checkNotNull(rootClassName, "rootClassName must not be null");
        this.message = Preconditions.checkNotNull(message, "message must not be null");
        this.stack = Preconditions.checkNotNull(stack, "stack must not be null");
    }

    public String getRootClassName() {
        return rootClassName;
    }

    public String getMessage() {
        return message;
    }

    public String getStack() {
        return stack;
    }
}

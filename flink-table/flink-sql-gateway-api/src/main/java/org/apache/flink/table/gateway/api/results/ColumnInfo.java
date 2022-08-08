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

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

/** A column info represents a table column's structure with column name, column type. */
public class ColumnInfo {

    private static final String FIELD_NAME_NAME = "name";
    private static final String FIELD_NAME_TYPE = "type";

    @JsonProperty(FIELD_NAME_NAME)
    private String name;

    @JsonProperty(FIELD_NAME_TYPE)
    @JsonSerialize(using = SqlGatewayLogicalTypeJsonSerializer.class)
    @JsonDeserialize(using = SqlGatewayLogicalTypeJsonDeserializer.class)
    private LogicalType type;

    @JsonCreator
    public ColumnInfo(
            @JsonProperty(FIELD_NAME_NAME) String name,
            @JsonProperty(FIELD_NAME_TYPE) LogicalType type) {
        this.name = Preconditions.checkNotNull(name, "name must not be null");
        this.type = Preconditions.checkNotNull(type, "type must not be null");
    }

    public String getName() {
        return name;
    }

    public LogicalType getType() {
        return type;
    }
}

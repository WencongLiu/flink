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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_COLUMN_INFOS;
import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_DATA;
import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_NEXT_TOKEN;
import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_RESULT_TYPE;

/** Json deserializer for {@link ResultSet}. */
public class ResultSetJsonDeserializer extends JsonDeserializer<ResultSet> {
    @Override
    public ResultSet deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException, JsonProcessingException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        ResolvedSchema resolvedSchema;
        ResultSet.ResultType resultType;
        List<RowData> data = new ArrayList<>();
        Long nextToken = 0L;

        // Get result type
        JsonParser resultTypeParser = node.get(FIELD_NAME_RESULT_TYPE).traverse();
        resultTypeParser.nextToken();
        resultType = ctx.readValue(resultTypeParser, ResultSet.ResultType.class);

        // Get next token
        JsonParser nextTokenParser = node.get(FIELD_NAME_NEXT_TOKEN).traverse();
        nextTokenParser.nextToken();
        nextToken = ctx.readValue(nextTokenParser, Long.class);

        // Get column infos
        JsonParser columnParser = node.get(FIELD_NAME_COLUMN_INFOS).traverse();
        columnParser.nextToken();
        ColumnInfo[] columnInfos = ctx.readValue(columnParser, ColumnInfo[].class);
        List<Column> columns = new ArrayList<>();
        for (ColumnInfo columnInfo : columnInfos) {
            LogicalType logicalType = columnInfo.getLogicalType();
            columns.add(
                    Column.physical(
                            columnInfo.getName(), DataTypeUtils.toInternalDataType(logicalType)));
        }

        // Generate schema
        resolvedSchema = ResolvedSchema.of(columns);
        RowType rowType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(rowType);
        JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
                new JsonRowDataDeserializationSchema(
                        rowType, resultTypeInfo, false, false, TimestampFormat.ISO_8601);

        // Get row data
        JsonParser rowDataParser = node.get(FIELD_NAME_DATA).traverse();
        rowDataParser.nextToken();
        String[] rowDataStrings = ctx.readValue(rowDataParser, String[].class);
        for (String rowDataString : rowDataStrings) {
            data.add(
                    jsonRowDataDeserializationSchema.deserialize(
                            rowDataString.getBytes(StandardCharsets.UTF_8)));
        }
        return new ResultSet(resultType, nextToken, resolvedSchema, data);
    }
}

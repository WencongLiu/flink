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

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_COLUMN_INFOS;
import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_DATA;
import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_NEXT_TOKEN;
import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_RESULT_TYPE;

/** Json serializer for {@link ResultSet}. */
public class ResultSetJsonSerializer extends JsonSerializer<ResultSet> {

    @Override
    public void serialize(
            ResultSet resultSet, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();

        // Serialize result type
        serializerProvider.defaultSerializeField(
                FIELD_NAME_RESULT_TYPE, resultSet.getResultType(), jsonGenerator);

        // Serialize next token
        serializerProvider.defaultSerializeField(
                FIELD_NAME_NEXT_TOKEN, resultSet.getNextToken(), jsonGenerator);

        // Serialize column infos
        List<Column> columns = resultSet.getResultSchema().getColumns();
        List<ColumnInfo> columnInfos = new ArrayList<>();
        for (Column column : columns) {
            columnInfos.add(
                    ColumnInfo.create(column.getName(), column.getDataType().getLogicalType()));
        }
        serializerProvider.defaultSerializeField(
                FIELD_NAME_COLUMN_INFOS, columnInfos, jsonGenerator);

        // Serialize RowData
        JsonRowDataSerializationSchema serializationSchema =
                new JsonRowDataSerializationSchema(
                        (RowType)
                                resultSet
                                        .getResultSchema()
                                        .toPhysicalRowDataType()
                                        .getLogicalType(),
                        TimestampFormat.ISO_8601,
                        JsonFormatOptions.MapNullKeyMode.LITERAL,
                        "null",
                        true);
        List<String> rowDataStrings = new ArrayList<>();
        for (RowData row : resultSet.getData()) {
            byte[] rowByte = serializationSchema.serialize(row);
            rowDataStrings.add(new String(rowByte, StandardCharsets.UTF_8));
        }
        serializerProvider.defaultSerializeField(FIELD_NAME_DATA, rowDataStrings, jsonGenerator);
        jsonGenerator.writeEndObject();
    }
}

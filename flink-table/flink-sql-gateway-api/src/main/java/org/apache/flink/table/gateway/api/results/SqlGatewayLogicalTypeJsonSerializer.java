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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.dataview.NullSerializer;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Sql Gateway JSON serializer for {@link LogicalType}.
 *
 * <p>Since types are used frequently in every plan, the serializer tries to create a compact JSON
 * whenever possible. The compact representation is {@link LogicalType#asSerializableString()},
 * otherwise generic serialization is used that excludes default values.
 *
 * @see SqlGatewayLogicalTypeJsonDeserializer for the reverse operation.
 */
@Internal
final class SqlGatewayLogicalTypeJsonSerializer extends JsonSerializer<LogicalType> {

    // Common fields
    static final String FIELD_NAME_TYPE_NAME = "type";
    static final String FIELD_NAME_NULLABLE = "nullable";

    static final String FIELD_NAME_LENGTH = "length";

    // 2. TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE
    static final String FIELD_NAME_PRECISION = "precision";

    static final String FIELD_NAME_SCALE = "scale";

    // 3. RAW
    static final String FIELD_NAME_CLASS = "class";
    static final String FIELD_NAME_EXTERNAL_DATA_TYPE = "externalDataType";
    static final String FIELD_NAME_SPECIAL_SERIALIZER = "specialSerializer";
    static final String FIELD_VALUE_EXTERNAL_SERIALIZER_NULL = "NULL";

    // Collection Type
    // 1. MAP
    static final String FIELD_NAME_KEY_TYPE = "keyType";
    static final String FIELD_NAME_VALUE_TYPE = "valueType";

    // 2. ARRAY, MULTISET
    static final String FIELD_NAME_ELEMENT_TYPE = "elementType";

    // 3. ROW
    static final String FIELD_NAME_FIELDS = "fields";
    static final String FIELD_NAME_FIELD_NAME = "name";
    static final String FIELD_NAME_FIELD_TYPE = "fieldType";

    @Override
    public void serialize(
            LogicalType logicalType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {

        serializeInternal(logicalType, jsonGenerator, serializerProvider);
    }

    private static void serializeInternal(
            LogicalType logicalType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, logicalType.getTypeRoot().name());
        jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, logicalType.isNullable());
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        // Sql Gateway Rest Endpoint support 21 kinds of LogicalType
        switch (typeRoot) {
                // 1. LogicalTypes that has no other fields
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case BOOLEAN:
                break;
                // 2. LogicalTypes that has the field length of value 0
            case CHAR:
            case BINARY:
            case VARCHAR:
            case VARBINARY:
                serializeZeroLengthString(jsonGenerator);
                break;
                // 3. Other LogicalTypes
            case DECIMAL:
                serializeDecimal(logicalType, jsonGenerator);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final TimestampType timestampType = (TimestampType) logicalType;
                serializeTimestamp(timestampType.getPrecision(), jsonGenerator);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                final ZonedTimestampType zonedTimestampType = (ZonedTimestampType) logicalType;
                serializeTimestamp(zonedTimestampType.getPrecision(), jsonGenerator);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) logicalType;
                serializeTimestamp(localZonedTimestampType.getPrecision(), jsonGenerator);
                break;
            case ARRAY:
                serializeCollection(
                        ((ArrayType) logicalType).getElementType(),
                        jsonGenerator,
                        serializerProvider);
                break;
            case MULTISET:
                serializeCollection(
                        ((MultisetType) logicalType).getElementType(),
                        jsonGenerator,
                        serializerProvider);
                break;
            case MAP:
                serializeMap((MapType) logicalType, jsonGenerator, serializerProvider);
                break;
            case ROW:
                serializeRow((RowType) logicalType, jsonGenerator, serializerProvider);
                break;
            case RAW:
                if (logicalType instanceof RawType) {
                    serializeSpecializedRaw(
                            (RawType<?>) logicalType, jsonGenerator, serializerProvider);
                    break;
                }
                // fall through
            default:
                throw new SqlGatewayException(
                        String.format(
                                "%s: %s",
                                "This kind of LogicalType is not supported by Sql Gateway Rest Endpoint",
                                logicalType.getTypeRoot().name()));
        }

        jsonGenerator.writeEndObject();
    }

    private static void serializeZeroLengthString(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeNumberField(FIELD_NAME_LENGTH, 0);
    }

    private static void serializeDecimal(LogicalType logicalType, JsonGenerator jsonGenerator)
            throws IOException {
        DecimalType decimalType = (DecimalType) logicalType;
        jsonGenerator.writeNumberField(FIELD_NAME_PRECISION, decimalType.getPrecision());
        jsonGenerator.writeNumberField(FIELD_NAME_SCALE, decimalType.getScale());
        jsonGenerator.writeNumberField(FIELD_NAME_LENGTH, 0);
    }

    private static void serializeTimestamp(int precision, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeNumberField(FIELD_NAME_PRECISION, precision);
    }

    private static void serializeCollection(
            LogicalType elementType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeFieldName(FIELD_NAME_ELEMENT_TYPE);
        serializeInternal(elementType, jsonGenerator, serializerProvider);
    }

    private static void serializeMap(
            MapType mapType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeFieldName(FIELD_NAME_KEY_TYPE);
        serializeInternal(mapType.getKeyType(), jsonGenerator, serializerProvider);
        jsonGenerator.writeFieldName(FIELD_NAME_VALUE_TYPE);
        serializeInternal(mapType.getValueType(), jsonGenerator, serializerProvider);
    }

    private static void serializeRow(
            RowType rowType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeArrayFieldStart(FIELD_NAME_FIELDS);
        for (RowType.RowField rowField : rowType.getFields()) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(FIELD_NAME_FIELD_NAME, rowField.getName());
            jsonGenerator.writeFieldName(FIELD_NAME_FIELD_TYPE);
            serializeInternal(rowField.getType(), jsonGenerator, serializerProvider);
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
    }

    private static void serializeSpecializedRaw(
            RawType<?> rawType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStringField(FIELD_NAME_CLASS, rawType.getOriginatingClass().getName());
        final TypeSerializer<?> serializer = rawType.getTypeSerializer();
        if (serializer.equals(NullSerializer.INSTANCE)) {
            jsonGenerator.writeStringField(
                    FIELD_NAME_SPECIAL_SERIALIZER, FIELD_VALUE_EXTERNAL_SERIALIZER_NULL);
        } else if (serializer instanceof ExternalSerializer) {
            final ExternalSerializer<?, ?> externalSerializer =
                    (ExternalSerializer<?, ?>) rawType.getTypeSerializer();
            if (externalSerializer.isInternalInput()) {
                throw new TableException(
                        "Asymmetric external serializers are currently not supported. "
                                + "The input must not be internal if the output is external.");
            }
            serializerProvider.defaultSerializeField(
                    FIELD_NAME_EXTERNAL_DATA_TYPE, externalSerializer.getDataType(), jsonGenerator);
        } else {
            throw new TableException("Unsupported special case for RAW type.");
        }
    }
}

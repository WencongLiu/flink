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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.apache.flink.table.gateway.api.results.SqlGatewayLogicalTypeJsonSerializer.FIELD_NAME_ELEMENT_TYPE;
import static org.apache.flink.table.gateway.api.results.SqlGatewayLogicalTypeJsonSerializer.FIELD_NAME_FIELDS;
import static org.apache.flink.table.gateway.api.results.SqlGatewayLogicalTypeJsonSerializer.FIELD_NAME_FIELD_NAME;
import static org.apache.flink.table.gateway.api.results.SqlGatewayLogicalTypeJsonSerializer.FIELD_NAME_FIELD_TYPE;
import static org.apache.flink.table.gateway.api.results.SqlGatewayLogicalTypeJsonSerializer.FIELD_NAME_KEY_TYPE;
import static org.apache.flink.table.gateway.api.results.SqlGatewayLogicalTypeJsonSerializer.FIELD_NAME_LENGTH;
import static org.apache.flink.table.gateway.api.results.SqlGatewayLogicalTypeJsonSerializer.FIELD_NAME_NULLABLE;
import static org.apache.flink.table.gateway.api.results.SqlGatewayLogicalTypeJsonSerializer.FIELD_NAME_PRECISION;
import static org.apache.flink.table.gateway.api.results.SqlGatewayLogicalTypeJsonSerializer.FIELD_NAME_TYPE_NAME;
import static org.apache.flink.table.gateway.api.results.SqlGatewayLogicalTypeJsonSerializer.FIELD_NAME_VALUE_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonDeserializer.deserializeSpecializedRaw;

/**
 * JSON deserializer for {@link LogicalType}.
 *
 * @see SqlGatewayLogicalTypeJsonSerializer for the reverse operation
 */
@Internal
final class SqlGatewayLogicalTypeJsonDeserializer extends JsonDeserializer<LogicalType> {
    private static final long serialVersionUID = 1L;

    @Override
    public LogicalType deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {

        jsonParser.setCodec(new ObjectMapper());
        JsonNode logicalTypeNode = jsonParser.getCodec().readTree(jsonParser);
        final SerdeContext serdeContext = createDefaultSerdeContext();
        return deserialize(logicalTypeNode, serdeContext);
    }

    static LogicalType deserialize(JsonNode logicalTypeNode, SerdeContext serdeContext) {
        if (logicalTypeNode.isTextual()) {
            return deserializeWithCompactSerialization(logicalTypeNode.asText(), serdeContext);
        } else {
            return deserializeWithExtendedSerialization(logicalTypeNode, serdeContext);
        }
    }

    private static LogicalType deserializeWithCompactSerialization(
            String serializableString, SerdeContext serdeContext) {
        final DataTypeFactory dataTypeFactory =
                serdeContext.getFlinkContext().getCatalogManager().getDataTypeFactory();
        return dataTypeFactory.createLogicalType(serializableString);
    }

    private static LogicalType deserializeWithExtendedSerialization(
            JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final LogicalType logicalType = deserializeFromRoot(logicalTypeNode, serdeContext);
        if (logicalTypeNode.has(FIELD_NAME_NULLABLE)) {
            final boolean isNullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
            return logicalType.copy(isNullable);
        }
        return logicalType.copy(true);
    }

    private static LogicalType deserializeFromRoot(
            JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final LogicalTypeRoot typeRoot =
                LogicalTypeRoot.valueOf(logicalTypeNode.get(FIELD_NAME_TYPE_NAME).asText());
        switch (typeRoot) {
            case VARCHAR:
            case VARBINARY:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case DOUBLE:
            case FLOAT:
            case NULL:
            case INTEGER:
            case DECIMAL:
            case DATE:
            case SYMBOL:
                return deserializeZeroLengthString(typeRoot, logicalTypeNode);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return deserializeTimestamp(typeRoot, logicalTypeNode);
            case MAP:
                return deserializeMap(logicalTypeNode, serdeContext);
            case ARRAY:
            case MULTISET:
                return deserializeCollection(typeRoot, logicalTypeNode, serdeContext);
            case ROW:
                return deserializeRow(logicalTypeNode, serdeContext);
            case RAW:
                return deserializeSpecializedRaw(logicalTypeNode, serdeContext);
            default:
                throw new TableException("Sql Gateway doesn't support this type root: " + typeRoot);
        }
    }

    private static LogicalType deserialize(
            LogicalTypeRoot typeRoot, JsonNode logicalTypeNode) {
        final int length = logicalTypeNode.get(FIELD_NAME_LENGTH).asInt();
        if (length != 0) {
            throw new TableException("String length should be 0.");
        }
        switch (typeRoot) {
            case CHAR:
                return CharType.ofEmptyLiteral();
            case VARCHAR:
                return VarCharType.ofEmptyLiteral();
            case BINARY:
                return BinaryType.ofEmptyLiteral();
            case VARBINARY:
                return VarBinaryType.ofEmptyLiteral();
            default:
                throw new TableException("String type root expected.");
        }
    }

    private static LogicalType deserializeTimestamp(
            LogicalTypeRoot typeRoot, JsonNode logicalTypeNode) {
        final int precision = logicalTypeNode.get(FIELD_NAME_PRECISION).asInt();
        final TimestampKind kind = TimestampKind.REGULAR;
        switch (typeRoot) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TimestampType(true, kind, precision);
            case TIMESTAMP_WITH_TIME_ZONE:
                return new ZonedTimestampType(true, kind, precision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new LocalZonedTimestampType(true, kind, precision);
            default:
                throw new TableException("Timestamp type root expected.");
        }
    }

    private static LogicalType deserializeCollection(
            LogicalTypeRoot typeRoot, JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final JsonNode elementNode = logicalTypeNode.get(FIELD_NAME_ELEMENT_TYPE);
        final LogicalType elementType = deserialize(elementNode, serdeContext);
        switch (typeRoot) {
            case ARRAY:
                return new ArrayType(elementType);
            case MULTISET:
                return new MultisetType(elementType);
            default:
                throw new TableException("Collection type root expected.");
        }
    }

    private static LogicalType deserializeMap(JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final JsonNode keyNode = logicalTypeNode.get(FIELD_NAME_KEY_TYPE);
        final LogicalType keyType = deserialize(keyNode, serdeContext);
        final JsonNode valueNode = logicalTypeNode.get(FIELD_NAME_VALUE_TYPE);
        final LogicalType valueType = deserialize(valueNode, serdeContext);
        return new MapType(keyType, valueType);
    }

    private static LogicalType deserializeRow(JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final ArrayNode fieldNodes = (ArrayNode) logicalTypeNode.get(FIELD_NAME_FIELDS);
        final List<RowField> fields = new ArrayList<>();
        for (JsonNode fieldNode : fieldNodes) {
            final String fieldName = fieldNode.get(FIELD_NAME_FIELD_NAME).asText();
            final LogicalType fieldType =
                    deserialize(fieldNode.get(FIELD_NAME_FIELD_TYPE), serdeContext);
            fields.add(new RowField(fieldName, fieldType, null));
        }
        return new RowType(fields);
    }

    // Create the default SerdeContext
    private static SerdeContext createDefaultSerdeContext(){
        CatalogManager catalogManager = createEmptyCatalogManager();
        TableConfig tableConfig = TableConfig.getDefault();
        ResourceManager resourceManager = ResourceManager.createResourceManager(
                new URL[0],
                Thread.currentThread().getContextClassLoader(),
                tableConfig.getConfiguration());
        ModuleManager moduleManager = new ModuleManager();
        FunctionCatalog functionCatalog = new FunctionCatalog(
                tableConfig,
                resourceManager,
                catalogManager,
                moduleManager);
        PlannerContext plannerContext =
                new PlannerContext(
                        false,
                        tableConfig,
                        moduleManager,
                        functionCatalog,
                        catalogManager,
                        asRootSchema(new CatalogManagerCalciteSchema(catalogManager, true)),
                        Collections.emptyList(),
                        Thread.currentThread().getContextClassLoader());
        return new SerdeContext(
                new ParserImpl(null, null, plannerContext::createCalciteParser, null),
                plannerContext.getFlinkContext(),
                plannerContext.getTypeFactory(),
                plannerContext.createFrameworkConfig().getOperatorTable());
    }

    // Create the empty catalog manager
    public static CatalogManager createEmptyCatalogManager() {
        String DEFAULT_CATALOG = TableConfigOptions.TABLE_CATALOG_NAME.defaultValue();
        String DEFAULT_DATABASE = TableConfigOptions.TABLE_DATABASE_NAME.defaultValue();
        final CatalogManager.Builder builder = CatalogManager.newBuilder()
                .classLoader(SqlGatewayLogicalTypeJsonDeserializer.class.getClassLoader())
                .config(new Configuration())
                .defaultCatalog(DEFAULT_CATALOG, new GenericInMemoryCatalog(DEFAULT_CATALOG, DEFAULT_DATABASE))
                .executionConfig(new ExecutionConfig());
        return builder.build();
    }
}

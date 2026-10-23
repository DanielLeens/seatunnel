/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.json.debezium;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonToken;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;

public class DebeziumJsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;

    private static final String OP_KEY = "op";
    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete
    public static final String DATA_PAYLOAD = "payload";
    private static final String DATA_BEFORE = "before";
    private static final String DATA_AFTER = "after";
    private static final String DATA_TS = "ts_ms";
    private static final String DATA_SCHEMA = "schema";
    private static final String SCHEMA_FIELD = "field";
    private static final String SCHEMA_FIELDS = "fields";
    private static final String SCHEMA_NAME = "name";

    private static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of %s operation is null, "
                    + "if you are using Debezium Postgres Connector, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

    public static final String FORMAT = "Debezium";

    private final SeaTunnelRowType rowType;

    private final JsonDeserializationSchema jsonDeserializer;

    private final DebeziumRowConverter debeziumRowConverter;

    private final boolean ignoreParseErrors;

    private final boolean debeziumEnabledSchema;

    private final TablePath tablePath;

    /** Cached {@link TablePath#toString()} to avoid rebuilding the table id on every row. */
    private final String tableId;

    public DebeziumJsonDeserializationSchema(CatalogTable catalogTable, boolean ignoreParseErrors) {
        this(catalogTable, ignoreParseErrors, false);
    }

    public DebeziumJsonDeserializationSchema(
            CatalogTable catalogTable, boolean ignoreParseErrors, boolean debeziumEnabledSchema) {
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.ignoreParseErrors = ignoreParseErrors;
        this.jsonDeserializer =
                new JsonDeserializationSchema(catalogTable, false, ignoreParseErrors);
        this.debeziumRowConverter = new DebeziumRowConverter(rowType);
        this.debeziumEnabledSchema = debeziumEnabledSchema;
        this.tablePath = Optional.of(catalogTable).map(CatalogTable::getTablePath).orElse(null);
        this.tableId = tablePath != null ? tablePath.toString() : null;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedOperationException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<SeaTunnelRow>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) {
        deserializeMessage(message, out, tablePath);
    }

    public void deserializeMessage(
            byte[] message, Collector<SeaTunnelRow> out, TablePath tablePath) {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }

        try {
            // Keep selective parsing while retaining only the logical names needed by TIME.
            JsonNode root = readDebeziumEnvelope(message);
            JsonNode payload = debeziumEnabledSchema ? root.get(DATA_PAYLOAD) : root;
            parsePayload(out, tablePath, root, payload);
        } catch (Exception e) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw CommonError.jsonOperationError(FORMAT, new String(message), e);
            }
        }
    }

    public void parsePayload(Collector<SeaTunnelRow> out, JsonNode payload) throws IOException {
        parsePayload(out, tablePath, payload);
    }

    void parsePayload(Collector<SeaTunnelRow> out, JsonNode root, JsonNode payload)
            throws IOException {
        parsePayload(out, tablePath, root, payload);
    }

    void parsePayload(
            Collector<SeaTunnelRow> out, TablePath tablePath, JsonNode root, JsonNode payload)
            throws IOException {
        parsePayload(
                out,
                tablePath,
                payload,
                extractFieldSchemaNames(root, DATA_BEFORE),
                extractFieldSchemaNames(root, DATA_AFTER));
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    private void parsePayload(Collector<SeaTunnelRow> out, TablePath tablePath, JsonNode payload)
            throws IOException {
        parsePayload(out, tablePath, payload, Collections.emptyMap(), Collections.emptyMap());
    }

    private void parsePayload(
            Collector<SeaTunnelRow> out,
            TablePath tablePath,
            JsonNode payload,
            Map<String, String> beforeFieldSchemaNames,
            Map<String, String> afterFieldSchemaNames)
            throws IOException {
        String op = payload.get(OP_KEY).asText();
        JsonNode tsNode = payload.get(DATA_TS);
        String resolvedTableId = resolveTableId(tablePath);
        Long eventTime = tsNode != null ? tsNode.asLong() : null;

        switch (op) {
            case OP_CREATE:
            case OP_READ:
                SeaTunnelRow insert =
                        debeziumRowConverter.parse(payload.get(DATA_AFTER), afterFieldSchemaNames);
                insert.setRowKind(RowKind.INSERT);
                applyRowMeta(insert, resolvedTableId, eventTime);
                out.collect(insert);
                break;
            case OP_UPDATE:
                SeaTunnelRow before =
                        debeziumRowConverter.parse(
                                payload.get(DATA_BEFORE), beforeFieldSchemaNames);
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                }
                before.setRowKind(RowKind.UPDATE_BEFORE);
                applyRowMeta(before, resolvedTableId, eventTime);

                SeaTunnelRow after =
                        debeziumRowConverter.parse(payload.get(DATA_AFTER), afterFieldSchemaNames);
                after.setRowKind(RowKind.UPDATE_AFTER);
                applyRowMeta(after, resolvedTableId, eventTime);
                out.collect(before);
                out.collect(after);
                break;
            case OP_DELETE:
                SeaTunnelRow delete =
                        debeziumRowConverter.parse(
                                payload.get(DATA_BEFORE), beforeFieldSchemaNames);
                if (delete == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
                }
                delete.setRowKind(RowKind.DELETE);
                applyRowMeta(delete, resolvedTableId, eventTime);
                out.collect(delete);
                break;
            default:
                throw new IllegalStateException(format("Unknown operation type '%s'.", op));
        }
    }

    /**
     * Extracts direct before/after field logical names. Nested ROW schemas are deliberately not
     * flattened: the existing converter's flat field-name lookup cannot disambiguate them.
     */
    static Map<String, String> extractFieldSchemaNames(JsonNode root, String envelopeField) {
        JsonNode schema = root == null ? null : root.get(DATA_SCHEMA);
        JsonNode fields = schema == null ? null : schema.get(SCHEMA_FIELDS);
        if (fields == null || !fields.isArray()) {
            return Collections.emptyMap();
        }
        for (JsonNode field : fields) {
            if (!envelopeField.equals(getText(field, SCHEMA_FIELD))) {
                continue;
            }
            JsonNode envelopeFields = field.get(SCHEMA_FIELDS);
            if (envelopeFields == null || !envelopeFields.isArray()) {
                return Collections.emptyMap();
            }
            Map<String, String> fieldSchemaNames = new HashMap<>();
            Iterator<JsonNode> iterator = envelopeFields.elements();
            while (iterator.hasNext()) {
                JsonNode envelopeFieldSchema = iterator.next();
                String fieldName = getText(envelopeFieldSchema, SCHEMA_FIELD);
                String schemaName = getText(envelopeFieldSchema, SCHEMA_NAME);
                if (fieldName != null && schemaName != null) {
                    fieldSchemaNames.put(fieldName, schemaName);
                }
            }
            return fieldSchemaNames;
        }
        return Collections.emptyMap();
    }

    private static String getText(JsonNode node, String fieldName) {
        JsonNode value = node == null ? null : node.get(fieldName);
        return value == null || value.isNull() ? null : value.asText();
    }

    /**
     * Resolves the table id string for the emitted row. Call sites normally pass {@link
     * #tablePath}; the identity check reuses the cached {@link #tableId}, while the fallback covers
     * the public {@link #deserializeMessage(byte[], Collector, TablePath)} contract if a different
     * path is supplied.
     */
    private String resolveTableId(TablePath tablePath) {
        if (tablePath == null) {
            return null;
        }
        if (tablePath == this.tablePath) {
            return tableId;
        }
        return tablePath.toString();
    }

    /** Applies table id and optional event-time metadata to a newly parsed row. */
    private static void applyRowMeta(SeaTunnelRow row, String tableId, Long eventTime) {
        if (tableId != null) {
            row.setTableId(tableId);
        }
        if (eventTime != null) {
            MetadataUtil.setEventTime(row, eventTime);
        }
    }

    /**
     * Parses a Debezium JSON envelope while skipping unused objects such as {@code source}, {@code
     * transaction}. Only CDC payload fields and direct before/after logical schema names are
     * retained; unrelated schema defaults, parameters, and nested field definitions are skipped.
     */
    private JsonNode readDebeziumEnvelope(byte[] message) throws IOException {
        ObjectMapper objectMapper = jsonDeserializer.getObjectMapper();
        try (JsonParser parser = objectMapper.getFactory().createParser(message)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("Expected START_OBJECT for Debezium JSON message");
            }
            if (debeziumEnabledSchema) {
                ObjectNode envelope = objectMapper.createObjectNode();
                JsonNode payload = null;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    if (DATA_PAYLOAD.equals(fieldName)) {
                        payload = readPayloadFields(objectMapper, parser);
                    } else if (DATA_SCHEMA.equals(fieldName)) {
                        envelope.set(DATA_SCHEMA, readSchemaField(objectMapper, parser, 2));
                    } else {
                        parser.skipChildren();
                    }
                }
                if (payload == null) {
                    throw new IOException("Missing payload field in schema-included Debezium JSON");
                }
                envelope.set(DATA_PAYLOAD, payload);
                return envelope;
            }
            return readPayloadFields(objectMapper, parser);
        }
    }

    /**
     * Reads the current object value from {@code parser}, keeping only CDC payload fields and
     * skipping everything else (notably {@code source} and {@code transaction}).
     */
    private static JsonNode readPayloadFields(ObjectMapper objectMapper, JsonParser parser)
            throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected START_OBJECT for Debezium payload");
        }
        ObjectNode payload = objectMapper.createObjectNode();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if (DATA_BEFORE.equals(fieldName)
                    || DATA_AFTER.equals(fieldName)
                    || OP_KEY.equals(fieldName)
                    || DATA_TS.equals(fieldName)) {
                payload.set(fieldName, objectMapper.readTree(parser));
            } else {
                parser.skipChildren();
            }
        }
        return payload;
    }
    /**
     * Retains only field/name metadata for the two schema field levels consumed by
     * extractFieldSchemaNames. The fixed depth avoids materializing nested ROW schemas or large
     * unused default/parameter objects while preserving arbitrary JSON property order.
     */
    private static JsonNode readSchemaField(
            ObjectMapper objectMapper, JsonParser parser, int fieldLevels) throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            parser.skipChildren();
            return objectMapper.getNodeFactory().nullNode();
        }
        ObjectNode field = objectMapper.createObjectNode();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if (SCHEMA_FIELD.equals(fieldName) || SCHEMA_NAME.equals(fieldName)) {
                // Debezium logical and field names are strings; never materialize arbitrary
                // objects supplied under these metadata keys.
                if (parser.currentToken() == JsonToken.VALUE_STRING) {
                    field.put(fieldName, parser.getText());
                } else {
                    parser.skipChildren();
                }
            } else if (SCHEMA_FIELDS.equals(fieldName)
                    && fieldLevels > 0
                    && parser.currentToken() == JsonToken.START_ARRAY) {
                ArrayNode fields = objectMapper.createArrayNode();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    fields.add(readSchemaField(objectMapper, parser, fieldLevels - 1));
                }
                field.set(SCHEMA_FIELDS, fields);
            } else {
                parser.skipChildren();
            }
        }
        return field;
    }
}

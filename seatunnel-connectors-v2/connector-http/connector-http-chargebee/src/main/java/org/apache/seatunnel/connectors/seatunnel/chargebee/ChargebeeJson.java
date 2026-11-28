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

package org.apache.seatunnel.connectors.seatunnel.chargebee;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Validates bounded response envelopes and projects Chargebee records without schema coercion.
 *
 * <p>Nested provider values, integer and decimal precision, soft-delete markers and resource
 * versions remain in the JSON payload. Only top-level custom field names are normalized.
 */
final class ChargebeeJson {
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .maxStringLength(32 * 1024 * 1024)
                                                    .build())
                                    .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private ChargebeeJson() {}

    /**
     * Parses a complete UTF-8 JSON object without logging any provider fields.
     *
     * @param bytes the decoded response under the transport limit
     * @return a validated JSON object
     * @throws Failure if the content is malformed or has duplicate properties
     */
    static JsonNode parse(byte[] bytes) throws Failure {
        JsonNode root;
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes));
            root = JSON.readTree(bytes);
        } catch (CharacterCodingException ignored) {
            throw failure("response is not valid UTF-8 JSON");
        } catch (IOException | RuntimeException ignored) {
            throw failure("invalid JSON response");
        }
        if (root == null || !root.isObject()) {
            throw failure("response must be a JSON object");
        }
        return root;
    }

    /**
     * Unwraps one API response and hashes its raw records and exact continuation.
     *
     * <p>The replay digest includes all rows, including rows filtered by the local date cursor. The
     * singleton response has a resource object and no list wrapper.
     */
    static Page page(JsonNode root, ChargebeeResource resource, int limit) throws Failure {
        if (root == null) {
            return new Page(List.of(), null, fingerprint(List.of("unavailable subscription")));
        }
        String next = nextOffset(root.get("next_offset"));
        List<JsonNode> records = new ArrayList<>();
        MessageDigest digest = sha256();
        update(digest, next == null ? "" : next);
        if (resource.singleton()) {
            if (next != null) {
                throw failure("scheduled subscription singleton returned a list offset");
            }
            addRecord(records, digest, root, resource);
        } else {
            JsonNode list = root.get("list");
            if (list == null || !list.isArray() || list.size() > limit) {
                throw failure("invalid or oversized list response");
            }
            for (JsonNode item : list) {
                addRecord(records, digest, item, resource);
            }
        }
        return new Page(records, next, hex(digest.digest()));
    }

    private static void addRecord(
            List<JsonNode> records,
            MessageDigest digest,
            JsonNode wrapper,
            ChargebeeResource resource)
            throws Failure {
        JsonNode value = wrapper.get(resource.envelope);
        if (value == null || !value.isObject()) {
            throw failure("list entry is missing the selected resource object");
        }
        records.add(value);
        update(digest, encode(value));
    }

    private static String nextOffset(JsonNode value) throws Failure {
        if (value == null || value.isNull()) {
            return null;
        }
        String next = value.textValue();
        if ("".equals(next)) {
            return null;
        }
        if (!ChargebeeConfig.validOffset(next)) {
            throw failure("invalid next_offset");
        }
        return next;
    }

    /**
     * Reads a parent identifier for inclusion as one encoded route segment.
     *
     * @param row the parent entity's JSON object
     * @param kind the parent resource selected from the fixed registry
     * @return a validated identifier
     * @throws Failure when an identifier cannot safely specify the child scope
     */
    static String parentId(JsonNode row, ChargebeeResource kind) throws Failure {
        String id = identifier(row.get(kind.identityKey()));
        if (id == null || id.equals(".") || id.equals("..")) {
            throw failure("hidden parent has an invalid resource id");
        }
        return id;
    }

    /**
     * Applies the fixed time filter and emits the resource identity plus its complete projection.
     *
     * <p>The source moves top-level custom field values into a documented array. Child linkage
     * comes from the verified request scope even when the service omits that field.
     */
    static Record project(
            JsonNode raw, ChargebeeResource resource, String parentId, long from, long through)
            throws Failure {
        Instant cursor = timestamp(raw, resource.cursor);
        if (cursor != null
                && (cursor.getEpochSecond() < from || cursor.getEpochSecond() > through)) {
            return null;
        }
        String id = identifier(raw.get(resource.identityKey()));
        ObjectNode data = (ObjectNode) raw.deepCopy();
        switch (resource) {
            case CONTACT:
                attach(data, "customer_id", parentId);
                break;
            case QUOTE_LINE_GROUP:
                attach(data, "quote_id", parentId);
                break;
            case ATTACHED_ITEM:
                verifyLink(data, "parent_item_id", parentId);
                break;
            case SUBSCRIPTION_WITH_SCHEDULED_CHANGES:
                verifyLink(data, "id", parentId);
                attach(data, "subscription_id", parentId);
                id = parentId;
                break;
            default:
                break;
        }
        moveCustomFields(data);
        return new Record(id, parentId, cursor == null ? null : cursor.toString(), encode(data));
    }

    private static void attach(ObjectNode data, String name, String parent) throws Failure {
        verifyLink(data, name, parent);
        data.put(name, parent);
    }

    private static void verifyLink(ObjectNode data, String name, String parent) throws Failure {
        String link = identifier(data.get(name));
        if (link != null && !link.equals(parent)) {
            throw failure("child record does not match the requested parent");
        }
    }

    private static String identifier(JsonNode value) throws Failure {
        if (value == null || value.isNull()) {
            return null;
        }
        String id = value.textValue();
        if (id == null
                || id.isBlank()
                || id.length() > 1024
                || !StandardCharsets.UTF_8.newEncoder().canEncode(id)
                || id.codePoints().anyMatch(Character::isISOControl)) {
            throw failure("invalid resource identifier");
        }
        return id;
    }

    private static Instant timestamp(JsonNode raw, String field) throws Failure {
        JsonNode value = field == null ? null : raw.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw failure("invalid resource cursor timestamp in seconds");
        }
        try {
            return Instant.ofEpochSecond(value.longValue());
        } catch (DateTimeException ignored) {
            throw failure("invalid resource cursor timestamp in seconds");
        }
    }

    private static void moveCustomFields(ObjectNode data) throws Failure {
        JsonNode provided = data.get("custom_fields");
        if (provided != null && !provided.isNull() && !provided.isArray()) {
            throw failure("custom_fields must be an array or null");
        }
        ArrayNode fields =
                provided == null || provided.isNull()
                        ? JSON.createArrayNode()
                        : (ArrayNode) provided;
        Iterator<Map.Entry<String, JsonNode>> properties = data.fields();
        while (properties.hasNext()) {
            Map.Entry<String, JsonNode> property = properties.next();
            if (property.getKey().startsWith("cf_")) {
                fields.addObject().put("name", property.getKey()).set("value", property.getValue());
                properties.remove();
            }
        }
        data.set("custom_fields", fields);
    }

    private static String encode(JsonNode node) throws Failure {
        try {
            return JSON.writeValueAsString(node);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not serialize Chargebee record");
        }
    }

    static String fingerprint(List<String> values) {
        MessageDigest digest = sha256();
        for (String value : values) {
            update(digest, value);
        }
        return hex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 15, 16));
            result.append(Character.forDigit(value & 15, 16));
        }
        return result.toString();
    }

    static Failure failure(String message) {
        return new Failure(message);
    }

    /**
     * Holds validated raw records and the page digest used by an in-progress split.
     *
     * <p>The DIGEST covers the opaque next offset and complete ordered resource objects.
     */
    static final class Page {
        final List<JsonNode> records;
        final String next;
        final String digest;

        Page(List<JsonNode> records, String next, String digest) {
            this.records = records;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Holds projected data ready to publish at the corresponding raw position.
     *
     * <p>Child linkage and the canonical cursor are separate from the provider JSON.
     */
    static final class Record {
        final String id;
        final String parentId;
        final String timestamp;
        final String json;

        Record(String id, String parentId, String timestamp, String json) {
            this.id = id;
            this.parentId = parentId;
            this.timestamp = timestamp;
            this.json = json;
        }
    }

    /**
     * Carries a fixed safe diagnostic for a failed API read.
     *
     * <p>Request targets, provider bodies and authentication bytes stay out of job diagnostics.
     */
    static final class Failure extends IOException {
        Failure(String message) {
            super("Chargebee source: " + message);
        }
    }
}

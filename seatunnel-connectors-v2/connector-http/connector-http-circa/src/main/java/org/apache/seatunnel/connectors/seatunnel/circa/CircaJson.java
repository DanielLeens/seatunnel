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

package org.apache.seatunnel.connectors.seatunnel.circa;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.List;

/**
 * Validates Circa response envelopes and projects raw provider records.
 *
 * <p>Numeric precision and dynamic fields stay in the JSON payload. Cursor timestamps are parsed
 * separately from date-only business fields.
 */
final class CircaJson {
    private static final int PAGE_SIZE = 25;
    private static final int UNPAGED_LIMIT = 10000;
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

    private CircaJson() {}

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
     * Creates the vendor's JSON GET filter. Hidden discovery deliberately excludes time bounds.
     *
     * <p>Teams have no request body. Field lists use the single fixed discriminator.
     */
    static byte[] query(CircaResource resource, int page, Instant lower, Instant upper) {
        if (!resource.paged && resource.fieldFor == null) {
            return null;
        }
        ObjectNode body = JSON.createObjectNode();
        if (resource.fieldFor != null) {
            body.put("fields_for", resource.fieldFor);
        } else {
            body.put("page", page);
            if (resource == CircaResource.EVENTS) {
                body.put("include_undated", true);
            }
            if (lower != null) {
                body.putObject("updated_at")
                        .put("min", lower.toString())
                        .put("max", upper.toString());
            }
        }
        return body.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Validates each page before a reader can emit any of its rows.
     *
     * <p>Explicit page metadata wins over short responses. Total entries are informational because
     * the count may change relative to the total number of pages.
     */
    static Page page(JsonNode root, CircaResource resource, int requested) throws Failure {
        JsonNode data = root.path("data");
        int limit = resource.paged ? PAGE_SIZE : UNPAGED_LIMIT;
        if (!data.isArray() || data.size() > limit) {
            throw failure("invalid data array for the selected resource");
        }
        boolean next = false;
        JsonNode pages = root.path("pagination");
        if (resource.paged) {
            if (pages.isMissingNode() || pages.isNull()) {
                next = data.size() == PAGE_SIZE;
            } else {
                int current = integer(pages.path("current_page"));
                int total = integer(pages.path("total_pages"));
                int size = integer(pages.path("per_page"));
                int entries = integer(pages.path("total_entries"));
                if (!pages.isObject()
                        || current != requested
                        || total < 0
                        || entries < 0
                        || size != PAGE_SIZE
                        || requested > total && !data.isEmpty()) {
                    throw failure("invalid or repeated pagination position");
                }
                next = requested < total;
            }
        } else if (!pages.isNull() && !pages.isMissingNode()) {
            throw failure("unexpected pagination for an unpaged resource");
        }
        List<JsonNode> records = new ArrayList<>(data.size());
        for (JsonNode row : data) {
            identifier(row);
            records.add(row);
        }
        String content = fingerprint(List.of(data.toString()));
        return new Page(
                records, next, content, fingerprint(List.of(content, Boolean.toString(next))));
    }

    private static int integer(JsonNode node) {
        return node.isIntegralNumber() && node.canConvertToInt() ? node.intValue() : -1;
    }

    static String identifier(JsonNode row) throws Failure {
        String value = row.path("id").textValue();
        if (!row.isObject()
                || value == null
                || value.isBlank()
                || value.length() > 512
                || ".".equals(value)
                || "..".equals(value)
                || value.codePoints().anyMatch(Character::isISOControl)
                || !StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
            throw failure("record has an invalid opaque id");
        }
        return value;
    }

    static Record project(
            JsonNode row, CircaResource resource, String parent, Instant lower, Instant upper)
            throws Failure {
        Instant modified = null;
        JsonNode cursor = row.path("updated_at");
        if (resource.incremental
                && (!cursor.isMissingNode() && !cursor.isNull() || lower != null)) {
            try {
                String value = cursor.textValue();
                if (value == null) {
                    throw failure("record is missing its required updated_at timestamp");
                }
                modified = CircaConfig.timestamp(value);
            } catch (DateTimeException ignored) {
                throw failure("record updated_at is not an ISO 8601 timestamp");
            }
        }
        if (lower != null && (modified.isBefore(lower) || modified.isAfter(upper))) {
            return null;
        }
        return new Record(
                identifier(row),
                parent,
                modified == null ? null : modified.toString(),
                row.toString());
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
     * Binds an ordered page's shape and continuation independently of saved record payloads.
     *
     * <p>The content digest detects repeated data when older responses omit pagination metadata.
     */
    static final class Page {
        final List<JsonNode> rows;
        final boolean next;
        final String content;
        final String digest;

        Page(List<JsonNode> rows, boolean next, String content, String digest) {
            this.rows = rows;
            this.next = next;
            this.content = content;
            this.digest = digest;
        }
    }

    /**
     * Carries one validated projection to the engine publisher.
     *
     * <p>Child records keep the enclosing parent ID independently of any provider field.
     */
    static final class Record {
        final String id;
        final String parent;
        final String cursor;
        final String data;

        Record(String id, String parent, String cursor, String data) {
            this.id = id;
            this.parent = parent;
            this.cursor = cursor;
            this.data = data;
        }
    }

    /**
     * Carries a fixed safe error that excludes secrets, opaque IDs and raw response data.
     *
     * <p>Transport workers preserve this type while unwrapping asynchronous results.
     */
    static final class Failure extends IOException {
        private static final long serialVersionUID = 1L;

        Failure(String message) {
            super("Circa source: " + message);
        }
    }
}

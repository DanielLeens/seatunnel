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

package org.apache.seatunnel.connectors.seatunnel.clarifai;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

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
 * Validates Clarifai response envelopes and projects raw provider records.
 *
 * <p>Numeric precision and dynamic fields stay in the JSON payload. Configured cursor timestamps
 * are parsed independently of attributes preserved in the original payload.
 */
final class ClarifaiJson {
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

    private ClarifaiJson() {}

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

    static final int SUCCESS = 10000;
    static final int TRY_AGAIN = 10030;
    static final int THROTTLED = 11005;

    static int status(JsonNode root) throws Failure {
        JsonNode code = root.path("status").path("code");
        if (!code.isIntegralNumber() || !code.canConvertToInt()) {
            throw failure("missing or invalid numeric Clarifai status code");
        }
        return code.intValue();
    }

    static boolean warning(int status) {
        return status == 10001 || status == 10002;
    }

    /**
     * Validates a Clarifai list envelope and hashes ordered records before any are published.
     *
     * <p>A successful protobuf response may omit an empty repeated field. A deprecation warning
     * requires an explicit list, so a truncated warning envelope cannot be treated as empty.
     */
    static Page page(JsonNode root, ClarifaiResource resource, int limit) throws Failure {
        int code = status(root);
        if (code != SUCCESS && !warning(code)) {
            if (code == 11007) {
                throw failure("Clarifai status 11007, token has insufficient read scopes");
            }
            throw failure("Clarifai status " + code);
        }
        JsonNode data = root.path(resource.key);
        if (data.isMissingNode() && code == SUCCESS) {
            data = JSON.createArrayNode();
        }
        if (!data.isArray() || data.size() > limit) {
            throw failure("invalid list envelope for the selected resource");
        }
        List<JsonNode> records = new ArrayList<>(data.size());
        for (JsonNode row : data) {
            identifier(row);
            records.add(row);
        }
        boolean next = records.size() == limit;
        String content = fingerprint(List.of(data.toString()));
        return new Page(
                records, next, content, fingerprint(List.of(content, Boolean.toString(next))));
    }

    static String identifier(JsonNode row) throws Failure {
        String value = row.path("id").textValue();
        if (!row.isObject() || !identifier(value)) {
            throw failure("record has an invalid opaque id");
        }
        return value;
    }

    static boolean identifier(String value) {
        return value != null
                && !value.isBlank()
                && value.length() <= 512
                && !".".equals(value)
                && !"..".equals(value)
                && value.codePoints().noneMatch(Character::isISOControl)
                && StandardCharsets.UTF_8.newEncoder().canEncode(value);
    }

    static Record project(
            JsonNode row,
            ClarifaiResource resource,
            String app,
            String model,
            Instant lower,
            Instant upper)
            throws Failure {
        Instant modified = null;
        JsonNode cursor = row.path("modified_at");
        if (resource.incremental && !cursor.isNull() && !cursor.isMissingNode()) {
            try {
                String value = cursor.textValue();
                if (value == null) {
                    throw failure("record modified_at is not an ISO 8601 timestamp");
                }
                modified = ClarifaiConfig.timestamp(value);
            } catch (DateTimeException ignored) {
                throw failure("record modified_at is not an ISO 8601 timestamp");
            }
        }
        if (modified != null
                && lower != null
                && (modified.isBefore(lower) || modified.isAfter(upper))) {
            return null;
        }
        return new Record(
                identifier(row),
                app,
                model,
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
     * <p>The content digest detects a response that repeats the previous page.
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
     * <p>App and model parents come from the requested path, independently of payload attributes.
     */
    static final class Record {
        final String id;
        final String app;
        final String model;
        final String cursor;
        final String data;

        Record(String id, String app, String model, String cursor, String data) {
            this.id = id;
            this.app = app;
            this.model = model;
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
            super("Clarifai source: " + message);
        }
    }
}

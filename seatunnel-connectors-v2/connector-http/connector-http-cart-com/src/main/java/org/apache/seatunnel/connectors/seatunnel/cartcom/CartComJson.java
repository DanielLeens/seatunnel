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

package org.apache.seatunnel.connectors.seatunnel.cartcom;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
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
 * Validates REST envelopes and preserves arbitrary provider record values in compact JSON.
 *
 * <p>The vendor's paging URL can name a merchant origin even for central-router requests. Only a
 * validated next page number is retained. The next request always uses the configured origin,
 * resource and original date window.
 */
final class CartComJson {
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

    private CartComJson() {}

    /**
     * Parses a complete and previously bounded UTF-8 object.
     *
     * @param bytes decoded payload
     * @return the validated JSON root
     * @throws Failure if the body has invalid encoding, duplicate properties or invalid JSON
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
     * Checks and projects one page before any rows from it can be emitted.
     *
     * <p>The checkpoint fingerprint includes raw records even when their time is outside the
     * configured window, so replay always uses the same raw offset.
     */
    static Page page(JsonNode root, CartComConfig config, int current, Instant ceiling)
            throws Failure {
        CartComResource resource = config.resource;
        JsonNode collection = root.get(resource.id);
        JsonNode alternate =
                resource.alternateCollection == null
                        ? null
                        : root.get(resource.alternateCollection);
        if (collection != null && alternate != null && !collection.equals(alternate)) {
            throw failure("response contains conflicting resource collections");
        }
        JsonNode rows = collection == null ? alternate : collection;
        if (rows == null || !rows.isArray() || rows.size() > config.pageSize) {
            throw failure("invalid or oversized resource collection");
        }
        int next = nextPage(root.get("next_page"), resource, current);
        List<Record> results = new ArrayList<>(rows.size());
        MessageDigest hash = sha256();
        update(hash, Integer.toString(next));
        for (JsonNode row : rows) {
            if (!row.isObject()) {
                throw failure("collection contains a non-object record");
            }
            String raw = encode(row);
            update(hash, raw);
            results.add(project(row, raw, config, ceiling));
        }
        return new Page(results, next, hex(hash.digest()));
    }

    private static Record project(JsonNode row, String raw, CartComConfig config, Instant ceiling)
            throws Failure {
        JsonNode id = row.path("id");
        if (!id.isIntegralNumber() && (!id.isTextual() || id.textValue().isEmpty())) {
            throw failure("record is missing a valid id");
        }
        JsonNode update = row.get("updated_at");
        Instant instant = null;
        if (update != null && !update.isNull()) {
            try {
                instant = CartComConfig.timestamp(update.textValue());
            } catch (DateTimeException ignored) {
                throw failure("record has an invalid updated_at timestamp");
            }
        }
        if (config.resource.incremental
                && instant != null
                && (instant.isBefore(config.lower) || !instant.isBefore(ceiling))) {
            return null;
        }
        return new Record(id.asText(), instant == null ? null : instant.toString(), raw);
    }

    /**
     * Extracts only a sequential page index from an untrusted response URI.
     *
     * <p>The origin and all other query fields are discarded. An invalid resource route or
     * repeated/skipped index fails the page before its rows are published.
     */
    private static int nextPage(JsonNode token, CartComResource resource, int current)
            throws Failure {
        if (token == null || token.isNull() || token.isTextual() && token.textValue().isEmpty()) {
            return 0;
        }
        if (!token.isTextual() || token.textValue().length() > 8192) {
            throw failure("invalid next_page");
        }
        try {
            URI next = URI.create(token.textValue());
            String path = next.getRawPath();
            String query = next.getRawQuery();
            if (next.getRawUserInfo() != null
                    || next.getRawFragment() != null
                    || query == null
                    || path == null
                    || (next.isAbsolute()
                            && (!"https".equalsIgnoreCase(next.getScheme())
                                    || next.getHost() == null))
                    || (!path.equals(resource.path)
                            && !path.equals(resource.path + ".json")
                            && !(path.isEmpty() && next.getRawAuthority() == null))) {
                throw failure("next_page does not identify the selected resource");
            }
            String index = null;
            for (String pair : query.split("&", -1)) {
                int separator = pair.indexOf('=');
                String name = separator < 0 ? pair : pair.substring(0, separator);
                if ("page".equals(URLDecoder.decode(name, StandardCharsets.UTF_8))) {
                    if (index != null || separator < 0) {
                        throw failure("invalid or duplicated next_page index");
                    }
                    index =
                            URLDecoder.decode(
                                    pair.substring(separator + 1), StandardCharsets.UTF_8);
                }
            }
            if (index == null || !index.equals(Integer.toString(current + 1))) {
                throw failure("next_page must advance exactly one page");
            }
            return current + 1;
        } catch (IllegalArgumentException ignored) {
            throw failure("invalid next_page");
        }
    }

    private static String encode(JsonNode node) throws Failure {
        try {
            return JSON.writeValueAsString(node);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not serialize a resource record");
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

    static Failure failure(String reason) {
        return new Failure(reason);
    }

    /**
     * Carries one verified raw page offset and its matching row projection.
     *
     * <p>A null member of rows represents an out-of-window raw offset.
     */
    static final class Page {
        final List<Record> rows;
        final int next;
        final String digest;

        Page(List<Record> rows, int next, String digest) {
            this.rows = rows;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Holds the output fields before the engine checkpoint lock publishes a row.
     *
     * <p>The full JSON remains separate from the chosen identifier and normalized timestamp.
     */
    static final class Record {
        final String id;
        final String timestamp;
        final String json;

        Record(String id, String timestamp, String json) {
            this.id = id;
            this.timestamp = timestamp;
            this.json = json;
        }
    }

    /**
     * Carries a local message safe to display in job diagnostics.
     *
     * <p>Provider response bodies, origins, and authentication material are omitted.
     */
    static final class Failure extends IOException {
        Failure(String message) {
            super("CartCom source: " + message);
        }
    }
}

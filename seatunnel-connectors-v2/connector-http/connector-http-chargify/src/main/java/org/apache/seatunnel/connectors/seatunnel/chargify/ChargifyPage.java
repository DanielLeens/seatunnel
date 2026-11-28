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
package org.apache.seatunnel.connectors.seatunnel.chargify;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Validates one numbered response and computes its successor.
 *
 * <p>Comparing adjacent page identities prevents endless replay when the provider ignores the page
 * number. Partial-page state also binds payload values and order.
 */
final class ChargifyPage {
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .build())
                                    .build())
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    final List<SeaTunnelRow> rows;
    final ChargifySplit next;
    final String fingerprint;

    private ChargifyPage(List<SeaTunnelRow> rows, ChargifySplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Reads, validates and maps the entire native response before exposing any of its rows.
     *
     * <p>Even a short nonempty response advances one numbered page. Only an empty array declares
     * the current resource complete.
     */
    static ChargifyPage read(
            ChargifyTransport client,
            ChargifyConfig config,
            ChargifySplit state,
            String table,
            long deadline)
            throws ChargifyReadException, InterruptedException {
        ChargifyResource resource = config.plan.get(state.resourceIndex);
        JsonNode entries =
                records(client.get(resource, state.page, deadline), resource, config.pageSize);
        List<SeaTunnelRow> rows = new ArrayList<>(entries.size());
        Set<String> identities = new TreeSet<>();
        MessageDigest digest = ChargifyConfig.sha256();
        for (JsonNode entry : entries) {
            JsonNode record = resource.wrapper == null ? entry : entry.path(resource.wrapper);
            if (!record.isObject()) {
                throw resource.failure(
                        "expected an object in the native " + resource.id + " collection");
            }
            String id = identifier(record, resource);
            if (!identities.add(id)) {
                throw resource.failure("duplicate provider id in one page");
            }
            String payload = payload(record, resource);
            hashField(digest, payload);
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {resource.id, id, config.site, config.region, payload});
            row.setTableId(table);
            rows.add(row);
        }
        String fingerprint = encode(digest);
        if (state.published < 0
                || state.published > rows.size()
                || state.published > 0 && !fingerprint.equals(state.fingerprint)) {
            throw resource.failure(
                    "provider page changed since checkpoint or has an invalid row offset");
        }
        String cohort = null;
        if (!identities.isEmpty()) {
            MessageDigest identityDigest = ChargifyConfig.sha256();
            identities.forEach(id -> hashField(identityDigest, id));
            cohort = encode(identityDigest);
            if (cohort.equals(state.previousIdentities)) {
                throw resource.failure("adjacent pages repeat the same provider identifiers");
            }
        }
        ChargifyTransport.checkDeadline(resource, deadline);
        return new ChargifyPage(rows, state.advance(cohort), fingerprint);
    }

    private static JsonNode records(byte[] bytes, ChargifyResource resource, int size)
            throws ChargifyReadException {
        JsonNode root;
        try {
            String text =
                    StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            root = JSON.readTree(text);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("expected a single valid UTF-8 JSON response");
        }
        if (root == null || root.isMissingNode()) {
            throw resource.failure("missing response document");
        }
        JsonNode array = resource == ChargifyResource.INVOICES ? root.path("invoices") : root;
        if (!array.isArray() || array.size() > size) {
            throw resource.failure("expected the native collection array within page_size");
        }
        return array;
    }

    private static String identifier(JsonNode record, ChargifyResource resource)
            throws ChargifyReadException {
        if (resource == ChargifyResource.INVOICES) {
            JsonNode uid = record.path("uid");
            if (uid.isTextual()) {
                String value = uid.textValue();
                if (!value.isBlank()
                        && value.length() <= 256
                        && value.codePoints()
                                .noneMatch(
                                        character ->
                                                Character.isISOControl(character)
                                                        || character >= Character.MIN_SURROGATE
                                                                && character
                                                                        <= Character
                                                                                .MAX_SURROGATE)) {
                    return value;
                }
            }
            throw resource.failure("invalid invoice uid");
        }
        JsonNode id = record.path("id");
        if (!id.isIntegralNumber() || id.bigIntegerValue().signum() <= 0) {
            throw resource.failure("expected a positive integral provider id");
        }
        return id.asText();
    }

    private static String payload(JsonNode record, ChargifyResource resource)
            throws ChargifyReadException {
        try {
            return JSON.writeValueAsString(record);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("could not encode the provider record");
        }
    }

    private static String encode(MessageDigest digest) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    }

    private static void hashField(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        digest.update((byte) (length >>> 24));
        digest.update((byte) (length >>> 16));
        digest.update((byte) (length >>> 8));
        digest.update((byte) length);
        digest.update(bytes);
    }
}

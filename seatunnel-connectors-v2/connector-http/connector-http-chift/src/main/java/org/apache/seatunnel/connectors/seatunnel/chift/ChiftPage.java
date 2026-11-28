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
package org.apache.seatunnel.connectors.seatunnel.chift;

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Validates one complete Chift collection or numbered execution page.
 *
 * <p>Scope, page metadata, and all returned records are checked before a successor becomes
 * available. An active partial page binds both returned data and metadata to its saved offset.
 */
final class ChiftPage {
    private static final int MAX_COLLECTION_RECORDS = 50000;
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
    final ChiftSplit next;
    final String fingerprint;

    private ChiftPage(List<SeaTunnelRow> rows, ChiftSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Reads data for the current resource and prepares its next immutable position.
     *
     * <p>The reader applies that position only once every selected row has been emitted.
     */
    static ChiftPage read(
            ChiftTransport transport,
            ChiftConfig config,
            ChiftSplit state,
            String table,
            long deadline)
            throws ChiftReadException, InterruptedException {
        ChiftResource resource = config.plan.get(state.resourceIndex);
        String parent = state.parent(resource);
        JsonNode document = document(transport.get(resource, state, deadline), resource);
        boolean execution = resource == ChiftResource.EXECUTIONS;
        JsonNode records = execution ? document.path("items") : document;
        if (!records.isArray()
                || records.size() > (execution ? config.pageSize : MAX_COLLECTION_RECORDS)) {
            throw resource.failure("expected a bounded native resource array");
        }
        long total =
                execution ? validateCount(document, records.size(), config, state) : records.size();
        boolean hasNext = execution && (long) state.page * config.pageSize < total;
        boolean discover =
                resource == ChiftResource.CONSUMERS
                                && config.plan.contains(ChiftResource.CONNECTIONS)
                        || resource == ChiftResource.SYNCS
                                && config.plan.contains(ChiftResource.EXECUTIONS);
        if (discover && records.size() > config.maxParents) {
            throw resource.failure("parent discovery exceeds max_parents");
        }
        List<String> parents = discover ? new ArrayList<>(records.size()) : List.of();
        Set<String> identities = new HashSet<>();
        boolean emit = config.resources.contains(resource);
        List<SeaTunnelRow> output = new ArrayList<>();
        MessageDigest digest = ChiftConfig.sha256();
        hashField(digest, Long.toString(total));
        for (JsonNode record : records) {
            if (!record.isObject()) {
                throw resource.failure("expected a provider record object");
            }
            String id = id(record.path(resource.idField), execution, resource);
            String identity = execution ? ChiftConfig.digest(id) : id.toLowerCase(Locale.ROOT);
            if (!identities.add(identity)) {
                throw resource.failure("duplicate provider id in one response");
            }
            if (execution && state.previousHashes.contains(identity)) {
                throw resource.failure("adjacent execution pages reuse a provider id");
            }
            if (discover) {
                parents.add(id);
            }
            validateParent(record, parent, resource);
            String payload = json(record, resource);
            hashField(digest, payload);
            if (emit) {
                Instant started =
                        execution ? ChiftTime.start(record.path("start"), config.timezone) : null;
                if (!execution
                        || (config.start == null || !started.isBefore(config.start))
                                && (state.cutoff == null || started.isBefore(state.cutoff))) {
                    SeaTunnelRow row =
                            new SeaTunnelRow(
                                    new Object[] {
                                        resource.id,
                                        id,
                                        parent,
                                        started == null ? null : started.toString(),
                                        payload
                                    });
                    row.setTableId(table);
                    output.add(row);
                }
            }
        }
        String fingerprint =
                Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        if (state.published < 0
                || state.published > output.size()
                || state.published > 0 && !fingerprint.equals(state.fingerprint)) {
            throw resource.failure(
                    "provider page changed since checkpoint or has an invalid row offset");
        }
        ChiftTransport.checkDeadline(resource, deadline);
        return new ChiftPage(
                output,
                state.advance(config, resource, total, hasNext, parents, identities),
                fingerprint);
    }

    private static long validateCount(
            JsonNode envelope, int rows, ChiftConfig config, ChiftSplit state)
            throws ChiftReadException {
        ChiftResource operation = ChiftResource.EXECUTIONS;
        long page = dimension(envelope.path("page"), operation);
        long size = dimension(envelope.path("size"), operation);
        long total = dimension(envelope.path("total"), operation);
        if (page != state.page
                || size != config.pageSize
                || (state.total >= 0 && state.total != total)) {
            throw operation.failure("execution page, size or total changed");
        }
        long remaining = total - ((page - 1) * size);
        if (remaining < 0 || rows != Math.min(remaining, size) || (page > 1 && rows == 0)) {
            throw operation.failure("execution row count does not match page metadata");
        }
        return total;
    }

    private static long dimension(JsonNode field, ChiftResource resource)
            throws ChiftReadException {
        if (!field.isIntegralNumber() || !field.canConvertToLong() || field.longValue() < 0L) {
            throw resource.failure("expected nonnegative integer execution pagination metadata");
        }
        return field.longValue();
    }

    private static void validateParent(JsonNode record, String parent, ChiftResource resource)
            throws ChiftReadException {
        if (parent != null) {
            String key = resource == ChiftResource.CONNECTIONS ? "consumerid" : "syncid";
            if (record.hasNonNull(key)
                    && !parent.equalsIgnoreCase(id(record.get(key), false, resource))) {
                throw resource.failure("provider record belongs to a different parent");
            }
        }
    }

    private static String id(JsonNode value, boolean opaque, ChiftResource resource)
            throws ChiftReadException {
        String text = value.textValue();
        if (opaque) {
            if (text != null
                    && !text.isBlank()
                    && text.length() <= 256
                    && text.codePoints()
                            .noneMatch(
                                    character ->
                                            Character.isISOControl(character)
                                                    || character >= Character.MIN_SURROGATE
                                                            && character
                                                                    <= Character.MAX_SURROGATE)) {
                return text;
            }
        } else if (ChiftConfig.isUuid(text)) {
            return text;
        }
        throw resource.failure(
                opaque ? "expected an opaque execution id" : "expected a UUID provider id");
    }

    static JsonNode document(byte[] bytes, ChiftResource resource) throws ChiftReadException {
        try {
            String text =
                    StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            JsonNode value = JSON.readTree(text);
            if (value != null && (value.isObject() || value.isArray())) {
                return value;
            }
        } catch (IOException | RuntimeException invalid) {
            // Parser errors can contain private provider data or bearer values.
        }
        throw resource.failure("expected a single valid UTF-8 JSON document");
    }

    static byte[] credentials(ChiftConfig config, ChiftResource operation)
            throws ChiftReadException {
        try {
            return JSON.writeValueAsBytes(
                    Map.of(
                            "clientId",
                            config.clientId,
                            "clientSecret",
                            config.clientSecret,
                            "accountId",
                            config.accountId));
        } catch (IOException | RuntimeException invalid) {
            throw operation.failure("could not prepare the token request");
        }
    }

    private static String json(JsonNode record, ChiftResource resource) throws ChiftReadException {
        try {
            return JSON.writeValueAsString(record);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("could not encode the provider record");
        }
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

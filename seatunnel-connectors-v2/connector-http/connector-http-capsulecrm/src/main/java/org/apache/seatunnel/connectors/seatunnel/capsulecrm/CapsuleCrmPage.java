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
package org.apache.seatunnel.connectors.seatunnel.capsulecrm;

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
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates one scoped Capsule response before any output is exposed to the engine.
 *
 * <p>Resume fingerprints include every raw record in order, including excluded opportunities, plus
 * the validated continuation needed to move past the response.
 */
final class CapsuleCrmPage {
    private static final int MAX_RECORDS = 50000;
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
    final CapsuleCrmSplit next;
    final String fingerprint;

    private CapsuleCrmPage(List<SeaTunnelRow> rows, CapsuleCrmSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Retrieves a batch and computes its successor without committing reader progress.
     *
     * <p>The reader accepts the successor only after publishing every selected row of the page.
     */
    static CapsuleCrmPage read(
            CapsuleCrmTransport client,
            CapsuleCrmConfig config,
            CapsuleCrmSplit state,
            String table,
            long deadline)
            throws CapsuleCrmReadException, InterruptedException {
        CapsuleCrmResource resource = config.plan.get(state.resourceIndex);
        String parent = state.parent(resource);
        CapsuleCrmTransport.PageReply response = client.get(resource, parent, state.page, deadline);
        List<JsonNode> records = parse(response.bytes, resource, config.pageSize);
        boolean discover = resource == CapsuleCrmResource.PARTIES && config.discoverParents;
        boolean emit = config.resources.contains(resource);
        List<String> parents = discover ? new ArrayList<>(state.parents) : state.parents;
        Set<String> discoveryIds = discover ? new HashSet<>(parents) : Set.of();
        Set<String> pageIds = new HashSet<>();
        List<SeaTunnelRow> rows = new ArrayList<>();
        MessageDigest digest = CapsuleCrmConfig.sha256();
        hashField(digest, Integer.toString(response.next));
        for (JsonNode record : records) {
            String id =
                    resource == CapsuleCrmResource.SITE
                            ? siteId(record, resource)
                            : identifier(record.path("id"), resource);
            if (!pageIds.add(id)) {
                throw resource.failure("duplicate provider id in one page");
            }
            if (discover && isOrganisation(record, resource) && discoveryIds.add(id)) {
                if (parents.size() == config.maxParents) {
                    throw resource.failure("organisation discovery exceeds max_parents");
                }
                parents.add(id);
            }
            if (resource == CapsuleCrmResource.EMPLOYEES
                    && record.hasNonNull("organisation")
                    && !parent.equals(organisationId(record.get("organisation"), resource))) {
                throw resource.failure("employee organisation differs from the requested parent");
            }
            String payload = payload(record, resource);
            hashField(digest, payload);
            String updated = updated(record, resource);
            if (emit && withinWindow(config, state, resource, updated)) {
                String owner =
                        resource == CapsuleCrmResource.TAGS
                                        || resource == CapsuleCrmResource.CUSTOM_FIELDS
                                ? config.entity
                                : parent;
                SeaTunnelRow row =
                        new SeaTunnelRow(new Object[] {resource.id, id, owner, updated, payload});
                row.setTableId(table);
                rows.add(row);
            }
        }
        String fingerprint =
                Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        if (state.published < 0
                || state.published > rows.size()
                || state.published > 0 && !fingerprint.equals(state.fingerprint)) {
            throw resource.failure(
                    "provider page changed since checkpoint or has an invalid row offset");
        }
        CapsuleCrmTransport.checkDeadline(resource, deadline);
        return new CapsuleCrmPage(
                rows, state.advance(config, resource, response.next, parents), fingerprint);
    }

    private static boolean isOrganisation(JsonNode record, CapsuleCrmResource resource)
            throws CapsuleCrmReadException {
        String kind = record.path("type").asText("");
        if (!"organisation".equals(kind) && !"person".equals(kind)) {
            throw resource.failure("expected party type person or organisation for discovery");
        }
        return "organisation".equals(kind);
    }

    private static String organisationId(JsonNode item, CapsuleCrmResource resource)
            throws CapsuleCrmReadException {
        return identifier(item.isObject() ? item.path("id") : item, resource);
    }

    private static List<JsonNode> parse(byte[] bytes, CapsuleCrmResource resource, int pageSize)
            throws CapsuleCrmReadException {
        JsonNode envelope;
        try {
            String text =
                    StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            envelope = JSON.readTree(text);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("expected a single valid UTF-8 JSON response");
        }
        if (envelope == null || !envelope.isObject()) {
            throw resource.failure("expected a Capsule API object envelope");
        }
        JsonNode records = envelope.path(resource.envelope);
        if (resource == CapsuleCrmResource.SITE) {
            if (!records.isObject()) {
                throw resource.failure("expected the site object envelope");
            }
            return List.of(records);
        }
        int maximum = resource.pageSizeSupported ? pageSize : MAX_RECORDS;
        if (!records.isArray() || records.size() > maximum) {
            throw resource.failure(
                    "expected the selected resource array within its response limit");
        }
        List<JsonNode> result = new ArrayList<>(records.size());
        for (JsonNode item : records) {
            if (!item.isObject()) {
                throw resource.failure("expected each page record to be a JSON object");
            }
            result.add(item);
        }
        return result;
    }

    private static String siteId(JsonNode record, CapsuleCrmResource resource)
            throws CapsuleCrmReadException {
        JsonNode label = record.path("subdomain");
        if (!label.isTextual() || !label.textValue().matches("[A-Za-z0-9-]{1,128}")) {
            throw resource.failure("expected the site subdomain as its identifier");
        }
        return label.textValue();
    }

    private static String identifier(JsonNode field, CapsuleCrmResource resource)
            throws CapsuleCrmReadException {
        if (field.isIntegralNumber()
                && (field.bigIntegerValue().signum() > 0
                        || resource == CapsuleCrmResource.ACTIVITY_TYPES)) {
            return field.asText();
        }
        if (resource == CapsuleCrmResource.ACTIVITY_TYPES
                && field.isTextual()
                && field.textValue().matches("[A-Za-z0-9_-]{1,128}")) {
            return field.textValue();
        }
        throw resource.failure("invalid provider identifier");
    }

    private static String updated(JsonNode record, CapsuleCrmResource resource)
            throws CapsuleCrmReadException {
        JsonNode field = record.path("updatedAt");
        if (field.isMissingNode() || field.isNull()) {
            return null;
        }
        if (!field.isTextual() || field.textValue().length() > 128) {
            throw resource.failure("invalid updatedAt value");
        }
        return field.textValue();
    }

    private static boolean withinWindow(
            CapsuleCrmConfig config,
            CapsuleCrmSplit state,
            CapsuleCrmResource resource,
            String value)
            throws CapsuleCrmReadException {
        if (resource != CapsuleCrmResource.OPPORTUNITIES
                || (config.start == null && state.cutoff == null)) {
            return true;
        }
        if (value == null) {
            throw resource.failure("bounded opportunity reads require updatedAt");
        }
        try {
            Instant instant = OffsetDateTime.parse(value).toInstant();
            return (config.start == null || !instant.isBefore(config.start))
                    && (state.cutoff == null || instant.isBefore(state.cutoff));
        } catch (DateTimeParseException invalid) {
            throw resource.failure("invalid opportunity updatedAt timestamp");
        }
    }

    private static String payload(JsonNode record, CapsuleCrmResource resource)
            throws CapsuleCrmReadException {
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

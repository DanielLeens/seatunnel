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
package org.apache.seatunnel.connectors.seatunnel.castoredc;

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
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validates and selects records from a Castor document or HAL page.
 *
 * <p>The digest binds the whole page, including time-filtered records, and the validated successor
 * to partially emitted checkpoint state.
 */
final class CastorEdcPage {
    private static final int MAX_RECORDS = 50000;
    private static final Pattern STUDY_ID =
            Pattern.compile("[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}");
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
    final CastorEdcSplit next;
    final String fingerprint;

    private CastorEdcPage(List<SeaTunnelRow> rows, CastorEdcSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Fetches and validates the next raw result on the acquisition thread.
     *
     * <p>Parent discoveries and the next page are held back until the collector publishes the
     * entire page, including pages where filtering produces no rows.
     */
    static CastorEdcPage read(
            CastorEdcTransport client,
            CastorEdcConfig config,
            CastorEdcSplit state,
            String table,
            long deadline)
            throws CastorEdcReadException, InterruptedException {
        CastorEdcResource resource = config.plan.get(state.resourceIndex);
        String parent = state.parent(resource);
        CastorEdcTransport.PageReply response = client.get(resource, state, deadline);
        JsonNode document = parseDocument(response.bytes, resource);
        List<JsonNode> records = records(document, resource);
        int next = CastorEdcHal.next(document, response.request, resource, state, records.size());
        boolean discover = resource == CastorEdcResource.STUDY && config.discoverStudies;
        boolean emit = config.resources.contains(resource);
        List<String> studies = discover ? new ArrayList<>(state.studies) : state.studies;
        Set<String> discovered = discover ? new HashSet<>(studies) : Set.of();
        Set<String> identities = new HashSet<>();
        List<SeaTunnelRow> rows = new ArrayList<>();
        MessageDigest digest = CastorEdcConfig.sha256();
        hashField(digest, Integer.toString(next));
        for (JsonNode record : records) {
            String payload = payload(record, resource);
            hashField(digest, payload);
            String id = identity(record, resource, payload);
            if (resource != CastorEdcResource.AUDIT_TRIAL && !identities.add(id)) {
                throw resource.failure("duplicate provider id in a response");
            }
            if (discover && discovered.add(id)) {
                if (studies.size() == config.maxStudies) {
                    throw resource.failure("study discovery exceeds max_studies");
                }
                studies.add(id);
            }
            if (parent != null
                    && record.hasNonNull("study_id")
                    && !parent.equals(studyId(record.get("study_id"), resource))) {
                throw resource.failure("record belongs to a different study");
            }
            if (emit) {
                Instant time = CastorEdcTime.time(record, resource, config.timezone);
                if (selected(time, state, config, resource)) {
                    String study = resource == CastorEdcResource.STUDY ? id : parent;
                    SeaTunnelRow row =
                            new SeaTunnelRow(
                                    new Object[] {
                                        resource.id,
                                        id,
                                        study,
                                        time == null ? null : time.toString(),
                                        payload
                                    });
                    row.setTableId(table);
                    rows.add(row);
                }
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
        CastorEdcTransport.checkDeadline(resource, deadline);
        return new CastorEdcPage(
                rows, state.advance(config, resource, next, records.size(), studies), fingerprint);
    }

    static JsonNode parseDocument(byte[] bytes, CastorEdcResource resource)
            throws CastorEdcReadException {
        try {
            String text =
                    StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            JsonNode value = JSON.readTree(text);
            if (value != null && (value.isArray() || value.isObject())) {
                return value;
            }
        } catch (IOException | RuntimeException invalid) {
            // Parsing exceptions can contain fragments of private study records or OAuth tokens.
        }
        throw resource.failure("expected a single valid UTF-8 JSON document");
    }

    private static List<JsonNode> records(JsonNode document, CastorEdcResource resource)
            throws CastorEdcReadException {
        JsonNode values;
        switch (resource) {
            case STUDY_STATISTICS:
                if (!document.isObject()) {
                    throw resource.failure("expected a study statistics object");
                }
                return List.of(document);
            case USER:
            case STUDY_USER:
                values =
                        document.isArray()
                                ? document
                                : document.path("_embedded").path(resource.envelope);
                if (resource == CastorEdcResource.USER && values.isObject()) {
                    return List.of(values);
                }
                break;
            case COUNTRY:
            case AUDIT_TRIAL:
                values = document.path(resource.envelope);
                break;
            default:
                values = document.path("_embedded").path(resource.envelope);
        }
        if (!values.isArray() || values.size() > MAX_RECORDS) {
            throw resource.failure(
                    "expected the selected resource array within its response limit");
        }
        List<JsonNode> result = new ArrayList<>(values.size());
        for (JsonNode item : values) {
            if (!item.isObject()) {
                throw resource.failure("a resource record must be a JSON object");
            }
            result.add(item);
        }
        return result;
    }

    private static String identity(JsonNode record, CastorEdcResource resource, String payload)
            throws CastorEdcReadException {
        switch (resource) {
            case STUDY:
                JsonNode primary = record.path("study_id");
                JsonNode legacy = record.path("crf_id");
                if (!hasValue(primary)) {
                    return studyId(legacy, resource);
                }
                String actual = studyId(primary, resource);
                if (hasValue(legacy) && !actual.equals(studyId(legacy, resource))) {
                    throw resource.failure("conflicting study identifier fields");
                }
                return actual;
            case STUDY_STATISTICS:
                return studyId(record.path("study_id"), resource);
            case AUDIT_TRIAL:
                JsonNode marker = record.path("uuid");
                return hasValue(marker)
                        ? identifier(marker, resource)
                        : "sha256:" + CastorEdcConfig.digest(payload);
            case STUDY_ROLE:
                JsonNode legacyRole = record.path("uuid");
                return identifier(
                        hasValue(legacyRole) ? legacyRole : record.path("name"), resource);
            default:
                return identifier(record.path("id"), resource);
        }
    }

    private static boolean hasValue(JsonNode value) {
        return !value.isMissingNode() && !value.isNull();
    }

    private static String studyId(JsonNode raw, CastorEdcResource resource)
            throws CastorEdcReadException {
        if (!raw.isTextual() || !STUDY_ID.matcher(raw.textValue()).matches()) {
            throw resource.failure("expected a UUID study identifier");
        }
        return raw.textValue().toUpperCase(Locale.ROOT);
    }

    private static String identifier(JsonNode field, CastorEdcResource resource)
            throws CastorEdcReadException {
        if (field.isIntegralNumber() && field.bigIntegerValue().signum() > 0) {
            return field.asText();
        }
        if (field.isTextual()) {
            String value = field.textValue();
            if (!value.isBlank()
                    && value.length() <= 256
                    && value.codePoints()
                            .noneMatch(
                                    character ->
                                            Character.isISOControl(character)
                                                    || character >= Character.MIN_SURROGATE
                                                            && character
                                                                    <= Character.MAX_SURROGATE)) {
                return value;
            }
        }
        throw resource.failure("invalid provider identifier");
    }

    private static boolean selected(
            Instant time, CastorEdcSplit state, CastorEdcConfig config, CastorEdcResource resource)
            throws CastorEdcReadException {
        if (time == null) {
            if (resource == CastorEdcResource.AUDIT_TRIAL || resource == CastorEdcResource.STUDY) {
                if (config.start != null || state.cutoff != null) {
                    throw resource.failure(
                            "the selected time window requires a provider timestamp");
                }
            }
            return config.start == null && state.cutoff == null
                    || resource != CastorEdcResource.USER
                            && resource != CastorEdcResource.STUDY_USER;
        }
        return (config.start == null || !time.isBefore(config.start))
                && (state.cutoff == null || time.isBefore(state.cutoff));
    }

    private static String payload(JsonNode record, CastorEdcResource resource)
            throws CastorEdcReadException {
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

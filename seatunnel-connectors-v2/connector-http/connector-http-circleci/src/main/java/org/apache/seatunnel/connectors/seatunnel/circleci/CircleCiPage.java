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
package org.apache.seatunnel.connectors.seatunnel.circleci;

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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Checks an entire response before handing any rows or successor state to the reader.
 *
 * <p>A partial checkpoint binds the full response, including filtered rows and its successor token.
 */
final class CircleCiPage {
    static final int MAX_ROWS = 20000;
    private static final int MAX_TOKEN_LENGTH = 4096;
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
    final CircleCiSplit next;
    final String fingerprint;

    private CircleCiPage(List<SeaTunnelRow> rows, CircleCiSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Reads and validates a complete page or a single provider record.
     *
     * <p>Discoveries are collected before output time filtering and committed with the page.
     */
    static CircleCiPage read(
            CircleCiTransport transport,
            CircleCiConfig settings,
            CircleCiSplit state,
            String table,
            long deadline)
            throws CircleCiReadException, InterruptedException {
        CircleCiResource resource = settings.plan.get(state.resourceIndex);
        byte[] response = transport.get(resource, state, deadline);
        String fingerprint = CircleCiConfig.digest(response);
        if (state.published > 0 && !fingerprint.equals(state.fingerprint)) {
            throw resource.failure("provider page changed since checkpoint");
        }
        JsonNode document = document(response, resource);
        String continuation =
                resource.shape == CircleCiResource.Shape.PAGE
                        ? nextToken(document, resource)
                        : null;
        Iterable<JsonNode> records = records(document, resource);
        boolean discover = resource == CircleCiResource.WORKFLOW_JOBS && settings.discoversJobs();
        Set<Long> discoveries = discover ? new LinkedHashSet<>(state.jobNumbers) : Set.of();
        boolean emit = settings.resources.contains(resource);
        String parent = resource.parent(settings, state);
        Set<String> ids = new HashSet<>();
        List<SeaTunnelRow> output = new ArrayList<>();
        for (JsonNode record : records) {
            CircleCiTransport.checkDeadline(resource, deadline);
            if (!record.isObject()) {
                throw resource.failure("expected a provider record object");
            }
            String id = identity(record, resource, settings, state);
            if (!ids.add(id)) {
                throw resource.failure("duplicate provider record identity in one response");
            }
            if (discover) {
                discover(record, settings, discoveries, resource);
            }
            Instant time = time(record, resource);
            if (emit && inWindow(time, settings.start, state.cutoff)) {
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {
                                    resource.id,
                                    id,
                                    parent,
                                    time == null ? null : time.toString(),
                                    json(record, resource)
                                });
                row.setTableId(table);
                output.add(row);
            }
        }
        if (state.published > output.size()) {
            throw resource.failure("checkpoint row offset exceeds the response length");
        }
        List<Long> jobs = discover ? List.copyOf(discoveries) : state.jobNumbers;
        CircleCiSplit next = state.after(settings, resource, continuation, jobs);
        CircleCiTransport.checkDeadline(resource, deadline);
        return new CircleCiPage(output, next, fingerprint);
    }

    private static JsonNode document(byte[] response, CircleCiResource resource)
            throws CircleCiReadException {
        try {
            String decoded =
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .decode(ByteBuffer.wrap(response))
                            .toString();
            JsonNode root = JSON.readTree(decoded);
            if (root == null || root.isNull()) {
                throw resource.failure("expected a JSON response");
            }
            return root;
        } catch (CircleCiReadException failure) {
            throw failure;
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("invalid UTF-8 JSON response");
        }
    }

    private static String json(JsonNode record, CircleCiResource resource)
            throws CircleCiReadException {
        try {
            return JSON.writeValueAsString(record);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("could not preserve the provider record as JSON");
        }
    }

    private static Iterable<JsonNode> records(JsonNode root, CircleCiResource resource)
            throws CircleCiReadException {
        if (resource.shape == CircleCiResource.Shape.OBJECT) {
            if (!root.isObject()) {
                throw resource.failure("expected a root JSON object");
            }
            return List.of(root);
        }
        JsonNode array = resource.shape == CircleCiResource.Shape.PAGE ? root.path("items") : root;
        if (!array.isArray() || array.size() > MAX_ROWS) {
            throw resource.failure("expected a provider array with at most 20000 records");
        }
        return array;
    }

    private static String nextToken(JsonNode envelope, CircleCiResource resource)
            throws CircleCiReadException {
        if (!envelope.isObject() || !envelope.has("next_page_token")) {
            throw resource.failure("missing native next_page_token metadata");
        }
        JsonNode field = envelope.get("next_page_token");
        if (field.isNull() || "".equals(field.textValue())) {
            return null;
        }
        String value = field.textValue();
        if (value == null
                || value.isBlank()
                || value.length() > MAX_TOKEN_LENGTH
                || value.codePoints().anyMatch(CircleCiPage::unsafeCharacter)) {
            throw resource.failure("invalid native next_page_token metadata");
        }
        return value;
    }

    private static String identity(
            JsonNode row,
            CircleCiResource resource,
            CircleCiConfig settings,
            CircleCiSplit position)
            throws CircleCiReadException {
        switch (resource) {
            case SELF_COLLABORATIONS:
                return row.hasNonNull("id")
                        ? text(row, "id", resource)
                        : text(row, "vcs-type", resource) + ":" + text(row, "slug", resource);
            case ME:
                return text(
                        row, row.hasNonNull("analytics_id") ? "analytics_id" : "login", resource);
            case PROJECTS:
                return text(row, "vcs_url", resource);
            case SPECIFIC_PROJECT:
                checkProject(row, "slug", settings, resource);
                return text(row, "id", resource);
            case WORKFLOW:
                String workflow = text(row, "id", resource);
                if (!settings.workflowIds.get(position.workflowIndex).equalsIgnoreCase(workflow)) {
                    throw resource.failure("workflow ID does not match the requested workflow");
                }
                return workflow;
            case JOBS:
                checkProject(row.path("project"), "slug", settings, resource);
                long number = jobNumber(row.path("number"), resource);
                if (position.jobNumber(settings) != number) {
                    throw resource.failure("job number does not match the requested job");
                }
                return Long.toString(number);
            case INSIGHTS_BRANCHES:
                JsonNode branches = row.path("branches");
                if (!branches.isArray() || branches.size() > 5000) {
                    throw resource.failure("expected an Insights array with at most 5000 branches");
                }
                for (JsonNode branch : branches) {
                    if (!branch.isTextual()) {
                        throw resource.failure("Insights branch names must be strings");
                    }
                }
                return insightProject(row, settings, resource);
            case INSIGHTS_METRICS:
                return row.hasNonNull("project_id")
                        ? insightProject(row, settings, resource)
                        : settings.projectSlug;
            default:
                return text(row, "id", resource);
        }
    }

    private static String insightProject(
            JsonNode row, CircleCiConfig settings, CircleCiResource resource)
            throws CircleCiReadException {
        String id = text(row, "project_id", resource);
        String project = settings.projectSlug.substring(settings.projectSlug.lastIndexOf('/') + 1);
        if (settings.projectSlug.startsWith("circleci/")
                && CircleCiConfig.isUuid(project)
                && !project.equalsIgnoreCase(id)) {
            throw resource.failure("Insights project ID does not match the requested project");
        }
        return id;
    }

    private static void checkProject(
            JsonNode row, String key, CircleCiConfig settings, CircleCiResource resource)
            throws CircleCiReadException {
        if (!CircleCiConfig.sameProject(text(row, key, resource), settings.projectSlug)) {
            throw resource.failure("project slug does not match the requested project");
        }
    }

    private static void discover(
            JsonNode row, CircleCiConfig settings, Set<Long> discoveries, CircleCiResource resource)
            throws CircleCiReadException {
        if (!row.hasNonNull("job_number")) {
            return;
        }
        long number = jobNumber(row.get("job_number"), resource);
        if (CircleCiConfig.sameProject(text(row, "project_slug", resource), settings.projectSlug)) {
            discoveries.add(number);
            if (discoveries.size() > settings.maxJobs) {
                throw resource.failure("job discovery exceeds max_jobs");
            }
        }
    }

    private static long jobNumber(JsonNode field, CircleCiResource resource)
            throws CircleCiReadException {
        if (!field.isIntegralNumber() || !field.canConvertToLong() || field.longValue() < 0) {
            throw resource.failure("expected a nonnegative 64-bit job number");
        }
        return field.longValue();
    }

    private static String text(JsonNode row, String key, CircleCiResource resource)
            throws CircleCiReadException {
        String value = row.path(key).textValue();
        if (value == null
                || value.isBlank()
                || value.length() > 4096
                || value.codePoints().anyMatch(CircleCiPage::unsafeCharacter)) {
            throw resource.failure("invalid or missing " + key);
        }
        return value;
    }

    private static boolean unsafeCharacter(int character) {
        return Character.isISOControl(character)
                || character >= Character.MIN_SURROGATE && character <= Character.MAX_SURROGATE;
    }

    private static Instant time(JsonNode row, CircleCiResource resource)
            throws CircleCiReadException {
        if (!resource.hasTime() || !resource.timeRequired && !row.hasNonNull(resource.timeField)) {
            return null;
        }
        String value = text(row, resource.timeField, resource);
        if (value.length() <= 64) {
            try {
                return OffsetDateTime.parse(value).toInstant();
            } catch (DateTimeParseException invalid) {
                // Avoid attaching an exception that contains a provider value.
            }
        }
        throw resource.failure("expected an ISO-8601 " + resource.timeField + " timestamp");
    }

    private static boolean inWindow(Instant time, Instant start, Instant cutoff) {
        return time == null
                || (start == null || !time.isBefore(start))
                        && (cutoff == null || time.isBefore(cutoff));
    }
}

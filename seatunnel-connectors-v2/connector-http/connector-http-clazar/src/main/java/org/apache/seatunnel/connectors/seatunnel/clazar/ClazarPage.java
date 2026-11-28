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
package org.apache.seatunnel.connectors.seatunnel.clazar;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import okhttp3.HttpUrl;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Validates a complete page before anything is exposed to the engine.
 *
 * <p>Logical resume coordinates come from the local request, never from a provider-supplied URL.
 */
final class ClazarPage {
    final List<SeaTunnelRow> rows;
    final ClazarSplit next;
    final String fingerprint;

    private ClazarPage(List<SeaTunnelRow> rows, ClazarSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Reads one bounded provider page and computes its tentative successor.
     *
     * <p>All native records and pagination fields are fingerprinted before applying the entity
     * output window.
     */
    static ClazarPage read(
            ClazarTransport client,
            ClazarConfig settings,
            ClazarSplit state,
            String table,
            long deadline)
            throws ClazarReadException, InterruptedException {
        ClazarResource resource = settings.resources.get(state.resourceIndex);
        HttpUrl request =
                resource.target(HttpUrl.get(settings.origin), state.page, settings.pageSize);
        byte[] response = client.get(resource, request, deadline);
        String fingerprint = ClazarConfig.digest(response);
        if (state.published > 0 && !fingerprint.equals(state.fingerprint)) {
            throw resource.failure("provider page changed since checkpoint");
        }
        JsonNode envelope = ClazarJson.object(response, resource.id);
        JsonNode results = envelope.path("results");
        if (!results.isArray() || results.size() > settings.pageSize) {
            throw resource.failure("expected a results array within page_size");
        }
        long expected = count(envelope.path("count"), state, resource);
        long seen = state.consumed + results.size();
        boolean more =
                continuation(
                        envelope.path("next"), request, state.page, settings.pageSize, resource);
        if (seen > expected || more && seen >= expected || !more && seen != expected) {
            throw resource.failure("continuation does not agree with the provider count");
        }
        List<SeaTunnelRow> output = new ArrayList<>(results.size());
        Set<String> ids = resource.entity ? new HashSet<>() : Set.of();
        for (JsonNode row : results) {
            ClazarTransport.checkDeadline(resource.id, deadline);
            if (!row.isObject()) {
                throw resource.failure("expected a provider record object");
            }
            String id = null;
            Instant modified = null;
            if (resource.entity) {
                id = row.path("id").textValue();
                if (!ClazarConfig.isUuid(id)) {
                    throw resource.failure("expected an entity UUID");
                }
                String identity = ClazarConfig.digest(id.toLowerCase(Locale.ROOT));
                if (!ids.add(identity) || state.previousIds.contains(identity)) {
                    throw resource.failure(
                            "entity identity repeated within or across adjacent pages");
                }
                modified = modified(row.path("last_modified_at"), resource);
            }
            if (includes(modified, settings.start, state.cutoff)) {
                SeaTunnelRow value =
                        new SeaTunnelRow(
                                new Object[] {
                                    resource.id,
                                    id,
                                    modified == null ? null : modified.toString(),
                                    ClazarJson.record(row, resource)
                                });
                value.setTableId(table);
                output.add(value);
            }
        }
        if (state.published > output.size()) {
            throw resource.failure("checkpoint row offset exceeds the response length");
        }
        ClazarTransport.checkDeadline(resource.id, deadline);
        return new ClazarPage(output, state.after(expected, seen, more, ids), fingerprint);
    }

    private static boolean continuation(
            JsonNode field, HttpUrl target, int page, int size, ClazarResource resource)
            throws ClazarReadException {
        if (field.isNull()) {
            return false;
        }
        String link = field.textValue();
        if (link == null
                || link.isBlank()
                || link.length() > 4096
                || link.codePoints().anyMatch(ClazarConfig::unsafeCharacter)) {
            throw resource.failure("missing or malformed next page link");
        }
        HttpUrl next = target.resolve(link);
        if (next == null
                || !next.scheme().equals(target.scheme())
                || !next.host().equals(target.host())
                || next.port() != target.port()
                || !next.encodedPath().equals(target.encodedPath())
                || !next.username().isEmpty()
                || !next.password().isEmpty()
                || next.fragment() != null) {
            throw resource.failure("next page link leaves the configured resource");
        }
        List<String> pages = next.queryParameterValues("page");
        List<String> sizes = next.queryParameterValues("page_size");
        if (!Set.of("page", "page_size").containsAll(next.queryParameterNames())
                || !pages.equals(List.of(Integer.toString(page + 1)))
                || !sizes.isEmpty() && !sizes.equals(List.of(Integer.toString(size)))) {
            throw resource.failure("next page link has inconsistent query parameters");
        }
        return true;
    }

    private static long count(JsonNode node, ClazarSplit state, ClazarResource resource)
            throws ClazarReadException {
        if (!node.isIntegralNumber() || !node.canConvertToLong() || node.longValue() < 0) {
            throw resource.failure("expected a nonnegative provider count");
        }
        long count = node.longValue();
        if (state.total >= 0 && count != state.total) {
            throw resource.failure("provider count changed during pagination");
        }
        return count;
    }

    private static Instant modified(JsonNode value, ClazarResource resource)
            throws ClazarReadException {
        String text = value.textValue();
        try {
            if (text != null && text.length() <= 64) {
                return OffsetDateTime.parse(text).toInstant();
            }
        } catch (DateTimeParseException invalid) {
            // Drop the provider value before reporting a malformed timestamp.
        }
        throw resource.failure("expected an ISO-8601 last_modified_at timestamp");
    }

    private static boolean includes(Instant modified, Instant start, Instant end) {
        return modified == null
                || (start == null || !modified.isBefore(start))
                        && (end == null || modified.isBefore(end));
    }
}

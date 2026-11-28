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
package org.apache.seatunnel.connectors.seatunnel.ciscomeraki;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiHttp.Reply;
import org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSplit.Position;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Executes and prepares one complete Cisco Meraki API response.
 *
 * <p>List, singleton, and wireless overview responses retain their distinct native shapes.
 */
final class CiscoMerakiApi {
    private final CiscoMerakiConfig config;
    private final CiscoMerakiHttp http;

    CiscoMerakiApi(CiscoMerakiConfig config, CiscoMerakiHttp http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Resolves the parent's scoped endpoint, fetches it, and prepares its continuation and records.
     *
     * <p>The replay digest also covers the normalized continuation and all unfiltered records.
     */
    Page read(
            CiscoMerakiResource resource,
            String organization,
            String network,
            CiscoMerakiSplit split,
            Position position,
            long deadline)
            throws InterruptedException {
        String path = resource.path(organization, network);
        Map<String, String> query = query(resource, split);
        String target =
                position.target == null
                        ? CiscoMerakiLink.requestTarget(path, query)
                        : checkedTarget(resource, path, query, position.target);
        Reply reply = http.request(resource, target, deadline);
        JsonNode raw = reply.body;
        if (raw.isObject() && raw.hasNonNull("errors")) {
            throw resource.failure("provider returned an error object");
        }
        String next =
                resource.paged ? CiscoMerakiLink.next(resource, path, query, reply.links) : null;
        if (next != null
                && (next.equals(target)
                        || position.visited.contains(CiscoMerakiConfig.hash(next)))) {
            throw resource.failure("repeated Link continuation");
        }
        List<Row> rows = rows(resource, organization, split, raw, deadline);
        http.remaining(resource, deadline);
        return new Page(rows, target, next, CiscoMerakiConfig.hash(raw.toString() + "\n" + next));
    }

    private static String checkedTarget(
            CiscoMerakiResource resource, String path, Map<String, String> query, String target) {
        if (!resource.paged
                || !target.equals(CiscoMerakiLink.scopedTarget(resource, path, query, target))) {
            throw resource.failure("invalid saved Link target");
        }
        return target;
    }

    private Map<String, String> query(CiscoMerakiResource resource, CiscoMerakiSplit split) {
        Map<String, String> query = new LinkedHashMap<>();
        if (resource.paged) {
            query.put("perPage", Integer.toString(config.pageSize));
        }
        if (resource == CiscoMerakiResource.API_REQUESTS) {
            query.put("t0", split.windowStart.toString());
            query.put("t1", split.windowEnd.toString());
        }
        return query;
    }

    private List<Row> rows(
            CiscoMerakiResource resource,
            String organization,
            CiscoMerakiSplit split,
            JsonNode raw,
            long deadline)
            throws InterruptedException {
        switch (resource) {
            case DATACENTERS:
                if (!raw.isObject() || !raw.path("continents").isArray()) {
                    throw resource.failure("expected a data-center object with a continents array");
                }
                return List.of(new Row(null, raw.toString()));
            case DEVICES:
                validateOverview(resource, raw, deadline);
                return List.of(new Row(null, raw.toString()));
            case SAML:
                if (!raw.isObject() || !raw.path("enabled").isBoolean()) {
                    throw resource.failure("expected a SAML settings object with enabled status");
                }
                return List.of(new Row(null, raw.toString()));
            case SETTINGS:
                if (!raw.isObject()) {
                    throw resource.failure("expected a network settings object");
                }
                return List.of(new Row(null, raw.toString()));
            default:
                return records(resource, organization, split, raw, deadline);
        }
    }

    private void validateOverview(CiscoMerakiResource resource, JsonNode raw, long deadline)
            throws InterruptedException {
        JsonNode items = raw.path("items");
        if (!raw.isObject() || !items.isArray() || items.size() > config.pageSize) {
            throw resource.failure("invalid wireless overview items array or page size");
        }
        for (JsonNode item : items) {
            http.remaining(resource, deadline);
            if (!item.isObject()) {
                throw resource.failure("wireless overview contains a nonrecord item");
            }
        }
    }

    private List<Row> records(
            CiscoMerakiResource resource,
            String organization,
            CiscoMerakiSplit split,
            JsonNode raw,
            long deadline)
            throws InterruptedException {
        int limit = resource.paged ? config.pageSize : config.maxItems;
        if (!raw.isArray() || raw.size() > limit) {
            throw resource.failure("expected a record array within the configured item limit");
        }
        List<Row> result = new ArrayList<>(raw.size());
        Set<String> ids = new HashSet<>();
        for (JsonNode item : raw) {
            http.remaining(resource, deadline);
            if (!item.isObject()) {
                throw resource.failure("expected a record object");
            }
            String id =
                    resource.identity == null ? null : identity(resource, item, resource.identity);
            if (id != null && !ids.add(id)) {
                throw resource.failure("duplicate native record identity");
            }
            if (resource == CiscoMerakiResource.NETWORKS
                    && item.hasNonNull("organizationId")
                    && !organization.equals(item.path("organizationId").textValue())) {
                throw resource.failure("network belongs to a different organization");
            }
            if (selected(resource, split, item)) {
                result.add(new Row(id, item.toString()));
            }
        }
        return result;
    }

    private static String identity(CiscoMerakiResource resource, JsonNode item, String name) {
        JsonNode field = item.path(name);
        if (!field.isTextual()
                || field.textValue().isBlank()
                || field.textValue().length() > 512
                || field.textValue().codePoints().anyMatch(Character::isISOControl)) {
            throw resource.failure("missing or invalid native identity");
        }
        return field.textValue();
    }

    private boolean selected(CiscoMerakiResource resource, CiscoMerakiSplit split, JsonNode item) {
        if (resource == CiscoMerakiResource.API_REQUESTS) {
            Instant timestamp = timestamp(resource, item.path("ts"));
            return !timestamp.isBefore(split.windowStart) && !timestamp.isAfter(split.windowEnd);
        }
        if (resource != CiscoMerakiResource.ADMINS || config.start == null && config.end == null) {
            return true;
        }
        JsonNode active = item.path("lastActive");
        if (active.isMissingNode() || active.isNull()) {
            return false;
        }
        Instant timestamp = timestamp(resource, active);
        return (config.start == null || !timestamp.isBefore(split.windowStart))
                && !timestamp.isAfter(split.windowEnd);
    }

    private static Instant timestamp(CiscoMerakiResource resource, JsonNode value) {
        if (value.isTextual() && value.textValue().length() <= 64) {
            try {
                return OffsetDateTime.parse(value.textValue()).toInstant();
            } catch (DateTimeException ignored) {
                // Report the field failure without the timestamp or a provider-supplied value.
            }
        }
        throw resource.failure("missing or invalid record timestamp");
    }

    /**
     * Keeps validated rows and scoped request targets in reader memory.
     *
     * <p>Checkpoints save the digest and numeric row position instead of the rows themselves.
     */
    static final class Page {
        final List<Row> rows;
        final String target;
        final String next;
        final String digest;

        Page(List<Row> rows, String target, String next, String digest) {
            this.rows = rows;
            this.target = target;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Represents one native row or operation response before publication.
     *
     * <p>Object-only resources and API requests have no synthetic identifier.
     */
    static final class Row {
        final String id;
        final String record;

        Row(String id, String record) {
            this.id = id;
            this.record = record;
        }
    }
}

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
package org.apache.seatunnel.connectors.seatunnel.claude;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles the native Claude Enterprise endpoints and their date-scoped response envelopes.
 *
 * <p>Record bodies retain native fields, null values, numeric precision, and grouped results.
 */
final class ClaudeApi {
    private final ClaudeConfig config;
    private final ClaudeHttp http;

    ClaudeApi(ClaudeConfig config, ClaudeHttp http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Obtains the next page for the active date slice and confirms all rows before publication.
     *
     * <p>Server cursors can only become encoded query values for the initial operation.
     */
    Page read(ClaudeResource resource, ClaudeSplit split, long deadline)
            throws InterruptedException {
        LocalDate until = split.windowEnd(resource);
        JsonNode raw = http.request(resource, request(resource, split, until), deadline);
        JsonNode data = raw.path("data");
        if (!raw.isObject() || !data.isArray()) {
            throw resource.failure("expected a canonical data array");
        }
        int limit =
                resource.kind == ClaudeResource.Kind.SUMMARY ? 1 : resource.limit(config.pageSize);
        if (data.size() > config.maxItems
                || resource != ClaudeResource.USER_COST_REPORT && data.size() > limit) {
            throw resource.failure("response exceeds the configured record or page size limit");
        }
        if (resource.kind == ClaudeResource.Kind.SUMMARY && data.size() != 1) {
            throw resource.failure("expected one summary for the requested UTC day");
        }

        String next = continuation(resource, raw, split);
        String organization = null;
        String watermark = null;
        if (resource.isReport()) {
            organization = text(resource, raw.path("organization_id"), "organization_id", 512);
            if (split.organizationId != null && !split.organizationId.equals(organization)) {
                throw resource.failure("report organization changed during the scan");
            }
            JsonNode refresh = raw.path("data_refreshed_at");
            if (refresh.isMissingNode()) {
                throw resource.failure("missing report data_refreshed_at");
            }
            if (!refresh.isNull()) {
                watermark = text(resource, refresh, "data_refreshed_at", 64);
                timestamp(resource, watermark, "data_refreshed_at");
            }
            if (watermark == null
                    && resource.kind == ClaudeResource.Kind.USER_REPORT
                    && !data.isEmpty()) {
                throw resource.failure(
                        "per-user data returned without a report freshness watermark");
            }
        }

        List<Row> rows = new ArrayList<>(data.size());
        LocalDate lastBucket = split.lastBucket;
        for (JsonNode item : data) {
            http.remaining(resource, deadline);
            if (!item.isObject()) {
                throw resource.failure("expected a record object");
            }
            if (resource.kind == ClaudeResource.Kind.BUCKET_REPORT) {
                if (!item.path("results").isArray()) {
                    throw resource.failure("expected a bucket results array");
                }
                if (watermark == null && !item.path("results").isEmpty()) {
                    throw resource.failure(
                            "bucket results returned without a report freshness watermark");
                }
                lastBucket = bucketDay(resource, split.windowStart, until, item, lastBucket);
            } else if (resource.kind == ClaudeResource.Kind.SUMMARY) {
                bucketDay(resource, split.windowStart, until, item, null);
            }
            rows.add(new Row(identity(resource, item), item.toString()));
        }

        http.remaining(resource, deadline);
        return new Page(
                rows, next, ClaudeConfig.hash(raw.toString()), organization, watermark, lastBucket);
    }

    private String request(ClaudeResource resource, ClaudeSplit split, LocalDate until) {
        StringBuilder target = new StringBuilder(resource.route);
        if (resource.kind == ClaudeResource.Kind.DAILY) {
            target.append("?date=")
                    .append(split.windowStart)
                    .append("&limit=")
                    .append(resource.limit(config.pageSize));
        } else if (resource.kind == ClaudeResource.Kind.SUMMARY) {
            target.append("?starting_date=")
                    .append(split.windowStart)
                    .append("&ending_date=")
                    .append(until);
        } else {
            target.append("?starting_at=")
                    .append(encode(ClaudeResource.midnight(split.windowStart)))
                    .append("&ending_at=")
                    .append(encode(ClaudeResource.midnight(until)))
                    .append("&limit=")
                    .append(resource.limit(config.pageSize));
            if (resource.kind == ClaudeResource.Kind.BUCKET_REPORT) {
                target.append("&bucket_width=1d");
            }
            for (String group : resource.groupBy()) {
                target.append("&group_by%5B%5D=").append(group);
            }
        }
        if (split.cursor != null) {
            target.append("&page=").append(encode(split.cursor));
        }
        return target.toString();
    }

    private String continuation(ClaudeResource resource, JsonNode raw, ClaudeSplit split) {
        JsonNode value = raw.path("next_page");
        if (value.isMissingNode()) {
            throw resource.failure("missing next_page in the response");
        }
        String next = value.isNull() ? null : text(resource, value, "next_page", 4096);
        if (resource.kind == ClaudeResource.Kind.SUMMARY && next != null) {
            throw resource.failure("activity summaries cannot carry a next_page cursor");
        }
        if (resource.isReport()) {
            JsonNode hasMore = raw.path("has_more");
            if (!hasMore.isBoolean() || hasMore.booleanValue() != (next != null)) {
                throw resource.failure("has_more and next_page disagree");
            }
        }
        if (next != null
                && (next.equals(split.cursor) || split.visited.contains(ClaudeConfig.hash(next)))) {
            throw resource.failure("repeated next_page cursor");
        }
        return next;
    }

    private static String identity(ClaudeResource resource, JsonNode item) {
        if (resource.identityField == null) {
            return null;
        }
        JsonNode value = item.path(resource.identityField);
        if (resource == ClaudeResource.USERS && value.isNull()) {
            return null;
        }
        if (resource.nestedIdentityField != null) {
            value = value.path(resource.nestedIdentityField);
        }
        return text(resource, value, "native identity", 4096);
    }

    private static LocalDate bucketDay(
            ClaudeResource resource,
            LocalDate windowStart,
            LocalDate windowEnd,
            JsonNode item,
            LocalDate previous) {
        Instant from =
                timestamp(
                        resource,
                        text(resource, item.path("starting_at"), "starting_at", 64),
                        "starting_at");
        Instant to =
                timestamp(
                        resource,
                        text(resource, item.path("ending_at"), "ending_at", 64),
                        "ending_at");
        LocalDate day = from.atOffset(ZoneOffset.UTC).toLocalDate();
        if (!from.equals(day.atStartOfDay().toInstant(ZoneOffset.UTC))
                || !to.equals(day.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC))
                || day.isBefore(windowStart)
                || !day.isBefore(windowEnd)
                || previous != null && !day.isAfter(previous)) {
            throw resource.failure("bucket is out of order or outside its requested UTC slice");
        }
        return day;
    }

    private static Instant timestamp(ClaudeResource resource, String value, String field) {
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeException ex) {
            throw resource.failure("invalid " + field + " timestamp");
        }
    }

    private static String text(ClaudeResource resource, JsonNode value, String field, int limit) {
        if (!value.isTextual() || !ClaudeSplit.validValue(value.textValue(), limit)) {
            throw resource.failure("missing or invalid " + field);
        }
        return value.textValue();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * Holds a validated page and its report metadata in memory.
     *
     * <p>A replay digest covers the original envelope before any records are published.
     */
    static final class Page {
        final List<Row> rows;
        final String next;
        final String digest;
        final String organization;
        final String watermark;
        final LocalDate lastBucket;

        Page(
                List<Row> rows,
                String next,
                String digest,
                String organization,
                String watermark,
                LocalDate lastBucket) {
            this.rows = rows;
            this.next = next;
            this.digest = digest;
            this.organization = organization;
            this.watermark = watermark;
            this.lastBucket = lastBucket;
        }
    }

    /**
     * Pairs a projected native identity with one unmodified record object.
     *
     * <p>Multiple grouped rows can legitimately have the same user identity.
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

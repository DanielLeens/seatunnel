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
package org.apache.seatunnel.connectors.seatunnel.chartmogul;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Issues fixed ChartMogul GET requests and validates their native list responses.
 *
 * <p>Customer and activity pages use the server cursor. Complete metric responses keep the returned
 * period-end dates.
 */
final class ChartMogulApi {
    private final ChartMogulConfig config;
    private final ChartMogulHttp http;

    ChartMogulApi(ChartMogulConfig config, ChartMogulHttp http) {
        this.config = config;
        this.http = http;
    }

    Page list(ChartMogulResource resource, ChartMogulSplit position, long deadline)
            throws InterruptedException {
        JsonNode response = http.request(resource, target(resource, position), deadline);
        if (!response.isObject()
                || response.hasNonNull("error")
                || response.hasNonNull("errors")
                || response.path("code").asInt(0) >= 400) {
            throw resource.failure("invalid ChartMogul response object");
        }
        JsonNode raw = response.path("entries");
        if (!raw.isArray()) {
            throw resource.failure("expected an entries array");
        }
        if (raw.size() > resource.limit(config)) {
            throw resource.failure("response exceeds the configured record count");
        }
        String cursor = nextCursor(resource, response);
        String cursorHash = cursor == null ? null : ChartMogulConfig.hash(cursor);
        if (cursorHash != null && position.seen.contains(cursorHash)) {
            throw resource.failure("repeated cursor");
        }
        List<Row> rows = new ArrayList<>(raw.size());
        Set<String> ids = new HashSet<>();
        for (JsonNode item : raw) {
            http.remaining(resource, deadline);
            Row row = row(resource, item, position.windowEnd);
            if (!ids.add(row.id)) {
                throw resource.failure("duplicate ID in the response");
            }
            rows.add(row);
        }
        String digest = ChartMogulConfig.hash(response.toString());
        http.remaining(resource, deadline);
        return new Page(rows, cursor, cursorHash, digest);
    }

    private String target(ChartMogulResource resource, ChartMogulSplit position) {
        String query;
        if (resource.interval != null) {
            query =
                    "start-date="
                            + ChartMogulConfig.metricDate(config.start)
                            + "&end-date="
                            + ChartMogulConfig.metricDate(position.windowEnd)
                            + "&interval="
                            + resource.interval;
        } else if (resource == ChartMogulResource.ACTIVITIES) {
            query =
                    "per-page="
                            + config.pageSize
                            + "&start-date="
                            + encode(config.start.toString())
                            + "&end-date="
                            + encode(position.windowEnd.toString());
        } else {
            query = "per_page=" + config.pageSize;
        }
        if (position.cursor != null) {
            query += "&cursor=" + encode(position.cursor);
        }
        return resource.path + "?" + query;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String nextCursor(ChartMogulResource resource, JsonNode response) {
        JsonNode more = response.path("has_more");
        if (resource.interval != null) {
            if (response.hasNonNull("cursor")
                    || !more.isMissingNode() && (!more.isBoolean() || more.booleanValue())) {
                throw resource.failure("unexpected pagination on a complete metric response");
            }
            return null;
        }
        if (!more.isBoolean()) {
            throw resource.failure("missing or invalid has_more");
        }
        return more.booleanValue() ? text(resource, response.path("cursor"), "cursor") : null;
    }

    private Row row(ChartMogulResource resource, JsonNode item, Instant upper) {
        if (!item.isObject()) {
            throw resource.failure("expected a record object");
        }
        if (resource == ChartMogulResource.CUSTOMERS) {
            JsonNode id = item.path("id");
            if (!id.isIntegralNumber() || id.bigIntegerValue().signum() < 0) {
                throw resource.failure("missing or invalid customer id");
            }
            String uuid =
                    item.hasNonNull("uuid") ? text(resource, item.path("uuid"), "uuid") : null;
            String time =
                    item.hasNonNull("customer-since")
                            ? dateTime(resource, item, "customer-since").toString()
                            : null;
            return new Row(id.asText(), uuid, time, item.toString(), true);
        }
        if (resource == ChartMogulResource.ACTIVITIES) {
            String uuid = text(resource, item.path("uuid"), "uuid");
            Instant date = dateTime(resource, item, "date");
            return new Row(
                    uuid,
                    uuid,
                    date.toString(),
                    item.toString(),
                    config.includesActivity(date, upper));
        }
        String date = metricDate(resource, item);
        JsonNode count = item.path("customers");
        if (!count.isNumber()
                || count.decimalValue().signum() < 0
                || count.decimalValue().stripTrailingZeros().scale() > 0) {
            throw resource.failure("missing or invalid customer count");
        }
        return new Row(date, null, date, item.toString(), true);
    }

    private static String metricDate(ChartMogulResource resource, JsonNode item) {
        String value = text(resource, item.path("date"), "date");
        try {
            if (value.length() != 10 || LocalDate.parse(value).getYear() < 1) {
                throw resource.failure("invalid metric period-end date");
            }
            return value;
        } catch (DateTimeException ex) {
            throw resource.failure("invalid metric period-end date");
        }
    }

    private static Instant dateTime(ChartMogulResource resource, JsonNode item, String field) {
        try {
            return ChartMogulConfig.parseTime(text(resource, item.path(field), field));
        } catch (DateTimeException ex) {
            throw resource.failure("invalid " + field + " timestamp");
        }
    }

    private static String text(ChartMogulResource resource, JsonNode node, String field) {
        if (!node.isTextual() || !validOpaque(node.textValue())) {
            throw resource.failure("missing or invalid " + field);
        }
        return node.textValue();
    }

    static boolean validOpaque(String value) {
        return !value.isBlank()
                && value.length() <= 4096
                && value.codePoints()
                        .noneMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE);
    }

    /**
     * Holds an accepted response and the opaque continuation required for the next page.
     *
     * <p>The digest covers all native records, even if the output time window excludes some.
     */
    static final class Page {
        final List<Row> rows;
        final String cursor;
        final String cursorHash;
        final String digest;

        Page(List<Row> rows, String cursor, String cursorHash, String digest) {
            this.rows = rows;
            this.cursor = cursor;
            this.cursorHash = cursorHash;
            this.digest = digest;
        }
    }

    /**
     * Holds the exported identifiers, time, and native JSON for one response entry.
     *
     * <p>Activity time normalization does not alter the original record.
     */
    static final class Row {
        final String id;
        final String uuid;
        final String time;
        final String record;
        final boolean included;

        Row(String id, String uuid, String time, String record, boolean included) {
            this.id = id;
            this.uuid = uuid;
            this.time = time;
            this.record = record;
            this.included = included;
        }
    }
}

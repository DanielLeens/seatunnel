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
package org.apache.seatunnel.connectors.seatunnel.churnkey;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reads Churnkey's native session and grouped-count arrays.
 *
 * <p>Only the session list receives limit and skip. The aggregation never assumes list paging.
 */
final class ChurnkeyApi {
    private final ChurnkeyConfig config;
    private final ChurnkeyHttp http;

    ChurnkeyApi(ChurnkeyConfig config, ChurnkeyHttp http) {
        this.config = config;
        this.http = http;
    }

    Page list(ChurnkeyResource resource, ChurnkeySplit position, long deadline)
            throws InterruptedException {
        JsonNode raw = http.request(resource, target(resource, position.skip), deadline);
        if (!raw.isArray()) {
            throw resource.failure("expected a top-level array");
        }
        if (raw.size() > resource.limit(config)) {
            throw resource.failure("response exceeds the configured record count");
        }
        String digest = ChurnkeyConfig.hash(raw.toString());
        if (resource.paged && !raw.isEmpty() && position.seen.contains(digest)) {
            throw resource.failure("repeated nonempty session page");
        }
        List<Row> rows = new ArrayList<>(raw.size());
        Set<String> ids = new HashSet<>();
        for (JsonNode item : raw) {
            http.remaining(resource, deadline);
            Row row = row(resource, item);
            if (resource.paged && !ids.add(row.id)) {
                throw resource.failure("duplicate session ID in the response");
            }
            rows.add(row);
        }
        http.remaining(resource, deadline);
        long next = resource.paged && !raw.isEmpty() ? position.skip + raw.size() : -1;
        return new Page(rows, next, digest);
    }

    private String target(ChurnkeyResource resource, long skip) {
        if (resource.paged) {
            return resource.path
                    + "?limit="
                    + config.pageSize
                    + "&skip="
                    + skip
                    + "&bounced="
                    + config.bounced;
        }
        String target = resource.path + "?bounced=" + config.bounced;
        return config.breakdown.isEmpty() ? target : target + "&breakdown=" + config.breakdown;
    }

    private static Row row(ChurnkeyResource resource, JsonNode item) {
        if (!item.isObject()) {
            throw resource.failure("expected a record object");
        }
        if (resource.paged) {
            String id = text(resource, item.path("_id"), "_id", 4096);
            return new Row(id, createdAt(resource, item.path("createdAt")), item.toString());
        }
        JsonNode count = item.path("count");
        if (!count.isIntegralNumber() || count.bigIntegerValue().signum() < 0) {
            throw resource.failure("missing or invalid aggregation count");
        }
        return new Row(null, month(resource, item.path("month")), item.toString());
    }

    private static String createdAt(ChurnkeyResource resource, JsonNode value) {
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        String date = text(resource, value, "createdAt", 64);
        try {
            return OffsetDateTime.parse(date).toInstant().toString();
        } catch (DateTimeException ex) {
            throw resource.failure("invalid createdAt timestamp");
        }
    }

    private static String month(ChurnkeyResource resource, JsonNode value) {
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        String month = text(resource, value, "month", 7);
        try {
            if (month.length() == 7 && YearMonth.parse(month).getYear() >= 1) {
                return month;
            }
        } catch (DateTimeException ex) {
            throw resource.failure("invalid aggregation month");
        }
        throw resource.failure("invalid aggregation month");
    }

    private static String text(
            ChurnkeyResource resource, JsonNode value, String field, int maximum) {
        if (!value.isTextual()
                || value.textValue().isBlank()
                || value.textValue().length() > maximum
                || value.textValue().codePoints().anyMatch(Character::isISOControl)) {
            throw resource.failure("missing or invalid " + field);
        }
        return value.textValue();
    }

    /**
     * Holds an accepted response and its continuation, if it came from the session list.
     *
     * <p>The digest covers the full raw array for checkpoint validation.
     */
    static final class Page {
        final List<Row> rows;
        final long next;
        final String digest;

        Page(List<Row> rows, long next, String digest) {
            this.rows = rows;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Holds the native JSON and its optional identity and time.
     *
     * <p>Grouped counts have no session ID.
     */
    static final class Row {
        final String id;
        final String time;
        final String record;

        Row(String id, String time, String record) {
            this.id = id;
            this.time = time;
            this.record = record;
        }
    }
}

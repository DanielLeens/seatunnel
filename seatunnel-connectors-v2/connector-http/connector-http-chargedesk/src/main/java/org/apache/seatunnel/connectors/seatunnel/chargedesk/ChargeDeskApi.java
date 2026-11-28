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
package org.apache.seatunnel.connectors.seatunnel.chargedesk;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implements the native count and offset contract for each ChargeDesk collection.
 *
 * <p>Short pages advance by their confirmed row count. An empty response proves exhaustion.
 */
final class ChargeDeskApi {
    private final ChargeDeskConfig config;
    private final ChargeDeskHttp http;

    ChargeDeskApi(ChargeDeskConfig config, ChargeDeskHttp http) {
        this.config = config;
        this.http = http;
    }

    Page list(ChargeDeskResource resource, ChargeDeskSplit position, long deadline)
            throws InterruptedException {
        int size = resource.requestCount(config.pageSize, position.offset);
        JsonNode response = http.request(resource, target(resource, position, size), deadline);
        if (!response.isObject()
                || response.hasNonNull("error")
                || response.hasNonNull("errors")
                || response.path("code").asInt(0) >= 400) {
            throw resource.failure("invalid ChargeDesk response object");
        }
        JsonNode raw = response.path("data");
        if (!raw.isArray()) {
            throw resource.failure("expected a data array");
        }
        int count = integer(resource, response, "count");
        int reportedOffset = integer(resource, response, "offset");
        if (count != raw.size() || count > size || reportedOffset != position.offset) {
            throw resource.failure("inconsistent response count or offset");
        }
        int next = position.offset + count;
        if (next > resource.maxOffset) {
            throw resource.failure("native offset limit exceeded");
        }
        String history = count == 0 ? null : ChargeDeskConfig.hash(raw.toString());
        if (history != null && position.seen.contains(history)) {
            throw resource.failure("repeated nonempty offset page");
        }
        List<Row> rows = new ArrayList<>(count);
        Set<String> ids = new HashSet<>();
        boolean timeRequired = resource.dated() && position.hasWindow(config);
        for (JsonNode record : raw) {
            http.remaining(resource, deadline);
            Row row = row(resource, record, timeRequired);
            if (!ids.add(row.id)) {
                throw resource.failure("duplicate ID in response");
            }
            rows.add(row);
        }
        String digest = ChargeDeskConfig.hash(response.toString());
        http.remaining(resource, deadline);
        return new Page(rows, count == 0 ? -1 : next, history, digest);
    }

    private String target(ChargeDeskResource resource, ChargeDeskSplit position, int size) {
        String target = "/v1/" + resource.name + "?count=" + size + "&offset=" + position.offset;
        if (resource.timeFilter == null) {
            return target;
        }
        if (config.start != null && config.start > 0) {
            // One extra second protects the requested inclusive boundary from a strict server min.
            target += "&" + resource.timeFilter + "%5Bmin%5D=" + (config.start - 1);
        }
        if (position.windowEnd != null) {
            target += "&" + resource.timeFilter + "%5Bmax%5D=" + position.windowEnd;
        }
        return target;
    }

    private static int integer(ChargeDeskResource resource, JsonNode response, String field) {
        JsonNode value = response.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < 0) {
            throw resource.failure("missing or invalid response " + field);
        }
        return value.intValue();
    }

    private static Row row(ChargeDeskResource resource, JsonNode record, boolean timeRequired) {
        if (!record.isObject()) {
            throw resource.failure("expected a record object");
        }
        JsonNode nativeId = record.path(resource.identity);
        if (!nativeId.isTextual()
                || nativeId.textValue().isBlank()
                || nativeId.textValue().length() > 4096) {
            throw resource.failure("invalid " + resource.identity);
        }
        JsonNode time = record.path(resource.timeField);
        Long seconds;
        if (time.isMissingNode() || time.isNull()) {
            if (timeRequired) {
                throw resource.failure(
                        "missing " + resource.timeField + " for the output time window");
            }
            seconds = null;
        } else {
            seconds = seconds(resource, time);
        }
        return new Row(nativeId.textValue(), seconds, record.toString());
    }

    private static long seconds(ChargeDeskResource resource, JsonNode value) {
        long seconds;
        try {
            if (value.isNumber()) {
                seconds = value.decimalValue().longValueExact();
            } else if (value.isTextual() && value.textValue().matches("[0-9]{1,19}")) {
                seconds = Long.parseLong(value.textValue());
            } else {
                throw resource.failure("invalid " + resource.timeField + " epoch seconds");
            }
        } catch (ArithmeticException | NumberFormatException ex) {
            throw resource.failure("invalid " + resource.timeField + " epoch seconds");
        }
        if (seconds < 0 || seconds > ChargeDeskConfig.MAX_EPOCH_SECOND) {
            throw resource.failure("invalid " + resource.timeField + " epoch seconds");
        }
        return seconds;
    }

    /**
     * Carries one validated list response and its confirmed continuation.
     *
     * <p>Page digests cover records outside the requested output window as well.
     */
    static final class Page {
        final List<Row> rows;
        final int next;
        final String history;
        final String digest;

        Page(List<Row> rows, int next, String history, String digest) {
            this.rows = rows;
            this.next = next;
            this.history = history;
            this.digest = digest;
        }
    }

    /**
     * Holds native identity and optional epoch seconds alongside the JSON string.
     *
     * <p>The output time conversion leaves the JSON field unchanged.
     */
    static final class Row {
        final String id;
        final Long seconds;
        final String record;

        Row(String id, Long seconds, String record) {
            this.id = id;
            this.seconds = seconds;
            this.record = record;
        }
    }
}

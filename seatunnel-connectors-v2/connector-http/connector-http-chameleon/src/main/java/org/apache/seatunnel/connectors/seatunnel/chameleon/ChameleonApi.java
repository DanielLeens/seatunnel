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
package org.apache.seatunnel.connectors.seatunnel.chameleon;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonResource.Shape;
import org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSplit.Frame;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Constructs fixed Chameleon reads and decodes their native envelopes.
 *
 * <p>Continuation values are opaque query data. Cursor ordering is independent of the output's
 * timestamp filter.
 */
final class ChameleonApi {
    private final ChameleonConfig config;
    private final ChameleonHttp http;

    ChameleonApi(ChameleonConfig config, ChameleonHttp http) {
        this.config = config;
        this.http = http;
    }

    Page list(Frame frame, boolean timeRequired, long deadline) throws InterruptedException {
        ChameleonResource resource = frame.resource;
        JsonNode response = http.request(resource, target(frame), deadline);
        if (!response.isObject()
                || response.hasNonNull("error")
                || response.hasNonNull("errors")
                || response.path("code").asInt(0) >= 400) {
            throw resource.failure("invalid Chameleon response object");
        }
        JsonNode raw = records(resource, response);
        int limit = resource.shape == Shape.COMPLETE ? 1 : config.pageSize;
        if (raw.size() > limit) {
            throw resource.failure("response exceeds the configured record count");
        }
        String cursor = nextCursor(resource, response);
        String history = cursor == null ? null : ChameleonConfig.hash(cursor);
        if (history != null && frame.seen.contains(history)) {
            throw resource.failure("repeated cursor");
        }
        List<Row> rows = new ArrayList<>(raw.size());
        Set<String> ids = new HashSet<>();
        for (JsonNode item : raw) {
            http.remaining(resource, deadline);
            Row row = row(frame, item, timeRequired);
            if (!ids.add(row.id)) {
                throw resource.failure("duplicate ID in the response");
            }
            rows.add(row);
        }
        String digest = ChameleonConfig.hash(response.toString());
        http.remaining(resource, deadline);
        return new Page(rows, cursor == null ? 0 : frame.page + 1, cursor, history, digest);
    }

    private String target(Frame frame) {
        ChameleonResource resource = frame.resource;
        if (resource.shape == Shape.COMPLETE) {
            return resource.path;
        }
        String target = resource.path + "?limit=" + config.pageSize;
        if (frame.parentId != null) {
            target +=
                    "&id="
                            + encode(frame.parentId)
                            + "&expand%5Bprofile%5D=all&expand%5Bcompany%5D=all";
        }
        if (frame.cursor != null) {
            target += "&before=" + encode(frame.cursor);
        }
        return target;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static JsonNode records(ChameleonResource resource, JsonNode response) {
        JsonNode records = response.path(resource.items);
        if (resource == ChameleonResource.URL_GROUPS && response.has("urls")) {
            if (!records.isMissingNode()) {
                throw resource.failure("ambiguous URL Groups response arrays");
            }
            records = response.path("urls");
        }
        if (!records.isArray()) {
            throw resource.failure("expected a " + resource.items + " array");
        }
        return records;
    }

    private static String nextCursor(ChameleonResource resource, JsonNode response) {
        JsonNode cursor = response.path("cursor");
        if (cursor.isMissingNode() || cursor.isNull()) {
            return null;
        }
        if (!cursor.isObject()) {
            throw resource.failure("expected a cursor object");
        }
        JsonNode before = cursor.path("before");
        if (before.isMissingNode() || before.isNull()) {
            return null;
        }
        if (resource.shape == Shape.COMPLETE) {
            throw resource.failure("unexpected continuation on the complete HelpBar list");
        }
        return opaque(resource, before, "cursor.before");
    }

    private static Row row(Frame frame, JsonNode item, boolean timeRequired) {
        ChameleonResource resource = frame.resource;
        if (!item.isObject()) {
            throw resource.failure("expected a record object");
        }
        String id = opaque(resource, item.path("id"), "id");
        String survey = frame.parentId;
        if (resource == ChameleonResource.SURVEYS) {
            survey = id;
        } else if (survey != null
                && item.hasNonNull("survey_id")
                && !survey.equals(opaque(resource, item.path("survey_id"), "survey_id"))) {
            throw resource.failure("response belongs to a different survey");
        }
        String timestamp = resource.timestamp();
        JsonNode value = item.path(timestamp);
        Instant time = null;
        if (!value.isMissingNode() && !value.isNull()) {
            if (!value.isTextual()) {
                throw resource.failure("invalid " + timestamp);
            }
            try {
                time = ChameleonConfig.parseTime(value.textValue());
            } catch (DateTimeException ex) {
                throw resource.failure("invalid " + timestamp);
            }
        } else if (timeRequired) {
            throw resource.failure("missing " + timestamp + " for the output time window");
        }
        return new Row(id, survey, time, item.toString());
    }

    private static String opaque(ChameleonResource resource, JsonNode value, String field) {
        if (!value.isTextual() || !validOpaque(value.textValue())) {
            throw resource.failure("missing or invalid " + field);
        }
        return value.textValue();
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
     * Supplies one accepted response to the reader.
     *
     * <p>The page digest covers all raw records, including records excluded from the output window.
     */
    static final class Page {
        final List<Row> rows;
        final int next;
        final String cursor;
        final String history;
        final String digest;

        Page(List<Row> rows, int next, String cursor, String history, String digest) {
            this.rows = rows;
            this.next = next;
            this.cursor = cursor;
            this.history = history;
            this.digest = digest;
        }
    }

    /**
     * Retains the native record and its query-independent output identifiers.
     *
     * <p>The selected timestamp is parsed separately so its original representation remains intact.
     */
    static final class Row {
        final String id;
        final String survey;
        final Instant time;
        final String record;

        Row(String id, String survey, Instant time, String record) {
            this.id = id;
            this.survey = survey;
            this.time = time;
            this.record = record;
        }
    }
}

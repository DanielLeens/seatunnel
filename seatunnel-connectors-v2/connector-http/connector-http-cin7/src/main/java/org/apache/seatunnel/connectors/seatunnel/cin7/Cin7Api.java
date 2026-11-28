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
package org.apache.seatunnel.connectors.seatunnel.cin7;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7Resource.BASE_PATH;

/**
 * Issues the Cin7 Core V2 read requests and validates their complete response shapes.
 *
 * <p>The client prepares every row before the reader publishes any record from a page.
 */
final class Cin7Api {
    private final Cin7Config config;
    private final Cin7Http http;

    Cin7Api(Cin7Config config, Cin7Http http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Reads one one-based collection page and checks its pagination metadata.
     *
     * <p>Deals has its own small page limit and uses an empty page as the completion marker.
     */
    Page list(Cin7Resource resource, Cin7Split position, long deadline)
            throws InterruptedException {
        int limit = resource.pageLimit(config.pageSize);
        String query =
                resource == Cin7Resource.DEALS
                        ? "?page=" + position.page + "&limit=" + limit
                        : "?Page=" + position.page + "&Limit=" + limit;
        JsonNode raw =
                http.request(
                        resource, BASE_PATH + resource.route + query + resource.options, deadline);
        JsonNode items = raw.path(resource.array);
        if (!raw.isObject() || !items.isArray()) {
            throw resource.failure("expected the collection response array");
        }
        if (items.size() > limit) {
            throw resource.failure("response exceeds the requested page size");
        }

        long total = resource == Cin7Resource.DEALS ? -1 : total(resource, position, raw, items);
        boolean last =
                total < 0
                        ? items.isEmpty()
                        : (long) (position.page - 1) * limit + items.size() == total;
        String pageIdentity = Cin7Config.hash(items.toString());
        if (!items.isEmpty() && position.seen.contains(pageIdentity)) {
            throw resource.failure("repeated nonempty page");
        }
        List<Row> rows = new ArrayList<>(items.size());
        Set<String> identities = new HashSet<>();
        for (JsonNode item : items) {
            http.remaining(resource, deadline);
            Row row = row(resource, item);
            if (!identities.add(row.id) && resource != Cin7Resource.PRODUCT_AVAILABILITY) {
                throw resource.failure("duplicate record identity in the response");
            }
            rows.add(row);
        }
        http.remaining(resource, deadline);
        return new Page(rows, Cin7Config.hash(raw.toString()), pageIdentity, total, last);
    }

    private long total(Cin7Resource resource, Cin7Split position, JsonNode raw, JsonNode items) {
        JsonNode echoedPage = raw.path("Page");
        if (!echoedPage.isIntegralNumber()
                || !echoedPage.canConvertToInt()
                || echoedPage.intValue() != position.page) {
            throw resource.failure("invalid or missing Page in the response");
        }
        JsonNode declared = raw.path("Total");
        if (!declared.isIntegralNumber()
                || !declared.canConvertToLong()
                || declared.longValue() < 0) {
            throw resource.failure("invalid or missing Total in the response");
        }
        long total = declared.longValue();
        if (position.total >= 0 && total != position.total) {
            throw resource.failure("Total changed within the collection");
        }
        int limit = resource.pageLimit(config.pageSize);
        long offset = (long) (position.page - 1) * limit;
        if (offset > total || items.size() != Math.min(limit, total - offset)) {
            throw resource.failure("page record count is inconsistent with Total");
        }
        return total;
    }

    /**
     * Reads the single detail record for the native ID confirmed on its parent page.
     *
     * <p>The returned ID must refer to that same purchase or sale.
     */
    Page detail(Cin7Resource resource, String parent, long deadline) throws InterruptedException {
        String escaped = URLEncoder.encode(parent, StandardCharsets.UTF_8).replace("+", "%20");
        JsonNode raw =
                http.request(
                        resource,
                        BASE_PATH + resource.route + "?ID=" + escaped + resource.options,
                        deadline);
        Row detail = row(resource, raw);
        if (!parent.equalsIgnoreCase(detail.id)) {
            throw resource.failure("detail ID does not match its parent");
        }
        http.remaining(resource, deadline);
        return new Page(List.of(detail), null, null, -1, true);
    }

    private static Row row(Cin7Resource resource, JsonNode item) {
        if (!item.isObject()) {
            throw resource.failure("expected a record object");
        }
        String id = requiredText(resource, item, resource.id);
        if (resource == Cin7Resource.PRODUCT_AVAILABILITY) {
            ArrayNode stock = JsonNodeFactory.instance.arrayNode().add(id);
            for (String field : List.of("Location", "Bin", "Batch", "ExpiryDate")) {
                JsonNode value = item.path(field);
                if (value.isMissingNode() || value.isNull()) {
                    stock.addNull();
                } else if (value.isTextual()) {
                    stock.add(value.textValue());
                } else {
                    throw resource.failure("invalid " + field + " in stock identity");
                }
            }
            id = stock.toString();
        }
        return new Row(id, item.toString());
    }

    private static String requiredText(Cin7Resource resource, JsonNode item, String field) {
        JsonNode value = item.path(field);
        if (!value.isTextual()
                || value.textValue().isBlank()
                || value.textValue().length() > 4096
                || value.textValue().codePoints().anyMatch(Character::isISOControl)) {
            throw resource.failure("missing or invalid " + field);
        }
        return value.textValue();
    }

    /**
     * Holds accepted records and the metadata needed to continue or verify a list page.
     *
     * <p>A detail response contains only one confirmed row and has no paging metadata.
     */
    static final class Page {
        final List<Row> rows;
        final String digest;
        final String pageIdentity;
        final long total;
        final boolean last;

        Page(List<Row> rows, String digest, String pageIdentity, long total, boolean last) {
            this.rows = rows;
            this.digest = digest;
            this.pageIdentity = pageIdentity;
            this.total = total;
            this.last = last;
        }
    }

    /**
     * Carries a native record and its projected identity before publication.
     *
     * <p>Values such as SKU, taxes, currencies and nested transaction details stay in the JSON.
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

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
package org.apache.seatunnel.connectors.seatunnel.carequalitycommission;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.connectors.seatunnel.carequalitycommission.CareQualityCommissionResource.Shape;
import org.apache.seatunnel.connectors.seatunnel.carequalitycommission.CareQualityCommissionSplit.Frame;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Reads a CQC Syndication collection or detail using its own response contract.
 *
 * <p>Pagination links are validated as metadata. Every request target is built locally.
 */
final class CareQualityCommissionApi {
    private static final String PREFIX = "/public/v1";

    private final CareQualityCommissionConfig config;
    private final CareQualityCommissionHttp http;

    CareQualityCommissionApi(CareQualityCommissionConfig config, CareQualityCommissionHttp http) {
        this.config = config;
        this.http = http;
    }

    Page list(Frame frame, long deadline) throws InterruptedException {
        CareQualityCommissionResource resource = frame.resource;
        String path =
                resource.scoped
                        ? String.format(Locale.ROOT, resource.path, frame.parentId)
                        : resource.path;
        String target = PREFIX + path;
        if (resource.shape == Shape.PAGED) {
            target += "?" + query(frame.page);
        }
        JsonNode response = http.request(resource, target, deadline);
        if (!response.isObject() || response.hasNonNull("error") || response.hasNonNull("errors")) {
            throw resource.failure("invalid CQC response object");
        }
        if (resource.shape == Shape.OBJECT) {
            return new Page(
                    List.of(row(frame, response)),
                    0,
                    null,
                    CareQualityCommissionConfig.hash(response.toString()));
        }
        JsonNode raw = response.path(resource.items);
        if (!raw.isArray()) {
            throw resource.failure("expected a " + resource.items + " array");
        }
        int maximum = resource.shape == Shape.PAGED ? config.pageSize : config.maxItems;
        if (raw.size() > maximum) {
            throw resource.failure("response exceeds the configured record count");
        }
        int next = 0;
        String history = null;
        if (resource.shape == Shape.PAGED) {
            next = nextPage(frame, response, raw.size(), path);
            if (!raw.isEmpty()) {
                history = CareQualityCommissionConfig.hash(raw.toString());
                if (frame.history.contains(history)) {
                    throw resource.failure("repeated nonempty page");
                }
            }
        } else {
            verifyCompleteList(frame, response);
        }
        List<Row> rows = new ArrayList<>(raw.size());
        Set<String> ids = new HashSet<>();
        for (JsonNode item : raw) {
            http.remaining(resource, deadline);
            Row row = row(frame, item);
            if (!ids.add(row.id)) {
                throw resource.failure("duplicate ID in response");
            }
            rows.add(row);
        }
        String digest = CareQualityCommissionConfig.hash(response.toString());
        http.remaining(resource, deadline);
        return new Page(rows, next, history, digest);
    }

    private void verifyCompleteList(Frame frame, JsonNode response) {
        CareQualityCommissionResource resource = frame.resource;
        if (response.hasNonNull("nextPageUri")
                || response.hasNonNull("totalPages")
                        && integer(resource, response, "totalPages") > 1) {
            throw resource.failure("unexpected pagination on a complete list");
        }
        if (resource == CareQualityCommissionResource.PROVIDER_LOCATIONS) {
            String provider = optionalId(resource, response.get("providerId"), "providerId");
            if (provider != null && !provider.equals(frame.parentId)) {
                throw resource.failure("response belongs to a different provider");
            }
        }
    }

    private int nextPage(Frame frame, JsonNode response, int count, String path) {
        CareQualityCommissionResource resource = frame.resource;
        int page = integer(resource, response, "page");
        int limit = integer(resource, response, "perPage");
        int pages = integer(resource, response, "totalPages");
        if (response.hasNonNull("total") && integer(resource, response, "total") < count) {
            throw resource.failure("pagination total is smaller than the response size");
        }
        if (page != frame.page
                || limit != config.pageSize
                || page > pages && !(page == 1 && pages == 0 && count == 0)) {
            throw resource.failure("inconsistent page, perPage or totalPages");
        }
        int next = page < pages ? page + 1 : 0;
        JsonNode link = response.path("nextPageUri");
        if (!link.isMissingNode()
                && (next > 0
                        ? !link.isTextual() || !validNext(link.textValue(), path, next)
                        : !link.isNull())) {
            throw resource.failure("invalid nextPageUri for the selected collection");
        }
        return next;
    }

    private boolean validNext(String supplied, String path, int next) {
        if (supplied.length() > 4096) {
            return false;
        }
        try {
            URI uri = new URI(supplied);
            boolean authority =
                    uri.isAbsolute()
                            ? "https".equalsIgnoreCase(uri.getScheme())
                                    && CareQualityCommissionConfig.API_HOST.equalsIgnoreCase(
                                            uri.getHost())
                                    && (uri.getPort() == -1 || uri.getPort() == 443)
                            : uri.getRawAuthority() == null;
            String rawPath = uri.getRawPath();
            return authority
                    && uri.getRawUserInfo() == null
                    && uri.getRawFragment() == null
                    && (path.equals(rawPath) || (PREFIX + path).equals(rawPath))
                    && (query(next).equals(uri.getRawQuery())
                            || ("perPage=" + config.pageSize + "&page=" + next)
                                    .equals(uri.getRawQuery()));
        } catch (URISyntaxException ex) {
            return false;
        }
    }

    private String query(int page) {
        return "page=" + page + "&perPage=" + config.pageSize;
    }

    private static Row row(Frame frame, JsonNode item) {
        CareQualityCommissionResource resource = frame.resource;
        if (!item.isObject()) {
            throw resource.failure("expected a record object");
        }
        String provider = optionalId(resource, item.get("providerId"), "providerId");
        String location = optionalId(resource, item.get("locationId"), "locationId");
        String id;
        if (resource == CareQualityCommissionResource.PROVIDER_LOCATIONS) {
            String organisation =
                    optionalId(resource, item.get("organisationId"), "organisationId");
            id = organisation == null ? location : organisation;
            if (id == null || (location != null && !id.equals(location))) {
                throw resource.failure("missing or conflicting provider location ID");
            }
            if (provider != null && !provider.equals(frame.parentId)) {
                throw resource.failure("location belongs to a different provider");
            }
            provider = frame.parentId;
            location = id;
        } else {
            id = optionalId(resource, item.get(resource.identity), resource.identity);
            if (id == null) {
                throw resource.failure("missing " + resource.identity);
            }
        }
        if (resource.shape == Shape.OBJECT && !frame.parentId.equals(id)) {
            throw resource.failure("detail ID differs from its saved parent");
        }
        return new Row(id, provider, location, item.toString());
    }

    private static String optionalId(
            CareQualityCommissionResource resource, JsonNode field, String name) {
        if (field == null || field.isNull()) {
            return null;
        }
        if (!field.isTextual() || !CareQualityCommissionResource.validPathId(field.textValue())) {
            throw resource.failure("invalid " + name);
        }
        return field.textValue();
    }

    private static int integer(
            CareQualityCommissionResource resource, JsonNode response, String field) {
        JsonNode item = response.path(field);
        if (!item.isIntegralNumber() || !item.canConvertToInt() || item.intValue() < 0) {
            throw resource.failure("missing or invalid pagination " + field);
        }
        return item.intValue();
    }

    /**
     * Holds the current response records, position digest and next locally generated page.
     *
     * <p>The reader retains this response only while its traversal frame is active.
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
     * Projects native and associated identifiers while retaining the intact JSON record.
     *
     * <p>Provider and location identifiers remain independent of the resource identifier.
     */
    static final class Row {
        final String id;
        final String provider;
        final String location;
        final String record;

        Row(String id, String provider, String location, String record) {
            this.id = id;
            this.provider = provider;
            this.location = location;
            this.record = record;
        }
    }
}

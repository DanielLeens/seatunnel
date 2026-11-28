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
package org.apache.seatunnel.connectors.seatunnel.castoredc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import okhttp3.HttpUrl;

import java.util.List;
import java.util.Objects;

/**
 * Resolves continuation using Castor's documented response metadata.
 *
 * <p>Provider URLs never become request destinations. Only a validated adjacent page number is
 * passed back to the fixed route builder.
 */
final class CastorEdcHal {
    private CastorEdcHal() {}

    static int next(
            JsonNode root,
            HttpUrl request,
            CastorEdcResource resource,
            CastorEdcSplit state,
            int recordCount)
            throws CastorEdcReadException {
        JsonNode links = root.path("_links");
        if (!links.isMissingNode() && !links.isNull() && !links.isObject()) {
            throw resource.failure("invalid HAL link envelope");
        }
        JsonNode next = links.path("next");
        int link = 0;
        if (!next.isMissingNode() && !next.isNull()) {
            JsonNode href = next.path("href");
            if (!href.isTextual()
                    || href.textValue().isBlank()
                    || href.textValue().length() > 8192) {
                throw resource.failure("invalid HAL next link");
            }
            link = validate(href.textValue(), request, resource, state.page);
        }
        boolean audit = resource.pagination == CastorEdcResource.Paging.AUDIT;
        Long advertisedPage = number(root.path("page"), resource);
        Long pageSize = number(root.path("page_size"), resource);
        Long total = number(root.path("total_items"), resource);
        if (resource == CastorEdcResource.COUNTRY) {
            total = number(root.path("totals"), resource);
        }
        Long count =
                number(
                        audit ? root.path("extra").path("total_pages") : root.path("page_count"),
                        resource);
        if (advertisedPage != null && advertisedPage != state.page
                || pageSize != null && recordCount > pageSize
                || audit && count == null) {
            throw resource.failure("invalid page metadata");
        }
        boolean canUseCount = audit || resource.pagination == CastorEdcResource.Paging.HAL;
        if (count != null) {
            if (count == 0L && (state.page != 1 || recordCount != 0)
                    || count != 0L && state.page > count
                    || link != 0 && state.page >= count) {
                throw resource.failure("page count disagrees with the requested page or HAL link");
            }
            if (state.page < count && link == 0) {
                if (!canUseCount) {
                    throw resource.failure(
                            "more pages advertised without a supported continuation");
                }
                link = state.page + 1;
            }
        }
        if (resource.pagination == CastorEdcResource.Paging.NONE && link != 0) {
            throw resource.failure("unexpected continuation for a complete resource");
        }
        if (resource.pagination == CastorEdcResource.Paging.HAL
                && link == 0
                && count == null
                && total == null
                && pageSize != null
                && pageSize > 0L
                && recordCount == pageSize) {
            throw resource.failure("full page lacks terminal metadata");
        }
        long seen = state.seenRecords + recordCount;
        if (total != null && (seen > total || link == 0 && seen < total)) {
            throw resource.failure("record count disagrees with pagination metadata");
        }
        return link;
    }

    private static Long number(JsonNode node, CastorEdcResource resource)
            throws CastorEdcReadException {
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (!node.isNumber()) {
            throw resource.failure("invalid numeric paging metadata");
        }
        try {
            long value = node.decimalValue().longValueExact();
            if (value >= 0L) {
                return value;
            }
        } catch (ArithmeticException invalid) {
            // Dimensions must be finite nonnegative integers.
        }
        throw resource.failure("invalid numeric paging metadata");
    }

    private static int validate(
            String href, HttpUrl request, CastorEdcResource resource, int current)
            throws CastorEdcReadException {
        HttpUrl next = request.resolve(href);
        if (next == null
                || !request.scheme().equals(next.scheme())
                || !request.host().equals(next.host())
                || request.port() != next.port()
                || (!request.encodedPath().equals(next.encodedPath())
                        && !(request.encodedPath() + "/").equals(next.encodedPath()))
                || !next.username().isEmpty()
                || !next.password().isEmpty()
                || next.fragment() != null) {
            throw resource.failure("HAL link leaves the requested API scope");
        }
        for (String key : next.queryParameterNames()) {
            List<String> values = next.queryParameterValues(key);
            if (values.size() != 1
                    || (!"page".equals(key)
                            && (!request.queryParameterNames().contains(key)
                                    || !Objects.equals(
                                            values.get(0), request.queryParameter(key))))) {
                throw resource.failure("HAL link changes the selected request filters");
            }
        }
        int expected = current + 1;
        if (!Integer.toString(expected).equals(next.queryParameter("page"))) {
            throw resource.failure("HAL link does not advance by one page");
        }
        return expected;
    }
}

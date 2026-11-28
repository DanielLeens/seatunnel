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

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Saves an active ChartMogul resource, cursor and row position.
 *
 * <p>The time bound is frozen once for both activity and calendar metric requests.
 */
final class ChartMogulSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    private final int resourceCount;
    final Instant windowEnd;
    int resourceIndex;
    int acceptedPages;
    int page = 1;
    int row;
    String cursor;
    String digest;

    // The history belongs to the active resource and contains only continuation hashes.
    final Set<String> seen = new HashSet<>();

    ChartMogulSplit(ChartMogulConfig config, Instant windowEnd) {
        this(config.scope, config.resources.size(), windowEnd);
        skipFutureWindow(config);
    }

    private ChartMogulSplit(String scope, int resourceCount, Instant windowEnd) {
        this.scope = scope;
        this.resourceCount = resourceCount;
        this.windowEnd = windowEnd;
    }

    @Override
    public String splitId() {
        return "chartmogul-account";
    }

    boolean finished() {
        return resourceIndex == resourceCount;
    }

    /**
     * Takes an independent snapshot before the reader advances a cursor or emits another row.
     *
     * <p>The already fetched response is kept in memory by the reader and never serialized.
     */
    ChartMogulSplit copy() {
        ChartMogulSplit copy = new ChartMogulSplit(scope, resourceCount, windowEnd);
        copy.resourceIndex = resourceIndex;
        copy.acceptedPages = acceptedPages;
        copy.page = page;
        copy.row = row;
        copy.cursor = cursor;
        copy.digest = digest;
        copy.seen.addAll(seen);
        return copy;
    }

    void validate(ChartMogulConfig config) {
        boolean changedWindow =
                (config.end != null || config.start == null)
                        && !Objects.equals(config.end, windowEnd);
        if (!config.scope.equals(scope)
                || changedWindow
                || config.start != null && windowEnd == null) {
            throw failure("credential, resource, page-size or time scope changed");
        }
        if (resourceCount != config.resources.size()
                || resourceIndex < 0
                || resourceIndex > resourceCount
                || acceptedPages < page - 1
                || acceptedPages > config.scanPages
                || page < 1
                || page > config.maxPages
                || row < 0
                || digest == null && row != 0
                || digest != null && !validHash(digest)
                || seen.size() != page - 1
                || seen.stream().anyMatch(hash -> !validHash(hash))
                || (page == 1
                        ? cursor != null
                        : cursor == null
                                || !ChartMogulApi.validOpaque(cursor)
                                || !seen.contains(ChartMogulConfig.hash(cursor)))) {
            throw failure("invalid cursor or row position");
        }
        if (finished()) {
            if (page != 1 || digest != null) {
                throw failure("invalid completed position");
            }
            return;
        }
        ChartMogulResource resource = config.resources.get(resourceIndex);
        if (row > resource.limit(config)
                || resource.interval != null && page != 1
                || resource.dated() && futureWindow(config)) {
            throw failure("position outside the selected resource");
        }
    }

    private static boolean validHash(String value) {
        return value != null
                && value.length() == 64
                && value.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f');
    }

    void nextPage(String next, String cursorHash) {
        cursor = next;
        ++page;
        row = 0;
        digest = null;
        seen.add(cursorHash);
    }

    void nextResource(ChartMogulConfig config) {
        ++resourceIndex;
        page = 1;
        row = 0;
        cursor = null;
        digest = null;
        seen.clear();
        skipFutureWindow(config);
    }

    private void skipFutureWindow(ChartMogulConfig config) {
        if (futureWindow(config)) {
            while (!finished() && config.resources.get(resourceIndex).dated()) {
                ++resourceIndex;
            }
        }
    }

    private boolean futureWindow(ChartMogulConfig config) {
        return config.start != null && windowEnd != null && windowEnd.isBefore(config.start);
    }

    private static ChartMogulResource.Failure failure(String reason) {
        return new ChartMogulResource.Failure("checkpoint", reason);
    }
}

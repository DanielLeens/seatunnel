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

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Saves the confirmed offset, row index and optional frozen upper bound for one company scan.
 *
 * <p>The API key and JSON responses never appear in this checkpoint.
 */
final class ChargeDeskSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    private final int resourceCount;
    final Long windowEnd;
    int resourceIndex;
    int acceptedPages;
    int offset;
    int page = 1;
    int row;
    String digest;

    // A bounded set of the completed raw page hashes detects repeated data at new offsets.
    final Set<String> seen = new HashSet<>();

    ChargeDeskSplit(ChargeDeskConfig config, Long windowEnd) {
        this(config.scope, config.resources.size(), windowEnd);
        skipEmptyWindow(config);
    }

    private ChargeDeskSplit(String scope, int resourceCount, Long windowEnd) {
        this.scope = scope;
        this.resourceCount = resourceCount;
        this.windowEnd = windowEnd;
    }

    @Override
    public String splitId() {
        return "chargedesk-company";
    }

    boolean finished() {
        return resourceIndex == resourceCount;
    }

    boolean hasWindow(ChargeDeskConfig config) {
        return config.start != null || windowEnd != null;
    }

    /**
     * Copies mutable position and history for an independent engine checkpoint.
     *
     * <p>Confirmed response contents remain solely in the active reader.
     */
    ChargeDeskSplit copy() {
        ChargeDeskSplit copy = new ChargeDeskSplit(scope, resourceCount, windowEnd);
        copy.resourceIndex = resourceIndex;
        copy.acceptedPages = acceptedPages;
        copy.offset = offset;
        copy.page = page;
        copy.row = row;
        copy.digest = digest;
        copy.seen.addAll(seen);
        return copy;
    }

    void validate(ChargeDeskConfig config) {
        boolean changedWindow =
                (config.end != null || config.start == null)
                        && !Objects.equals(config.end, windowEnd);
        if (!config.scope.equals(scope)
                || changedWindow
                || config.start != null && windowEnd == null
                || windowEnd != null
                        && (windowEnd < 0 || windowEnd > ChargeDeskConfig.MAX_EPOCH_SECOND)) {
            throw failure("credential, resource, page-size or time scope changed");
        }
        if (resourceCount != config.resources.size()
                || resourceIndex < 0
                || resourceIndex > resourceCount
                || acceptedPages < page - 1
                || acceptedPages > config.scanPages
                || page < 1
                || page > config.maxPages
                || offset < page - 1
                || offset > (long) (page - 1) * config.pageSize
                || row < 0
                || digest == null && row != 0
                || digest != null && !validHash(digest)
                || seen.size() > page - 1
                || seen.stream().anyMatch(hash -> !validHash(hash))) {
            throw failure("invalid offset or row position");
        }
        if (finished()) {
            if (page != 1 || digest != null) {
                throw failure("invalid completed position");
            }
            return;
        }
        ChargeDeskResource resource = config.resources.get(resourceIndex);
        if (offset > resource.maxOffset
                || row > resource.requestCount(config.pageSize, offset)
                || resource.dated() && emptyWindow(config)) {
            throw failure("position outside the selected collection");
        }
    }

    private static boolean validHash(String value) {
        return value != null
                && value.length() == 64
                && value.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f');
    }

    void nextPage(int next, String history) {
        offset = next;
        ++page;
        row = 0;
        digest = null;
        seen.add(history);
    }

    void nextResource(ChargeDeskConfig config) {
        ++resourceIndex;
        offset = 0;
        page = 1;
        row = 0;
        digest = null;
        seen.clear();
        skipEmptyWindow(config);
    }

    private void skipEmptyWindow(ChargeDeskConfig config) {
        if (emptyWindow(config)) {
            while (!finished() && config.resources.get(resourceIndex).dated()) {
                ++resourceIndex;
            }
        }
    }

    private boolean emptyWindow(ChargeDeskConfig config) {
        return config.start != null && windowEnd != null && config.start >= windowEnd;
    }

    private static ChargeDeskResource.Failure failure(String reason) {
        return new ChargeDeskResource.Failure("checkpoint", reason);
    }
}

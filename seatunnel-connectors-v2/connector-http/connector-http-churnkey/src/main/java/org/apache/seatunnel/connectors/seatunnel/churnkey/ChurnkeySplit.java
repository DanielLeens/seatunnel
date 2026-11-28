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

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.HashSet;
import java.util.Set;

/**
 * Saves the active Churnkey collection, page digest, and exact row position.
 *
 * <p>The split keeps identifiers of accepted pages without the headers or JSON payloads.
 */
final class ChurnkeySplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    private final int resourceCount;
    int resourceIndex;
    int acceptedPages;
    long skip;
    int page = 1;
    int row;
    String digest;

    // The history is limited by the page budget and reset on entering the next collection.
    final Set<String> seen = new HashSet<>();

    ChurnkeySplit(ChurnkeyConfig config) {
        this(config.scope, config.resources.size());
    }

    private ChurnkeySplit(String scope, int resourceCount) {
        this.scope = scope;
        this.resourceCount = resourceCount;
    }

    @Override
    public String splitId() {
        return "churnkey-app";
    }

    boolean finished() {
        return resourceIndex == resourceCount;
    }

    /**
     * Captures an independent checkpoint at the current confirmed row.
     *
     * <p>Only active session pagination maintains a history of earlier offsets.
     */
    ChurnkeySplit copy() {
        ChurnkeySplit copy = new ChurnkeySplit(scope, resourceCount);
        copy.resourceIndex = resourceIndex;
        copy.acceptedPages = acceptedPages;
        copy.skip = skip;
        copy.page = page;
        copy.row = row;
        copy.digest = digest;
        copy.seen.addAll(seen);
        return copy;
    }

    void validate(ChurnkeyConfig config) {
        if (!config.scope.equals(scope)) {
            throw failure("credential, resource, paging or grouping scope changed");
        }
        if (resourceCount != config.resources.size()
                || resourceIndex < 0
                || resourceIndex > resourceCount
                || acceptedPages < page - 1
                || acceptedPages > config.scanPages
                || page < 1
                || page > config.maxPages
                || skip < page - 1
                || skip > (long) (page - 1) * config.pageSize
                || row < 0
                || digest == null && row != 0
                || digest != null && !validHash(digest)
                || seen.size() != page - 1
                || seen.stream().anyMatch(hash -> !validHash(hash))) {
            throw failure("invalid offset or row position");
        }
        if (finished()) {
            if (page != 1 || digest != null) {
                throw failure("invalid completed position");
            }
            return;
        }
        ChurnkeyResource resource = config.resources.get(resourceIndex);
        if (row > resource.limit(config) || !resource.paged && page != 1) {
            throw failure("position outside the selected collection");
        }
    }

    private static boolean validHash(String value) {
        return value != null
                && value.length() == 64
                && value.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f');
    }

    void nextPage(long next, String pageDigest) {
        skip = next;
        ++page;
        row = 0;
        digest = null;
        seen.add(pageDigest);
    }

    void nextResource() {
        ++resourceIndex;
        skip = 0;
        page = 1;
        row = 0;
        digest = null;
        seen.clear();
    }

    private static ChurnkeyResource.Failure failure(String reason) {
        return new ChurnkeyResource.Failure("checkpoint", reason);
    }
}

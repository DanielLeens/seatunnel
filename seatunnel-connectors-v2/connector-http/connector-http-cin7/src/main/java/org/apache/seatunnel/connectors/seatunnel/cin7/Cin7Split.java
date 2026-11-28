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

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.HashSet;
import java.util.Set;

/**
 * Saves a Cin7 Core collection position and the outstanding detail for its current parent.
 *
 * <p>Only numeric positions and digests are serialized. The parent ID is found again in the
 * verified list page.
 */
final class Cin7Split implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    private final int collectionCount;
    int resourceIndex;
    int acceptedPages;
    int page = 1;
    int row;
    long total = -1;
    String digest;
    boolean detailPending;

    // Previous nonempty page identities are bounded by the per-collection page limit.
    final Set<String> seen = new HashSet<>();

    Cin7Split(Cin7Config config) {
        this(config.scope, config.collections.size());
    }

    private Cin7Split(String scope, int collectionCount) {
        this.scope = scope;
        this.collectionCount = collectionCount;
    }

    @Override
    public String splitId() {
        return "cin7-core-account";
    }

    boolean finished() {
        return resourceIndex == collectionCount;
    }

    /**
     * Makes an independent snapshot for the coordinator or the reader.
     *
     * <p>The page history remains mutable in the active split.
     */
    Cin7Split copy() {
        Cin7Split copy = new Cin7Split(scope, collectionCount);
        copy.resourceIndex = resourceIndex;
        copy.acceptedPages = acceptedPages;
        copy.page = page;
        copy.row = row;
        copy.total = total;
        copy.digest = digest;
        copy.detailPending = detailPending;
        copy.seen.addAll(seen);
        return copy;
    }

    /**
     * Validates checkpoint compatibility and position invariants before any HTTPS request.
     *
     * <p>The reader verifies the saved row against the corresponding raw response after fetching
     * it.
     */
    void validate(Cin7Config config) {
        if (!config.scope.equals(scope)) {
            throw failure("credential, resource, or paging scope changed");
        }
        if (collectionCount != config.collections.size()
                || resourceIndex < 0
                || resourceIndex > collectionCount
                || page < 1
                || page > config.maxPages
                || acceptedPages < page - 1 + (digest == null ? 0 : 1)
                || acceptedPages > config.scanPages
                || row < 0
                || total < -1
                || digest == null && (row != 0 || detailPending)
                || digest != null && !validHash(digest)
                || seen.size() != page - 1
                || seen.stream().anyMatch(hash -> !validHash(hash))) {
            throw failure("invalid page or row position");
        }
        if (finished()) {
            if (page != 1 || digest != null || total != -1) {
                throw failure("invalid completed position");
            }
            return;
        }
        Cin7Resource resource = config.collections.get(resourceIndex);
        int limit = resource.pageLimit(config.pageSize);
        if (row > limit
                || detailPending && (row == limit || config.selectedDetail(resource) == null)) {
            throw failure("position outside the selected collection");
        }
        if (resource == Cin7Resource.DEALS) {
            if (total != -1) {
                throw failure("invalid Total in the Deals position");
            }
        } else if (page == 1 && digest == null
                ? total != -1
                : total < (long) (page - 1) * limit + row) {
            throw failure("invalid Total in the saved position");
        }
    }

    private static boolean validHash(String value) {
        return value != null
                && value.length() == 64
                && value.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f');
    }

    void nextPage(String pageIdentity) {
        ++page;
        row = 0;
        digest = null;
        seen.add(pageIdentity);
    }

    void nextResource() {
        ++resourceIndex;
        page = 1;
        row = 0;
        total = -1;
        digest = null;
        detailPending = false;
        seen.clear();
    }

    private static Cin7Resource.Failure failure(String reason) {
        return new Cin7Resource.Failure("checkpoint", reason);
    }
}

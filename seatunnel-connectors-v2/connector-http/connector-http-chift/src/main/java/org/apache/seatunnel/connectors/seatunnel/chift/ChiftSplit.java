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
package org.apache.seatunnel.connectors.seatunnel.chift;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Immutable position in one Chift account scan.
 *
 * <p>Only the active family of parent UUIDs and bounded adjacent-page identity hashes are retained.
 */
final class ChiftSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final Instant cutoff;
    final int resourceIndex;
    final int page;
    final long total;
    final List<String> parents;
    final int parentIndex;
    final int requests;
    final Set<String> previousHashes;
    final String fingerprint;
    final int published;

    ChiftSplit(ChiftConfig config) {
        this(config.scope, config.cutoff(), 0, 1, -1L, List.of(), 0, 0, Set.of(), null, 0);
    }

    private ChiftSplit(
            String scope,
            Instant cutoff,
            int resourceIndex,
            int page,
            long total,
            List<String> parents,
            int parentIndex,
            int requests,
            Set<String> previousHashes,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.cutoff = cutoff;
        this.resourceIndex = resourceIndex;
        this.page = page;
        this.total = total;
        this.parents = List.copyOf(parents);
        this.parentIndex = parentIndex;
        this.requests = requests;
        this.previousHashes = Set.copyOf(previousHashes);
        this.fingerprint = fingerprint;
        this.published = published;
    }

    String parent(ChiftResource resource) {
        return resource.child ? parents.get(parentIndex) : null;
    }

    ChiftSplit partial(String digest, int offset) {
        return new ChiftSplit(
                scope,
                cutoff,
                resourceIndex,
                page,
                total,
                parents,
                parentIndex,
                requests,
                previousHashes,
                digest,
                offset);
    }

    ChiftSplit advance(
            ChiftConfig config,
            ChiftResource resource,
            long validatedTotal,
            boolean hasNextPage,
            List<String> discovered,
            Set<String> identities) {
        if (hasNextPage) {
            return new ChiftSplit(
                    scope,
                    cutoff,
                    resourceIndex,
                    page + 1,
                    validatedTotal,
                    parents,
                    parentIndex,
                    requests + 1,
                    identities,
                    null,
                    0);
        }
        if (resource.child && parentIndex + 1 < parents.size()) {
            return new ChiftSplit(
                    scope,
                    cutoff,
                    resourceIndex,
                    1,
                    -1L,
                    parents,
                    parentIndex + 1,
                    requests + 1,
                    Set.of(),
                    null,
                    0);
        }
        int next = resourceIndex + 1;
        if (next < config.plan.size() && config.plan.get(next).child && discovered.isEmpty()) {
            next++;
        }
        List<String> pending =
                next < config.plan.size() && config.plan.get(next).child ? discovered : List.of();
        return new ChiftSplit(
                scope, cutoff, next, 1, -1L, pending, 0, requests + 1, Set.of(), null, 0);
    }

    @Override
    public String splitId() {
        return "chift-account";
    }
}

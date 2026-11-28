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
package org.apache.seatunnel.connectors.seatunnel.clazar;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/**
 * Serializable progress for one scan over the selected Clazar collections.
 *
 * <p>Only confirmed page transitions advance the count and resource position.
 */
final class ClazarSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final Instant cutoff;
    final int resourceIndex;
    final int page;
    /**
     * The provider's total and the raw count are independent of local date filtering.
     *
     * <p>The total is unknown until the first page is confirmed.
     */
    final long total;

    final long consumed;
    /**
     * Previous entity page identities help detect page-number drift without holding all records.
     *
     * <p>Analytics results can legitimately contain equal rows and have no such check.
     */
    final Set<String> previousIds;

    final int requests;
    /**
     * The old response and cursor position remain stable until all returned rows are published.
     *
     * <p>A restored partial page must match the saved fingerprint.
     */
    final String fingerprint;

    final int published;

    private ClazarSplit(
            String scope,
            Instant cutoff,
            int resourceIndex,
            int page,
            long total,
            long consumed,
            Set<String> previousIds,
            int requests,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.cutoff = cutoff;
        this.resourceIndex = resourceIndex;
        this.page = page;
        this.total = total;
        this.consumed = consumed;
        this.previousIds = previousIds;
        this.requests = requests;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    static ClazarSplit first(ClazarConfig settings) {
        return new ClazarSplit(
                settings.scope, settings.cutoff(), 0, 1, -1, 0, Set.of(), 0, null, 0);
    }

    @Override
    public String splitId() {
        return "clazar-scan";
    }

    ClazarSplit partial(String digest, int offset) {
        return new ClazarSplit(
                scope,
                cutoff,
                resourceIndex,
                page,
                total,
                consumed,
                previousIds,
                requests,
                digest,
                offset);
    }

    ClazarSplit after(long reportedTotal, long seen, boolean more, Set<String> identities) {
        if (more) {
            return new ClazarSplit(
                    scope,
                    cutoff,
                    resourceIndex,
                    page + 1,
                    reportedTotal,
                    seen,
                    Set.copyOf(identities),
                    requests + 1,
                    null,
                    0);
        }
        return new ClazarSplit(
                scope, cutoff, resourceIndex + 1, 1, -1, 0, Set.of(), requests + 1, null, 0);
    }

    /**
     * Verifies the restored coordinates before any network request can be constructed.
     *
     * <p>Even an empty or terminal checkpoint stays bound to its configured read scope.
     */
    void validate(ClazarConfig settings) {
        if (!settings.scope.equals(scope)) {
            throw ClazarConfig.invalid("checkpoint", "read scope changed");
        }
        boolean fixedCutoff = settings.end != null || settings.start == null;
        if (fixedCutoff
                ? !Objects.equals(settings.end, cutoff)
                : cutoff == null || !cutoff.isAfter(settings.start)) {
            throw ClazarConfig.invalid("checkpoint", "invalid time cutoff");
        }
        if (resourceIndex < 0
                || resourceIndex > settings.resources.size()
                || page < 1
                || page > requests + 1
                || requests < 0
                || requests > settings.maxRequests
                || published < 0
                || published > settings.pageSize
                || (published == 0) != (fingerprint == null)
                || previousIds.size() > settings.pageSize
                || consumed < 0
                || (page == 1 && (total != -1 || consumed != 0 || !previousIds.isEmpty()))
                || (page > 1 && (total <= 0 || consumed >= total))
                || (resourceIndex == settings.resources.size() && (page != 1 || published != 0))) {
            throw ClazarConfig.invalid("checkpoint", "invalid page progress");
        }
    }
}

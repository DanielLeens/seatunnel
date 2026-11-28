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
package org.apache.seatunnel.connectors.seatunnel.capsulecrm;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.List;

/**
 * Immutable position in one account scan.
 *
 * <p>The reader retains only the undiscarded organisation identifiers and page progress, including
 * the frozen opportunity cutoff and the digest of a partially emitted HTTP result.
 */
final class CapsuleCrmSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final Instant cutoff;
    final int resourceIndex;
    final int page;
    final List<String> parents;
    final int parentIndex;
    final int requests;
    final String fingerprint;
    final int published;

    CapsuleCrmSplit(CapsuleCrmConfig config) {
        this(config.scope, config.cutoff(), 0, 1, List.of(), 0, 0, null, 0);
    }

    private CapsuleCrmSplit(
            String scope,
            Instant cutoff,
            int resourceIndex,
            int page,
            List<String> parents,
            int parentIndex,
            int requests,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.cutoff = cutoff;
        this.resourceIndex = resourceIndex;
        this.page = page;
        this.parents = List.copyOf(parents);
        this.parentIndex = parentIndex;
        this.requests = requests;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    String parent(CapsuleCrmResource resource) {
        return resource == CapsuleCrmResource.EMPLOYEES ? parents.get(parentIndex) : null;
    }

    CapsuleCrmSplit partial(String digest, int offset) {
        return new CapsuleCrmSplit(
                scope, cutoff, resourceIndex, page, parents, parentIndex, requests, digest, offset);
    }

    CapsuleCrmSplit advance(
            CapsuleCrmConfig config,
            CapsuleCrmResource resource,
            int nextPage,
            List<String> discovered) {
        if (nextPage != 0) {
            return new CapsuleCrmSplit(
                    scope,
                    cutoff,
                    resourceIndex,
                    nextPage,
                    discovered,
                    parentIndex,
                    requests + 1,
                    null,
                    0);
        }
        if (resource == CapsuleCrmResource.EMPLOYEES && parentIndex + 1 < parents.size()) {
            return new CapsuleCrmSplit(
                    scope,
                    cutoff,
                    resourceIndex,
                    1,
                    parents,
                    parentIndex + 1,
                    requests + 1,
                    null,
                    0);
        }
        int next = resourceIndex + 1;
        if (next < config.plan.size()
                && config.plan.get(next) == CapsuleCrmResource.EMPLOYEES
                && discovered.isEmpty()) {
            next++;
        }
        return new CapsuleCrmSplit(
                scope,
                cutoff,
                next,
                1,
                resource == CapsuleCrmResource.EMPLOYEES ? List.of() : discovered,
                0,
                requests + 1,
                null,
                0);
    }

    @Override
    public String splitId() {
        return "capsule-crm-account";
    }
}

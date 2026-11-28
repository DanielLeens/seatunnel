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

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.List;

/**
 * Immutable reader position within the selected regional study scan.
 *
 * <p>Offset and digest are retained only for a partially published response. Parent identifiers are
 * retained until all selected children are consumed.
 */
final class CastorEdcSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final Instant cutoff;
    final int resourceIndex;
    final int page;
    final long seenRecords;
    final List<String> studies;
    final int studyIndex;
    final int requests;
    final String fingerprint;
    final int published;

    CastorEdcSplit(CastorEdcConfig config) {
        this(config.scope, config.cutoff(), 0, 1, 0L, List.of(), 0, 0, null, 0);
    }

    private CastorEdcSplit(
            String scope,
            Instant cutoff,
            int resourceIndex,
            int page,
            long seenRecords,
            List<String> studies,
            int studyIndex,
            int requests,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.cutoff = cutoff;
        this.resourceIndex = resourceIndex;
        this.page = page;
        this.seenRecords = seenRecords;
        this.studies = List.copyOf(studies);
        this.studyIndex = studyIndex;
        this.requests = requests;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    String parent(CastorEdcResource resource) {
        return resource.child ? studies.get(studyIndex) : null;
    }

    CastorEdcSplit partial(String digest, int offset) {
        return new CastorEdcSplit(
                scope,
                cutoff,
                resourceIndex,
                page,
                seenRecords,
                studies,
                studyIndex,
                requests,
                digest,
                offset);
    }

    CastorEdcSplit advance(
            CastorEdcConfig config,
            CastorEdcResource resource,
            int nextPage,
            int rawCount,
            List<String> discovered) {
        if (nextPage != 0) {
            return new CastorEdcSplit(
                    scope,
                    cutoff,
                    resourceIndex,
                    nextPage,
                    seenRecords + rawCount,
                    discovered,
                    studyIndex,
                    requests + 1,
                    null,
                    0);
        }
        if (resource.child && studyIndex + 1 < studies.size()) {
            return new CastorEdcSplit(
                    scope,
                    cutoff,
                    resourceIndex,
                    1,
                    0L,
                    studies,
                    studyIndex + 1,
                    requests + 1,
                    null,
                    0);
        }
        int next = resourceIndex + 1;
        while (next < config.plan.size() && config.plan.get(next).child && discovered.isEmpty()) {
            next++;
        }
        List<String> retain = next == config.plan.size() ? List.of() : discovered;
        return new CastorEdcSplit(scope, cutoff, next, 1, 0L, retain, 0, requests + 1, null, 0);
    }

    @Override
    public String splitId() {
        return "castor-edc-account";
    }
}

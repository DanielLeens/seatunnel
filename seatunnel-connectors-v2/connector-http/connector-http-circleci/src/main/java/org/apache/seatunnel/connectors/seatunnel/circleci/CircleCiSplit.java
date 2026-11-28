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
package org.apache.seatunnel.connectors.seatunnel.circleci;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable ownership of one CircleCI traversal.
 *
 * <p>Persist only routing/progress information. Provider payload and authentication are never
 * included. A cursor moves forward only when its response has been fully published.
 */
final class CircleCiSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final int MAX_CONTINUATIONS_PER_PARENT = 4096;

    final String scope;
    final Instant cutoff;
    final int resourceIndex;
    final int workflowIndex;
    final int jobIndex;
    /**
     * Ordered project-scoped job numbers discovered across fully committed workflow job pages.
     *
     * <p>The order becomes the job-detail request sequence after discovery is complete.
     */
    final List<Long> jobNumbers;

    final String token;
    /**
     * Digests of previously observed tokens for one endpoint and parent.
     *
     * <p>A fresh parent resets history because provider tokens need not be unique across parents.
     */
    final Set<String> seenTokens;

    final int requests;
    /**
     * Fingerprint of the entire response while a suffix remains unpublished.
     *
     * <p>Filtering cannot hide upstream changes from the page replay check.
     */
    final String fingerprint;

    final int published;

    private CircleCiSplit(
            String scope,
            Instant cutoff,
            int resourceIndex,
            int workflowIndex,
            int jobIndex,
            List<Long> jobNumbers,
            String token,
            Set<String> seenTokens,
            int requests,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.cutoff = cutoff;
        this.resourceIndex = resourceIndex;
        this.workflowIndex = workflowIndex;
        this.jobIndex = jobIndex;
        this.jobNumbers = jobNumbers;
        this.token = token;
        this.seenTokens = seenTokens;
        this.requests = requests;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    static CircleCiSplit first(CircleCiConfig options) {
        return new CircleCiSplit(
                options.scope, options.cutoff(), 0, 0, 0, List.of(), null, Set.of(), 0, null, 0);
    }

    @Override
    public String splitId() {
        return "circleci-scan";
    }

    long jobNumber(CircleCiConfig options) {
        return options.jobNumber == null ? jobNumbers.get(jobIndex) : options.jobNumber;
    }

    CircleCiSplit partial(String digest, int offset) {
        return new CircleCiSplit(
                scope,
                cutoff,
                resourceIndex,
                workflowIndex,
                jobIndex,
                jobNumbers,
                token,
                seenTokens,
                requests,
                digest,
                offset);
    }

    /**
     * Validates a page transition before its rows can be published.
     *
     * <p>Reset token history on every parent boundary. A workflow without any matching, numbered
     * jobs has no job detail request to schedule.
     */
    CircleCiSplit after(
            CircleCiConfig settings,
            CircleCiResource resource,
            String nextToken,
            List<Long> discoveries)
            throws CircleCiReadException {
        if (nextToken != null) {
            String digest = CircleCiConfig.digest(nextToken);
            if (seenTokens.contains(digest)) {
                throw resource.failure("pagination token repeated");
            }
            if (seenTokens.size() >= MAX_CONTINUATIONS_PER_PARENT) {
                throw resource.failure("pagination exceeds 4096 continuations for one parent");
            }
            Set<String> tokens = new HashSet<>(seenTokens);
            tokens.add(digest);
            return new CircleCiSplit(
                    scope,
                    cutoff,
                    resourceIndex,
                    workflowIndex,
                    jobIndex,
                    discoveries,
                    nextToken,
                    Set.copyOf(tokens),
                    requests + 1,
                    null,
                    0);
        }
        if (resource.perWorkflow() && workflowIndex + 1 < settings.workflowIds.size()) {
            return new CircleCiSplit(
                    scope,
                    cutoff,
                    resourceIndex,
                    workflowIndex + 1,
                    0,
                    discoveries,
                    null,
                    Set.of(),
                    requests + 1,
                    null,
                    0);
        }
        if (resource == CircleCiResource.JOBS
                && settings.discoversJobs()
                && jobIndex + 1 < discoveries.size()) {
            return new CircleCiSplit(
                    scope,
                    cutoff,
                    resourceIndex,
                    0,
                    jobIndex + 1,
                    discoveries,
                    null,
                    Set.of(),
                    requests + 1,
                    null,
                    0);
        }
        int next = resourceIndex + 1;
        if (next < settings.plan.size()
                && settings.plan.get(next) == CircleCiResource.JOBS
                && settings.discoversJobs()
                && discoveries.isEmpty()) {
            next++;
        }
        List<Long> remaining = resource == CircleCiResource.JOBS ? List.of() : discoveries;
        return new CircleCiSplit(
                scope, cutoff, next, 0, 0, remaining, null, Set.of(), requests + 1, null, 0);
    }

    /**
     * Rejects incompatible progress before it can construct a request.
     *
     * <p>Cursor and selector coordinates are interpreted only in the restored configuration scope.
     */
    void validate(CircleCiConfig settings) {
        if (!settings.scope.equals(scope)) {
            throw CircleCiConfig.invalid("checkpoint", "read scope changed");
        }
        boolean fixedCutoff = settings.end != null || settings.start == null;
        if (fixedCutoff
                ? !Objects.equals(settings.end, cutoff)
                : cutoff == null || !cutoff.isAfter(settings.start)) {
            throw CircleCiConfig.invalid("checkpoint", "invalid time cutoff");
        }
        if (resourceIndex < 0
                || resourceIndex > settings.plan.size()
                || workflowIndex < 0
                || jobIndex < 0
                || requests < 0
                || requests > settings.maxRequests
                || published < 0
                || published > CircleCiPage.MAX_ROWS
                || (published == 0) != (fingerprint == null)
                || jobNumbers.size() > settings.maxJobs
                || seenTokens.size() > MAX_CONTINUATIONS_PER_PARENT
                || (token == null) != seenTokens.isEmpty()
                || token != null && !seenTokens.contains(CircleCiConfig.digest(token))) {
            throw CircleCiConfig.invalid("checkpoint", "invalid read progress");
        }
        if (resourceIndex == settings.plan.size()) {
            if (published != 0 || workflowIndex != 0 || jobIndex != 0 || token != null) {
                throw CircleCiConfig.invalid("checkpoint", "invalid terminal progress");
            }
            return;
        }
        CircleCiResource resource = settings.plan.get(resourceIndex);
        if (resource.perWorkflow() && workflowIndex >= settings.workflowIds.size()
                || resource == CircleCiResource.JOBS
                        && settings.discoversJobs()
                        && jobIndex >= jobNumbers.size()
                || token != null && resource.shape != CircleCiResource.Shape.PAGE) {
            throw CircleCiConfig.invalid("checkpoint", "invalid resource progress");
        }
    }
}

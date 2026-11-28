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
package org.apache.seatunnel.connectors.seatunnel.claude;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

/**
 * Stores the frozen date ranges and the exact row position of the active analytics slice.
 *
 * <p>The checkpoint retains opaque continuations and digests without credentials or raw records.
 */
final class ClaudeSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    final LocalDate scanStart;
    final LocalDate adoptionEnd;
    final LocalDate reportEnd;
    private final int resourceCount;
    int resourceIndex;
    LocalDate windowStart;
    int page = 1;
    int row;
    int acceptedPages;
    String cursor;
    String digest;
    String organizationId;
    LocalDate lastBucket;

    // Digests of prior cursor values prevent cycles within the current date slice.
    final Set<String> visited = new HashSet<>();

    ClaudeSplit(ClaudeConfig config, LocalDate start, LocalDate adoptionEnd, LocalDate reportEnd) {
        this(config.scope, config.resources.size(), start, adoptionEnd, reportEnd);
        skipEmptyResources(config);
    }

    private ClaudeSplit(
            String scope,
            int resourceCount,
            LocalDate start,
            LocalDate adoptionEnd,
            LocalDate reportEnd) {
        this.scope = scope;
        this.resourceCount = resourceCount;
        this.scanStart = start;
        this.adoptionEnd = adoptionEnd;
        this.reportEnd = reportEnd;
        this.windowStart = start;
    }

    @Override
    public String splitId() {
        return "claude-enterprise-analytics";
    }

    boolean finished() {
        return resourceIndex == resourceCount;
    }

    LocalDate endFor(ClaudeResource resource) {
        return resource.isReport() ? reportEnd : adoptionEnd;
    }

    LocalDate windowEnd(ClaudeResource resource) {
        LocalDate planned = windowStart.plusDays(resource.days);
        LocalDate stop = endFor(resource);
        return planned.isBefore(stop) ? planned : stop;
    }

    /**
     * Copies the current range and cursor positions for an independent snapshot.
     *
     * <p>The active reader can advance without mutating the saved cursor history.
     */
    ClaudeSplit copy() {
        ClaudeSplit saved =
                new ClaudeSplit(scope, resourceCount, scanStart, adoptionEnd, reportEnd);
        saved.resourceIndex = resourceIndex;
        saved.windowStart = windowStart;
        saved.page = page;
        saved.row = row;
        saved.acceptedPages = acceptedPages;
        saved.cursor = cursor;
        saved.digest = digest;
        saved.organizationId = organizationId;
        saved.lastBucket = lastBucket;
        saved.visited.addAll(visited);
        return saved;
    }

    /**
     * Validates the persisted scan scope and position before a provider request can be issued.
     *
     * <p>Replay compares the saved digest with the newly fetched response before row publication.
     */
    void validate(ClaudeConfig config) {
        if (!config.scope.equals(scope)) {
            throw failure("credential, resource, paging, or date scope changed");
        }
        if (scanStart == null
                || scanStart.isBefore(ClaudeConfig.EARLIEST)
                || config.start != null && !config.start.equals(scanStart)
                || adoptionEnd == null
                || reportEnd == null
                || (config.end == null
                        ? !adoptionEnd.equals(reportEnd.minusDays(2))
                        : !adoptionEnd.equals(config.end.plusDays(1))
                                || !adoptionEnd.equals(reportEnd))
                || windowStart == null
                || windowStart.isBefore(scanStart)
                || resourceCount != config.resources.size()
                || resourceIndex < 0
                || resourceIndex > config.resources.size()
                || page < 1
                || page > config.maxPages
                || (page == 1) != (cursor == null)
                || cursor != null && !validValue(cursor, 4096)
                || row < 0
                || row > config.maxItems
                || digest == null && row != 0
                || digest != null && !validHash(digest)
                || organizationId != null && !validValue(organizationId, 512)
                || acceptedPages < page - 1 + (digest == null ? 0 : 1)
                || acceptedPages > config.scanPages
                || visited.size() != page - 1
                || visited.stream().anyMatch(value -> !validHash(value))) {
            throw failure("invalid window, cursor, or row position");
        }
        if (finished()) {
            if (page != 1
                    || digest != null
                    || lastBucket != null
                    || !windowStart.equals(scanStart)) {
                throw failure("invalid completed position");
            }
            return;
        }
        ClaudeResource resource = config.resources.get(resourceIndex);
        long elapsed = windowStart.toEpochDay() - scanStart.toEpochDay();
        if (!windowStart.isBefore(endFor(resource))
                || elapsed % resource.days != 0
                || resource.kind == ClaudeResource.Kind.SUMMARY && page != 1
                || lastBucket != null
                        && (resource.kind != ClaudeResource.Kind.BUCKET_REPORT
                                || page == 1
                                || lastBucket.isBefore(windowStart)
                                || !lastBucket.isBefore(windowEnd(resource)))
                || resource.isReport() && digest != null && organizationId == null) {
            throw failure("position outside the selected resource slice");
        }
    }

    void nextPage(String next, LocalDate last) {
        if (cursor != null) {
            visited.add(ClaudeConfig.hash(cursor));
        } else {
            visited.add(ClaudeConfig.hash(""));
        }
        if (last != null) {
            lastBucket = last;
        }
        cursor = next;
        digest = null;
        row = 0;
        ++page;
    }

    void nextSlice(ClaudeConfig config, ClaudeResource resource) {
        windowStart = windowEnd(resource);
        page = 1;
        row = 0;
        cursor = null;
        digest = null;
        visited.clear();
        lastBucket = null;
        if (!windowStart.isBefore(endFor(resource))) {
            ++resourceIndex;
            windowStart = scanStart;
            skipEmptyResources(config);
        }
    }

    private void skipEmptyResources(ClaudeConfig config) {
        while (resourceIndex < config.resources.size()
                && !scanStart.isBefore(endFor(config.resources.get(resourceIndex)))) {
            ++resourceIndex;
        }
    }

    static boolean validValue(String value, int limit) {
        return value != null
                && !value.isBlank()
                && value.length() <= limit
                && value.codePoints().noneMatch(Character::isISOControl);
    }

    private static boolean validHash(String value) {
        return value != null
                && value.length() == 64
                && value.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f');
    }

    private static ClaudeResource.Failure failure(String reason) {
        return new ClaudeResource.Failure("checkpoint", reason);
    }
}

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
package org.apache.seatunnel.connectors.seatunnel.ciscomeraki;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Tracks an organization page, its active network page, and the selected child operation.
 *
 * <p>Only scoped request positions and digests are persisted. Parent identities come from replayed
 * pages rather than saved JSON objects.
 */
final class CiscoMerakiSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    final Instant windowStart;
    final Instant windowEnd;

    final Position org = new Position();
    final Position network = new Position();
    final Position leaf = new Position();

    int acceptedPages;
    int child;
    boolean organizationEmitted;
    boolean networkEmitted;
    boolean complete;

    CiscoMerakiSplit(CiscoMerakiConfig config, Instant windowStart, Instant windowEnd) {
        this(config.scope, windowStart, windowEnd);
    }

    private CiscoMerakiSplit(String scope, Instant windowStart, Instant windowEnd) {
        this.scope = scope;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String splitId() {
        return "cisco-meraki-organizations";
    }

    boolean finished() {
        return complete;
    }

    /**
     * Copies the traversal relative to its frozen time window.
     *
     * <p>Independent position objects prevent a late reader update from changing a saved
     * checkpoint.
     */
    CiscoMerakiSplit copy() {
        CiscoMerakiSplit saved = new CiscoMerakiSplit(scope, windowStart, windowEnd);
        saved.org.copyFrom(org);
        saved.network.copyFrom(network);
        saved.leaf.copyFrom(leaf);
        saved.acceptedPages = acceptedPages;
        saved.child = child;
        saved.organizationEmitted = organizationEmitted;
        saved.networkEmitted = networkEmitted;
        saved.complete = complete;
        return saved;
    }

    /**
     * Checks state and configuration before the reader can issue an HTTPS request.
     *
     * <p>Each saved continuation is separately checked against its actual restored parent path.
     */
    void validate(CiscoMerakiConfig config) {
        if (!config.scope.equals(scope)) {
            throw failure("credential, resource, paging, or date scope changed");
        }
        int confirmed = org.confirmed() + network.confirmed() + leaf.confirmed();
        if (windowStart == null
                || windowEnd == null
                || windowStart.isAfter(windowEnd)
                || (config.start == null
                        ? !windowStart.equals(windowEnd.minus(Duration.ofDays(30)))
                        : !config.start.equals(windowStart))
                || config.end != null && !config.end.equals(windowEnd)
                || config.resources.contains(CiscoMerakiResource.API_REQUESTS)
                        && Duration.between(windowStart, windowEnd).compareTo(Duration.ofDays(31))
                                > 0
                || acceptedPages < confirmed
                || acceptedPages > config.scanPages
                || child < 0
                || child > config.children.size()) {
            throw failure("invalid date window or scan position");
        }
        if (complete) {
            if (!org.initial()
                    || !network.initial()
                    || !leaf.initial()
                    || child != 0
                    || organizationEmitted
                    || networkEmitted) {
                throw failure("invalid completed position");
            }
            return;
        }
        org.validate(config, CiscoMerakiResource.ORGANIZATIONS);
        if ((!organizationEmitted && (child != 0 || !network.initial() || !leaf.initial()))
                || organizationEmitted && (org.digest == null || org.row == config.pageSize)) {
            throw failure("invalid organization position");
        }
        if (child == config.children.size()) {
            if (!network.initial() || !leaf.initial() || networkEmitted) {
                throw failure("invalid completed child position");
            }
            return;
        }
        CiscoMerakiResource resource = config.children.get(child);
        if (resource == CiscoMerakiResource.NETWORKS) {
            network.validate(config, resource);
            if (networkEmitted
                            && (network.digest == null
                                    || network.row == config.pageSize
                                    || !config.resources.contains(CiscoMerakiResource.SETTINGS))
                    || !networkEmitted && !leaf.initial()) {
                throw failure("invalid network position");
            }
            leaf.validate(config, CiscoMerakiResource.SETTINGS);
        } else {
            if (!network.initial() || networkEmitted) {
                throw failure("invalid child operation position");
            }
            leaf.validate(config, resource);
        }
    }

    private static CiscoMerakiResource.Failure failure(String reason) {
        return new CiscoMerakiResource.Failure("checkpoint", reason);
    }

    private static boolean validHash(String value) {
        return value != null
                && value.length() == 64
                && value.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f');
    }

    /**
     * Stores one collection's page target and the output position within its verified response.
     *
     * <p>Consumed targets are represented only by hashes to bound loop detection and checkpoint
     * size.
     */
    static final class Position implements Serializable {
        private static final long serialVersionUID = 1L;

        String target;
        String digest;
        int row;
        int page = 1;
        final Set<String> visited = new HashSet<>();

        void copyFrom(Position old) {
            target = old.target;
            digest = old.digest;
            row = old.row;
            page = old.page;
            visited.addAll(old.visited);
        }

        private int confirmed() {
            return page - 1 + (digest == null ? 0 : 1);
        }

        boolean initial() {
            return target == null && digest == null && row == 0 && page == 1 && visited.isEmpty();
        }

        void validate(CiscoMerakiConfig config, CiscoMerakiResource resource) {
            int rowLimit;
            switch (resource) {
                case ORGANIZATIONS:
                case NETWORKS:
                case API_REQUESTS:
                    rowLimit = config.pageSize;
                    break;
                case ADMINS:
                    rowLimit = config.maxItems;
                    break;
                default:
                    rowLimit = 1;
            }
            if (page < 1
                    || page > config.maxPages
                    || !resource.paged && page != 1
                    || (page == 1) != (target == null)
                    || target != null && (target.length() > 16384 || !target.startsWith("/"))
                    || row < 0
                    || row > rowLimit
                    || digest == null && row != 0
                    || digest != null && !validHash(digest)
                    || visited.size() != page - 1
                    || visited.stream().anyMatch(d -> !validHash(d))) {
                throw failure("invalid page, continuation, or row");
            }
        }

        void next(String previous, String next) {
            target = next;
            visited.add(CiscoMerakiConfig.hash(previous));
            digest = null;
            row = 0;
            ++page;
        }

        void reset() {
            target = null;
            digest = null;
            row = 0;
            page = 1;
            visited.clear();
        }
    }
}

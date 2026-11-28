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
package org.apache.seatunnel.connectors.seatunnel.carequalitycommission;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.seatunnel.carequalitycommission.CareQualityCommissionResource.Shape;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Saves the active Syndication page and, when present, its current parent's child collection.
 *
 * <p>Only page digests, identifiers and row positions are stored. Credentials and response payloads
 * remain outside the checkpoint.
 */
final class CareQualityCommissionSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    final List<Frame> frames = new ArrayList<>(2);
    int rootIndex;
    int acceptedPages;

    CareQualityCommissionSplit(CareQualityCommissionConfig config) {
        this(config.scope);
        frames.add(new Frame(config.roots.get(0), null));
    }

    private CareQualityCommissionSplit(String scope) {
        this.scope = scope;
    }

    @Override
    public String splitId() {
        return "cqc-syndication";
    }

    boolean finished() {
        return frames.isEmpty();
    }

    /**
     * Copies the complete traversal state for an independent checkpoint.
     *
     * <p>Parent and child progress can advance after the copy is taken.
     */
    CareQualityCommissionSplit copy() {
        CareQualityCommissionSplit saved = new CareQualityCommissionSplit(scope);
        saved.rootIndex = rootIndex;
        saved.acceptedPages = acceptedPages;
        frames.forEach(frame -> saved.frames.add(frame.copy()));
        return saved;
    }

    void validate(CareQualityCommissionConfig config) {
        if (!config.scope.equals(scope)) {
            throw failure("credential, resources or page-size scope changed");
        }
        if (rootIndex < 0
                || rootIndex > config.roots.size()
                || frames.size() > 2
                || finished() != (rootIndex == config.roots.size())
                || acceptedPages < 0
                || acceptedPages > config.scanPages) {
            throw failure("invalid Syndication traversal position");
        }
        if (finished()) {
            return;
        }
        Frame root = frames.get(0);
        root.validate(config);
        if (root.resource != config.roots.get(rootIndex) || root.parentId != null) {
            throw failure("invalid Syndication resource");
        }
        if (frames.size() == 2) {
            Frame child = frames.get(1);
            child.validate(config);
            if (config.children(root.resource).isEmpty()
                    || !root.emitted
                    || root.child == 0
                    || child.resource != config.children(root.resource).get(root.child - 1)) {
                throw failure("invalid child collection order");
            }
        }
    }

    void nextRoot(CareQualityCommissionConfig config) {
        frames.clear();
        ++rootIndex;
        if (rootIndex < config.roots.size()) {
            frames.add(new Frame(config.roots.get(rootIndex), null));
        }
    }

    private static CareQualityCommissionResource.Failure failure(String message) {
        return new CareQualityCommissionResource.Failure("checkpoint", message);
    }

    /**
     * Tracks the current row and child boundary in one Syndication or detail response.
     *
     * <p>The reader validates the parent ID using the restored parent page before child I/O.
     */
    static final class Frame implements Serializable {
        private static final long serialVersionUID = 1L;
        final CareQualityCommissionResource resource;
        final String parentId;
        int page = 1;
        int row;
        int child;
        boolean emitted;
        String digest;

        // Hashes of nonempty offset pages guard against repeated data within this collection.
        final Set<String> history = new HashSet<>();

        Frame(CareQualityCommissionResource resource, String parentId) {
            this.resource = resource;
            this.parentId = parentId;
        }

        Frame copy() {
            Frame saved = new Frame(resource, parentId);
            saved.page = page;
            saved.row = row;
            saved.child = child;
            saved.emitted = emitted;
            saved.digest = digest;
            saved.history.addAll(history);
            return saved;
        }

        void validate(CareQualityCommissionConfig config) {
            if (resource == null) {
                throw failure("missing resource");
            }
            int children = config.children(resource).size();
            int rows = config.maxItems;
            if (resource.shape == Shape.PAGED) {
                rows = config.pageSize;
            } else if (resource.shape == Shape.OBJECT) {
                rows = 1;
            }
            if (resource.scoped != (parentId != null)
                    || parentId != null && !CareQualityCommissionResource.validPathId(parentId)
                    || page < 1
                    || page > config.maxPages
                    || history.size() > page - 1
                    || history.stream().anyMatch(hash -> !validHash(hash))
                    || resource.shape != Shape.PAGED && page != 1
                    || row < 0
                    || row > rows
                    || child < 0
                    || child > children
                    || !emitted && child != 0
                    || digest == null && (row != 0 || emitted)
                    || digest != null && !validHash(digest)) {
                throw failure("invalid response row or page position");
            }
        }

        private static boolean validHash(String value) {
            return value != null
                    && value.length() == 64
                    && value.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f');
        }

        void advanceRow() {
            ++row;
            child = 0;
            emitted = false;
        }

        void nextPage(int next, String pageHash) {
            if (pageHash != null) {
                history.add(pageHash);
            }
            page = next;
            row = 0;
            child = 0;
            emitted = false;
            digest = null;
        }
    }
}

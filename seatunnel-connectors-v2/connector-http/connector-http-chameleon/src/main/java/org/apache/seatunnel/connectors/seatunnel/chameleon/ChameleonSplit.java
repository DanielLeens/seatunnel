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
package org.apache.seatunnel.connectors.seatunnel.chameleon;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonResource.Shape;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Saves the active workspace page and, when present, its current Microsurvey responses.
 *
 * <p>The checkpoint stores page digests, the current cursor and identifiers, and the fixed time
 * bound. Credentials and response payloads remain outside its state.
 */
final class ChameleonSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    final Instant windowEnd;
    final List<Frame> frames = new ArrayList<>(2);
    int rootIndex;
    int acceptedPages;

    ChameleonSplit(ChameleonConfig config, Instant windowEnd) {
        this(config.scope, windowEnd);
        if (config.start != null && windowEnd != null && !config.start.isBefore(windowEnd)) {
            rootIndex = config.roots.size();
        } else {
            frames.add(new Frame(config.roots.get(0), null));
        }
    }

    private ChameleonSplit(String scope, Instant windowEnd) {
        this.scope = scope;
        this.windowEnd = windowEnd;
    }

    boolean hasWindow(ChameleonConfig config) {
        return config.start != null || windowEnd != null;
    }

    @Override
    public String splitId() {
        return "chameleon-workspace";
    }

    boolean finished() {
        return frames.isEmpty();
    }

    /**
     * Copies the complete traversal state for an independent checkpoint.
     *
     * <p>Parent and child progress can advance after the copy is taken.
     */
    ChameleonSplit copy() {
        ChameleonSplit saved = new ChameleonSplit(scope, windowEnd);
        saved.rootIndex = rootIndex;
        saved.acceptedPages = acceptedPages;
        frames.forEach(frame -> saved.frames.add(frame.copy()));
        return saved;
    }

    void validate(ChameleonConfig config) {
        boolean changedWindow =
                (config.end != null || config.start == null)
                        && !Objects.equals(config.end, windowEnd);
        boolean invalidWindow =
                config.start != null
                        && (windowEnd == null || !config.start.isBefore(windowEnd) && !finished());
        if (!config.scope.equals(scope) || changedWindow || invalidWindow) {
            throw failure("credential, resource, page-size or time scope changed");
        }
        if (rootIndex < 0
                || rootIndex > config.roots.size()
                || frames.size() > 2
                || finished() != (rootIndex == config.roots.size())
                || acceptedPages < 0
                || acceptedPages > config.scanPages) {
            throw failure("invalid workspace traversal position");
        }
        if (finished()) {
            return;
        }
        Frame root = frames.get(0);
        root.validate(config);
        if (root.resource != config.roots.get(rootIndex) || root.parentId != null) {
            throw failure("invalid workspace resource");
        }
        if (frames.size() == 2) {
            Frame child = frames.get(1);
            child.validate(config);
            if (root.resource != ChameleonResource.SURVEYS
                    || !root.emitted
                    || root.child == 0
                    || child.parentId == null
                    || child.resource != config.children.get(root.child - 1)) {
                throw failure("invalid survey response order");
            }
        }
    }

    void nextRoot(ChameleonConfig config) {
        frames.clear();
        ++rootIndex;
        if (rootIndex < config.roots.size()) {
            frames.add(new Frame(config.roots.get(rootIndex), null));
        }
    }

    private static ChameleonResource.Failure failure(String message) {
        return new ChameleonResource.Failure("checkpoint", message);
    }

    /**
     * Tracks the current row and child boundary in one workspace or Microsurvey response.
     *
     * <p>The reader validates the owning Microsurvey using the restored parent page before child
     * I/O.
     */
    static final class Frame implements Serializable {
        private static final long serialVersionUID = 1L;
        final ChameleonResource resource;
        final String parentId;
        int page = 1;
        int row;
        int child;
        boolean emitted;
        String digest;
        String cursor;

        // A bounded history of hashed continuation tokens detects cycles.
        final Set<String> seen = new HashSet<>();

        Frame(ChameleonResource resource, String parentId) {
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
            saved.cursor = cursor;
            saved.seen.addAll(seen);
            return saved;
        }

        void validate(ChameleonConfig config) {
            if (resource == null) {
                throw failure("missing resource");
            }
            int children = resource == ChameleonResource.SURVEYS ? config.children.size() : 0;
            int rows = resource.shape == Shape.COMPLETE ? 1 : config.pageSize;
            if ((resource == ChameleonResource.SURVEY_RESPONSES) != (parentId != null)
                    || parentId != null && !ChameleonApi.validOpaque(parentId)
                    || page < 1
                    || page > config.maxPages
                    || resource.shape == Shape.COMPLETE && page != 1
                    || !validHistory()
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

        private boolean validHistory() {
            if (seen.size() > page - 1 || seen.stream().anyMatch(hash -> !validHash(hash))) {
                return false;
            }
            if (resource.shape != Shape.CURSOR || page == 1) {
                return cursor == null;
            }
            return cursor != null
                    && ChameleonApi.validOpaque(cursor)
                    && seen.size() == page - 1
                    && seen.contains(ChameleonConfig.hash(cursor));
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

        void nextPage(int next, String nextCursor, String history) {
            cursor = nextCursor;
            if (history != null) {
                seen.add(history);
            }
            page = next;
            row = 0;
            child = 0;
            emitted = false;
            digest = null;
        }
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.circa;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/**
 * Exposes one Circa collection to the Source V2 SPI.
 *
 * <p>Each job has one ordered traversal of the selected Circa resource.
 */
public final class CircaSource
        implements SeaTunnelSource<SeaTunnelRow, CircaSource.Split, CircaSource.State> {
    static final String IDENTIFIER = "Circa";
    private static final long serialVersionUID = 1L;
    private final CircaConfig config;
    private final CatalogTable table;

    CircaSource(CircaConfig config) {
        this.config = config;
        table =
                CatalogTableUtil.getCatalogTable(
                        config.resource.id,
                        new SeaTunnelRowType(
                                new String[] {"id", "parent_id", "cursor_timestamp", "data"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.STRING_TYPE, BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE, BasicType.STRING_TYPE
                                }));
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return List.of(table);
    }

    /**
     * Creates a worker for the sole traversal split.
     *
     * @param context the source worker's engine context
     * @return the source reader
     */
    @Override
    public SourceReader<SeaTunnelRow, Split> createReader(SourceReader.Context context) {
        return new CircaReader(config, context, table.getTablePath().toString());
    }

    /**
     * Defers network work until the reader receives its split.
     *
     * @param context the source coordinator's engine context
     * @return the coordinator with an initial pending split
     */
    @Override
    public SourceSplitEnumerator<Split, State> createEnumerator(
            SourceSplitEnumerator.Context<Split> context) {
        Split first = new Split(config.scope);
        return new Enumerator(config, context, new State(config.scope, first));
    }

    /**
     * Reuses the pending traversal if the coordinator was checkpointed before assignment.
     *
     * @param context the source coordinator's engine context
     * @param checkpointState saved assignment status
     * @return the restored coordinator
     */
    @Override
    public SourceSplitEnumerator<Split, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Split> context, State checkpointState) {
        return new Enumerator(config, context, checkpointState);
    }

    /**
     * Captures fixed read bounds and both positions of a selected resource traversal.
     *
     * <p>The scope digest binds a restored read to its authority, resource, key, and read options.
     * Provider record data and authorization are excluded from split state.
     */
    public static final class Split implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String scope;
        final Instant ceiling;
        final int pages;
        final Position parent;
        final Position leaf;
        final boolean finished;

        Split(String scope) {
            this(scope, null, 0, new Position(), new Position(), false);
        }

        Split(
                String scope,
                Instant ceiling,
                int pages,
                Position parent,
                Position leaf,
                boolean finished) {
            this.scope = scope;
            this.ceiling = ceiling;
            this.pages = pages;
            this.parent = parent;
            this.leaf = leaf;
            this.finished = finished;
        }

        @Override
        public String splitId() {
            return IDENTIFIER + "-" + scope;
        }
    }

    /**
     * Stores one active paginated frame independent of any loaded page data.
     *
     * <p>An ordered raw-page digest is mandatory once the page counted toward the job's budget.
     */
    static final class Position implements Serializable {
        private static final long serialVersionUID = 1L;
        final int page;
        final int offset;
        final String digest;
        final String previous;

        Position() {
            this(1, 0, null, null);
        }

        Position(int page, int offset, String digest, String previous) {
            this.page = page;
            this.offset = offset;
            this.digest = digest;
            this.previous = previous;
        }

        boolean empty() {
            return page == 1 && offset == 0 && digest == null && previous == null;
        }
    }

    /**
     * Stores coordinator assignment status.
     *
     * <p>If the split is already assigned, progress belongs to the reader checkpoint.
     */
    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final String scope;
        final Split pending;

        State(String scope, Split pending) {
            this.scope = scope;
            this.pending = pending;
        }
    }

    /**
     * Assigns or reassigns the sole traversal split.
     *
     * <p>Assignment and checkpoint capture share the same monitor.
     */
    private static final class Enumerator implements SourceSplitEnumerator<Split, State> {
        private final CircaConfig config;
        private final Context<Split> context;
        private Split pending;
        private boolean started;
        private boolean noMoreSplits;

        Enumerator(CircaConfig config, Context<Split> context, State checkpoint) {
            if (context.currentParallelism() != 1) {
                throw new IllegalArgumentException("source parallelism must be 1");
            }
            if (checkpoint == null || !config.scope.equals(checkpoint.scope)) {
                throw new IllegalArgumentException(
                        "source authentication scope, resource or read options changed during restore");
            }
            this.config = config;
            this.context = context;
            pending = checkpoint.pending;
            if (pending != null) {
                CircaReader.validate(config, pending);
            }
        }

        @Override
        public void open() {}

        /**
         * Makes the pending traversal eligible for assignment.
         *
         * <p>Registration may happen before the coordinator starts.
         */
        @Override
        public synchronized void run() {
            started = true;
            assign();
        }

        @Override
        public synchronized void close() {
            started = false;
        }

        /**
         * Retains a returned split if the reader is not yet ready.
         *
         * @param splits the returned traversal, if any
         * @param subtaskId reader index registered by the engine
         */
        @Override
        public synchronized void addSplitsBack(List<Split> splits, int subtaskId) {
            if (splits.isEmpty()) {
                return;
            }
            if (subtaskId != 0 || splits.size() != 1 || pending != null) {
                throw new IllegalArgumentException("expected one returned Circa traversal");
            }
            Split restored = splits.get(0);
            CircaReader.validate(config, restored);
            pending = restored;
            noMoreSplits = false;
            assign();
        }

        @Override
        public synchronized int currentUnassignedSplitSize() {
            return pending == null ? 0 : 1;
        }

        @Override
        public synchronized void handleSplitRequest(int subtaskId) {
            assign();
        }

        @Override
        public synchronized void registerReader(int subtaskId) {
            assign();
        }

        /**
         * Captures an unassigned traversal while assignment is serialized on the same monitor.
         *
         * @param checkpointId the coordinator checkpoint identifier
         * @return the pending traversal, if the reader does not already own it
         */
        @Override
        public synchronized State snapshotState(long checkpointId) {
            return new State(config.scope, pending);
        }

        private void assign() {
            if (!started || !context.registeredReaders().contains(0)) {
                return;
            }
            if (pending != null) {
                context.assignSplit(0, pending);
                pending = null;
            }
            if (!noMoreSplits) {
                context.signalNoMoreSplits(0);
                noMoreSplits = true;
            }
        }
    }
}

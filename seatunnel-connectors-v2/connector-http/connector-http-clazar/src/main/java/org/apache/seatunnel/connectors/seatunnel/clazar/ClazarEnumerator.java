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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.Serializable;
import java.util.List;

/**
 * Assigns the sole Clazar scan to its single source reader.
 *
 * <p>After assignment, only the reader advances the pending resource plan.
 */
final class ClazarEnumerator
        implements SourceSplitEnumerator<ClazarSplit, ClazarEnumerator.Snapshot> {
    private final Context<ClazarSplit> context;
    private final String scope;
    // Kept until ownership is successfully transferred to the registered worker.
    private ClazarSplit pending;
    private boolean running;
    // Final assignment is independent of whether the assigned split was restored or new.
    private boolean finalAssignmentSent;

    ClazarEnumerator(Context<ClazarSplit> context, ClazarConfig config, Snapshot saved) {
        this.context = context;
        scope = config.scope;
        if (context.currentParallelism() != 1) {
            throw ClazarConfig.invalid("parallelism", "Clazar requires source parallelism 1");
        }
        if (saved != null && !scope.equals(saved.scope)) {
            throw ClazarConfig.invalid("checkpoint", "read scope changed");
        }
        pending = saved == null ? ClazarSplit.first(config) : saved.pending;
    }

    /**
     * Opens the enumerator without consulting Clazar.
     *
     * <p>Network requests belong exclusively to the worker after ownership is assigned.
     */
    @Override
    public void open() {}

    /**
     * Enables assignment after the coordinator is running.
     *
     * <p>The reader may register before or after this hook.
     */
    @Override
    public synchronized void run() {
        running = true;
        offer();
    }

    /**
     * Stops future split assignment in this coordinator.
     *
     * <p>Reader cancellation is handled separately by the source lifecycle.
     */
    @Override
    public synchronized void close() {
        running = false;
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId) {
        offer();
    }

    @Override
    public synchronized void registerReader(int subtaskId) {
        offer();
    }

    @Override
    public synchronized int currentUnassignedSplitSize() {
        return pending == null ? 0 : 1;
    }

    /**
     * Returns one previously assigned traversal to coordinator ownership.
     *
     * <p>Its saved progress takes precedence over creation of a fresh traversal.
     */
    @Override
    public synchronized void addSplitsBack(List<ClazarSplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        if (splits.size() != 1 || pending != null || !scope.equals(splits.get(0).scope)) {
            throw ClazarConfig.invalid("checkpoint", "could not return the Clazar split");
        }
        pending = splits.get(0);
        offer();
    }

    /**
     * Saves only pre-assignment coordinator state once the worker owns the split.
     *
     * <p>The ownership marker avoids reconstructing a scan after an assigned or completed job.
     */
    @Override
    public synchronized Snapshot snapshotState(long checkpointId) {
        return new Snapshot(scope, pending);
    }

    private void offer() {
        if (!running || !context.registeredReaders().contains(0)) {
            return;
        }
        if (pending != null) {
            context.assignSplit(0, pending);
            pending = null;
        }
        if (!finalAssignmentSent) {
            // A restored coordinator with no split has no new provider work to assign.
            context.signalNoMoreSplits(0);
            finalAssignmentSent = true;
        }
    }

    /**
     * Serialized coordinator state with the optional unassigned split.
     *
     * <p>The scope fingerprint guards against switching Clazar identity or resource selection on
     * restore.
     */
    static final class Snapshot implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String scope;
        private final ClazarSplit pending;

        private Snapshot(String scope, ClazarSplit pending) {
            this.scope = scope;
            this.pending = pending;
        }
    }
}

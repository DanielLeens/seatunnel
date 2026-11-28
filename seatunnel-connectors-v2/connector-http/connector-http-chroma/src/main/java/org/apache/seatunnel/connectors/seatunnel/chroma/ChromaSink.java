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

package org.apache.seatunnel.connectors.seatunnel.chroma;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SinkWriter.Context;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.chroma.ChromaConfig.Mode;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Creates ordered Chroma writers for a declared sink and restores previously acknowledged progress.
 * Writer state binds checkpoint replay to the same destination and projection.
 */
public final class ChromaSink implements SeaTunnelSink<SeaTunnelRow, ChromaSink.State, Void, Void> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Chroma";
    private final ChromaConfig config;
    private final CatalogTable table;
    private String job;

    ChromaSink(ChromaConfig config, CatalogTable table) {
        this.config = config;
        this.table = table;
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(table);
    }

    @Override
    public void setJobContext(JobContext context) {
        job = context.getJobId();
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, State> createWriter(Context context) throws IOException {
        String identity =
                (job == null ? "" : job + ":")
                        + UUID.randomUUID()
                        + ":"
                        + context.getIndexOfSubtask();
        return create(context, new State(config.fingerprint, identity, 0, false, null));
    }

    /**
     * Restores the completed mutation sequence and avoids re-running an acknowledged scope cleanup.
     *
     * <p>The checkpoint fence relates to this destination's exact collection UUID.
     */
    @Override
    public SinkWriter<SeaTunnelRow, Void, State> restoreWriter(Context context, List<State> states)
            throws IOException {
        if (states.isEmpty()) {
            return createWriter(context);
        }
        State state = states.get(0);
        if (states.size() != 1
                || !config.fingerprint.equals(state.fingerprint)
                || state.position < 0
                || state.run == null
                || !state.run.endsWith(":" + context.getIndexOfSubtask())
                || state.position > 0 && state.collection == null
                || state.overwriteDone && state.collection == null) {
            throw new IOException("Chroma sink: writer checkpoint does not match the sink");
        }
        return create(context, state);
    }

    @Override
    public Optional<Serializer<State>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    private ChromaWriter create(Context context, State state) throws IOException {
        if (config.mode != Mode.APPEND && context.getNumberOfParallelSubtasks() != 1) {
            throw new IOException(
                    "Chroma sink: overwrite and append_dedup require sink parallelism 1");
        }
        ChromaWriter writer = new ChromaWriter(config, table.getSeaTunnelRowType(), state);
        context.enableDeferredTerminalWriteOutcomes();
        context.registerFlushAction(writer::flush);
        return writer;
    }

    /**
     * Stores the run and next sequence after a completed checkpoint plus the overwrite fence. The
     * serialized state contains no authentication or user data.
     */
    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final String run;
        final long position;
        final boolean overwriteDone;
        final UUID collection;

        State(
                String fingerprint,
                String run,
                long position,
                boolean overwriteDone,
                UUID collection) {
            this.fingerprint = fingerprint;
            this.run = run;
            this.position = position;
            this.overwriteDone = overwriteDone;
            this.collection = collection;
        }
    }
}

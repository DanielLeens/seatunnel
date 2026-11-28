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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;
import java.util.Map;

/**
 * Bounded source for Clazar marketplace entities and analytics.
 *
 * <p>All resource rows share a fixed envelope and retain their complete response object.
 */
public final class ClazarSource
        implements SeaTunnelSource<SeaTunnelRow, ClazarSplit, ClazarEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "Clazar";

    private final ClazarConfig options;
    private final CatalogTable table;

    ClazarSource(ClazarConfig options) {
        this.options = options;
        TableSchema schema =
                TableSchema.builder()
                        .column(text("resource", false, "Clazar resource name"))
                        .column(text("record_id", true, "Provider entity UUID where available"))
                        .column(text("record_time", true, "Provider timestamp normalized to UTC"))
                        .column(
                                text(
                                        "payload",
                                        false,
                                        "Complete provider result object encoded as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "clazar", "records"),
                        schema,
                        Map.of(),
                        List.of(),
                        "Clazar records");
    }

    private static PhysicalColumn text(String name, boolean nullable, String description) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, null, nullable, null, description);
    }

    @Override
    public String getPluginName() {
        return NAME;
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
     * Creates the worker without contacting Clazar.
     *
     * <p>Each assigned split determines the specific Clazar requests to make.
     */
    @Override
    public SourceReader<SeaTunnelRow, ClazarSplit> createReader(SourceReader.Context context) {
        return new ClazarReader(context, options, table.getTableId().toTablePath().getFullName());
    }

    /**
     * Initializes the deterministic resource traversal for one reader.
     *
     * <p>The coordinator assigns ownership but does not fetch provider data.
     */
    @Override
    public SourceSplitEnumerator<ClazarSplit, ClazarEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<ClazarSplit> context) {
        return new ClazarEnumerator(context, options, null);
    }

    /**
     * Restores coordinator ownership before considering another assignment.
     *
     * <p>Once a worker accepts the split, only its checkpoint owns the traversal.
     */
    @Override
    public SourceSplitEnumerator<ClazarSplit, ClazarEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<ClazarSplit> context,
            ClazarEnumerator.Snapshot checkpoint) {
        return new ClazarEnumerator(context, options, checkpoint);
    }
}

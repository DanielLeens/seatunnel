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
 * Bounded source for CircleCI account, project and execution metadata.
 *
 * <p>All resource rows share a fixed envelope and retain their complete response object.
 */
public final class CircleCiSource
        implements SeaTunnelSource<SeaTunnelRow, CircleCiSplit, CircleCiEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "CircleCI";

    private final CircleCiConfig options;
    private final CatalogTable table;

    CircleCiSource(CircleCiConfig options) {
        this.options = options;
        TableSchema schema =
                TableSchema.builder()
                        .column(text("resource", false, "CircleCI resource name"))
                        .column(text("record_id", false, "Exact provider identifier"))
                        .column(
                                text(
                                        "parent_id",
                                        true,
                                        "Owning organization, workflow or project scope"))
                        .column(text("record_time", true, "Provider timestamp normalized to UTC"))
                        .column(
                                text(
                                        "payload",
                                        false,
                                        "Complete provider result object encoded as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "circleci", "records"),
                        schema,
                        Map.of(),
                        List.of(),
                        "CircleCI records");
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
     * Creates the worker without contacting CircleCI.
     *
     * <p>Each assigned split determines the specific CircleCI requests to make.
     */
    @Override
    public SourceReader<SeaTunnelRow, CircleCiSplit> createReader(SourceReader.Context context) {
        return new CircleCiReader(context, options, table.getTableId().toTablePath().getFullName());
    }

    /**
     * Initializes the deterministic resource traversal for one reader.
     *
     * <p>The coordinator assigns ownership but does not fetch provider data.
     */
    @Override
    public SourceSplitEnumerator<CircleCiSplit, CircleCiEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<CircleCiSplit> context) {
        return new CircleCiEnumerator(context, options, null);
    }

    /**
     * Restores coordinator ownership before considering another assignment.
     *
     * <p>Once a worker accepts the split, only its checkpoint owns the traversal.
     */
    @Override
    public SourceSplitEnumerator<CircleCiSplit, CircleCiEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<CircleCiSplit> context,
            CircleCiEnumerator.Snapshot checkpoint) {
        return new CircleCiEnumerator(context, options, checkpoint);
    }
}

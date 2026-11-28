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
package org.apache.seatunnel.connectors.seatunnel.chargify;

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
 * Bounded source for a Maxio Advanced Billing site.
 *
 * <p>All resource rows share a fixed envelope and retain their complete response object.
 */
public final class ChargifySource
        implements SeaTunnelSource<SeaTunnelRow, ChargifySplit, ChargifyEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "Chargify";

    private final ChargifyConfig options;
    private final CatalogTable table;

    ChargifySource(ChargifyConfig options) {
        this.options = options;
        TableSchema schema =
                TableSchema.builder()
                        .column(text("resource", false, "Chargify resource name"))
                        .column(text("record_id", false, "Exact provider identifier"))
                        .column(text("site", false, "Site DNS label"))
                        .column(text("region", false, "Selected provider region"))
                        .column(
                                text(
                                        "payload",
                                        false,
                                        "Complete provider result object encoded as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "chargify", "records"),
                        schema,
                        Map.of(),
                        List.of(),
                        "Chargify account records");
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
     * Creates the worker without contacting Chargify.
     *
     * <p>Each assigned split determines the specific Chargify requests to make.
     */
    @Override
    public SourceReader<SeaTunnelRow, ChargifySplit> createReader(SourceReader.Context context) {
        return new ChargifyReader(context, options, table.getTableId().toTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<ChargifySplit, ChargifyEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<ChargifySplit> context) {
        return new ChargifyEnumerator(context, options, null);
    }

    /**
     * Restores coordinator ownership before considering another assignment.
     *
     * <p>Once a worker accepts the split, only its checkpoint owns the traversal.
     */
    @Override
    public SourceSplitEnumerator<ChargifySplit, ChargifyEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<ChargifySplit> context,
            ChargifyEnumerator.Snapshot checkpoint) {
        return new ChargifyEnumerator(context, options, checkpoint);
    }
}

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
package org.apache.seatunnel.connectors.seatunnel.castoredc;

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
 * Bounded source for Castor EDC study records and definitions.
 *
 * <p>All resource rows share a fixed envelope and retain their complete response object.
 */
public final class CastorEdcSource
        implements SeaTunnelSource<SeaTunnelRow, CastorEdcSplit, CastorEdcEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "CastorEdc";

    private final CastorEdcConfig options;
    private final CatalogTable table;

    CastorEdcSource(CastorEdcConfig options) {
        this.options = options;
        TableSchema schema =
                TableSchema.builder()
                        .column(text("resource", false, "Castor EDC resource name"))
                        .column(text("record_id", false, "Exact provider identifier"))
                        .column(text("study_id", true, "Verified owning study identifier"))
                        .column(
                                text(
                                        "record_time",
                                        true,
                                        "Normalized provider timestamp when present"))
                        .column(
                                text(
                                        "payload",
                                        false,
                                        "Complete provider result object encoded as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "castoredc", "records"),
                        schema,
                        Map.of(),
                        List.of(),
                        "Castor EDC account records");
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
     * Creates the worker without contacting CastorEdc.
     *
     * <p>Each assigned split determines the specific Castor EDC requests to make.
     */
    @Override
    public SourceReader<SeaTunnelRow, CastorEdcSplit> createReader(SourceReader.Context context) {
        return new CastorEdcReader(
                context, options, table.getTableId().toTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<CastorEdcSplit, CastorEdcEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<CastorEdcSplit> context) {
        return new CastorEdcEnumerator(context, options, null);
    }

    /**
     * Restores coordinator ownership before considering another assignment.
     *
     * <p>Once a worker accepts the split, only its checkpoint owns the traversal.
     */
    @Override
    public SourceSplitEnumerator<CastorEdcSplit, CastorEdcEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<CastorEdcSplit> context,
            CastorEdcEnumerator.Snapshot checkpoint) {
        return new CastorEdcEnumerator(context, options, checkpoint);
    }
}

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
package org.apache.seatunnel.connectors.seatunnel.cimis;

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
 * Bounded source for a CIMIS weather observations and reference data.
 *
 * <p>All resource rows share a fixed envelope and retain their complete response object.
 */
public final class CimisSource
        implements SeaTunnelSource<SeaTunnelRow, CimisSplit, CimisEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "Cimis";

    private final CimisConfig options;
    private final CatalogTable table;

    CimisSource(CimisConfig options) {
        this.options = options;
        TableSchema schema =
                TableSchema.builder()
                        .column(text("resource", false, "Cimis resource name"))
                        .column(text("record_id", false, "Exact provider identifier"))
                        .column(text("target", true, "Requested weather location"))
                        .column(text("date", true, "Original provider weather date"))
                        .column(text("hour", true, "Original provider hour when present"))
                        .column(text("provider_name", true, "Observation provider name"))
                        .column(text("provider_type", true, "Station or spatial provider type"))
                        .column(text("provider_owner", true, "Observation provider owner"))
                        .column(
                                text(
                                        "payload",
                                        false,
                                        "Complete provider result object encoded as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "cimis", "records"),
                        schema,
                        Map.of(),
                        List.of(),
                        "Cimis account records");
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
     * Creates the worker without contacting Cimis.
     *
     * <p>Each assigned split determines the specific Cimis requests to make.
     */
    @Override
    public SourceReader<SeaTunnelRow, CimisSplit> createReader(SourceReader.Context context) {
        return new CimisReader(context, options, table.getTableId().toTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<CimisSplit, CimisEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<CimisSplit> context) {
        return new CimisEnumerator(context, options, null);
    }

    /**
     * Restores coordinator ownership before considering another assignment.
     *
     * <p>Once a worker accepts the split, only its checkpoint owns the traversal.
     */
    @Override
    public SourceSplitEnumerator<CimisSplit, CimisEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<CimisSplit> context,
            CimisEnumerator.Snapshot checkpoint) {
        return new CimisEnumerator(context, options, checkpoint);
    }
}

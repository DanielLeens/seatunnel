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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

/**
 * Tests the JDBC sink's per-table config resolution used by runtime newly added tables.
 *
 * <p>The covered regression risk is creating a runtime writer for the wrong target table when
 * database overrides or table prefixes/suffixes are configured.
 */
public class JdbcSinkFactoryTest {

    /** Verifies that runtime tables reuse the same naming rules as startup tables. */
    @Test
    public void testResolveSinkTableUsesDynamicNamingRules() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(JdbcSinkOptions.DATABASE.key(), "target_db");
                                put(JdbcSinkOptions.TABLE_PREFIX.key(), "ods_");
                                put(JdbcSinkOptions.TABLE_SUFFIX.key(), "_sync");
                            }
                        });
        CatalogTable sourceTable =
                CatalogTable.of(
                        TableIdentifier.of("mysql", "source_db", null, "orders"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.INT_TYPE)
                                                .build())
                                .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        null);

        JdbcSinkFactory.ResolvedSinkTable resolvedSinkTable =
                JdbcSinkFactory.resolveSinkTable(config, sourceTable);

        Assertions.assertEquals(
                TablePath.of("target_db", "ods_orders_sync"),
                resolvedSinkTable.getCatalogTable().getTablePath());
        Assertions.assertEquals(
                "ods_orders_sync", resolvedSinkTable.getOptions().get(JdbcSinkOptions.TABLE));
        Assertions.assertEquals(
                "target_db", resolvedSinkTable.getOptions().get(JdbcSinkOptions.DATABASE));
        Assertions.assertEquals(
                Collections.singletonList("id"),
                resolvedSinkTable
                        .getCatalogTable()
                        .getTableSchema()
                        .getPrimaryKey()
                        .getColumnNames());
    }
}

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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.auto.service.AutoService;

/**
 * Registers the Chroma vector sink and maps SeaTunnel catalog fields to document inputs. Connecting
 * and mutation are deferred to the sink writer.
 */
@AutoService(Factory.class)
public final class ChromaSinkFactory
        implements TableSinkFactory<SeaTunnelRow, ChromaSink.State, Void, Void> {
    @Override
    public String factoryIdentifier() {
        return ChromaSink.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        ChromaConfig.ENDPOINT, ChromaConfig.COLLECTION, ChromaConfig.EMBEDDING_MODE)
                .optional(
                        ChromaConfig.TOKEN,
                        ChromaConfig.USERNAME,
                        ChromaConfig.PASSWORD,
                        ChromaConfig.TENANT,
                        ChromaConfig.DATABASE,
                        ChromaConfig.BATCH_SIZE,
                        ChromaConfig.TLS_TRUST_CERTIFICATE,
                        ChromaConfig.MODE,
                        ChromaConfig.NAMESPACE,
                        ChromaConfig.STREAM,
                        ChromaConfig.NAMESPACE_FIELD,
                        ChromaConfig.STREAM_FIELD,
                        ChromaConfig.PRIMARY_KEYS,
                        ChromaConfig.TEXT_FIELDS,
                        ChromaConfig.METADATA_FIELDS,
                        ChromaConfig.FIELD_NAME_MAPPINGS,
                        ChromaConfig.OMIT_RAW_TEXT,
                        ChromaConfig.SPLITTER,
                        ChromaConfig.SEPARATORS,
                        ChromaConfig.KEEP_SEPARATOR,
                        ChromaConfig.MARKDOWN_LEVEL,
                        ChromaConfig.CODE_LANGUAGE,
                        ChromaConfig.CHUNK_SIZE,
                        ChromaConfig.CHUNK_OVERLAP,
                        ChromaConfig.TOKENIZER_DATA_FILE,
                        ChromaConfig.EMBEDDING_TOKEN,
                        ChromaConfig.EMBEDDING_URL,
                        ChromaConfig.EMBEDDING_MODEL,
                        ChromaConfig.EMBEDDING_DEPLOYMENT,
                        ChromaConfig.EMBEDDING_FIELD,
                        ChromaConfig.EMBEDDING_DIMENSIONS,
                        ChromaConfig.MAX_ROW_BYTES,
                        ChromaConfig.MAX_BUFFER_BYTES,
                        ChromaConfig.MAX_BODY_BYTES,
                        ChromaConfig.MAX_CHUNKS_PER_ROW,
                        ChromaConfig.CONNECT_TIMEOUT,
                        ChromaConfig.READ_TIMEOUT,
                        ChromaConfig.REQUEST_TIMEOUT,
                        ChromaConfig.FLUSH_TIMEOUT,
                        ChromaConfig.MAX_ATTEMPTS)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, ChromaSink.State, Void, Void> createSink(
            TableSinkFactoryContext context) {
        CatalogTable input = context.getCatalogTable();
        ChromaConfig options = new ChromaConfig(context.getOptions(), input.getTablePath());
        return () -> new ChromaSink(options, input);
    }
}

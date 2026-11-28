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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.chroma.ChromaConfig.Mode;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Buffers bounded input in order and acknowledges the Chroma document writes at checkpoint
 * boundaries. External writes are visible before checkpoints and cannot be rolled back.
 */
final class ChromaWriter implements SinkWriter<SeaTunnelRow, Void, ChromaSink.State> {
    private final ChromaConfig config;
    private final String run;
    private final ChromaHttp http;
    private final ChromaApi data;
    private final ChromaEmbeddings embeddings;
    private final ChromaDocument documents;
    private final JsonSerializationSchema serializer;
    private final List<ChromaDocument.Record> pending = new ArrayList<>();
    private long position;
    private long pendingBytes;
    private int pendingChunks;
    private boolean overwriteDone;
    private boolean closed;

    ChromaWriter(ChromaConfig config, SeaTunnelRowType row, ChromaSink.State state) {
        this.config = config;
        this.run = state.run;
        this.position = state.position;
        this.overwriteDone = state.overwriteDone;
        this.serializer = new JsonSerializationSchema(row);
        this.http = new ChromaHttp(config);
        this.data = new ChromaApi(config, http, state.collection);
        this.embeddings = new ChromaEmbeddings(config, http);
        this.documents = new ChromaDocument(config, new ChromaSplitter(config, http));
    }

    /**
     * Captures the exact routing, text and metadata before adding a row to the bounded buffer.
     * Deletes and before-images affect only the deduplicated write mode.
     */
    @Override
    public synchronized void write(SeaTunnelRow row) throws IOException {
        ensureActive();
        boolean deletion =
                row.getRowKind() == RowKind.DELETE || row.getRowKind() == RowKind.UPDATE_BEFORE;
        try {
            byte[] serialized;
            try {
                serialized = serializer.serialize(row);
            } catch (RuntimeException ignored) {
                throw new IOException("Chroma sink: cannot serialize the input row");
            }
            if (serialized.length > config.maxRow) {
                throw new IOException("Chroma sink: input exceeds max_row_bytes");
            }
            if (position == Long.MAX_VALUE) {
                throw new IOException("Chroma sink: input sequence exhausted");
            }
            ObjectNode input = ChromaJson.readObject(serialized);
            JsonNode cdc = input.path(ChromaDocument.CDC_DELETED);
            deletion |=
                    cdc.isTextual() && !cdc.textValue().isEmpty()
                            || cdc.isBoolean() && cdc.booleanValue()
                            || cdc.isNumber() && cdc.decimalValue().signum() != 0;
            if (deletion && config.mode != Mode.APPEND_DEDUP) {
                return;
            }
            ChromaDocument.Record record = documents.prepare(input, deletion, position);
            if (!pending.isEmpty()
                    && (pendingBytes + record.bytes > config.maxBuffer
                            || pendingChunks + record.chunks.size() > config.batchSize)) {
                flush();
            }
            pending.add(record);
            pendingBytes += record.bytes;
            pendingChunks += record.chunks.size();
            position++;
            if (pending.size() >= config.batchSize || pendingChunks >= config.batchSize) {
                flush();
            }
        } catch (IllegalArgumentException | IOException error) {
            abortPrepare();
            if (error instanceof IOException) {
                throw (IOException) error;
            }
            throw new IOException("Chroma sink: invalid row routing, selector, or value");
        }
    }

    /**
     * Validates all outgoing payloads before cleanup. Repeated dedup keys are written in input
     * order. The connector never deletes every document in a collection or resends an unknown
     * mutation here.
     */
    synchronized void flush() throws IOException {
        ensureActive();
        boolean overwrite = config.mode == Mode.OVERWRITE && !overwriteDone;
        if (pending.isEmpty() && !overwrite) {
            return;
        }
        http.beginFlush();
        try {
            List<ChromaDocument.Chunk> chunks = new ArrayList<>(pendingChunks);
            for (ChromaDocument.Record record : pending) {
                chunks.addAll(record.chunks);
            }
            List<ArrayNode> vectors = embeddings.embed(chunks);
            List<List<byte[]>> byRecord = new ArrayList<>(pending.size());
            List<ObjectNode> all = new ArrayList<>(pendingChunks);
            int offset = 0;
            for (ChromaDocument.Record record : pending) {
                List<ObjectNode> collection =
                        config.mode == Mode.APPEND_DEDUP
                                ? new ArrayList<>(record.chunks.size())
                                : all;
                for (int index = 0; index < record.chunks.size(); index++) {
                    collection.add(documents.document(record, run, index, vectors.get(offset++)));
                }
                if (config.mode == Mode.APPEND_DEDUP) {
                    byRecord.add(data.batches(collection));
                }
            }
            List<byte[]> append = data.batches(all);
            data.open();
            if (overwrite) {
                data.delete(documents.fixedScope().key, null);
                overwriteDone = true;
            }
            if (config.mode == Mode.APPEND_DEDUP) {
                for (int index = 0; index < pending.size(); index++) {
                    ChromaDocument.Record record = pending.get(index);
                    data.delete(record.scope.key, record.id);
                    data.upsert(byRecord.get(index));
                }
            } else {
                data.upsert(append);
            }
            clear();
        } catch (IOException error) {
            abortPrepare();
            throw error;
        } finally {
            http.endFlush();
        }
    }

    @Override
    public synchronized Optional<Void> prepareCommit() throws IOException {
        flush();
        return Optional.empty();
    }

    /**
     * Stores completed progress and the overwrite fence without rows, vectors or credentials. A
     * barrier can only store progress after every buffered mutation has been acknowledged.
     */
    @Override
    public synchronized List<ChromaSink.State> snapshotState(long checkpointId) throws IOException {
        flush();
        return List.of(
                new ChromaSink.State(
                        config.fingerprint, run, position, overwriteDone, data.collection()));
    }

    @Override
    public void abortPrepare() {
        close();
    }

    /**
     * Discards pending rows on abort or close and releases the socket independently of writer
     * state. A successful job uses its final prepare barrier to flush before closing.
     */
    @Override
    public void close() {
        http.close();
        synchronized (this) {
            closed = true;
            clear();
        }
    }

    private void clear() {
        pending.clear();
        pendingBytes = 0;
        pendingChunks = 0;
    }

    private void ensureActive() throws IOException {
        if (closed) {
            throw new IOException("Chroma sink: writer is closed");
        }
    }
}

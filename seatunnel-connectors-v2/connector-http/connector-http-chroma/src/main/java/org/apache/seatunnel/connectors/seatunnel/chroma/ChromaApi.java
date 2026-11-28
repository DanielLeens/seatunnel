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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Uses the collection-scoped Chroma v2 REST API for writes and filtered deletes.
 *
 * <p>A restored writer keeps the acknowledged collection UUID, even when the name is reused.
 */
final class ChromaApi {
    static final String SCOPE = "_st_scope";
    static final String RECORD_ID = "_ab_record_id";
    private final ChromaConfig config;
    private final ChromaHttp http;
    private UUID collection;

    ChromaApi(ChromaConfig config, ChromaHttp http, UUID collection) {
        this.config = config;
        this.http = http;
        this.collection = collection;
    }

    /**
     * Resolves the collection before data mutations and validates its established dimension.
     *
     * <p>The returned UUID identifies the collection throughout the writer lifecycle.
     */
    void open() throws IOException {
        if (collection != null) {
            return;
        }
        ObjectNode request = ChromaJson.object();
        request.put("name", config.collection);
        request.put("get_or_create", true);
        JsonNode response =
                http.mutation(
                        config.collectionsUrl,
                        ChromaJson.bytes(request),
                        "get or create collection");
        if (response == null
                || !response.isObject()
                || !response.path("id").isTextual()
                || !config.collection.equals(response.path("name").textValue())) {
            throw ChromaHttp.failure(
                    "get or create collection", "invalid collection acknowledgement");
        }
        JsonNode dimension = response.path("dimension");
        if (!dimension.isMissingNode()
                && !dimension.isNull()
                && (!dimension.isIntegralNumber()
                        || !dimension.canConvertToInt()
                        || dimension.intValue() != config.dimensions)) {
            throw ChromaHttp.failure(
                    "get or create collection",
                    "collection and configured embedding dimensions differ");
        }
        String id = response.path("id").textValue();
        try {
            UUID value = UUID.fromString(id);
            if (value.toString().equalsIgnoreCase(id)) {
                collection = value;
                return;
            }
        } catch (IllegalArgumentException ignored) {
            // Omit provider-controlled UUID text from diagnostics.
        }
        throw ChromaHttp.failure("get or create collection", "invalid collection UUID");
    }

    UUID collection() {
        return collection;
    }

    /**
     * Builds and bounds all outgoing batches before an overwrite or replacement can delete data.
     *
     * <p>The aligned arrays carry each embedding, metadata value and document at its ID index.
     */
    List<byte[]> batches(List<ObjectNode> records) throws IOException {
        List<byte[]> result = new ArrayList<>();
        Batch batch = new Batch();
        for (ObjectNode record : records) {
            long nextBytes = Batch.memberBytes(record);
            if (batch.size() > 0
                    && (batch.size() == config.batchSize
                            || batch.bytes + nextBytes > config.maxBody)) {
                result.add(batch.finish(config.maxBody));
                batch = new Batch();
            }
            if (batch.bytes + nextBytes > config.maxBody) {
                throw ChromaHttp.failure("upsert", "one document exceeds max_body_bytes");
            }
            batch.add(record, nextBytes);
        }
        if (batch.size() > 0) {
            result.add(batch.finish(config.maxBody));
        }
        return result;
    }

    /**
     * Removes a single owned stream or key. The equality conjunction cannot match unowned rows.
     *
     * <p>Older servers return JSON null, while current servers return a count of removed records.
     */
    void delete(String scope, String record) throws IOException {
        ObjectNode request = ChromaJson.object();
        if (record == null) {
            request.set("where", equality(SCOPE, scope));
        } else {
            ArrayNode terms = ChromaJson.array();
            terms.add(equality(SCOPE, scope));
            terms.add(equality(RECORD_ID, record));
            request.putObject("where").set("$and", terms);
        }
        JsonNode ack = http.mutation(operation("delete"), ChromaJson.bytes(request), "delete");
        if (ack != null && !ack.isNull()) {
            JsonNode deleted = ack.path("deleted");
            if (!ack.isObject()
                    || !deleted.isIntegralNumber()
                    || !deleted.canConvertToLong()
                    || deleted.longValue() < 0L
                    || ack.has("error")
                    || ack.has("message")) {
                throw ChromaHttp.failure("delete", "invalid mutation acknowledgement");
            }
        }
    }

    /**
     * Sends new or replayed chunk IDs once. The stable IDs permit checkpoint replay with upserts.
     *
     * <p>Transport failures are surfaced instead of retrying an unknown mutation outcome.
     */
    void upsert(List<byte[]> batches) throws IOException {
        for (byte[] request : batches) {
            JsonNode ack = http.mutation(operation("upsert"), request, "upsert");
            if (ack != null && !ack.isNull() && !(ack.isObject() && ack.isEmpty())) {
                throw ChromaHttp.failure("upsert", "invalid mutation acknowledgement");
            }
        }
    }

    private URI operation(String name) {
        return URI.create(config.collectionsUrl.toASCIIString() + "/" + collection + "/" + name);
    }

    private static ObjectNode equality(String name, String value) {
        ObjectNode filter = ChromaJson.object();
        filter.putObject(name).put("$eq", value);
        return filter;
    }

    /**
     * Keeps alignment and accounts for JSON framing before serializing a batch.
     *
     * <p>Each member contributes its independently encoded size and at most four commas.
     */
    private static final class Batch {
        private final ObjectNode body = ChromaJson.object();
        private final ArrayNode ids = body.putArray("ids");
        private final ArrayNode documents = body.putArray("documents");
        private final ArrayNode embeddings = body.putArray("embeddings");
        private final ArrayNode metadatas = body.putArray("metadatas");
        private long bytes = ChromaJson.bytes(body).length;

        int size() {
            return ids.size();
        }

        static long memberBytes(ObjectNode record) {
            return 4L
                    + ChromaJson.bytes(record.path("id")).length
                    + ChromaJson.bytes(record.path("document")).length
                    + ChromaJson.bytes(record.path("embedding")).length
                    + ChromaJson.bytes(record.path("metadata")).length;
        }

        void add(ObjectNode record, long encodedBytes) {
            ids.add(record.path("id"));
            documents.add(record.path("document"));
            embeddings.add(record.path("embedding"));
            metadatas.add(record.path("metadata"));
            bytes += encodedBytes;
        }

        byte[] finish(int maxBytes) throws IOException {
            byte[] encoded = ChromaJson.bytes(body);
            if (encoded.length > maxBytes) {
                throw ChromaHttp.failure("upsert", "request body exceeds max_body_bytes");
            }
            return encoded;
        }
    }
}

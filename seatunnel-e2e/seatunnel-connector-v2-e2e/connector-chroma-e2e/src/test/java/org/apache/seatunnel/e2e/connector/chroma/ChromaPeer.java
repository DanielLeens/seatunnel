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

package org.apache.seatunnel.e2e.connector.chroma;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Implements an independent HTTPS Chroma datastore and synthetic embedding providers. Each case
 * records API requests and persists the actual mutated documents outside the engine.
 */
public final class ChromaPeer {
    static final String DATABASE = "chroma-db.fixture.invalid";
    static final String OPENAI = "api.openai.com";
    static final String AZURE = "test-deployment.openai.azure.com";
    static final String COHERE = "api.cohere.com";
    static final String COMPATIBLE = "compatible.chroma-fixture.invalid";
    static final String PASSWORD = "chroma-fixture-password";
    static final String DB_TOKEN = "fixture-db-token";
    static final String EMBED_TOKEN = "fixture-embed-token";
    static final String PRIVATE = "synthetic-private-provider-data";
    static final int TRUSTED = 18443;
    static final int UNTRUSTED = 18444;
    static final String COLLECTIONS =
            "/api/v2/tenants/default_tenant/databases/default_database/collections";
    static final String ID = "92b37e23-da7d-4dec-9f2c-742f060ed4a4";
    static final String WRONG_HOST = "chroma-wrong-san.fixture.invalid";
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path base;
    private final Map<String, Case> scenarios = new HashMap<>();

    private ChromaPeer(Path base) {
        this.base = base;
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        ChromaPeer peer = new ChromaPeer(root);
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpsServer trusted = peer.listen("trusted", TRUSTED, executor);
        HttpsServer untrusted = peer.listen("untrusted", UNTRUSTED, executor);
        try {
            Files.createFile(root.resolve("ready"));
            while (!Files.exists(root.resolve("stop"))) {
                Thread.sleep(50);
            }
        } finally {
            trusted.stop(0);
            untrusted.stop(0);
            executor.shutdownNow();
        }
    }

    private HttpsServer listen(String identity, int port, ExecutorService executor)
            throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        char[] password = PASSWORD.toCharArray();
        try (InputStream input = Files.newInputStream(base.resolve(identity + ".p12"))) {
            store.load(input, password);
        }
        KeyManagerFactory keys =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, password);
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(keys.getKeyManagers(), null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", port), 32);
        server.setHttpsConfigurator(new HttpsConfigurator(ssl));
        server.createContext("/", this::respond);
        server.setExecutor(executor);
        server.start();
        return server;
    }

    private synchronized void respond(HttpExchange exchange) throws IOException {
        Case test = null;
        try {
            String name = Files.readString(base.resolve("active")).trim();
            test = scenarios.get(name);
            if (test == null) {
                JsonNode spec = JSON.readTree(Files.readString(base.resolve(name + ".input")));
                test = new Case(name, spec.path("fault").asText(), spec.path("dimensions").asInt());
                scenarios.put(name, test);
            }
            require("POST".equals(exchange.getRequestMethod()), "Expected POST");
            require(
                    "application/json"
                            .equals(exchange.getRequestHeaders().getFirst("Content-Type")),
                    "Expected JSON");
            String host = exchange.getRequestHeaders().getFirst("Host").split(":", 2)[0];
            String path = exchange.getRequestURI().getPath();
            JsonNode body = JSON.readTree(exchange.getRequestBody().readNBytes(8 * 1024 * 1024));
            require(body != null && body.isObject(), "Expected a JSON object");
            if (!DATABASE.equals(host)) {
                embed(exchange, test, host, path, body);
                return;
            }
            validateDatabaseAuth(exchange, test);
            if (path.equals(COLLECTIONS)) {
                report(test, host, path, "open", body);
                require(
                        test.name.equals(body.path("name").asText())
                                && body.path("get_or_create").asBoolean(),
                        "Invalid collection creation");
                if ("redirect".equals(test.fault)) {
                    exchange.getResponseHeaders()
                            .set(
                                    "Location",
                                    "https://"
                                            + COMPATIBLE
                                            + ":"
                                            + TRUSTED
                                            + "/private/v1/embeddings");
                    send(exchange, 307, "{}");
                } else if ("slow".equals(test.fault)) {
                    slow(exchange, test);
                } else {
                    send(
                            exchange,
                            200,
                            JSON.createObjectNode()
                                    .put("id", ID)
                                    .put("name", test.name)
                                    .put("dimension", test.dimensions));
                }
            } else if (path.equals(COLLECTIONS + "/" + ID + "/upsert")) {
                report(test, host, path, "upsert", body);
                int call = test.calls.merge("upsert", 1, Integer::sum);
                if ("throttle".equals(test.fault) && call == 1) {
                    exchange.getResponseHeaders().set("Retry-After", "0");
                    send(exchange, 429, "{}");
                    return;
                }
                if ("false-ack".equals(test.fault)) {
                    send(exchange, 200, "false");
                    return;
                }
                if ("malformed-ack".equals(test.fault)) {
                    send(exchange, 200, "{");
                    return;
                }
                if ("large-ack".equals(test.fault)) {
                    send(exchange, 200, " ".repeat(8192) + "{}");
                    return;
                }
                accept(test, body);
                if ("ambiguous".equals(test.fault)) {
                    send(exchange, 503, "{\"message\":\"" + PRIVATE + "\"}");
                } else if ("no-content".equals(test.fault)) {
                    exchange.sendResponseHeaders(204, -1);
                } else {
                    send(exchange, 200, "{}");
                }
            } else if (path.equals(COLLECTIONS + "/" + ID + "/delete")) {
                report(test, host, path, "delete", body);
                JsonNode filter = body.path("where");
                require(filter.size() == 1 && body.size() == 1, "Expected a scoped delete");
                String scope;
                String key = null;
                if (filter.has("$and")) {
                    JsonNode conditions = filter.path("$and");
                    require(conditions.size() == 2, "Expected exact scope and record");
                    scope = equality(conditions.path(0), "_st_scope");
                    key = equality(conditions.path(1), "_ab_record_id");
                } else {
                    scope = equality(filter, "_st_scope");
                }
                String record = key;
                int count = test.docs.size();
                test.docs
                        .values()
                        .removeIf(
                                row ->
                                        scope.equals(
                                                        row.path("metadata")
                                                                .path("_st_scope")
                                                                .textValue())
                                                && (record == null
                                                        || record.equals(
                                                                row.path("metadata")
                                                                        .path("_ab_record_id")
                                                                        .textValue())));
                persist(test);
                send(
                        exchange,
                        200,
                        JSON.createObjectNode().put("deleted", count - test.docs.size()));
            } else {
                throw new IllegalArgumentException("Unexpected Chroma route");
            }
        } catch (IllegalArgumentException failure) {
            if (test != null) {
                Files.writeString(base.resolve(test.name + ".invalid"), failure.getMessage());
            }
            send(exchange, 400, "{}");
        } finally {
            exchange.close();
        }
    }

    private static String equality(JsonNode filter, String key) {
        JsonNode predicate = filter.path(key);
        require(
                filter.size() == 1 && predicate.size() == 1 && predicate.path("$eq").isTextual(),
                "Expected a one-field equality predicate");
        return predicate.path("$eq").textValue();
    }

    private static void validateDatabaseAuth(HttpExchange exchange, Case test) {
        String token = exchange.getRequestHeaders().getFirst("x-chroma-token");
        String auth = exchange.getRequestHeaders().getFirst("Authorization");
        if ("basic".equals(test.fault)) {
            require(
                    token == null && "Basic Zml4dHVyZTpmaXh0dXJlLXBhc3M=".equals(auth),
                    "Wrong Chroma Basic header");
        } else {
            require(DB_TOKEN.equals(token) && auth == null, "Wrong Chroma Cloud header");
        }
        require(
                exchange.getRequestHeaders().getFirst("api-key") == null,
                "Leaked embedding credential to database");
    }

    private void accept(Case test, JsonNode body) throws IOException {
        JsonNode ids = body.path("ids");
        JsonNode docs = body.path("documents");
        JsonNode metadatas = body.path("metadatas");
        JsonNode vectors = body.path("embeddings");
        int size = ids.size();
        require(
                ids.isArray()
                        && docs.isArray()
                        && metadatas.isArray()
                        && vectors.isArray()
                        && size > 0
                        && size <= 128
                        && docs.size() == size
                        && metadatas.size() == size
                        && vectors.size() == size,
                "Upsert arrays are misaligned or exceed batch limit");
        for (int i = 0; i < size; i++) {
            String id = ids.get(i).textValue();
            JsonNode vector = vectors.get(i);
            require(
                    id != null
                            && !id.isEmpty()
                            && docs.get(i).isTextual()
                            && metadatas.get(i).isObject()
                            && vector.isArray()
                            && vector.size() == test.dimensions,
                    "Invalid persisted document");
            for (JsonNode value : vector) {
                require(
                        value.isNumber() && Double.isFinite(value.doubleValue()),
                        "Invalid vector value");
            }
            ObjectNode document = JSON.createObjectNode().put("id", id);
            document.set("document", docs.get(i));
            document.set("metadata", metadatas.get(i));
            document.set("embedding", vector);
            test.docs.put(id, document);
        }
        persist(test);
    }

    private void embed(HttpExchange exchange, Case active, String host, String path, JsonNode body)
            throws IOException {
        boolean azure = AZURE.equals(host);
        boolean cohere = COHERE.equals(host);
        boolean compatible = COMPATIBLE.equals(host);
        require(
                azure || cohere || compatible || OPENAI.equals(host),
                "Unexpected provider hostname");
        String expected =
                azure
                        ? "/openai/v1/embeddings"
                        : cohere
                                ? "/v2/embed"
                                : compatible ? "/private/v1/embeddings" : "/v1/embeddings";
        require(
                expected.equals(path)
                        && exchange.getRequestHeaders().getFirst("x-chroma-token") == null,
                "Wrong embedding path or leaked Chroma token");
        String bearer = exchange.getRequestHeaders().getFirst("Authorization");
        String key = exchange.getRequestHeaders().getFirst("api-key");
        boolean anonymous = compatible && "anonymous".equals(active.fault);
        require(
                azure
                        ? EMBED_TOKEN.equals(key) && bearer == null
                        : key == null
                                && (anonymous
                                        ? bearer == null
                                        : ("Bearer " + EMBED_TOKEN).equals(bearer)),
                "Wrong embedding credentials");
        String model =
                azure
                        ? "fixture-deployment"
                        : cohere
                                ? "embed-english-v3.0"
                                : compatible
                                        ? "fixture-openai-compatible"
                                        : "text-embedding-ada-002";
        require(model.equals(body.path("model").asText()), "Wrong embedding model");
        JsonNode texts = body.path(cohere ? "texts" : "input");
        require(texts.isArray() && texts.size() > 0 && texts.size() <= 16, "Wrong text batch");
        if (cohere) {
            require(
                    "search_document".equals(body.path("input_type").asText())
                            && "NONE".equals(body.path("truncate").asText())
                            && "float".equals(body.path("embedding_types").path(0).asText()),
                    "Wrong Cohere request");
        }
        for (JsonNode text : texts) {
            require(text.isTextual() && !text.asText().isBlank(), "Empty text");
        }
        report(active, host, path, "embedding", body);
        int call = active.calls.merge("embedding", 1, Integer::sum);
        if ("embed-throttle".equals(active.fault) && call == 1) {
            exchange.getResponseHeaders().set("Retry-After", "0");
            send(exchange, 429, error("TOO_MANY_REQUESTS"));
            return;
        }
        if ("embed-overflow".equals(active.fault)) {
            send(exchange, 200, "{\"data\":[{\"index\":0,\"embedding\":[1e400,0,1]}]}");
            return;
        }
        ObjectNode result = JSON.createObjectNode();
        ArrayNode vectors =
                cohere ? result.putObject("embeddings").putArray("float") : result.putArray("data");
        for (int i = texts.size() - 1; i >= 0; i--) {
            if (cohere) {
                vectors.add(embedding(texts.get(texts.size() - 1 - i).asText(), active.dimensions));
            } else {
                vectors.addObject()
                        .put("index", "embed-bad-index".equals(active.fault) ? 0 : i)
                        .set("embedding", embedding(texts.get(i).asText(), active.dimensions));
            }
        }
        send(exchange, 200, result);
    }

    private static ArrayNode embedding(String text, int dimensions) {
        ArrayNode vector = JSON.createArrayNode();
        vector.add(text.endsWith("alpha") ? 0.5 : -0.5);
        for (int i = 1; i < dimensions; i++) {
            vector.add(i == dimensions - 1 ? 0.25 : 0.0);
        }
        return vector;
    }

    private void report(Case active, String host, String path, String type, JsonNode body)
            throws IOException {
        ObjectNode record =
                JSON.createObjectNode().put("host", host).put("path", path).put("operation", type);
        record.set("body", body);
        Files.writeString(
                base.resolve(active.name + ".requests"),
                record + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    private void persist(Case active) throws IOException {
        Path file = base.resolve(active.name + ".documents");
        Path temporary = base.resolve(active.name + ".tmp");
        Files.writeString(temporary, JSON.writeValueAsString(active.docs.values()));
        Files.move(
                temporary,
                file,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    private void slow(HttpExchange exchange, Case test) throws IOException {
        exchange.sendResponseHeaders(200, 0);
        byte[] block = " ".repeat(4096).getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = exchange.getResponseBody()) {
            for (int index = 0; index < 4096; index++) {
                output.write(block);
                output.flush();
                Thread.sleep(50);
            }
        } catch (IOException interruptedConnection) {
            Files.writeString(base.resolve(test.name + ".disconnect"), "closed");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void send(HttpExchange exchange, int code, ObjectNode body) throws IOException {
        send(exchange, code, body.toString());
    }

    private static void send(HttpExchange exchange, int code, String body) throws IOException {
        byte[] data = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, data.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(data);
        }
    }

    private static ObjectNode error(String type) {
        return JSON.createObjectNode().put("error", type);
    }

    private static void require(boolean expected, String message) {
        if (!expected) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Keeps accepted documents and request counts independent of engine storage.
     *
     * <p>Each scenario explicitly specifies protocol behavior and the expected vector dimension.
     */
    private static final class Case {
        final String name;
        final String fault;
        final int dimensions;
        final Map<String, Integer> calls = new HashMap<>();
        final Map<String, ObjectNode> docs = new LinkedHashMap<>();

        Case(String name, String fault, int dimensions) {
            this.name = name;
            this.fault = fault;
            this.dimensions = dimensions;
        }
    }
}

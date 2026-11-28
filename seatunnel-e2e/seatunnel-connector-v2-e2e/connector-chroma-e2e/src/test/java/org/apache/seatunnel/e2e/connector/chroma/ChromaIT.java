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

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import javax.net.ssl.HttpsURLConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Executes the Chroma sink through real Zeta jobs and reads results from Chroma v2 and an
 * independent HTTPS peer. Embedding routes, database acknowledgements, scoped mutations and
 * restores are observable.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChromaIT extends TestSuiteBase {
    private static final String CONF = "/chroma.conf";
    private static final String ROOT = "/tmp/chroma-https-fixture";
    private static final String RANKS =
            "https://openaipublic.blob.core.windows.net/encodings/r50k_base.tiktoken";
    private static final String RANK_SHA =
            "306cd27f03c1a714eca7108e03d66b7dc042abe8c258b44c199a7ed9838dd930";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> HOSTS =
            List.of(
                    ChromaPeer.DATABASE,
                    ChromaPeer.OPENAI,
                    ChromaPeer.AZURE,
                    ChromaPeer.COHERE,
                    ChromaPeer.COMPATIBLE);
    private static final String DB = url(ChromaPeer.DATABASE, ChromaPeer.TRUSTED);

    private static final String REAL = "http://chroma-v2-fixture:8000";
    private static final List<Double> VECTOR = List.of(1.0, 0.25, -0.5);

    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private GenericContainer<?> database;
    private Future<Container.ExecResult> fixture;
    private String active;

    @BeforeAll
    void start() throws Exception {
        downloadRanks();
        Files.writeString(directory.resolve("untrusted-ranks"), "invalid local rank data");
        KeyStore trusted = identity("trusted");
        identity("untrusted");
        Files.write(directory.resolve("trust.der"), trusted.getCertificate("fixture").getEncoded());
        database =
                new GenericContainer<>(DockerImageName.parse("chromadb/chroma:0.6.3"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("chroma-v2-fixture")
                        .withExposedPorts(8000)
                        .withEnv("ANONYMIZED_TELEMETRY", "false")
                        .waitingFor(
                                Wait.forHttp("/api/v2/heartbeat")
                                        .forPort(8000)
                                        .withStartupTimeout(Duration.ofSeconds(90)));
        database.start();
        engine = new Engine(directory);
        engine.startUp();
        fixture = tasks.submit(engine::runPeer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void verifyPeerContract() throws Exception {
        if (active != null) {
            assertFalse(
                    engine.exists(active + ".invalid"),
                    () -> {
                        try {
                            return engine.contents(active + ".invalid");
                        } catch (Exception failure) {
                            return "Could not read provider assertion";
                        }
                    });
        }
    }

    @AfterAll
    void stop() throws Exception {
        try {
            if (engine != null) {
                try {
                    if (fixture != null) {
                        engine.touch("stop");
                        success(fixture.get(15, TimeUnit.SECONDS));
                    }
                } finally {
                    engine.tearDown();
                }
            }
        } finally {
            if (database != null) {
                database.stop();
            }
            tasks.shutdownNow();
        }
    }

    @Test
    void persistsExactVectorTextAndScalarizedMetadataOnTheRealServer() throws Exception {
        Settings options = real("real-exact");
        options.rows.add(row("INSERT", 7, "héllo 世界", "ns", "feed", VECTOR));
        options.aliases.put("body", "paragraph").put("context.team", "business");
        success(run(options));

        List<JsonNode> result = realStored(options);
        assertEquals(1, result.size());
        JsonNode document = result.get(0);
        assertEquals("paragraph: héllo 世界", document.path("document").asText());
        assertVector(document, 1.0, 0.25, -0.5);
        JsonNode metadata = document.path("metadata");
        assertEquals(7L, metadata.path("id").longValue());
        assertEquals("研发", metadata.path("business").textValue());
        assertEquals(names("Zoë", "李"), JSON.readTree(metadata.path("users.*.name").textValue()));
        assertEquals("null", metadata.path("optional").textValue());
        assertEquals("namespace-a_articles", metadata.path("_ab_stream").textValue());
        assertEquals(scope("namespace-a", "articles"), metadata.path("_st_scope").textValue());
    }

    @Test
    void acceptsOneHundredTwentyEightPlusTwoRowsAndIndependentParallelAppends() throws Exception {
        Settings options = real("real-batch");
        options.rowNum = 130;
        for (int index = 0; index < 130; index++) {
            options.rows.add(row("INSERT", 8, "same row", "ns", "feed", VECTOR));
        }
        success(run(options));
        assertEquals(130, realStored(options).size());

        options.rowNum = 2;
        options.rows = rows(row("INSERT", 8, "same row", "ns", "feed", VECTOR));
        options.parallelism = 2;
        options.splitNum = 2;
        success(run(options));
        List<JsonNode> result = realStored(options);
        assertEquals(134, result.size());
        Set<String> ids = new HashSet<>();
        for (JsonNode row : result) {
            assertTrue(ids.add(row.path("id").textValue()));
            assertEquals("body: same row", row.path("document").textValue());
            assertVector(row, 1.0, 0.25, -0.5);
        }
    }

    @Test
    void scopesOverwriteByExactNamespaceAndStreamOnTheRealServer() throws Exception {
        Settings options = real("real-overwrite");
        options.namespace = "a_b";
        options.stream = "c";
        options.rows.add(row("INSERT", 1, "old matching", "ns", "feed", VECTOR));
        success(run(options));
        options.namespace = "a";
        options.stream = "b_c";
        options.rows.set(
                0, row("INSERT", 2, "other stream with colliding label", "ns", "feed", VECTOR));
        success(run(options));

        options.mode = "overwrite";
        options.namespace = "a_b";
        options.stream = "c";
        options.rows.set(0, row("INSERT", 3, "replacement", "ns", "feed", VECTOR));
        success(run(options));
        List<String> texts = realTexts(options);
        assertEquals(2, texts.size());
        assertEquals(
                Set.of("body: replacement", "body: other stream with colliding label"),
                Set.copyOf(texts));
    }

    @Test
    void handlesDedupRepeatedKeysShrinkingChunksAndTombstonesOnTheRealServer() throws Exception {
        Settings options = real("real-dedup");
        options.mode = "append_dedup";
        options.keys = names("tenant", "id");
        options.streamField = "feed";
        options.chunk = 8;
        options.rows.add(
                row(
                        "INSERT",
                        7,
                        "one two three four five six seven eight nine ten eleven",
                        "ns",
                        "feed",
                        VECTOR));
        success(run(options));
        assertTrue(realStored(options).size() > 1);

        options.rowNum = 3;
        options.rows =
                rows(
                        row("UPDATE_AFTER", 7, "interim", "ns", "feed", VECTOR),
                        row("UPDATE_AFTER", 7, "final", "ns", "feed", VECTOR),
                        row("INSERT", 7, "other scope", "ns", "archive", VECTOR));
        success(run(options));
        List<String> replaced = realTexts(options);
        assertEquals(2, replaced.size());
        assertEquals(Set.of("body: final", "body: other scope"), Set.copyOf(replaced));

        options.rowNum = 1;
        options.rows = rows(row("DELETE", 7, null, "ns", "feed", null));
        success(run(options));
        assertEquals(List.of("body: other scope"), realTexts(options));

        ObjectNode legacy = row("INSERT", 7, null, "ns", "archive", null);
        ((ArrayNode) legacy.path("fields"))
                .set(8, JSON.getNodeFactory().textNode("2025-12-31T00:00:00Z"));
        options.rows = rows(legacy);
        success(run(options));
        assertTrue(realStored(options).isEmpty());
    }

    @Test
    void validatesPinnedEmbeddingProtocolsAndPreservesInputVectorOrder() throws Exception {
        for (String mode :
                List.of("openai", "azure_openai", "cohere", "openai_compatible", "fake")) {
            Settings options = settings("embed-" + mode.replace('_', '-'));
            options.remote(mode);
            options.rowNum = 2;
            options.rows =
                    rows(
                            row("INSERT", 1, "alpha", "n", "s", null),
                            row("INSERT", 2, "beta", "n", "s", null));
            startCase(options);
            success(run(options));
            List<JsonNode> result = stored(options);
            assertEquals(List.of("body: alpha", "body: beta"), text(options));
            for (JsonNode row : result) {
                assertEquals(options.dimensions, row.path("embedding").size());
            }
            if ("fake".equals(mode)) {
                assertTrue(requests(options, "embedding").isEmpty());
                assertNotEquals(result.get(0).path("embedding"), result.get(1).path("embedding"));
            } else {
                assertEquals(1, requests(options, "embedding").size());
                assertEquals(0.5, result.get(0).path("embedding").get(0).doubleValue());
                assertEquals(-0.5, result.get(1).path("embedding").get(0).doubleValue());
                assertEquals(
                        0.25,
                        result.get(1).path("embedding").get(options.dimensions - 1).doubleValue());
            }
        }

        Settings anonymous = settings("anonymous-compatible");
        anonymous.remote("openai_compatible");
        anonymous.fault = "anonymous";
        anonymous.embedToken = "";
        anonymous.rows = rows(row("INSERT", 3, "alpha", "n", "s", null));
        startCase(anonymous);
        success(run(anonymous));
        assertVector(stored(anonymous).get(0), 0.5, 0.0, 0.25);
    }

    @Test
    void splitsWithVerifiedGpt2RanksAndRespectsSplitAndOmitSettings() throws Exception {
        for (String splitter : List.of("separator", "markdown", "code")) {
            Settings options = settings("split-" + splitter);
            options.splitter = splitter;
            options.chunk = 8;
            options.overlap = 1;
            options.keep = true;
            options.language = "java";
            options.markdown = 2;
            String input =
                    "separator".equals(splitter)
                            ? "alpha beta gamma delta\n\nepsilon zeta eta theta\n\nend"
                            : "markdown".equals(splitter)
                                    ? "# Intro\nalpha beta gamma\n## Detail\ndelta epsilon zeta\nend"
                                    : "\nclass Sample {\n int field;\n public int count() { return 42; }\n}\nend";
            options.rows = rows(row("INSERT", 1, input, "n", "s", VECTOR));
            startCase(options);
            success(run(options));
            List<JsonNode> result = stored(options);
            assertTrue(result.size() >= 2);
            assertTrue(text(options).stream().anyMatch(value -> value.contains("end")));
            assertTrue(text(options).stream().noneMatch(value -> value.contains("\ufffd")));
            for (JsonNode row : result) {
                assertVector(row, 1.0, 0.25, -0.5);
            }
        }

        Settings omit = real("real-omit");
        omit.omit = true;
        omit.rows = rows(row("INSERT", 9, "alpha", "n", "s", VECTOR));
        success(run(omit));
        assertEquals(List.of(""), realTexts(omit));
        assertVector(realStored(omit).get(0), 1.0, 0.25, -0.5);
    }

    @Test
    void rejectsInvalidVectorsRanksAndReservedMetadataBeforeAnyMutation() throws Exception {
        Settings badVector = settings("wrong-vector");
        badVector.mode = "overwrite";
        badVector.rows = rows(row("INSERT", 1, "bad vector", "n", "s", List.of(1.0, 0.0)));
        startCase(badVector);
        failure(run(badVector), "embedding");
        assertTrue(operations(badVector).isEmpty());

        Settings badRanks = settings("wrong-ranks");
        badRanks.ranks = ROOT + "/untrusted-ranks";
        badRanks.chunk = 4;
        badRanks.rows =
                rows(row("INSERT", 1, "must split with verified rank data", "n", "s", VECTOR));
        startCase(badRanks);
        failure(run(badRanks), "tokenizer");
        assertTrue(operations(badRanks).isEmpty());

        Settings reserved = settings("wrong-metadata");
        reserved.aliases.put("body", "_st_scope");
        reserved.rows = rows(row("INSERT", 1, "alpha", "n", "s", VECTOR));
        startCase(reserved);
        failure(run(reserved), "field_name_mappings");
        assertTrue(operations(reserved).isEmpty());
    }

    @Test
    void preservesZeroVectorsAndRejectsEmptyNullAndFloatOverflow() throws Exception {
        Settings zero = real("real-zero-vector");
        zero.rows = rows(row("INSERT", 1, "zero", "n", "s", List.of(0.0, 0.0, 0.0)));
        success(run(zero));
        assertVector(realStored(zero).get(0), 0.0, 0.0, 0.0);

        int index = 0;
        for (List<Double> vector :
                Arrays.asList(null, List.<Double>of(), List.of(1.0, 1e300, -0.5))) {
            Settings invalid = settings("bad-vector-" + ++index);
            invalid.rows = rows(row("INSERT", 1, "invalid", "n", "s", vector));
            startCase(invalid);
            failure(run(invalid), "embedding");
            assertTrue(operations(invalid).isEmpty());
        }

        Settings remote = settings("bad-remote-vector");
        remote.remote("openai_compatible");
        remote.fault = "embed-overflow";
        remote.rows = rows(row("INSERT", 1, "alpha", "n", "s", null));
        startCase(remote);
        failure(run(remote), "nonfinite or nonnumeric component");
        assertEquals(List.of("embedding"), operations(remote));
    }

    @Test
    void submitsAtMostOneHundredTwentyEightAlignedDocumentsPerRequest() throws Exception {
        Settings options = settings("wire-batch");
        options.rowNum = 130;
        for (int i = 0; i < 130; i++) {
            options.rows.add(row("INSERT", i, "same", "n", "s", VECTOR));
        }
        startCase(options);
        success(run(options));
        List<JsonNode> writes = requests(options, "upsert");
        assertEquals(2, writes.size());
        assertEquals(128, writes.get(0).path("body").path("ids").size());
        assertEquals(2, writes.get(1).path("body").path("ids").size());
        assertEquals(130, stored(options).size());
    }

    @Test
    void failsBeforeScopedDeleteOnBadEmbeddingIdentityOrWrongCollectionDimensions()
            throws Exception {
        Settings malformed = settings("bad-embedding");
        malformed.mode = "overwrite";
        malformed.remote("openai_compatible");
        malformed.fault = "embed-bad-index";
        malformed.rowNum = 2;
        malformed.rows =
                rows(
                        row("INSERT", 1, "alpha", "n", "s", null),
                        row("INSERT", 2, "beta", "n", "s", null));
        startCase(malformed);
        failure(run(malformed), "embedding");
        assertEquals(List.of("embedding"), operations(malformed));

        Settings mismatch = settings("bad-dimension");
        mismatch.mode = "overwrite";
        mismatch.fixtureDimensions = 4;
        mismatch.rows = rows(row("INSERT", 1, "alpha", "n", "s", VECTOR));
        startCase(mismatch);
        failure(run(mismatch), "collection and configured embedding dimensions differ");
        assertEquals(List.of("open"), operations(mismatch));
    }

    @Test
    void acceptsOnlyRecognizedAcknowledgementsAndAvoidsAmbiguousReplay() throws Exception {
        for (String fault :
                List.of("false-ack", "malformed-ack", "large-ack", "ambiguous", "no-content")) {
            Settings options = settings(fault);
            options.fault = fault;
            options.maxBody = 4096;
            options.rows = rows(row("INSERT", 1, "alpha", "n", "s", VECTOR));
            startCase(options);
            if ("no-content".equals(fault)) {
                success(run(options));
            } else {
                String message =
                        "false-ack".equals(fault)
                                ? "invalid mutation acknowledgement"
                                : "malformed-ack".equals(fault)
                                        ? "expected valid UTF-8 JSON"
                                        : "large-ack".equals(fault)
                                                ? "response exceeds"
                                                : "mutation outcome may be unknown";
                failure(run(options), message);
            }
            assertEquals(List.of("open", "upsert"), operations(options));
            assertEquals(
                    List.of("ambiguous", "no-content").contains(fault) ? 1 : 0,
                    stored(options).size());
        }
    }

    @Test
    void honorsRateLimitWithoutChangingBatchContents() throws Exception {
        Settings options = settings("throttle");
        options.fault = "throttle";
        options.rows = rows(row("INSERT", 1, "alpha", "n", "s", VECTOR));
        startCase(options);
        success(run(options));
        assertEquals(List.of("open", "upsert", "upsert"), operations(options));
        List<JsonNode> requests = requests(options, "upsert");
        assertEquals(requests.get(0).path("body"), requests.get(1).path("body"));
        assertEquals(List.of("body: alpha"), text(options));
    }

    @Test
    void validatesBasicAndCloudCredentialsTlsRedirectAndParallelRestrictions() throws Exception {
        Settings basic = settings("basic-auth");
        basic.fault = "basic";
        basic.token = "";
        basic.user = "fixture";
        basic.password = "fixture-pass";
        basic.rows = rows(row("INSERT", 1, "alpha", "n", "s", VECTOR));
        startCase(basic);
        success(run(basic));
        assertEquals(List.of("body: alpha"), text(basic));

        for (String failure : List.of("untrusted", "wrong-san", "redirect", "parallel")) {
            Settings options = settings(failure);
            options.rows = basic.rows;
            switch (failure) {
                case "untrusted":
                    options.endpoint = url(ChromaPeer.DATABASE, ChromaPeer.UNTRUSTED);
                    break;
                case "wrong-san":
                    options.endpoint = url(ChromaPeer.WRONG_HOST, ChromaPeer.TRUSTED);
                    break;
                case "redirect":
                    options.fault = failure;
                    break;
                case "parallel":
                    options.mode = "overwrite";
                    options.parallelism = 2;
                    break;
                default:
                    throw new AssertionError(failure);
            }
            startCase(options);
            failure(
                    run(options),
                    "redirect".equals(failure)
                            ? "HTTP 307"
                            : "parallel".equals(failure)
                                    ? "require sink parallelism 1"
                                    : "HTTP exchange failed");
            assertEquals(
                    "redirect".equals(failure) ? List.of("open") : List.of(), operations(options));
        }
    }

    @Test
    void closesTimedOutAndCanceledMutationsWithoutAcknowledgingRows() throws Exception {
        Settings options = settings("deadline");
        options.fault = "slow";
        options.deadline = 2200;
        options.rows = rows(row("INSERT", 1, "alpha", "n", "s", VECTOR));
        startCase(options);
        failure(run(options), "request deadline exceeded");
        awaitDisconnect(options);
        assertEquals(List.of("open"), operations(options));

        Settings canceled = settings("cancel");
        canceled.fault = "slow";
        canceled.deadline = 60000;
        canceled.rows = options.rows;
        startCase(canceled);
        String id = jobId();
        Future<Container.ExecResult> job =
                tasks.submit(() -> engine.executeJob(CONF, id, canceled.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> operations(canceled).size() == 1);
            success(engine.cancelJob(id));
            job.get(30, TimeUnit.SECONDS);
            awaitDisconnect(canceled);
            assertTrue(stored(canceled).isEmpty());
        } finally {
            if (!job.isDone()) {
                engine.cancelJob(id);
                job.cancel(true);
            }
        }
    }

    @Test
    void restoresFromRealZetaSavepointWithoutDeletingTheOwnedScopeAgain() throws Exception {
        Settings options = settings("restore-overwrite");
        options.mode = "overwrite";
        options.jobMode = "STREAMING";
        options.rowNum = 2;
        options.splitNum = 2;
        options.interval = 18000;
        options.rows = rows(row("INSERT", 1, "each split", "n", "s", VECTOR));
        startCase(options);
        String id = jobId();
        Future<Container.ExecResult> initial =
                tasks.submit(() -> engine.executeJob(CONF, id, options.variables()));
        try {
            awaitInserts(options, 1);
            success(engine.savepointJob(id));
            initial.get(30, TimeUnit.SECONDS);
            List<JsonNode> first = stored(options);
            assertEquals(1, first.size());
            Future<Container.ExecResult> restored =
                    tasks.submit(() -> engine.restoreJob(CONF, id, options.variables()));
            try {
                awaitInserts(options, 2);
                success(engine.cancelJob(id));
                restored.get(30, TimeUnit.SECONDS);
            } finally {
                if (!restored.isDone()) {
                    engine.cancelJob(id);
                    restored.cancel(true);
                }
            }
            List<JsonNode> after = stored(options);
            assertEquals(2, after.size());
            assertEquals(first.get(0), after.get(0));
            assertNotEquals(after.get(0).path("id"), after.get(1).path("id"));
            assertEquals(List.of("open", "delete", "upsert", "upsert"), operations(options));
        } finally {
            if (!initial.isDone()) {
                engine.cancelJob(id);
                initial.cancel(true);
            }
        }
    }

    private Settings real(String collection) {
        Settings options = settings(collection);
        options.endpoint = REAL;
        options.token = "";
        return options;
    }

    private List<String> realTexts(Settings options) throws Exception {
        return realStored(options).stream()
                .map(row -> row.path("document").textValue())
                .collect(Collectors.toList());
    }

    private List<JsonNode> realStored(Settings options) throws Exception {
        String path = ChromaPeer.COLLECTIONS + "/";
        JsonNode collection = provider(path + options.name, null);
        String id = collection.path("id").textValue();
        assertNotEquals(null, id);
        ObjectNode request = JSON.createObjectNode();
        request.set("include", names("documents", "metadatas", "embeddings"));
        JsonNode body = provider(path + id + "/get", request);
        List<JsonNode> output = new ArrayList<>();
        JsonNode ids = body.path("ids");
        assertTrue(ids.isArray(), body::toString);
        for (int i = 0; i < ids.size(); i++) {
            ObjectNode row = JSON.createObjectNode();
            row.set("id", ids.get(i));
            row.set("document", body.path("documents").get(i));
            row.set("metadata", body.path("metadatas").get(i));
            row.set("embedding", body.path("embeddings").get(i));
            output.add(row);
        }
        return output;
    }

    private JsonNode provider(String path, ObjectNode request) throws Exception {
        URI base =
                new URI(
                        "http",
                        null,
                        database.getHost(),
                        database.getMappedPort(8000),
                        null,
                        null,
                        null);
        HttpURLConnection connection =
                (HttpURLConnection) base.resolve(path).toURL().openConnection();
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(10000);
        try {
            if (request != null) {
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                try (OutputStream output = connection.getOutputStream()) {
                    output.write(JSON.writeValueAsBytes(request));
                }
            }
            assertEquals(200, connection.getResponseCode());
            try (InputStream response = connection.getInputStream()) {
                return JSON.readTree(response);
            }
        } finally {
            connection.disconnect();
        }
    }

    private Settings settings(String name) {
        return new Settings(name);
    }

    private void startCase(Settings options) throws Exception {
        verifyPeerContract();
        Path data = Files.createTempFile(directory, "chroma-case-", ".json");
        ObjectNode input =
                JSON.createObjectNode()
                        .put(
                                "dimensions",
                                options.fixtureDimensions == 0
                                        ? options.dimensions
                                        : options.fixtureDimensions)
                        .put("fault", options.fault);
        Files.writeString(data, input.toString());
        engine.install(data, options.name + ".input");
        Path marker = Files.createTempFile(directory, "chroma-case-", ".active");
        Files.writeString(marker, options.name);
        engine.install(marker, "active");
        active = options.name;
    }

    private Container.ExecResult run(Settings options) throws Exception {
        return engine.executeJob(CONF, jobId(), options.variables());
    }

    private List<JsonNode> stored(Settings options) throws Exception {
        String value = engine.contents(options.name + ".documents");
        if (value.isEmpty()) {
            return List.of();
        }
        List<JsonNode> result = new ArrayList<>();
        JSON.readTree(value).forEach(result::add);
        return result;
    }

    private List<String> text(Settings options) throws Exception {
        return stored(options).stream()
                .map(entry -> entry.path("document").asText())
                .collect(Collectors.toList());
    }

    private List<JsonNode> requests(Settings options, String command) throws Exception {
        List<JsonNode> result = new ArrayList<>();
        String log = engine.contents(options.name + ".requests");
        for (String entry : log.split("\n")) {
            if (!entry.isBlank()) {
                JsonNode json = JSON.readTree(entry);
                if (command == null || command.equals(json.path("operation").asText())) {
                    result.add(json);
                }
            }
        }
        return result;
    }

    private List<String> operations(Settings options) throws Exception {
        return requests(options, null).stream()
                .map(entry -> entry.path("operation").asText())
                .collect(Collectors.toList());
    }

    private void awaitInserts(Settings options, int count) {
        Awaitility.await()
                .atMost(45, TimeUnit.SECONDS)
                .until(
                        () ->
                                requests(options, "upsert").size() == count
                                        && stored(options).size() == count + 1);
    }

    private void awaitDisconnect(Settings options) {
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> engine.exists(options.name + ".disconnect"));
    }

    private static ObjectNode row(
            String kind, long id, String body, String tenant, String feed, List<Double> vector) {
        ObjectNode row = JSON.createObjectNode().put("kind", kind);
        ArrayNode data = row.putArray("fields").add(id).add(body).add(tenant).add(feed);
        if (vector == null) {
            data.addNull();
        } else {
            ArrayNode values = data.addArray();
            vector.forEach(values::add);
        }
        data.addArray().add("研发").add(true);
        ArrayNode users = data.addArray();
        users.addObject().put("name", "Zoë");
        users.addObject().put("name", "李");
        data.addNull();
        data.addNull();
        return row;
    }

    private static ArrayNode rows(ObjectNode... rows) {
        return JSON.createArrayNode().addAll(Arrays.asList(rows));
    }

    private static ArrayNode names(String... values) {
        ArrayNode array = JSON.createArrayNode();
        for (String value : values) {
            array.add(value);
        }
        return array;
    }

    private static String scope(String namespace, String stream) throws Exception {
        return hash(JSON.writeValueAsString(List.of(namespace, stream)));
    }

    private static String hash(String plain) throws Exception {
        byte[] input = plain.getBytes(StandardCharsets.UTF_8);
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(MessageDigest.getInstance("SHA-256").digest(input));
    }

    private static void assertVector(JsonNode doc, double... values) {
        JsonNode vector = doc.path("embedding");
        assertEquals(values.length, vector.size());
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], vector.get(i).asDouble());
        }
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static String url(String host, int port) {
        return "https://" + host + ":" + port;
    }

    private static String output(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> output(result));
    }

    private static void failure(Container.ExecResult result, String reason) {
        String output = output(result);
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(ChromaPeer.DB_TOKEN));
        assertFalse(output.contains(ChromaPeer.EMBED_TOKEN));
        assertFalse(output.contains(ChromaPeer.PRIVATE));
    }

    private static KeyStore identity(String name) throws Exception {
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Path target = directory.resolve(name + ".p12");
        String alt = HOSTS.stream().map(host -> "dns:" + host).collect(Collectors.joining(","));
        Process process =
                new ProcessBuilder(
                                keytool,
                                "-genkeypair",
                                "-alias",
                                "fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + ChromaPeer.DATABASE,
                                "-ext",
                                "SAN=" + alt,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                target.toString(),
                                "-storepass",
                                ChromaPeer.PASSWORD,
                                "-keypass",
                                ChromaPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(target)) {
            key.load(input, ChromaPeer.PASSWORD.toCharArray());
        }
        return key;
    }

    private static void downloadRanks() throws Exception {
        HttpsURLConnection connection =
                (HttpsURLConnection) URI.create(RANKS).toURL().openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setConnectTimeout(15000);
        connection.setReadTimeout(15000);
        try {
            assertEquals(200, connection.getResponseCode());
            byte[] data;
            try (InputStream response = connection.getInputStream()) {
                data = response.readNBytes(1048577);
            }
            assertTrue(data.length < 1048577);
            StringBuilder actual = new StringBuilder();
            for (byte value : MessageDigest.getInstance("SHA-256").digest(data)) {
                actual.append(String.format("%02x", value & 255));
            }
            assertEquals(RANK_SHA, actual.toString());
            Files.write(directory.resolve("ranks"), data);
        } finally {
            connection.disconnect();
        }
    }

    /**
     * Supplies synthetic jobs through the same CLI variables available in a normal installation.
     * The fixture independently checks API credentials, dimensions and request routes.
     */
    private static final class Settings {
        final String name;
        String endpoint = DB;
        String token = ChromaPeer.DB_TOKEN;
        String user = "";
        String password = "";
        String trust = ROOT + "/trust.der";
        int batch = 128;
        String mode = "append";
        String namespace = "namespace-a";
        String stream = "articles";
        String namespaceField = "";
        String streamField = "";
        String fault = "";
        String splitter = "separator";
        String language = "";
        String ranks = ROOT + "/ranks";
        String embedding = "from_field";
        String embedHost = "";
        String embedUrl = "";
        String embedToken = ChromaPeer.EMBED_TOKEN;
        String jobMode = "BATCH";
        boolean omit;
        boolean keep;
        int dimensions = 3;
        int fixtureDimensions;
        int chunk = 400;
        int overlap;
        int markdown = 1;
        int deadline = 30000;
        int attempts = 3;
        int maxRow = 262144;
        int maxBuffer = 4194304;
        int maxBody = 4194304;
        int maxChunks = 100;
        int parallelism = 1;
        int rowNum = 1;
        int splitNum = 1;
        int interval = 1;
        ArrayNode rows = JSON.createArrayNode();
        ArrayNode keys = JSON.createArrayNode();
        ArrayNode textFields = names("body");
        ArrayNode metadataFields = names("id", "context.team", "users.*.name", "optional");
        ArrayNode separators = names("\n\n", "\n", " ", "");
        ObjectNode aliases = JSON.createObjectNode();

        Settings(String name) {
            this.name = name;
        }

        void remote(String mode) {
            embedding = mode;
            switch (mode) {
                case "openai":
                    embedHost = ChromaPeer.OPENAI;
                    dimensions = 1536;
                    break;
                case "azure_openai":
                    embedHost = ChromaPeer.AZURE;
                    dimensions = 1536;
                    break;
                case "cohere":
                    embedHost = ChromaPeer.COHERE;
                    dimensions = 1024;
                    break;
                case "openai_compatible":
                    embedHost = ChromaPeer.COMPATIBLE;
                    dimensions = 3;
                    break;
                case "fake":
                    dimensions = 1536;
                    return;
                default:
                    throw new IllegalArgumentException(mode);
            }
            embedUrl =
                    url(embedHost, ChromaPeer.TRUSTED)
                            + ("openai_compatible".equals(mode) ? "/private/v1" : "");
        }

        String[] variables() {
            return new String[] {
                "chroma.endpoint=" + endpoint,
                "chroma.token=" + token,
                "chroma.user=" + user,
                "chroma.password=" + password,
                "chroma.trust=" + trust,
                "chroma.batch=" + batch,
                "chroma.collection=" + name,
                "chroma.jobMode=" + jobMode,
                "chroma.checkpoint=" + ("STREAMING".equals(jobMode) ? 1000 : 60000),
                "chroma.parallelism=" + parallelism,
                "chroma.rowNum=" + rowNum,
                "chroma.splitNum=" + splitNum,
                "chroma.interval=" + interval,
                "chroma.rows=" + rows,
                "chroma.mode=" + mode,
                "chroma.namespace=" + namespace,
                "chroma.stream=" + stream,
                "chroma.namespaceField=" + namespaceField,
                "chroma.streamField=" + streamField,
                "chroma.keys=" + keys,
                "chroma.textFields=" + textFields,
                "chroma.metadataFields=" + metadataFields,
                "chroma.aliases=" + aliases,
                "chroma.omit=" + omit,
                "chroma.splitter=" + splitter,
                "chroma.separators=" + separators,
                "chroma.keep=" + keep,
                "chroma.language=" + language,
                "chroma.markdown=" + markdown,
                "chroma.chunk=" + chunk,
                "chroma.overlap=" + overlap,
                "chroma.ranks=" + ranks,
                "chroma.embedding=" + embedding,
                "chroma.embedUrl=" + embedUrl,
                "chroma.embedToken=" + embedToken,
                "chroma.dimensions=" + dimensions,
                "chroma.maxRow=" + maxRow,
                "chroma.maxBuffer=" + maxBuffer,
                "chroma.maxBody=" + maxBody,
                "chroma.maxChunks=" + maxChunks,
                "chroma.deadline=" + deadline,
                "chroma.attempts=" + attempts
            };
        }
    }

    /**
     * Runs the Java 11 engine and mounts independent fixture identities and vocabulary data. Local
     * peer endpoints share the container network namespace through explicit host aliases.
     */
    private static final class Engine extends SeaTunnelContainer {
        private final Path fixtures;

        Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            HOSTS.forEach(host -> container.withExtraHost(host, "127.0.0.1"));
            container.withExtraHost(ChromaPeer.WRONG_HOST, "127.0.0.1");
            for (String file :
                    List.of(
                            "trusted.p12",
                            "untrusted.p12",
                            "trust.der",
                            "ranks",
                            "untrusted-ranks")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(file)), ROOT + "/" + file);
            }
            String javaPackage = ChromaPeer.class.getPackage().getName().replace('.', '/');
            try {
                for (String file : List.of("ChromaPeer.class", "ChromaPeer$Case.class")) {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Paths.get(ChromaPeer.class.getResource(file).toURI())),
                            ROOT + "/classes/" + javaPackage + "/" + file);
                }
                Path json =
                        Path.of(
                                ObjectMapper.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI());
                String target = Files.isDirectory(json) ? "/jackson" : "/jackson.jar";
                container.withCopyFileToContainer(MountableFile.forHostPath(json), ROOT + target);
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the HTTPS datastore fixture", e);
            }
        }

        Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/jackson:" + ROOT + "/jackson.jar",
                    ChromaPeer.class.getName(),
                    ROOT);
        }

        void install(Path source, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(source), ROOT + "/" + name);
        }

        boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + name).getExitCode() == 0;
        }

        void touch(String name) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + name));
        }

        String contents(String name) throws Exception {
            if (!exists(name)) {
                return "";
            }
            Container.ExecResult read = server.execInContainer("cat", ROOT + "/" + name);
            success(read);
            return read.getStdout();
        }
    }
}

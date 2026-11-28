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

package org.apache.seatunnel.e2e.connector.cartcom;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.cartcom.CartComRecoveryProbe;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the packaged CartCom Source through real Zeta jobs and an independent HTTPS peer.
 *
 * <p>The literal resource matrix and request scripts establish the expected provider wire bytes.
 * Transactional LocalFile receipts are the result oracle.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class CartComIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String START = "2024-01-01T00:00:00Z";
    private static final String END = "2024-01-02T00:00:00Z";
    private static final String ID = "9223372036854775810";
    private static final String TRAP = "https://cartcom-trap.invalid";
    private static final String EXTRA =
            "{\"numeric\":1234567890123456789.10001,"
                    + "\"unicode\":\"商品🧾\",\"null\":null,\"arrays\":[true,null,[-9,8]]}";
    private static final int DEFAULT_SIZE = 2;
    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private static String trust;
    private String wrongTrust;
    private String wrongNameTrust;
    private String active;

    @BeforeAll
    void startsOwnedServices() throws Exception {
        trust = identity("trusted", CartComPeer.HOST);
        wrongTrust = identity("untrusted", CartComPeer.HOST);
        wrongNameTrust = identity("wrong-name", "wrong-name.invalid");
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::peer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void checksThePeerAndRestoresTrust() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
        selectIdentity("trusted");
    }

    @AfterAll
    void stopsOwnedServices() throws Exception {
        try {
            if (engine != null) {
                try {
                    if (peer != null) {
                        engine.touch("stop");
                        success(peer.get(20, TimeUnit.SECONDS));
                    }
                } finally {
                    engine.tearDown();
                }
            }
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void readsSevenResourcesAndBothDocumentedOrderEnvelopeForms() throws Exception {
        List<Contract> catalog = catalog();
        assertEquals(7, catalog.size());
        for (Contract contract : catalog) {
            List<String> wrappers =
                    contract.alias == null
                            ? List.of(contract.wrapper)
                            : List.of(contract.wrapper, contract.alias);
            for (String wrapper : wrappers) {
                String name = contract.resource + "-" + wrapper;
                Settings settings = new Settings(name, contract.resource);
                boolean dated = contract.window;
                String raw = sample(ID, dated ? "2024-01-01T08:00:00+08:00" : null);
                program(name, List.of(step(settings, contract, 1, envelope(wrapper, null, raw))));
                success(run(settings));
                assertRows(name, List.of(expected(ID, dated ? START : null, raw)));
                assertTrue(engine.rows(name).get(0).contains("1234567890123456789.10001"));
                consumed(name, 1);
            }
        }
    }

    @Test
    void signsTheExactPageQueryAndKeepsTheOriginalWindowAndCentralOrigin() throws Exception {
        Settings settings = new Settings("central-ordered", "order_items");
        settings.central = true;
        Contract contract = named(settings.resource);
        String first = sample("1", "2024-01-01T00:00:00");
        String second = sample("2", "2024-01-01T00:00:00.0Z");
        String next =
                TRAP
                        + "/api/v1/order_items.json?page=2&count=1&sort=-id"
                        + "&updated_at=gte%3A1999-01-01&token=trap";
        program(
                settings.name,
                List.of(
                        step(settings, contract, 1, envelope("items", next, first)),
                        step(settings, contract, 2, envelope("order_items", null, second))));
        success(run(settings));
        assertRows(
                settings.name, List.of(expected("1", START, first), expected("2", START, second)));
        List<String> requests = engine.lines(settings.name + ".requests");
        assertEquals(2, requests.size());
        assertTrue(requests.get(1).contains("&page=2&sort=updated_at%2Cid&updated_at="));
        assertFalse(requests.get(1).contains("trap"));
        consumed(settings.name, 2);
    }

    @Test
    void traversesEmptyAndShortPagesWithoutLosingAnUnfilteredStatus() throws Exception {
        Settings settings = new Settings("status-pagination", "order_statuses");
        Contract contract = named(settings.resource);
        String older = sample("8", "2001-03-04T05:06:07.125");
        program(
                settings.name,
                List.of(
                        step(settings, contract, 1, envelope("order_statuses", "?page=2")),
                        step(
                                settings,
                                contract,
                                2,
                                envelope("order_statuses", "/api/v1/order_statuses?page=3", older)),
                        step(settings, contract, 3, envelope("order_statuses", null))));
        success(run(settings));
        assertRows(settings.name, List.of(expected("8", "2001-03-04T05:06:07.125Z", older)));
        consumed(settings.name, 3);
    }

    @Test
    void retainsEqualCursorAcrossPagesAndEnforcesTheHalfOpenReplayWindow() throws Exception {
        Settings settings = new Settings("inclusive-ties", "orders");
        settings.incremental = "2024-01-01T01:00:00Z";
        settings.lookback = 300;
        settings.lower = "2024-01-01T00:55:00Z";
        settings.size = 3;
        Contract contract = named(settings.resource);
        String before = sample("1", "2024-01-01T00:54:59.999Z");
        String equalA = sample("2", "2024-01-01T00:55:00Z");
        String equalB = sample("3", "2024-01-01T08:55:00+08:00");
        String atEnd = sample("4", END);
        String nullable = sample("5", null);
        program(
                settings.name,
                List.of(
                        step(
                                settings,
                                contract,
                                1,
                                envelope("orders", "/api/v1/orders?page=2", before, equalA)),
                        step(
                                settings,
                                contract,
                                2,
                                envelope("orders", null, equalB, atEnd, nullable))));
        success(run(settings));
        assertRows(
                settings.name,
                List.of(
                        expected("2", settings.lower, equalA),
                        expected("3", settings.lower, equalB),
                        expected("5", null, nullable)));
        consumed(settings.name, 2);
    }

    @Test
    void rejectsUntrustedOrRepeatedPagingIndicesBeforeEmittingTheirRows() throws Exception {
        Contract item = named("products");
        List<String> bad =
                List.of(
                        "/api/v1/products?page=1",
                        "/api/v1/products?page=3",
                        "/api/v1/products?page=2&page=2",
                        "/api/v1/orders?page=2",
                        "http://cartcom-trap.invalid/api/v1/products?page=2",
                        "https://user@cartcom-trap.invalid/api/v1/products?page=2");
        for (int i = 0; i < bad.size(); i++) {
            Settings settings = new Settings("bad-pagination-" + i, item.resource);
            fails(
                    settings,
                    List.of(
                            step(
                                    settings,
                                    item,
                                    1,
                                    envelope("products", bad.get(i), sample("1", START)))),
                    "next_page");
            assertRows(settings.name, List.of());
        }
        Settings budget = new Settings("page-budget", item.resource);
        budget.pages = 1;
        fails(
                budget,
                List.of(step(budget, item, 1, envelope("products", "/api/v1/products?page=2"))),
                "max_pages");
    }

    @Test
    void validatesEnvelopeIdsNullableUpdatesAndConflictingAliases() throws Exception {
        Contract orders = named("order_payments");
        List<String> bad =
                List.of(
                        "{\"payments\":{\"id\":4}}",
                        "{\"payments\":[{\"id\":false}]}",
                        "{\"payments\":[{\"id\":1,\"updated_at\":\"not-a-date\"}]}",
                        "{\"payments\":[{\"id\":1}],\"order_payments\":[{\"id\":2}]}",
                        "{\"payments\":[],\"payments\":[]}");
        for (int i = 0; i < bad.size(); i++) {
            Settings settings = new Settings("invalid-body-" + i, orders.resource);
            fails(settings, List.of(step(settings, orders, 1, bad.get(i))), "CartCom resource");
            assertRows(settings.name, List.of());
        }
        Settings valid = new Settings("matching-alias", orders.resource);
        String payment = sample("1", null);
        String response = "{\"payments\":[" + payment + "],\"order_payments\":[" + payment + "]}";
        program(valid.name, List.of(step(valid, orders, 1, response)));
        success(run(valid));
        assertRows(valid.name, List.of(expected("1", null, payment)));
        consumed(valid.name, 1);
    }

    @Test
    void keepsRetryDelayAndAttemptBudgetForIdempotentReads() throws Exception {
        Contract products = named("products");
        Settings when = new Settings("rfc1123-retry", products.resource);
        when.central = true;
        when.attempts = 2;
        String row = sample("7", START);
        program(
                when.name,
                List.of(
                        step(when, products, 1, "private-provider-message")
                                .put("status", 429)
                                .put("delay_seconds", 3),
                        step(when, products, 1, envelope("products", null, row))));
        success(run(when));
        assertRows(when.name, List.of(expected("7", START, row)));
        List<String> times = engine.lines(when.name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 1_800L);
        consumed(when.name, 2);
        Settings limit = new Settings("retry-limit", products.resource);
        limit.attempts = 2;
        ObjectNode unavailable =
                step(limit, products, 1, "private-provider-message").put("status", 503);
        fails(limit, List.of(unavailable, unavailable), "retry limit");
        Settings longDelay = new Settings("retry-deadline", products.resource);
        longDelay.attempts = 2;
        longDelay.deadline = 5000;
        fails(
                longDelay,
                List.of(
                        step(longDelay, products, 1, "busy")
                                .put("status", 429)
                                .put("Retry-After", "60")),
                "retry delay");
    }

    @Test
    void rejectsInvalidCredentialScopeAndNeverFollowsARedirect() throws Exception {
        Contract rows = named("orders");
        Settings bad = new Settings("bad-host", rows.resource);
        bad.host = "https://store.cart.test/path";
        fails(bad, List.of(), "store_host");
        for (int status : List.of(401, 403)) {
            Settings unauthorized = new Settings("denied-" + status, rows.resource);
            unauthorized.attempts = 3;
            fails(
                    unauthorized,
                    List.of(
                            step(unauthorized, rows, 1, "private-provider-message")
                                    .put("status", status)),
                    "HTTP " + status);
        }
        Settings redirect = new Settings("redirect", rows.resource);
        redirect.central = true;
        fails(
                redirect,
                List.of(step(redirect, rows, 1, "").put("status", 302).put("mode", "redirect")),
                "HTTP 302");
    }

    @Test
    void verifiesTenantAndCentralTlsAndRejectsUntrustedOrWrongNames() throws Exception {
        Settings invalid = new Settings("wrong-trust", "customers");
        invalid.certificate = wrongTrust;
        fails(invalid, List.of(), "TLS");
        selectIdentity("wrong-name");
        Settings wrongName = new Settings("wrong-name", "customers");
        wrongName.central = true;
        wrongName.certificate = wrongNameTrust;
        fails(wrongName, List.of(), "TLS");
    }

    @Test
    void strictlyDecodesUtf8AndBoundsCompressedAndExpandedBodies() throws Exception {
        Contract items = named("order_items");
        Settings utf = new Settings("bad-utf", items.resource);
        byte[] invalid =
                new byte[] {'{', '"', 'x', '"', ':', '"', (byte) 0xc3, (byte) 0x28, '"', '}'};
        fails(
                utf,
                List.of(
                        step(utf, items, 1, "")
                                .put("bytes", Base64.getEncoder().encodeToString(invalid))),
                "UTF-8");
        for (String mode : List.of("gzip", "json")) {
            Settings cap = new Settings("body-cap-" + mode, items.resource);
            cap.bytes = 1024;
            String wire =
                    envelope("items", null, "{\"id\":1,\"memo\":\"" + "x".repeat(2048) + "\"}");
            fails(cap, List.of(step(cap, items, 1, wire).put("mode", mode)), "max_response_bytes");
        }
        Settings gzip = new Settings("gzip-valid", items.resource);
        String row = sample("9", START);
        program(
                gzip.name,
                List.of(step(gzip, items, 1, envelope("items", null, row)).put("mode", "gzip")));
        success(run(gzip));
        assertRows(gzip.name, List.of(expected("9", START, row)));
        consumed(gzip.name, 1);
    }

    @Test
    void stopsATricklingBodyAtTheAbsoluteDeadline() throws Exception {
        Contract orders = named("orders");
        Settings settings = new Settings("absolute-deadline", orders.resource);
        settings.deadline = 1500;
        settings.readTimeout = 15000;
        fails(settings, List.of(step(settings, orders, 1, "{").put("mode", "trickle")), "deadline");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(settings.name + ".disconnect"));
        assertRows(settings.name, List.of());
    }

    @Test
    void activelyCancelsAnInFlightZetaRead() throws Exception {
        Contract orders = named("orders");
        Settings settings = new Settings("cancel-request", orders.resource);
        program(settings.name, List.of(step(settings, orders, 1, "{").put("mode", "trickle")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob(jobConfig(settings), job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(25, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".started"));
            success(engine.cancelJob(job));
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".disconnect"));
            assertRows(settings.name, List.of());
            consumed(settings.name, 1);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void restoresTheNumberedPageWithoutRepeatingTheCentralRequestAlreadyCommitted()
            throws Exception {
        Contract items = named("order_items");
        Settings settings = new Settings("saved-page", items.resource);
        settings.central = true;
        String first = sample("21", START), second = sample("22", START);
        String job =
                saveWhileWaiting(
                        settings,
                        List.of(
                                step(
                                        settings,
                                        items,
                                        1,
                                        envelope("items", "/api/v1/order_items?page=2", first)),
                                step(settings, items, 2, envelope("order_items", null, second))
                                        .put("mode", "partial")));
        program(
                "restored-page",
                List.of(step(settings, items, 2, envelope("order_items", null, second))));
        success(engine.restoreJob(jobConfig(settings), job, settings.variables()));
        assertRows(
                settings.name,
                List.of(expected("21", START, first), expected("22", START, second)));
        consumed("restored-page", 1);
    }

    @Test
    void restoresATerminalPipelineWithNoHttpTrafficOrDuplicateRows() throws Exception {
        Contract statuses = named("order_statuses");
        Settings settings = new Settings("terminal-original", statuses.resource);
        String raw = sample("7", null);
        List<JsonNode> result = List.of(expected("7", null, raw));
        program(
                settings.name,
                List.of(step(settings, statuses, 1, envelope("order_statuses", null, raw))));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/cart-com-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(settings.name, result));
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(settings.name, 1);
        program("terminal-resumed", List.of());
        Future<Container.ExecResult> restored =
                tasks.submit(
                        () ->
                                engine.restoreJob(
                                        "/cart-com-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(job)));
            long before = engine.completedAcrossPipelines(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(job) > before);
            assertRows(settings.name, result);
            consumed("terminal-resumed", 0);
        } finally {
            if (!restored.isDone()) {
                engine.cancelJob(job);
            }
            restored.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void aPackagedReaderRestoresA32RowOffsetAndRejectsMutatedRawPages() throws Exception {
        Contract products = named("products");
        Settings settings = new Settings("probe-settings", products.resource);
        settings.size = 34;
        List<String> seeded = new ArrayList<>();
        for (int i = 1; i <= 33; i++) {
            seeded.add(sample(Integer.toString(i), START));
        }
        seeded.add(sample("34", "2023-12-30T00:00:00Z"));
        String first = envelope("products", null, seeded.toArray(new String[0]));
        for (String mode : List.of("resume", "changed-selected", "changed-filtered")) {
            List<String> replay = new ArrayList<>(seeded);
            if (mode.equals("changed-selected")) {
                replay.set(32, sample("330", START));
            } else if (mode.equals("changed-filtered")) {
                replay.set(33, sample("340", "2023-12-30T00:00:00Z"));
            }
            String refetched = envelope("products", null, replay.toArray(new String[0]));
            program(
                    mode,
                    List.of(
                            step(settings, products, 1, first),
                            step(settings, products, 1, refetched)));
            success(engine.probe(mode));
            consumed(mode, 2);
        }
        program("zero", List.of());
        success(engine.probe("zero"));
        consumed("zero", 0);
    }

    /**
     * Saves a real job after the peer has paused a later response and checkpoints still advance.
     *
     * <p>The checkpoint includes committed data from the previous completed page.
     */
    private String saveWhileWaiting(Settings settings, List<ObjectNode> steps) throws Exception {
        program(settings.name, steps);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob(jobConfig(settings), job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".started"));
            long baseline = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(settings.name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(settings.name, steps.size());
        return job;
    }

    private static List<Contract> catalog() throws IOException {
        try (InputStream stream = CartComIT.class.getResourceAsStream("/cart-com-contracts.tsv")) {
            if (stream == null) {
                throw new IOException("missing independent Cart.com route table");
            }
            List<Contract> values = new ArrayList<>();
            for (String line :
                    new String(stream.readAllBytes(), StandardCharsets.UTF_8).split("\\R")) {
                if (!line.isBlank() && !line.startsWith("#")) {
                    values.add(new Contract(line.split("\\t", -1)));
                }
            }
            return values;
        }
    }

    private static Contract named(String resource) throws IOException {
        for (Contract entry : catalog()) {
            if (entry.resource.equals(resource)) {
                return entry;
            }
        }
        throw new IOException("missing route fixture " + resource);
    }

    private static String sample(String id, String updated) {
        return "{\"id\":"
                + id
                + ",\"updated_at\":"
                + (updated == null ? "null" : "\"" + updated + "\"")
                + ",\"unmodeled\":"
                + EXTRA
                + "}";
    }

    private static String envelope(String key, String link, String... records) {
        return "{\""
                + key
                + "\":["
                + String.join(",", records)
                + "],\"next_page\":"
                + JSON.getNodeFactory().textNode(link)
                + "}";
    }

    private static ObjectNode step(Settings settings, Contract contract, int page, String body) {
        String query = "count=" + settings.size;
        if (page != 1) {
            query += "&page=" + page;
        }
        if (contract.window) {
            query +=
                    "&sort="
                            + enc("updated_at,id")
                            + "&updated_at="
                            + enc("gte:" + settings.lower + " AND lt:" + settings.end);
        }
        return JSON.createObjectNode()
                .put("host", settings.central ? CartComPeer.CENTRAL : CartComPeer.HOST)
                .put("target", contract.path + "?" + query)
                .put("response", body);
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static JsonNode expected(String id, String updated, String wire) throws IOException {
        return JSON.createObjectNode()
                .put("id", id)
                .put("updated_at", updated)
                .set("data", JSON.readTree(wire));
    }

    private void assertRows(String name, List<JsonNode> wanted) throws Exception {
        List<JsonNode> actual = new ArrayList<>();
        for (String value : engine.rows(name)) {
            ObjectNode row = (ObjectNode) JSON.readTree(value);
            row.set("data", JSON.readTree(row.path("data").textValue()));
            actual.add(row);
        }
        assertEquals(counts(wanted), counts(actual));
    }

    private static Map<JsonNode, Integer> counts(List<JsonNode> values) {
        Map<JsonNode, Integer> counts = new HashMap<>();
        for (JsonNode value : values) {
            counts.merge(value, 1, Integer::sum);
        }
        return counts;
    }

    private void program(String name, List<ObjectNode> steps) throws Exception {
        Path file = Files.createTempFile(directory, "script-", ".json");
        JSON.writeValue(file.toFile(), steps);
        engine.install(file, name + ".json");
        Files.writeString(file, name);
        engine.install(file, "active");
        active = name;
    }

    private void selectIdentity(String name) throws Exception {
        Path signal = Files.createTempFile(directory, "tls-", ".txt");
        Files.writeString(signal, name);
        engine.install(signal, "identity");
        if (!name.equals(engine.localIdentity)) {
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> engine.lines("identity-ready").equals(List.of(name)));
            engine.localIdentity = name;
        }
    }

    private void consumed(String name, int requests) throws Exception {
        assertFalse(engine.exists(name + ".invalid"), name);
        assertEquals(requests, engine.lines(name + ".requests").size(), name);
    }

    private Container.ExecResult run(Settings settings) throws Exception {
        return engine.executeJob(jobConfig(settings), jobId(), settings.variables());
    }

    private static String jobConfig(Settings settings) {
        if (settings.central) {
            return "/cart-com-central.conf";
        }
        return settings.incremental == null ? "/cart-com.conf" : "/cart-com-incremental.conf";
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        for (String secret :
                List.of(
                        CartComPeer.TOKEN,
                        CartComPeer.SECRET,
                        CartComPeer.USER,
                        "private-provider-message")) {
            assertFalse(output.contains(secret), "credential or provider error leak");
        }
        consumed(settings.name, steps.size());
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
    }

    private static String identity(String name, String hostname) throws Exception {
        Path store = directory.resolve(name + ".p12");
        String san =
                "SAN=dns:"
                        + hostname
                        + (name.equals("wrong-name") ? "" : ",dns:" + CartComPeer.CENTRAL);
        Process keytool =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-alias",
                                "api",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + hostname,
                                "-ext",
                                san,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                CartComPeer.PASSWORD,
                                "-keypass",
                                CartComPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, CartComPeer.PASSWORD.toCharArray());
        }
        Files.writeString(
                directory.resolve(name + ".pem"),
                "-----BEGIN CERTIFICATE-----\n"
                        + Base64.getMimeEncoder(64, new byte[] {'\n'})
                                .encodeToString(keys.getCertificate("api").getEncoded())
                        + "\n-----END CERTIFICATE-----\n");
        return Engine.PEER + "/" + name + ".pem";
    }

    /**
     * Reads the literal provider route and wrapper contract for one resource.
     *
     * <p>The integration expectations are independent of the product enum.
     */
    private static final class Contract {
        final String resource;
        final String path;
        final String wrapper;
        final boolean window;
        final String alias;

        Contract(String[] row) {
            resource = row[0];
            path = row[1];
            wrapper = row[2];
            window = "window".equals(row[3]);
            alias = "-".equals(row[4]) ? null : row[4];
        }
    }

    /**
     * Supplies isolated job settings and applies fixture-specific time bounds.
     *
     * <p>Each executed job uses a unique output directory and a fixed page request window.
     */
    private static final class Settings {
        final String name;
        final String resource;
        String host = CartComPeer.HOST;
        String certificate = trust;
        String lower = START;
        String end = END;
        String incremental;
        boolean central;
        int size = DEFAULT_SIZE,
                pages = 100,
                bytes = 8388608,
                readTimeout = 120000,
                deadline = 180000,
                attempts = 1,
                lookback;

        Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        String[] variables() {
            return new String[] {
                "api.output=" + name,
                "api.resource=" + resource,
                "api.store_host=" + host,
                "api.token=" + CartComPeer.TOKEN,
                "api.site=" + CartComPeer.SITE,
                "api.user=" + CartComPeer.USER,
                "api.secret=" + CartComPeer.SECRET,
                "api.certificate=" + certificate,
                "api.start=" + START,
                "api.end=" + end,
                "api.incremental=" + (incremental == null ? START : incremental),
                "api.lookback=" + lookback,
                "api.page_size=" + size,
                "api.pages=" + pages,
                "api.bytes=" + bytes,
                "api.read_timeout=" + readTimeout,
                "api.deadline=" + deadline,
                "api.attempts=" + attempts
            };
        }
    }

    /**
     * Hosts normal packaged plugins and the Java HTTPS peer in one isolated Zeta worker.
     *
     * <p>Production hostnames resolve to loopback only inside this integration container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER = "/tmp/cartcom-peer";
        private final Path fixtures;
        private String localIdentity = "trusted";
        private final HttpClient checkpoints =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        private Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        /**
         * Passes entry-point variables as argv rather than a logged shell command.
         *
         * <p>This also preserves reserved characters in test credentials and timestamp options.
         */
        @Override
        protected Container.ExecResult executeCommand(
                GenericContainer<?> container, List<String> command)
                throws IOException, InterruptedException {
            return container.execInContainer(command.toArray(new String[0]));
        }

        /**
         * Stages only the owned peer, certificates and existing shaded JSON libraries.
         *
         * <p>The production API alias resolves to loopback only inside this container.
         */
        @Override
        protected void executeExtraCommands(GenericContainer<?> container)
                throws IOException, InterruptedException {
            super.executeExtraCommands(container);
            container.withExtraHost(CartComPeer.HOST, "127.0.0.1");
            container.withExtraHost(CartComPeer.CENTRAL, "127.0.0.1");
            container.withExtraHost("cartcom-trap.invalid", "127.0.0.1");
            for (String name :
                    List.of(
                            "trusted.p12",
                            "wrong-name.p12",
                            "trusted.pem",
                            "untrusted.pem",
                            "wrong-name.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), PEER + "/" + name);
            }
            try {
                stage(container, CartComPeer.class);
                stage(container, CartComRecoveryProbe.class);
                Set<Path> jars = new HashSet<>();
                for (Class<?> type :
                        List.of(ObjectMapper.class, JsonFactory.class, JsonProperty.class)) {
                    Path jar =
                            Path.of(
                                    type.getProtectionDomain()
                                            .getCodeSource()
                                            .getLocation()
                                            .toURI());
                    if (jars.add(jar)) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(jar),
                                PEER + "/libs/" + jar.getFileName());
                    }
                }
            } catch (URISyntaxException failure) {
                throw new IOException("Cannot stage the Java CartCom fixture", failure);
            }
        }

        private static void stage(GenericContainer<?> container, Class<?> type)
                throws IOException, URISyntaxException {
            Path folder = Path.of(type.getResource("").toURI());
            try (DirectoryStream<Path> files =
                    Files.newDirectoryStream(folder, type.getSimpleName() + "*.class")) {
                for (Path file : files) {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(file),
                            PEER
                                    + "/classes/"
                                    + type.getPackage().getName().replace('.', '/')
                                    + "/"
                                    + file.getFileName());
                }
            }
        }

        private Container.ExecResult peer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    PEER + "/classes:" + PEER + "/libs/*",
                    CartComPeer.class.getName(),
                    PEER);
        }

        private Container.ExecResult probe(String mode) throws Exception {
            ContainerUtil.copyAllConnectorJarToContainer(
                    server,
                    getConnectorModulePath(),
                    getConnectorNamePrefix(),
                    getConnectorType(),
                    SEATUNNEL_HOME);
            return server.execInContainer(
                    "java",
                    "-cp",
                    PEER
                            + "/classes:"
                            + PEER
                            + "/libs/*:/tmp/seatunnel/lib/*:/tmp/seatunnel/connectors/*",
                    CartComRecoveryProbe.class.getName(),
                    trust,
                    mode);
        }

        private void install(Path file, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(file), PEER + "/" + name);
        }

        private void touch(String name) throws Exception {
            success(server.execInContainer("touch", PEER + "/" + name));
        }

        private boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", PEER + "/" + name).getExitCode() == 0;
        }

        private List<String> lines(String name) throws Exception {
            if (!exists(name)) {
                return Collections.emptyList();
            }
            Container.ExecResult result = server.execInContainer("cat", PEER + "/" + name);
            success(result);
            return linesOf(result.getStdout());
        }

        private List<String> rows(String name) throws Exception {
            String path = "/tmp/seatunnel_mnt/cart-com-" + name;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + path
                                    + " ]; then find "
                                    + path
                                    + " -maxdepth 1 -type f -name 'receipt_*' -exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            return linesOf(result.getStdout());
        }

        private long completedAcrossPipelines(String id) throws Exception {
            URI uri =
                    new URI(
                            "http",
                            null,
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + id,
                            null,
                            null);
            HttpResponse<String> response =
                    checkpoints.send(
                            HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).build(),
                            BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0L;
            }
            long completed = 0L;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }

        private static List<String> linesOf(String output) {
            List<String> result = new ArrayList<>();
            for (String line : output.split("\\R")) {
                if (!line.isBlank()) {
                    result.add(line);
                }
            }
            return result;
        }
    }
}

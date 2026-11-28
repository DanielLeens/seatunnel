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
package org.apache.seatunnel.e2e.connector.chargedesk;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.chargedesk.ChargeDeskRecoveryProbe;
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
 * Verifies native ChargeDesk exports through the real Java 11 Zeta engine and HTTPS.
 *
 * <p>Independent literal provider fixtures validate every request and compare committed full rows,
 * including exact resource identity and local epoch windows. No fixture reads production resource
 * definitions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ChargeDeskIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String BASE = "/v1";
    private static final String EXTRA =
            ",\"extra\":{\"unicode\":\"数据🌏\",\"nullable\":null,\"array\":[false,null,[1,2]],\"integer\":9223372036854775808,\"decimal\":123456789012345678.12345678900}";
    private static String trust, wrongTrust, wrongNameTrust;
    @TempDir static Path directory;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startsOnlyOwnedTlsAndEngineServices() throws Exception {
        trust = identity("trusted", ChargeDeskPeer.HOST);
        wrongTrust = identity("untrusted", ChargeDeskPeer.HOST);
        wrongNameTrust = identity("wrong-name", "unrelated.invalid");
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::peer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedTrafficAndRestoresTheNormalCertificate() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
        selectIdentity("trusted");
    }

    @AfterAll
    void stopsOnlyOwnedServices() throws Exception {
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

    private static final long START = 1704067200L;
    private static final long END = 1704153600L;
    private static final String TIME = "2024-01-01T00:00:00Z";

    @Test
    void exportsAllFourCollectionsUsingNativeIdentitiesAndCompleteRecords() throws Exception {
        Settings s = new Settings("all-four");
        List<ObjectNode> steps = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (String resource : List.of("charges", "customers", "subscriptions", "products")) {
            String id = resource + "-one",
                    row =
                            record(
                                    resource,
                                    id,
                                    Long.toString(START),
                                    ",\"amount\":\"123456789012345678.123400\",\"transaction_id\":\"different-transaction\",\"gateway_url\":\"https://chargedesk-trap.invalid/private\"");
            steps.add(page(resource, 2, 0, row));
            steps.add(page(resource, 2, 1));
            wanted.add(expected(resource, id, TIME, row));
        }
        program(s.name, steps);
        success(run(s));
        consumed(s.name, 8);
        assertRows(s.name, wanted);
    }

    @Test
    void advancesByRawReturnedCountEvenWhenAShortSubscriptionPageIsEntirelyFiltered()
            throws Exception {
        Settings s = window("filtered-short", "subscriptions");
        String old = record("subscriptions", "old", "100", ""),
                fresh = record("subscriptions", "fresh", Long.toString(START), "");
        program(
                s.name,
                List.of(
                        page("subscriptions", 2, 0, old),
                        page("subscriptions", 2, 1, fresh),
                        page("subscriptions", 2, 2)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(s.name, List.of(expected("subscriptions", "fresh", TIME, fresh)));
    }

    @Test
    void includesStartExcludesEndAndPreservesUnfilteredProductsAcrossTheSameWindow()
            throws Exception {
        Settings s = window("boundaries");
        s.pageSize = 4;
        List<ObjectNode> steps = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (String resource : List.of("charges", "customers", "subscriptions", "products")) {
            String before = record(resource, "before-" + resource, Long.toString(START - 1), ""),
                    start = record(resource, "start-" + resource, "\"" + START + "\"", ""),
                    end = record(resource, "end-" + resource, Long.toString(END), "");
            steps.add(datedPage(resource, 4, 0, before, start, end));
            steps.add(datedPage(resource, 4, 3));
            wanted.add(expected(resource, "start-" + resource, TIME, start));
            if (resource.equals("products")) {
                wanted.add(expected(resource, "before-products", "2023-12-31T23:59:59Z", before));
                wanted.add(expected(resource, "end-products", "2024-01-02T00:00:00Z", end));
            }
        }
        program(s.name, steps);
        success(run(s));
        consumed(s.name, 8);
        assertRows(s.name, wanted);
    }

    @Test
    void keepsUndatedProductRecordsAndUsesNoUnsupportedCreatedFilters() throws Exception {
        Settings s = window("undated-product", "products");
        String row =
                record("products", "product-undated", "null", ",\"first_seen_note\":\"optional\"");
        program(s.name, List.of(page("products", 2, 0, row), page("products", 2, 1)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expected("products", "product-undated", null, row)));
        Settings empty = window("equal-window");
        empty.end = Long.toString(START);
        program(empty.name, List.of(page("products", 2, 0, row), page("products", 2, 1)));
        success(run(empty));
        consumed(empty.name, 2);
        assertRows(empty.name, List.of(expected("products", "product-undated", null, row)));
    }

    @Test
    void representsEpochZeroWithoutSubtractingIntoANegativeNativeFilter() throws Exception {
        Settings s = window("epoch-zero", "charges");
        s.start = "0";
        s.end = "1";
        String row = record("charges", "epoch", "0", "");
        program(
                s.name,
                List.of(
                        page("charges", 2, 0, row)
                                .put("query", "count=2&offset=0&occurred%5Bmax%5D=1"),
                        page("charges", 2, 1)
                                .put("query", "count=2&offset=1&occurred%5Bmax%5D=1")));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expected("charges", "epoch", "1970-01-01T00:00:00Z", row)));
    }

    @Test
    void retainsEverySameSecondRecordAcrossTheMaximumFiveHundredItemPage() throws Exception {
        Settings s = new Settings("same-second", "charges");
        s.pageSize = 500;
        List<String> rows = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (int i = 0; i < 501; i++) {
            String row = record("charges", "C" + i, Long.toString(START), "");
            rows.add(row);
            wanted.add(expected("charges", "C" + i, TIME, row));
        }
        program(
                s.name,
                List.of(
                        page("charges", 500, 0, rows.subList(0, 500).toArray(new String[0])),
                        page("charges", 500, 500, rows.get(500)),
                        page("charges", 500, 501)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(s.name, wanted);
    }

    @Test
    void reachesTheTenThousandOffsetThroughTwentyRealPagesAndConfirmsAnEmptyCap() throws Exception {
        Settings s = new Settings("native-cap", "products");
        s.pageSize = 500;
        List<ObjectNode> steps = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (int offset = 0; offset < 10000; offset += 500) {
            List<String> rows = new ArrayList<>();
            for (int i = offset; i < offset + 500; i++) {
                String row =
                        "{\"product_id\":\"P"
                                + i
                                + "\",\"first_seen\":"
                                + START
                                + ",\"amount\":\"12.30\"}";
                rows.add(row);
                wanted.add(expected("products", "P" + i, TIME, row));
            }
            steps.add(page("products", 500, offset, rows.toArray(new String[0])));
        }
        steps.add(page("products", 500, 10000));
        program(s.name, steps);
        success(run(s));
        consumed(s.name, 21);
        assertRows(s.name, wanted);
    }

    @Test
    void rejectsCountOffsetAndEnvelopeDisagreementsInsteadOfGuessingPagination() throws Exception {
        int i = 0;
        for (String response :
                List.of(
                        "{\"data\":[],\"count\":1,\"offset\":0}",
                        "{\"data\":[],\"count\":0,\"offset\":1}",
                        "{\"data\":[],\"count\":-1,\"offset\":0}",
                        "{\"data\":[],\"count\":0.5,\"offset\":0}",
                        "{\"data\":null,\"count\":0,\"offset\":0}",
                        "{\"data\":[]}")) {
            fails(
                    new Settings("bad-pagination-" + i++, "products"),
                    List.of(get("/v1/products", response).put("query", "count=2&offset=0")),
                    "ChargeDesk");
        }
        String row = record("products", "same", "null", "");
        fails(
                new Settings("duplicate-id", "products"),
                List.of(page("products", 2, 0, row, row)),
                "duplicate");
    }

    @Test
    void rejectsRepeatedNonemptyPagesAndFailsAtConfiguredReadBudgets() throws Exception {
        String row = record("customers", "C1", Long.toString(START), "");
        fails(
                new Settings("repeat-page", "customers"),
                List.of(page("customers", 2, 0, row), page("customers", 2, 1, row)),
                "repeated");
        Settings pages = new Settings("page-budget", "customers");
        pages.pages = 1;
        fails(pages, List.of(page("customers", 2, 0, row)), "max_pages_per_collection");
        Settings scan = new Settings("scan-budget", "charges", "customers");
        scan.scanPages = 1;
        fails(scan, List.of(page("charges", 2, 0)), "max_pages_per_scan");
    }

    @Test
    void validatesIntegralEpochValuesAndPreservesOptionalFullScanNulls() throws Exception {
        Settings optional = new Settings("undated-full", "customers");
        String row = record("customers", "undated", "null", "");
        program(optional.name, List.of(page("customers", 2, 0, row), page("customers", 2, 1)));
        success(run(optional));
        consumed(optional.name, 2);
        assertRows(optional.name, List.of(expected("customers", "undated", null, row)));
        int i = 0;
        for (String time : List.of("1.5", "true", "\"yesterday\"", "9223372036854775807")) {
            fails(
                    new Settings("bad-time-" + i++, "subscriptions"),
                    List.of(page("subscriptions", 2, 0, record("subscriptions", "s1", time, ""))),
                    "ChargeDesk");
        }
        fails(
                window("missing-window-time", "subscriptions"),
                List.of(page("subscriptions", 2, 0, record("subscriptions", "s1", "null", ""))),
                "ChargeDesk");
    }

    @Test
    void reportsAuthenticationPermissionAndProviderFailuresWithoutLeakingBasicCredentials()
            throws Exception {
        for (int status : List.of(401, 403, 404, 500)) {
            fails(
                    new Settings("http-" + status, "products"),
                    List.of(
                            get("/v1/products", "{\"error\":\"private-provider-message\"}")
                                    .put("query", "count=2&offset=0")
                                    .put("status", status)),
                    "HTTP " + status);
        }
        fails(
                new Settings("error-envelope", "products"),
                List.of(
                        get(
                                        "/v1/products",
                                        "{\"error\":\"private-provider-message\",\"count\":0,\"offset\":0,\"data\":[]}")
                                .put("query", "count=2&offset=0")),
                "ChargeDesk");
    }

    @Test
    void honorsRetryAfterAndTheConfiguredAttemptAndDelayBudgets() throws Exception {
        Settings s = new Settings("retry-floor", "products");
        s.retries = 1;
        program(
                s.name,
                List.of(
                        page("products", 2, 0).put("status", 429).put("Retry-After", "2"),
                        page("products", 2, 0)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 2000L);
        Settings exhausted = new Settings("retry-exhausted", "products");
        exhausted.retries = 1;
        ObjectNode unavailable = page("products", 2, 0).put("status", 503);
        fails(exhausted, List.of(unavailable, unavailable), "2 attempts");
        Settings capped = new Settings("retry-cap", "products");
        capped.retries = 1;
        capped.retryDelay = 250;
        fails(
                capped,
                List.of(page("products", 2, 0).put("status", 429).put("Retry-After", "3")),
                "max_retry_delay_ms");
    }

    @Test
    void pacesDifferentCollectionsWhileKeepingEverySuccessfulResponse() throws Exception {
        Settings s = new Settings("pace", "charges", "customers");
        s.interval = 600;
        program(s.name, List.of(page("charges", 2, 0), page("customers", 2, 0)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 600L);
    }

    @Test
    void verifiesBothTrustAndHostnameAndNeverForwardsBasicAuthThroughARedirect() throws Exception {
        Settings ca = new Settings("wrong-ca", "products");
        ca.trust = wrongTrust;
        fails(ca, List.of(), "certificate");
        selectIdentity("wrong-name");
        try {
            Settings san = new Settings("wrong-san", "products");
            san.trust = wrongNameTrust;
            fails(san, List.of(), "certificate");
        } finally {
            selectIdentity("trusted");
        }
        fails(
                new Settings("redirect", "products"),
                List.of(page("products", 2, 0).put("status", 302).put("mode", "redirect")),
                "HTTP 302");
    }

    @Test
    void acceptsGzipAndBoundsCompressedAndDecodedPayloads() throws Exception {
        Settings s = new Settings("gzip", "products");
        String row = record("products", "P1", "null", "");
        program(
                s.name,
                List.of(page("products", 2, 0, row).put("mode", "gzip"), page("products", 2, 1)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expected("products", "P1", null, row)));
        for (String mode : List.of("json", "gzip")) {
            Settings small = new Settings("body-" + mode, "products");
            small.bytes = 256;
            fails(
                    small,
                    List.of(
                            get(
                                            "/v1/products",
                                            "{\"count\":0,\"offset\":0,\"data\":[],\"padding\":\""
                                                    + "x".repeat(5000)
                                                    + "\"}")
                                    .put("query", "count=2&offset=0")
                                    .put("mode", mode)),
                    "max_response_bytes");
        }
        fails(
                new Settings("encoding", "products"),
                List.of(page("products", 2, 0).put("encoding", "br")),
                "encoding");
    }

    @Test
    void rejectsMalformedJsonUtf8AndNonrecordElementsBeforePublication() throws Exception {
        int i = 0;
        for (String body :
                List.of(
                        "null",
                        "{\"data\":[null],\"count\":1,\"offset\":0}",
                        "{\"data\":[{\"product_id\":1}],\"count\":1,\"offset\":0}",
                        "{\"data\":[],\"count\":0,\"offset\":0}{}",
                        "{\"data\":[],\"data\":[],\"count\":0,\"offset\":0}")) {
            Settings s = new Settings("json-" + i++, "products");
            fails(
                    s,
                    List.of(get("/v1/products", body).put("query", "count=2&offset=0")),
                    "ChargeDesk");
            assertRows(s.name, List.of());
        }
        fails(
                new Settings("utf8", "products"),
                List.of(
                        get("/v1/products", "")
                                .put("query", "count=2&offset=0")
                                .put(
                                        "bytes",
                                        Base64.getEncoder()
                                                .encodeToString(new byte[] {(byte) 0xc3, 0x28}))),
                "ChargeDesk");
    }

    @Test
    void rejectsInvalidOptionsBeforeExposingTheAuthenticationHeader() throws Exception {
        fails(new Settings("bad-resource", "unknown"), List.of(), "resources");
        fails(new Settings("repeated-selection", "products", "products"), List.of(), "resources");
        Settings size = new Settings("bad-size", "products");
        size.pageSize = 501;
        fails(size, List.of(), "page_size");
        Settings token = new Settings("bad-token", "products");
        token.token = "bad:secret";
        fails(token, List.of(), "token");
        Settings reversed = window("reversed", "charges");
        reversed.start = Long.toString(END);
        reversed.end = Long.toString(START);
        fails(reversed, List.of(), "ChargeDesk");
    }

    @Test
    void boundsATricklingBodyAndClosesItsSocketWithinTheDeadline() throws Exception {
        Settings s = new Settings("deadline", "products");
        s.request = 2000;
        s.operation = 3000;
        fails(s, List.of(page("products", 2, 0).put("mode", "trickle")), "ChargeDesk");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(s.name + ".disconnect"));
        long start = Long.parseLong(engine.lines(s.name + ".started").get(0)),
                end = Long.parseLong(engine.lines(s.name + ".disconnect").get(0));
        assertTrue(end - start < 6000L);
        assertRows(s.name, List.of());
    }

    @Test
    void cancelsTheActualEngineJobWithoutPublishingPartialData() throws Exception {
        Settings s = new Settings("cancel", "products");
        program(s.name, List.of(page("products", 2, 0).put("mode", "trickle")));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/chargedesk.conf", id, s.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".started"));
            success(engine.cancelJob(id));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".disconnect"));
            consumed(s.name, 1);
            assertRows(s.name, List.of());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(id);
                running.cancel(true);
            }
        }
    }

    @Test
    void restoresOnlyTheUnfinishedOffsetAfterThePreviousPageCommits() throws Exception {
        Settings s = new Settings("offset-savepoint", "customers");
        String first = record("customers", "C1", Long.toString(START), ""),
                second = record("customers", "C2", Long.toString(START), "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                page("customers", 2, 0, first),
                                page("customers", 2, 1, second).put("mode", "partial")));
        program(
                "offset-restored",
                List.of(page("customers", 2, 1, second), page("customers", 2, 2)));
        success(engine.restoreJob("/chargedesk.conf", id, s.variables()));
        consumed("offset-restored", 2);
        assertRows(
                s.name,
                List.of(
                        expected("customers", "C1", TIME, first),
                        expected("customers", "C2", TIME, second)));
    }

    @Test
    void restoresBetweenCollectionsWithoutReissuingTheCompletedChargeScan() throws Exception {
        Settings s = new Settings("between-savepoint", "charges", "customers");
        String first = record("charges", "C1", Long.toString(START), ""),
                second = record("customers", "U1", Long.toString(START), "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                page("charges", 2, 0, first),
                                page("charges", 2, 1),
                                page("customers", 2, 0, second).put("mode", "partial")));
        program(
                "between-restored",
                List.of(page("customers", 2, 0, second), page("customers", 2, 1)));
        success(engine.restoreJob("/chargedesk.conf", id, s.variables()));
        consumed("between-restored", 2);
        assertRows(
                s.name,
                List.of(
                        expected("charges", "C1", TIME, first),
                        expected("customers", "U1", TIME, second)));
    }

    @Test
    void restoresATerminalSourceWithoutAnotherAuthenticatedRequestOrDuplicateFile()
            throws Exception {
        Settings s = new Settings("terminal-savepoint", "products");
        String row = record("products", "P1", "null", "");
        List<JsonNode> wanted = List.of(expected("products", "P1", null, row));
        program(s.name, List.of(page("products", 2, 0, row), page("products", 2, 1)));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/chargedesk-terminal.conf", id, s.variables()));
        try {
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(s.name, wanted));
            success(engine.savepointJob(id));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(id);
                running.cancel(true);
            }
        }
        consumed(s.name, 2);
        program("terminal-restored", List.of());
        Future<Container.ExecResult> resumed =
                tasks.submit(
                        () -> engine.restoreJob("/chargedesk-terminal.conf", id, s.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(id)));
            long baseline = engine.completedAcrossPipelines(id);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(id) > baseline);
            consumed("terminal-restored", 0);
            assertRows(s.name, wanted);
        } finally {
            if (!resumed.isDone()) {
                engine.cancelJob(id);
            }
            resumed.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void serializes32Of33RowsAndRejectsChangesEvenToFilteredProviderRecords() throws Exception {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < 33; i++) {
            rows.add(
                    record(
                            "subscriptions",
                            "S" + i,
                            Long.toString(START),
                            ",\"description\":\"synthetic-response-record\""));
        }
        rows.add(record("subscriptions", "filtered", "100", ""));
        for (String mode : List.of("resume", "changed", "changed-filtered")) {
            List<String> replay = new ArrayList<>(rows);
            if (mode.equals("changed")) {
                replay.set(0, replay.get(0).replace("S0", "CHANGED"));
            }
            if (mode.equals("changed-filtered")) {
                replay.set(33, replay.get(33).replace("filtered", "CHANGED"));
            }
            List<ObjectNode> steps =
                    new ArrayList<>(
                            List.of(
                                    page("subscriptions", 50, 0, rows.toArray(new String[0])),
                                    page("subscriptions", 50, 0, replay.toArray(new String[0]))));
            if (mode.equals("resume")) {
                steps.add(page("subscriptions", 50, 34));
            }
            String name = "reader-" + mode;
            program(name, steps);
            success(engine.probe(mode));
            consumed(name, steps.size());
        }
        program("reader-zero", List.of());
        success(engine.probe("zero"));
        consumed("reader-zero", 0);
    }

    @Test
    void rejectsNonemptyNativeCeilingsAndClampsTheFinalApproachWithoutRequestingBeyondThem()
            throws Exception {
        for (String resource : List.of("products", "charges")) {
            int cap = resource.equals("products") ? 10000 : 50000;
            for (boolean nonempty : List.of(false, true)) {
                String mode =
                        "cap-"
                                + (resource.equals("charges") ? "charge-" : "")
                                + (nonempty ? "nonempty" : "empty");
                ObjectNode response =
                        nonempty
                                ? page(
                                        resource,
                                        500,
                                        cap,
                                        record(resource, "at-cap", Long.toString(START), ""))
                                : page(resource, 500, cap);
                program(mode, List.of(response));
                success(engine.probe(mode));
                consumed(mode, 1);
            }
        }
        program(
                "cap-clamp",
                List.of(
                        page(
                                "products",
                                1,
                                9999,
                                record("products", "last", Long.toString(START), "")),
                        page("products", 500, 10000)));
        success(engine.probe("cap-clamp"));
        consumed("cap-clamp", 2);
    }

    private String saveBlocked(Settings settings, List<ObjectNode> steps) throws Exception {
        program(settings.name, steps);
        String id = jobId();
        String config = settings.dated ? "/chargedesk-window.conf" : "/chargedesk.conf";
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(config, id, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".started"));
            long baseline = engine.getCompletedCheckpointCount(id);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(id) > baseline);
            success(engine.savepointJob(id));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(settings.name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(id);
                running.cancel(true);
            }
        }
        consumed(settings.name, steps.size());
        return id;
    }

    private static Settings window(String name, String... resources) {
        Settings s = new Settings(name, resources);
        s.dated = true;
        s.start = Long.toString(START);
        s.end = Long.toString(END);
        return s;
    }

    private void delayAtLeast(String name, long millis) throws Exception {
        List<String> times = engine.lines(name + ".times");
        assertTrue(
                Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= millis,
                times.toString());
    }

    private static String record(String resource, String id, String time, String extra) {
        String identity =
                resource.equals("charges")
                        ? "charge_id"
                        : resource.equals("customers")
                                ? "customer_id"
                                : resource.equals("subscriptions")
                                        ? "subscription_id"
                                        : "product_id";
        String timestamp = resource.equals("charges") ? "occurred" : "first_seen";
        return "{\"" + identity + "\":\"" + id + "\",\"" + timestamp + "\":" + time + extra + EXTRA
                + "}";
    }

    private static ObjectNode page(String resource, int requested, int offset, String... records) {
        String body =
                "{\"count\":"
                        + records.length
                        + ",\"offset\":"
                        + offset
                        + ",\"data\":"
                        + array(records)
                        + "}";
        return get("/v1/" + resource, body)
                .put("query", "count=" + requested + "&offset=" + offset);
    }

    private static ObjectNode datedPage(String resource, int count, int offset, String... records) {
        ObjectNode step = page(resource, count, offset, records);
        if (resource.equals("charges") || resource.equals("customers")) {
            String field = resource.equals("charges") ? "occurred" : "first_seen";
            step.put(
                    "query",
                    step.path("query").asText()
                            + "&"
                            + field
                            + "%5Bmin%5D="
                            + (START - 1)
                            + "&"
                            + field
                            + "%5Bmax%5D="
                            + END);
        }
        return step;
    }

    private static String array(String... records) {
        return "[" + String.join(",", records) + "]";
    }

    private static ObjectNode get(String path, String response) {
        return JSON.createObjectNode().put("path", path).put("query", "").put("response", response);
    }

    private static JsonNode expected(String resource, String id, String eventTime, String payload)
            throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put("id", id)
                        .put("event_time", eventTime);
        row.set("record", JSON.readTree(payload));
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>(), wanted = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(parsed.isObject() && parsed.size() == 4, line);
            assertTrue(parsed.path("record").isTextual(), line);
            ObjectNode row = (ObjectNode) parsed;
            row.set("record", JSON.readTree(parsed.path("record").textValue()));
            actual.merge(row, 1, Integer::sum);
        }
        expected.forEach(row -> wanted.merge(row, 1, Integer::sum));
        assertEquals(wanted, actual, name);
    }

    private void program(String name, List<ObjectNode> steps) throws Exception {
        ArrayNode script = JSON.createArrayNode();
        steps.forEach(script::add);
        Path file = Files.createTempFile(directory, "script-", ".json");
        Files.writeString(file, script.toString());
        engine.install(file, name + ".json");
        Path selected = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(selected, name);
        engine.install(selected, "active");
        active = name;
    }

    private void selectIdentity(String identity) throws Exception {
        if (identity.equals("trusted") && !engine.exists("identity")) {
            return;
        }
        Path selected = Files.createTempFile(directory, "identity-", ".txt");
        Files.writeString(selected, identity);
        engine.install(selected, "identity");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.lines("identity-ready").equals(List.of(identity)));
    }

    private void consumed(String name, int count) throws Exception {
        assertFalse(engine.exists(name + ".invalid"), name);
        assertEquals(count, engine.lines(name + ".requests").size(), name);
    }

    private Container.ExecResult run(Settings settings) throws Exception {
        return engine.executeJob(
                settings.dated ? "/chargedesk-window.conf" : "/chargedesk.conf",
                jobId(),
                settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(ChargeDeskPeer.TOKEN), "API key leaked");
        assertFalse(
                output.contains(
                        "Basic "
                                + Base64.getEncoder()
                                        .encodeToString(
                                                (ChargeDeskPeer.TOKEN + ":")
                                                        .getBytes(StandardCharsets.UTF_8))),
                "Basic credential leaked");
        assertFalse(output.contains("private-provider-message"), "provider payload leaked");
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
                                "SAN=dns:" + hostname,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                ChargeDeskPeer.PASSWORD,
                                "-keypass",
                                ChargeDeskPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, ChargeDeskPeer.PASSWORD.toCharArray());
        }
        Files.writeString(
                directory.resolve(name + ".pem"),
                "-----BEGIN CERTIFICATE-----\n"
                        + Base64.getMimeEncoder(64, new byte[] {'\n'})
                                .encodeToString(keys.getCertificate("api").getEncoded())
                        + "\n-----END CERTIFICATE-----\n");
        return Base64.getEncoder().encodeToString(keys.getCertificate("api").getEncoded());
    }

    /**
     * Synthetic job settings with explicit paging and transport limits.
     *
     * <p>Each invocation passes a fresh argument array to the real engine container.
     */
    private static final class Settings {
        final String name;
        final String[] resources;
        String trust = ChargeDeskIT.trust;
        String token = ChargeDeskPeer.TOKEN;
        int request = 60000;
        int operation = 70000;
        int retries;
        int interval;
        int retryDelay = 60000;
        int bytes = 8388608;
        int pages = 10000;
        int scanPages = 100000;
        String start = "unused", end = "unused";
        boolean dated;
        int pageSize = 2;

        Settings(String name, String... resources) {
            this.name = name;
            this.resources = resources;
        }

        String[] variables() {
            ArrayNode selection = JSON.createArrayNode();
            for (String resource : resources) {
                selection.add(resource);
            }
            List<String> values =
                    new ArrayList<>(
                            List.of(
                                    "api.output=" + name,
                                    "api.resources=" + selection,
                                    "api.token=" + token,
                                    "api.certificate=" + trust,
                                    "api.request_timeout=" + request,
                                    "api.operation_timeout=" + operation,
                                    "api.retries=" + retries,
                                    "api.retry_delay=" + retryDelay,
                                    "api.bytes=" + bytes,
                                    "api.pages=" + pages,
                                    "api.scan_pages=" + scanPages,
                                    "api.start=" + start,
                                    "api.end=" + end,
                                    "api.interval=" + interval,
                                    "api.page_size=" + pageSize));
            return values.toArray(new String[0]);
        }
    }

    /**
     * Hosts normal packaged plugins and independent Java TLS fixtures in one owned worker.
     *
     * <p>DNS and certificate trust overrides apply only to this integration-test container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER = "/tmp/chargedesk-peer";
        private final Path fixtures;
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
            container.withExtraHost(ChargeDeskPeer.HOST, "127.0.0.1");
            container.withExtraHost("chargedesk-trap.invalid", "127.0.0.1");
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
                stage(container, ChargeDeskPeer.class);
                stage(container, ChargeDeskRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java ChargeDesk fixture", failure);
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
                    ChargeDeskPeer.class.getName(),
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
                    ChargeDeskRecoveryProbe.class.getName(),
                    PEER + "/trusted.pem",
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
            String path = "/tmp/seatunnel_mnt/chargedesk-" + name;
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

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
package org.apache.seatunnel.e2e.connector.churnkey;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeyRecoveryProbe;
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
 * Verifies native Churnkey reads through the real Java 11 Zeta engine and HTTPS.
 *
 * <p>Independent literal provider fixtures assert both secret headers, manual session offsets and
 * unpaged grouped counts before comparing committed complete rows.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ChurnkeyIT extends TestSuiteBase {
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
        trust = identity("trusted", ChurnkeyPeer.HOST);
        wrongTrust = identity("untrusted", ChurnkeyPeer.HOST);
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

    private static final String TIME = "2024-02-29T00:00:00Z";

    @Test
    void exportsBothResourcesWithoutInventingIdentifiersForGroupedCounts() throws Exception {
        Settings s = new Settings("both");
        String row =
                session(
                        "s-one",
                        TIME,
                        ",\"customer\":{\"id\":\"cus-one\",\"planPrice\":9223372036854775808,\"currency\":\"usd\"},\"presentedOffers\":[{\"accepted\":false}]");
        String group =
                group(
                        "2024-02",
                        "9007199254740993",
                        ",\"billingInterval\":\"MONTH\",\"surveyResponse\":\"昂贵🌏\"");
        program(
                s.name,
                List.of(
                        sessions(2, 0, false, row),
                        sessions(2, 1, false),
                        aggregation(false, group)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name,
                List.of(
                        expected("sessions", "s-one", TIME, row),
                        expected("session-aggregation", null, "2024-02", group)));
    }

    @Test
    void readsPastTheOneHundredSessionDefaultAndPreservesTiedCreationTimes() throws Exception {
        Settings s = new Settings("one-hundred", "sessions");
        s.pageSize = 100;
        List<String> rows = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            String row = session("S" + i, TIME, "");
            rows.add(row);
            wanted.add(expected("sessions", "S" + i, TIME, row));
        }
        program(
                s.name,
                List.of(
                        sessions(100, 0, false, rows.subList(0, 100).toArray(new String[0])),
                        sessions(100, 100, false, rows.get(100)),
                        sessions(100, 101, false)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(s.name, wanted);
    }

    @Test
    void advancesSkipByRawShortPageSizeAndRequiresAnEmptyTerminalPage() throws Exception {
        Settings s = new Settings("short", "sessions");
        String first = session("first", TIME, ""), last = session("last", null, "");
        program(
                s.name,
                List.of(
                        sessions(2, 0, false, first),
                        sessions(2, 1, false, last),
                        sessions(2, 2, false)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name,
                List.of(
                        expected("sessions", "first", TIME, first),
                        expected("sessions", "last", null, last)));
    }

    @Test
    void forwardsBothBouncedPoliciesToSessionAndAggregationReads() throws Exception {
        for (boolean bounced : List.of(false, true)) {
            Settings s = new Settings("bounced-" + bounced);
            s.bounced = bounced;
            String row = session("native", TIME, ",\"bounced\":" + bounced),
                    total = group(null, "0", ",\"saveType\":null");
            program(
                    s.name,
                    List.of(
                            sessions(2, 0, bounced, row),
                            sessions(2, 1, bounced),
                            aggregation(bounced, total)));
            success(run(s));
            consumed(s.name, 3);
            assertRows(
                    s.name,
                    List.of(
                            expected("sessions", "native", TIME, row),
                            expected("session-aggregation", null, null, total)));
        }
    }

    @Test
    void encodesOrderedBreakdownsWithoutUsingSessionPagingForGroupedResults() throws Exception {
        Settings s = new Settings("dimensions", "session-aggregation");
        s.breakdown = new String[] {"month", "surveyResponse", "billingInterval"};
        String row =
                group("2024-02", "70", ",\"surveyResponse\":null,\"billingInterval\":\"YEAR\"");
        program(
                s.name,
                List.of(
                        aggregation(false, row)
                                .put(
                                        "query",
                                        "bounced=false&breakdown=month-surveyResponse-billingInterval")));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("session-aggregation", null, "2024-02", row)));
    }

    @Test
    void preservesIdenticalGroupedRowsAndOptionalMonthsAsANativeMultiset() throws Exception {
        Settings s = new Settings("no-group-id", "session-aggregation");
        String row =
                group(
                        null,
                        "2",
                        ",\"_id\":\"not-an-entity-key\",\"planId\":\"native\",\"trial\":false");
        program(s.name, List.of(aggregation(false, row, row)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(
                s.name,
                List.of(
                        expected("session-aggregation", null, null, row),
                        expected("session-aggregation", null, null, row)));
    }

    @Test
    void normalizesCreatedAtOffsetsWhileKeepingTheOriginalPayloadAndNulls() throws Exception {
        Settings s = new Settings("offset", "sessions");
        String
                row =
                        session(
                                "offset",
                                "2024-02-29T08:00:00+08:00",
                                ",\"updatedAt\":\"2024-03-01T00:00:00Z\""),
                nil = session("null", null, "");
        program(s.name, List.of(sessions(2, 0, false, row, nil), sessions(2, 2, false)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(
                s.name,
                List.of(
                        expected("sessions", "offset", TIME, row),
                        expected("sessions", "null", null, nil)));
    }

    @Test
    void permitsTheDocumentedTenThousandPageSizeAndRejectsLargerValuesBeforeHttp()
            throws Exception {
        Settings s = new Settings("max-size", "sessions");
        s.pageSize = 10000;
        String row = session("one", TIME, "");
        program(s.name, List.of(sessions(10000, 0, false, row), sessions(10000, 1, false)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expected("sessions", "one", TIME, row)));
        Settings bad = new Settings("too-large", "sessions");
        bad.pageSize = 10001;
        fails(bad, List.of(), "page_size");
    }

    @Test
    void rejectsRepeatedNonemptyPagesAndFiniteCollectionAndScanBudgets() throws Exception {
        String row = session("same", TIME, "");
        fails(
                new Settings("repeat", "sessions"),
                List.of(sessions(2, 0, false, row), sessions(2, 1, false, row)),
                "Churnkey");
        Settings pages = new Settings("page-budget", "sessions");
        pages.pages = 1;
        fails(pages, List.of(sessions(2, 0, false, row)), "max_pages_per_collection");
        Settings scan = new Settings("scan-budget");
        scan.scanPages = 1;
        fails(scan, List.of(sessions(2, 0, false)), "max_pages_per_scan");
    }

    @Test
    void rejectsOversizedResponsesAndInvalidGroupCountsAndMonthShapes() throws Exception {
        Settings small = new Settings("unpaged-count", "session-aggregation");
        small.items = 1;
        fails(
                small,
                List.of(aggregation(false, group(null, "1", ""), group("2024-01", "2", ""))),
                "Churnkey");
        fails(
                new Settings("over-page", "sessions"),
                List.of(
                        sessions(
                                2,
                                0,
                                false,
                                session("1", null, ""),
                                session("2", null, ""),
                                session("3", null, ""))),
                "Churnkey");
        int i = 0;
        for (String row :
                List.of(
                        group(null, "-1", ""),
                        group(null, "1.5", ""),
                        "{\"month\":\"2024-01\"}",
                        group("2024-13", "1", ""),
                        group("2024-2", "1", ""))) {
            fails(
                    new Settings("bad-group-" + i++, "session-aggregation"),
                    List.of(aggregation(false, row)),
                    "Churnkey");
        }
    }

    @Test
    void rejectsInvalidNativeSessionIdsDatesAndDuplicateSessionRecords() throws Exception {
        int i = 0;
        for (String row :
                List.of(
                        "{\"_id\":12}",
                        "{\"_id\":\"\"}",
                        "{\"createdAt\":\"" + TIME + "\"}",
                        session("bad", "not-a-date", ""))) {
            fails(
                    new Settings("bad-session-" + i++, "sessions"),
                    List.of(sessions(2, 0, false, row)),
                    "Churnkey");
        }
        String row = session("same", TIME, "");
        fails(
                new Settings("duplicate-id", "sessions"),
                List.of(sessions(2, 0, false, row, row)),
                "duplicate");
    }

    @Test
    void reportsPermissionAndProviderErrorsWithoutLeakingEitherCredential() throws Exception {
        for (int status : List.of(401, 403, 404, 500)) {
            fails(
                    new Settings("http-" + status, "sessions"),
                    List.of(
                            sessions(2, 0, false)
                                    .put("status", status)
                                    .put("response", "{\"error\":\"private-provider-message\"}")),
                    "HTTP " + status);
        }
        fails(
                new Settings("provider-error", "sessions"),
                List.of(
                        get("/v1/data/sessions", "{\"error\":\"private-provider-message\"}")
                                .put("query", "limit=2&skip=0&bounced=false")),
                "Churnkey");
    }

    @Test
    void verifiesCertificateTrustAndHostnameAndRefusesRedirectCredentialForwarding()
            throws Exception {
        Settings ca = new Settings("wrong-ca", "sessions");
        ca.trust = wrongTrust;
        fails(ca, List.of(), "certificate");
        selectIdentity("wrong-name");
        try {
            Settings san = new Settings("wrong-san", "sessions");
            san.trust = wrongNameTrust;
            fails(san, List.of(), "certificate");
        } finally {
            selectIdentity("trusted");
        }
        fails(
                new Settings("redirect", "sessions"),
                List.of(sessions(2, 0, false).put("status", 302).put("mode", "redirect")),
                "HTTP 302");
    }

    @Test
    void acceptsGzipAndBoundsBothWireAndExpandedResponseBodies() throws Exception {
        Settings s = new Settings("gzip", "sessions");
        String row = session("one", TIME, "");
        program(
                s.name,
                List.of(sessions(2, 0, false, row).put("mode", "gzip"), sessions(2, 1, false)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expected("sessions", "one", TIME, row)));
        for (String mode : List.of("json", "gzip")) {
            Settings small = new Settings("body-" + mode, "sessions");
            small.bytes = 256;
            fails(
                    small,
                    List.of(
                            sessions(
                                            2,
                                            0,
                                            false,
                                            session(
                                                    "one",
                                                    TIME,
                                                    ",\"padding\":\"" + "x".repeat(5000) + "\""))
                                    .put("mode", mode)),
                    "max_response_bytes");
        }
        fails(
                new Settings("encoding", "sessions"),
                List.of(sessions(2, 0, false).put("encoding", "br")),
                "encoding");
    }

    @Test
    void rejectsMalformedJsonUtf8AndNonrecordArrayElementsBeforePublication() throws Exception {
        int i = 0;
        for (String body :
                List.of("null", "{}", "[null]", "[]{}", "[{\"_id\":\"one\",\"_id\":\"two\"}]")) {
            Settings s = new Settings("json-" + i++, "sessions");
            fails(
                    s,
                    List.of(
                            get("/v1/data/sessions", body)
                                    .put("query", "limit=2&skip=0&bounced=false")),
                    "Churnkey");
            assertRows(s.name, List.of());
        }
        fails(
                new Settings("utf8", "sessions"),
                List.of(
                        sessions(2, 0, false)
                                .put(
                                        "bytes",
                                        Base64.getEncoder()
                                                .encodeToString(new byte[] {(byte) 0xc3, 0x28}))),
                "Churnkey");
    }

    @Test
    void honorsRetryAfterAndEnforcesRetryAndDelayCeilings() throws Exception {
        Settings s = new Settings("retry", "sessions");
        s.retries = 1;
        program(
                s.name,
                List.of(
                        sessions(2, 0, false).put("status", 429).put("Retry-After", "2"),
                        sessions(2, 0, false)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 2000);
        Settings exhausted = new Settings("retry-exhausted", "sessions");
        exhausted.retries = 1;
        ObjectNode bad = sessions(2, 0, false).put("status", 503);
        fails(exhausted, List.of(bad, bad), "2 attempts");
        Settings cap = new Settings("retry-cap", "sessions");
        cap.retries = 1;
        cap.retryDelay = 250;
        fails(
                cap,
                List.of(sessions(2, 0, false).put("status", 429).put("Retry-After", "3")),
                "max_retry_delay_ms");
    }

    @Test
    void pacesDifferentResourcesAndNeverAddsSessionOffsetsToAggregation() throws Exception {
        Settings s = new Settings("pace");
        s.interval = 600;
        program(s.name, List.of(sessions(2, 0, false), aggregation(false)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 600);
    }

    @Test
    void rejectsInvalidSelectionsDimensionsAndHeaderValuesBeforeHttp() throws Exception {
        fails(new Settings("unknown", "unknown"), List.of(), "resources");
        fails(new Settings("duplicate", "sessions", "sessions"), List.of(), "resources");
        Settings unknown = new Settings("unknown-dimension", "session-aggregation");
        unknown.breakdown = new String[] {"unknown"};
        fails(unknown, List.of(), "breakdown");
        Settings duplicate = new Settings("duplicate-dimension", "session-aggregation");
        duplicate.breakdown = new String[] {"month", "month"};
        fails(duplicate, List.of(), "breakdown");
        Settings token = new Settings("bad-token", "sessions");
        token.token = " ";
        fails(token, List.of(), "token");
        Settings app = new Settings("bad-app", "sessions");
        app.app = " ";
        fails(app, List.of(), "access_key");
    }

    @Test
    void boundsContinuousBodyTrafficAndClosesItsSocketAtTheDeadline() throws Exception {
        Settings s = new Settings("deadline", "sessions");
        s.request = 2000;
        s.operation = 3000;
        fails(s, List.of(sessions(2, 0, false).put("mode", "trickle")), "Churnkey");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(s.name + ".disconnect"));
        long start = Long.parseLong(engine.lines(s.name + ".started").get(0)),
                end = Long.parseLong(engine.lines(s.name + ".disconnect").get(0));
        assertTrue(end - start < 6000);
        assertRows(s.name, List.of());
    }

    @Test
    void cancelsTheActualEngineJobWithoutPublishingAPartialResponse() throws Exception {
        Settings s = new Settings("cancel", "sessions");
        program(s.name, List.of(sessions(2, 0, false).put("mode", "trickle")));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/churnkey.conf", id, s.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".started"));
            success(engine.cancelJob(id));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".disconnect"));
            assertRows(s.name, List.of());
            consumed(s.name, 1);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(id);
                running.cancel(true);
            }
        }
    }

    @Test
    void restoresAnUnfinishedOffsetWithoutReadingTheCommittedFirstPageAgain() throws Exception {
        Settings s = new Settings("offset-savepoint", "sessions");
        String first = session("first", TIME, ""), second = session("second", TIME, "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                sessions(2, 0, false, first),
                                sessions(2, 1, false, second).put("mode", "partial")));
        program("offset-restored", List.of(sessions(2, 1, false, second), sessions(2, 2, false)));
        success(engine.restoreJob("/churnkey.conf", id, s.variables()));
        consumed("offset-restored", 2);
        assertRows(
                s.name,
                List.of(
                        expected("sessions", "first", TIME, first),
                        expected("sessions", "second", TIME, second)));
    }

    @Test
    void restoresBetweenResourcesWithoutReissuingTheCompletedSessionScan() throws Exception {
        Settings s = new Settings("between-savepoint");
        String row = session("native", TIME, ""), total = group("2024-02", "0", "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                sessions(2, 0, false, row),
                                sessions(2, 1, false),
                                aggregation(false, total).put("mode", "partial")));
        program("between-restored", List.of(aggregation(false, total)));
        success(engine.restoreJob("/churnkey.conf", id, s.variables()));
        consumed("between-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expected("sessions", "native", TIME, row),
                        expected("session-aggregation", null, "2024-02", total)));
    }

    @Test
    void restoresATerminalSourceWithoutAuthenticatedTrafficOrDuplicateCommittedRows()
            throws Exception {
        Settings s = new Settings("terminal-savepoint", "session-aggregation");
        String row = group(null, "2", ",\"trial\":true");
        List<JsonNode> wanted = List.of(expected("session-aggregation", null, null, row));
        program(s.name, List.of(aggregation(false, row)));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/churnkey-terminal.conf", id, s.variables()));
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
        consumed(s.name, 1);
        program("terminal-restored", List.of());
        Future<Container.ExecResult> resumed =
                tasks.submit(() -> engine.restoreJob("/churnkey-terminal.conf", id, s.variables()));
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
    void serializes32Of33SessionsAndRejectsChangesBeforePublishingTheSuffix() throws Exception {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < 33; i++) {
            rows.add(session("S" + i, TIME, ",\"description\":\"synthetic-response-record\""));
        }
        for (String mode : List.of("resume", "changed")) {
            List<String> replay = new ArrayList<>(rows);
            if (mode.equals("changed")) {
                replay.set(0, replay.get(0).replace("S0", "CHANGED"));
            }
            List<ObjectNode> steps =
                    new ArrayList<>(
                            List.of(
                                    sessions(50, 0, false, rows.toArray(new String[0])),
                                    sessions(50, 0, false, replay.toArray(new String[0]))));
            if (mode.equals("resume")) {
                steps.add(sessions(50, 33, false));
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

    private String saveBlocked(Settings s, List<ObjectNode> steps) throws Exception {
        program(s.name, steps);
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/churnkey.conf", id, s.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".started"));
            long baseline = engine.getCompletedCheckpointCount(id);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(id) > baseline);
            success(engine.savepointJob(id));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(s.name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(id);
                running.cancel(true);
            }
        }
        consumed(s.name, steps.size());
        return id;
    }

    private void delayAtLeast(String name, long millis) throws Exception {
        List<String> times = engine.lines(name + ".times");
        assertTrue(
                Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= millis,
                times.toString());
    }

    private static String session(String id, String time, String extra) {
        return "{\"_id\":\""
                + id
                + "\",\"createdAt\":"
                + (time == null ? "null" : "\"" + time + "\"")
                + extra
                + EXTRA
                + "}";
    }

    private static String group(String month, String count, String extra) {
        return "{\"count\":"
                + count
                + (month == null ? "" : ",\"month\":\"" + month + "\"")
                + extra
                + EXTRA
                + "}";
    }

    private static ObjectNode sessions(int limit, long skip, boolean bounced, String... rows) {
        return get("/v1/data/sessions", array(rows))
                .put("query", "limit=" + limit + "&skip=" + skip + "&bounced=" + bounced);
    }

    private static ObjectNode aggregation(boolean bounced, String... rows) {
        return get("/v1/data/session-aggregation", array(rows)).put("query", "bounced=" + bounced);
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
        return engine.executeJob("/churnkey.conf", jobId(), settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(ChurnkeyPeer.TOKEN), "data API key leaked");
        assertFalse(output.contains(ChurnkeyPeer.APP), "app key leaked");
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
                                ChurnkeyPeer.PASSWORD,
                                "-keypass",
                                ChurnkeyPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, ChurnkeyPeer.PASSWORD.toCharArray());
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
        String trust = ChurnkeyIT.trust;
        String token = ChurnkeyPeer.TOKEN;
        int request = 60000;
        int operation = 70000;
        int retries;
        int interval;
        int retryDelay = 60000;
        int bytes = 8388608;
        int pages = 10000;
        int scanPages = 100000;
        String app = ChurnkeyPeer.APP;
        boolean bounced;
        String[] breakdown = new String[0];
        int items = 10000;
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
            ArrayNode dimensions = JSON.createArrayNode();
            for (String field : breakdown) {
                dimensions.add(field);
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
                                    "api.items=" + items,
                                    "api.app=" + app,
                                    "api.bounced=" + bounced,
                                    "api.breakdown=" + dimensions,
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
        private static final String PEER = "/tmp/churnkey-peer";
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
            container.withExtraHost(ChurnkeyPeer.HOST, "127.0.0.1");
            container.withExtraHost("churnkey-trap.invalid", "127.0.0.1");
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
                stage(container, ChurnkeyPeer.class);
                stage(container, ChurnkeyRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java Churnkey fixture", failure);
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
                    ChurnkeyPeer.class.getName(),
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
                    ChurnkeyRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/churnkey-" + name;
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

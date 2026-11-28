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
package org.apache.seatunnel.e2e.connector.chameleon;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonRecoveryProbe;
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
 * Verifies native Chameleon exports through the real Java 11 Zeta engine and HTTPS.
 *
 * <p>Independent literal provider fixtures validate every request and compare committed full rows,
 * including survey provenance and exact local timestamp windows. No fixture reads production
 * resource definitions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ChameleonIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String BASE = "/v3";
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
        trust = identity("trusted", ChameleonPeer.HOST);
        wrongTrust = identity("untrusted", ChameleonPeer.HOST);
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

    private static final String START = "2024-01-01T00:00:00Z";
    private static final String END = "2024-01-02T00:00:00Z";

    @Test
    void exportsAllTenResourcesWithExactSurveyProvenanceAndCompletePayloads() throws Exception {
        Settings s = new Settings("all-ten");
        List<ObjectNode> steps = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        String[][] contracts = {
            {"helpbars", "searches", "/edit/searches"},
            {"segments", "segments", "/edit/segments"},
            {"urls", "urls", "/edit/urls"},
            {"url_groups", "url_groups", "/edit/url_groups"},
            {"changes", "changes", "/edit/changes"},
            {"launchers", "launchers", "/edit/launchers"},
            {"tooltips", "tooltips", "/edit/tooltips"},
            {"tours", "tours", "/edit/tours"},
            {"surveys", "surveys", "/edit/surveys"}
        };
        for (String[] contract : contracts) {
            String id = contract[0].equals("surveys") ? "S1" : contract[0] + "-1";
            String field = contract[0].equals("changes") ? "created_at" : "updated_at";
            String row =
                    record(
                            id,
                            field,
                            "2024-01-01T08:00:00+08:00",
                            ",\"archived_at\":null,\"url\":\"https://chameleon-trap.invalid/content\"");
            steps.add(
                    contract[0].equals("helpbars")
                            ? get(BASE + contract[2], collection(contract[1], row))
                            : list(contract[2], contract[1], 2, null, null, row));
            wanted.add(
                    expected(
                            contract[0],
                            id,
                            contract[0].equals("surveys") ? id : null,
                            START,
                            row));
        }
        String response =
                record(
                        "response-1",
                        "updated_at",
                        START,
                        ",\"survey_id\":\"S1\",\"button_text\":\"Yes 👍\",\"input_text\":\"first\\nsecond\",\"profile\":{\"uid\":\"external-1\",\"company\":{\"uid\":\"c1\"}}");
        steps.add(responses("S1", 2, null, null, response));
        wanted.add(expected("survey_responses", "response-1", "S1", START, response));
        program(s.name, steps);
        success(run(s));
        consumed(s.name, 10);
        assertRows(s.name, wanted);
    }

    @Test
    void discoversOldSurveyParentsForChildOnlyWindowsAndKeepsEqualResponseIdsDistinct()
            throws Exception {
        Settings s = window("old-parents", "survey_responses");
        String a = record("S1", "updated_at", "2010-01-01T00:00:00Z", ""),
                b = record("S2", "updated_at", null, "");
        String first = record("same-response", "updated_at", START, ",\"survey_id\":\"S1\""),
                second = record("same-response", "updated_at", START, ",\"survey_id\":\"S2\"");
        program(
                s.name,
                List.of(
                        list("/edit/surveys", "surveys", 2, null, null, a, b),
                        responses("S1", 2, null, null, first),
                        responses("S2", 2, null, null, second)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name,
                List.of(
                        expected("survey_responses", "same-response", "S1", START, first),
                        expected("survey_responses", "same-response", "S2", START, second)));
    }

    @Test
    void appliesInclusiveStartExclusiveEndAndNormalizesOffsetsWithoutMutatingJson()
            throws Exception {
        Settings s = window("window", "tours");
        String before = record("before", "updated_at", "2023-12-31T23:59:59.999999999Z", ""),
                start = record("start", "updated_at", "2024-01-01T08:00:00+08:00", ""),
                tied = record("tied", "updated_at", START, ""),
                last = record("last", "updated_at", "2024-01-01T23:59:59.999999999Z", ""),
                end = record("end", "updated_at", END, "");
        s.pageSize = 5;
        program(
                s.name,
                List.of(
                        list(
                                "/edit/tours",
                                "tours",
                                5,
                                null,
                                null,
                                before,
                                start,
                                tied,
                                last,
                                end)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(
                s.name,
                List.of(
                        expected("tours", "start", null, START, start),
                        expected("tours", "tied", null, START, tied),
                        expected("tours", "last", null, "2024-01-01T23:59:59.999999999Z", last)));
        Settings empty = window("empty-window", "tours");
        empty.end = START;
        program(empty.name, List.of());
        success(run(empty));
        consumed(empty.name, 0);
        assertRows(empty.name, List.of());
    }

    @Test
    void traversesCreationOrderedPagesPastOldUpdatesToFindNewlyUpdatedOldObjects()
            throws Exception {
        Settings s = window("unordered", "segments");
        String
                first =
                        record(
                                "newly-created",
                                "updated_at",
                                START,
                                ",\"created_at\":\"2024-01-01T00:00:00Z\""),
                old = record("middle", "updated_at", "2010-01-01T00:00:00Z", ""),
                later =
                        record(
                                "old-created-new-update",
                                "updated_at",
                                START,
                                ",\"created_at\":\"2000-01-01T00:00:00Z\"");
        program(
                s.name,
                List.of(
                        list("/edit/segments", "segments", 2, null, "older", first),
                        list("/edit/segments", "segments", 2, "older", "oldest", old),
                        list("/edit/segments", "segments", 2, "oldest", null, later)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name,
                List.of(
                        expected("segments", "newly-created", null, START, first),
                        expected("segments", "old-created-new-update", null, START, later)));
    }

    @Test
    void followsEmptyAndShortCursorPagesTreatingContinuationAsAnOpaqueValue() throws Exception {
        Settings s = new Settings("opaque", "urls");
        String token = "https://chameleon-trap.invalid/a?x=+/=%中文";
        String row =
                record(
                        "url-1",
                        "updated_at",
                        null,
                        ",\"enabled\":\"off\",\"host\":\"example.invalid\"");
        program(
                s.name,
                List.of(
                        list("/edit/urls", "urls", 2, null, token),
                        list("/edit/urls", "urls", 2, token, "last", row),
                        list("/edit/urls", "urls", 2, "last", null)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(s.name, List.of(expected("urls", "url-1", null, null, row)));
    }

    @Test
    void acceptsEachDocumentedUrlGroupEnvelopeButRejectsAmbiguousDoubleArrays() throws Exception {
        String row = record("environment", "updated_at", START, ",\"name\":\"QA\"");
        for (String key : List.of("url_groups", "urls")) {
            Settings s = new Settings("alias-" + key, "url_groups");
            program(s.name, List.of(list("/edit/url_groups", key, 2, null, null, row)));
            success(run(s));
            consumed(s.name, 1);
            assertRows(s.name, List.of(expected("url_groups", "environment", null, START, row)));
        }
        fails(
                new Settings("ambiguous-alias", "url_groups"),
                List.of(
                        get(BASE + "/edit/url_groups", "{\"urls\":[],\"url_groups\":[]}")
                                .put("query", "limit=2")),
                "Chameleon");
    }

    @Test
    void keepsTheAccountHelpbarACompleteReadWithoutUnsupportedLimitOrCursorRequests()
            throws Exception {
        Settings empty = new Settings("empty-helpbar", "helpbars");
        program(empty.name, List.of(get(BASE + "/edit/searches", collection("searches"))));
        success(run(empty));
        consumed(empty.name, 1);
        assertRows(empty.name, List.of());
        fails(
                new Settings("helpbar-cursor", "helpbars"),
                List.of(
                        get(
                                BASE + "/edit/searches",
                                "{\"searches\":[],\"cursor\":{\"before\":\"later\"}}")),
                "Chameleon");
    }

    @Test
    void preservesAllFiveHundredItemsAtTheConfiguredPageLimit() throws Exception {
        Settings s = new Settings("five-hundred", "launchers");
        s.pageSize = 500;
        List<String> records = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            String row = record("L" + i, "updated_at", START, "");
            records.add(row);
            wanted.add(expected("launchers", "L" + i, null, START, row));
        }
        program(
                s.name,
                List.of(
                        list(
                                "/edit/launchers",
                                "launchers",
                                500,
                                null,
                                null,
                                records.toArray(new String[0]))));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, wanted);
    }

    @Test
    void rejectsCursorCyclesInvalidCursorShapesAndExplicitPageBudgets() throws Exception {
        fails(
                new Settings("cursor-cycle", "tours"),
                List.of(
                        list("/edit/tours", "tours", 2, null, "a"),
                        list("/edit/tours", "tours", 2, "a", "a")),
                "Chameleon");
        int i = 0;
        for (String cursor :
                List.of("[]", "{\"before\":\"\"}", "{\"before\":true}", "{\"before\":12}")) {
            fails(
                    new Settings("bad-cursor-" + i++, "tours"),
                    List.of(
                            get(BASE + "/edit/tours", "{\"tours\":[],\"cursor\":" + cursor + "}")
                                    .put("query", "limit=2")),
                    "Chameleon");
        }
        Settings pages = new Settings("page-budget", "tours");
        pages.pages = 1;
        fails(
                pages,
                List.of(list("/edit/tours", "tours", 2, null, "a")),
                "max_pages_per_collection");
        Settings scan = new Settings("scan-budget", "survey_responses");
        scan.scanPages = 1;
        fails(
                scan,
                List.of(
                        list(
                                "/edit/surveys",
                                "surveys",
                                2,
                                null,
                                null,
                                record("S1", "updated_at", null, ""))),
                "max_pages_per_scan");
    }

    @Test
    void usesCreatedTimeForChangesAndRequiresValidDatesOnlyWhenTheWindowNeedsThem()
            throws Exception {
        Settings changes = window("created-cursor", "changes");
        String row =
                record(
                        "change-1",
                        "created_at",
                        START,
                        ",\"updated_at\":\"2010-01-01T00:00:00Z\",\"kind\":\"destroy\",\"object\":{\"name\":[\"before\",null]}");
        program(changes.name, List.of(list("/edit/changes", "changes", 2, null, null, row)));
        success(run(changes));
        consumed(changes.name, 1);
        assertRows(changes.name, List.of(expected("changes", "change-1", null, START, row)));
        fails(
                window("missing-date", "segments"),
                List.of(
                        list(
                                "/edit/segments",
                                "segments",
                                2,
                                null,
                                null,
                                record("s1", "updated_at", null, ""))),
                "Chameleon");
        fails(
                new Settings("invalid-date", "segments"),
                List.of(
                        list(
                                "/edit/segments",
                                "segments",
                                2,
                                null,
                                null,
                                record("s1", "updated_at", "not-a-date", ""))),
                "Chameleon");
        fails(
                new Settings("foreign-survey", "survey_responses"),
                List.of(
                        list(
                                "/edit/surveys",
                                "surveys",
                                2,
                                null,
                                null,
                                record("S1", "updated_at", null, "")),
                        responses(
                                "S1",
                                2,
                                null,
                                null,
                                record("r1", "updated_at", START, ",\"survey_id\":\"OTHER\""))),
                "Chameleon");
    }

    @Test
    void reportsHttpAndSuccessfulStatusErrorEnvelopesWithoutLeakingTheSecret() throws Exception {
        for (int status : List.of(401, 403, 404, 500)) {
            fails(
                    new Settings("http-" + status, "segments"),
                    List.of(
                            list("/edit/segments", "segments", 2, null, null)
                                    .put("status", status)),
                    "HTTP " + status);
        }
        fails(
                new Settings("error-envelope", "segments"),
                List.of(
                        get(
                                        BASE + "/edit/segments",
                                        "{\"code\":401,\"messages\":[\"private-provider-message\"],\"segments\":[]}")
                                .put("query", "limit=2")),
                "Chameleon");
    }

    @Test
    void honorsTheLargestServerDelayAndDoesNotTreatConcurrencyCapacityAsSeconds() throws Exception {
        Settings s = new Settings("rate-headers", "segments");
        s.retries = 1;
        s.retryDelay = 3000;
        program(
                s.name,
                List.of(
                        list("/edit/segments", "segments", 2, null, null)
                                .put("status", 429)
                                .put("Retry-After", "1")
                                .put("X-Retry-After", "2")
                                .put("X-Ratelimit-Wait", "1")
                                .put("X-Ratelimit-Limit", "99999"),
                        list("/edit/segments", "segments", 2, null, null)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 2000L);
        int i = 0;
        for (String header : List.of("X-Retry-After", "X-Ratelimit-Wait")) {
            Settings one = new Settings("rate-native-" + i++, "segments");
            one.retries = 1;
            program(
                    one.name,
                    List.of(
                            list("/edit/segments", "segments", 2, null, null)
                                    .put("status", 429)
                                    .put(header, "1"),
                            list("/edit/segments", "segments", 2, null, null)));
            success(run(one));
            consumed(one.name, 2);
            delayAtLeast(one.name, 1000L);
        }
        Settings fallback = new Settings("rate-body", "segments");
        fallback.retries = 1;
        program(
                fallback.name,
                List.of(
                        get(BASE + "/edit/segments", "{\"code\":429,\"wait\":1,\"limit\":99999}")
                                .put("query", "limit=2")
                                .put("status", 429),
                        list("/edit/segments", "segments", 2, null, null)));
        success(run(fallback));
        consumed(fallback.name, 2);
        delayAtLeast(fallback.name, 1000L);
        Settings exhausted = new Settings("retry-exhausted", "segments");
        exhausted.retries = 1;
        ObjectNode unavailable =
                list("/edit/segments", "segments", 2, null, null).put("status", 503);
        fails(exhausted, List.of(unavailable, unavailable), "2 attempts");
    }

    @Test
    void pacesDifferentResourcesWithoutDroppingTheFirstPage() throws Exception {
        Settings s = new Settings("pace", "segments", "urls");
        s.interval = 600;
        program(
                s.name,
                List.of(
                        list("/edit/segments", "segments", 2, null, null),
                        list("/edit/urls", "urls", 2, null, null)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 600L);
    }

    @Test
    void verifiesTrustAndHostnameAndNeverFollowsRedirects() throws Exception {
        Settings ca = new Settings("wrong-ca", "segments");
        ca.trust = wrongTrust;
        fails(ca, List.of(), "certificate");
        selectIdentity("wrong-name");
        try {
            Settings san = new Settings("wrong-san", "segments");
            san.trust = wrongNameTrust;
            fails(san, List.of(), "certificate");
        } finally {
            selectIdentity("trusted");
        }
        fails(
                new Settings("redirect", "segments"),
                List.of(
                        list("/edit/segments", "segments", 2, null, null)
                                .put("mode", "redirect")
                                .put("status", 302)),
                "HTTP 302");
    }

    @Test
    void acceptsGzipAndBoundsBothWireAndDecodedBodies() throws Exception {
        Settings s = new Settings("gzip", "segments");
        String row = record("s1", "updated_at", START, "");
        program(
                s.name,
                List.of(
                        list("/edit/segments", "segments", 2, null, null, row)
                                .put("mode", "gzip")));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("segments", "s1", null, START, row)));
        for (String mode : List.of("json", "gzip")) {
            Settings small = new Settings("body-" + mode, "segments");
            small.bytes = 256;
            fails(
                    small,
                    List.of(
                            get(
                                            BASE + "/edit/segments",
                                            "{\"segments\":[],\"padding\":\""
                                                    + "x".repeat(5000)
                                                    + "\"}")
                                    .put("query", "limit=2")
                                    .put("mode", mode)),
                    "max_response_bytes");
        }
        fails(
                new Settings("encoding", "segments"),
                List.of(list("/edit/segments", "segments", 2, null, null).put("encoding", "br")),
                "encoding");
    }

    @Test
    void rejectsMalformedJsonUtf8DuplicateIdsAndInvalidEnvelopesBeforePublication()
            throws Exception {
        int i = 0;
        for (String body :
                List.of(
                        "null",
                        "{\"segments\":null}",
                        "{\"segments\":[null]}",
                        "{\"segments\":[{\"id\":12}]}",
                        "{\"segments\":[]}{}",
                        "{\"segments\":[],\"segments\":[]}")) {
            Settings s = new Settings("json-" + i++, "segments");
            fails(
                    s,
                    List.of(get(BASE + "/edit/segments", body).put("query", "limit=2")),
                    "Chameleon");
            assertRows(s.name, List.of());
        }
        fails(
                new Settings("utf8", "segments"),
                List.of(
                        get(BASE + "/edit/segments", "")
                                .put("query", "limit=2")
                                .put(
                                        "bytes",
                                        Base64.getEncoder()
                                                .encodeToString(new byte[] {(byte) 0xc3, 0x28}))),
                "Chameleon");
        String duplicate = record("same", "updated_at", START, "");
        fails(
                new Settings("duplicate-id", "segments"),
                List.of(list("/edit/segments", "segments", 2, null, null, duplicate, duplicate)),
                "duplicate");
    }

    @Test
    void rejectsInvalidOptionsBeforeAuthentication() throws Exception {
        fails(new Settings("bad-resource", "unknown"), List.of(), "resources");
        fails(new Settings("repeated-resource", "segments", "segments"), List.of(), "resources");
        Settings size = new Settings("bad-size", "segments");
        size.pageSize = 501;
        fails(size, List.of(), "page_size");
        Settings token = new Settings("bad-token", "segments");
        token.token = "bad secret";
        fails(token, List.of(), "token");
        Settings reversed = window("reversed", "segments");
        reversed.start = END;
        reversed.end = START;
        fails(reversed, List.of(), "Chameleon");
    }

    @Test
    void boundsContinuousBodyTrafficAndClosesItsSocketAtTheDeadline() throws Exception {
        Settings s = new Settings("deadline", "segments");
        s.request = 2000;
        s.operation = 3000;
        fails(
                s,
                List.of(list("/edit/segments", "segments", 2, null, null).put("mode", "trickle")),
                "Chameleon");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(s.name + ".disconnect"));
        long start = Long.parseLong(engine.lines(s.name + ".started").get(0)),
                end = Long.parseLong(engine.lines(s.name + ".disconnect").get(0));
        assertTrue(end - start < 6000L);
        assertRows(s.name, List.of());
    }

    @Test
    void cancelsARealJobWithoutPublishingAPartialResponse() throws Exception {
        Settings s = new Settings("cancel", "segments");
        program(
                s.name,
                List.of(list("/edit/segments", "segments", 2, null, null).put("mode", "trickle")));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/chameleon.conf", id, s.variables()));
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
    void restoresOnlyThePendingOpaquePageWithoutRepeatingCommittedRows() throws Exception {
        Settings s = new Settings("page-savepoint", "segments");
        String first = record("first", "updated_at", START, ""),
                second = record("second", "updated_at", START, "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                list("/edit/segments", "segments", 2, null, "opaque +/&=%", first),
                                list("/edit/segments", "segments", 2, "opaque +/&=%", null, second)
                                        .put("mode", "partial")));
        program(
                "page-restored",
                List.of(list("/edit/segments", "segments", 2, "opaque +/&=%", null, second)));
        success(engine.restoreJob("/chameleon.conf", id, s.variables()));
        consumed("page-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expected("segments", "first", null, START, first),
                        expected("segments", "second", null, START, second)));
    }

    @Test
    void restoresInsideAnOldSurveyPageAndRequestsOnlyItsUnfinishedResponses() throws Exception {
        Settings s = window("survey-savepoint", "survey_responses");
        String a = record("S1", "updated_at", "2010-01-01T00:00:00Z", ""),
                b = record("S2", "updated_at", null, "");
        String one = record("r1", "updated_at", START, ",\"survey_id\":\"S1\""),
                two = record("r2", "updated_at", START, ",\"survey_id\":\"S2\"");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                list("/edit/surveys", "surveys", 2, null, null, a, b),
                                responses("S1", 2, null, null, one),
                                responses("S2", 2, null, null, two).put("mode", "partial")));
        program(
                "survey-restored",
                List.of(
                        list("/edit/surveys", "surveys", 2, null, null, a, b),
                        responses("S2", 2, null, null, two)));
        success(engine.restoreJob("/chameleon-window.conf", id, s.variables()));
        consumed("survey-restored", 2);
        assertRows(
                s.name,
                List.of(
                        expected("survey_responses", "r1", "S1", START, one),
                        expected("survey_responses", "r2", "S2", START, two)));
    }

    @Test
    void rejectsChangedSurveyInventoryBeforeRequestingAnyRemainingResponse() throws Exception {
        Settings s = new Settings("changed-survey-savepoint", "survey_responses");
        String a = record("S1", "updated_at", null, ""), b = record("S2", "updated_at", null, "");
        String one = record("r1", "updated_at", START, ""),
                two = record("r2", "updated_at", START, "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                list("/edit/surveys", "surveys", 2, null, null, a, b),
                                responses("S1", 2, null, null, one),
                                responses("S2", 2, null, null, two).put("mode", "partial")));
        program(
                "changed-survey-restored",
                List.of(
                        list(
                                "/edit/surveys",
                                "surveys",
                                2,
                                null,
                                null,
                                a,
                                b.replace("9223372036854775808", "9223372036854775809"))));
        Container.ExecResult result = engine.restoreJob("/chameleon.conf", id, s.variables());
        assertNotEquals(0, result.getExitCode());
        assertTrue((result.getStdout() + result.getStderr()).contains("changed before replay"));
        consumed("changed-survey-restored", 1);
        assertRows(s.name, List.of(expected("survey_responses", "r1", "S1", START, one)));
    }

    @Test
    void restoresATerminalSourceWithoutAuthenticationOrDuplicateTransactionalRows()
            throws Exception {
        Settings s = new Settings("terminal-savepoint", "segments");
        String row = record("s1", "updated_at", START, "");
        List<JsonNode> wanted = List.of(expected("segments", "s1", null, START, row));
        program(s.name, List.of(list("/edit/segments", "segments", 2, null, null, row)));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/chameleon-terminal.conf", id, s.variables()));
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
                tasks.submit(
                        () -> engine.restoreJob("/chameleon-terminal.conf", id, s.variables()));
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
    void restoresThe32Of33ResponsePositionAndRejectsEvenFilteredRawRecordChanges()
            throws Exception {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < 33; i++) {
            rows.add(
                    record(
                            "R" + i,
                            "updated_at",
                            START,
                            ",\"input_text\":\"synthetic-response-record\""));
        }
        rows.add(record("filtered", "updated_at", "2010-01-01T00:00:00Z", ""));
        String a = record("S1", "updated_at", null, ""), b = record("S2", "updated_at", null, "");
        for (String mode :
                List.of("resume", "changed-child", "changed-parent", "changed-filtered")) {
            String name = "reader-" + mode;
            List<ObjectNode> steps =
                    new ArrayList<>(
                            List.of(
                                    list("/edit/surveys", "surveys", 50, null, null, a, b),
                                    responses("S1", 50, null, null, rows.toArray(new String[0])),
                                    list(
                                            "/edit/surveys",
                                            "surveys",
                                            50,
                                            null,
                                            null,
                                            mode.equals("changed-parent")
                                                    ? a.replace(
                                                            "9223372036854775808",
                                                            "9223372036854775809")
                                                    : a,
                                            b)));
            if (!mode.equals("changed-parent")) {
                List<String> replay = new ArrayList<>(rows);
                if (mode.equals("changed-child")) {
                    replay.set(0, replay.get(0).replace("R0", "CHANGED"));
                }
                if (mode.equals("changed-filtered")) {
                    replay.set(33, replay.get(33).replace("filtered", "CHANGED"));
                }
                steps.add(responses("S1", 50, null, null, replay.toArray(new String[0])));
            }
            if (mode.equals("resume")) {
                steps.add(responses("S2", 50, null, null));
            }
            program(name, steps);
            success(engine.probe(mode));
            consumed(name, steps.size());
        }
        program("reader-zero", List.of());
        success(engine.probe("zero"));
        consumed("reader-zero", 0);
    }

    private String saveBlocked(Settings settings, List<ObjectNode> steps) throws Exception {
        program(settings.name, steps);
        String id = jobId();
        String config = settings.dated ? "/chameleon-window.conf" : "/chameleon.conf";
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
        s.start = START;
        s.end = END;
        return s;
    }

    private void delayAtLeast(String name, long millis) throws Exception {
        List<String> times = engine.lines(name + ".times");
        assertTrue(
                Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= millis,
                times.toString());
    }

    private static String record(String id, String field, String time, String extra) {
        return "{\"id\":\""
                + id
                + "\",\""
                + field
                + "\":"
                + (time == null ? "null" : "\"" + time + "\"")
                + extra
                + EXTRA
                + "}";
    }

    private static String collection(String field, String... records) {
        return "{\"" + field + "\":" + array(records) + "}";
    }

    private static ObjectNode list(
            String path, String field, int limit, String before, String next, String... records) {
        ObjectNode body = JSON.createObjectNode();
        ArrayNode rows = JSON.createArrayNode();
        for (String row : records) {
            try {
                rows.add(JSON.readTree(row));
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid independent fixture", e);
            }
        }
        body.set(field, rows);
        ObjectNode cursor = body.putObject("cursor").put("limit", limit);
        if (next != null) {
            cursor.put("before", next);
        } else {
            cursor.putNull("before");
        }
        return get(BASE + path, body.toString())
                .put(
                        "query",
                        "limit=" + limit + (before == null ? "" : "&before=" + encode(before)));
    }

    private static ObjectNode responses(
            String survey, int limit, String before, String next, String... records) {
        return list("/analyze/responses", "responses", limit, before, next, records)
                .put(
                        "query",
                        "limit="
                                + limit
                                + "&id="
                                + encode(survey)
                                + "&expand%5Bprofile%5D=all&expand%5Bcompany%5D=all"
                                + (before == null ? "" : "&before=" + encode(before)));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String array(String... records) {
        return "[" + String.join(",", records) + "]";
    }

    private static ObjectNode get(String path, String response) {
        return JSON.createObjectNode().put("path", path).put("query", "").put("response", response);
    }

    private static JsonNode expected(
            String resource, String id, String survey, String eventTime, String payload)
            throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put("id", id)
                        .put("survey_id", survey)
                        .put("event_time", eventTime);
        row.set("record", JSON.readTree(payload));
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>(), wanted = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(parsed.isObject() && parsed.size() == 5, line);
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
                settings.dated ? "/chameleon-window.conf" : "/chameleon.conf",
                jobId(),
                settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(ChameleonPeer.TOKEN), "API key leaked");
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
                                ChameleonPeer.PASSWORD,
                                "-keypass",
                                ChameleonPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, ChameleonPeer.PASSWORD.toCharArray());
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
        String trust = ChameleonIT.trust;
        String token = ChameleonPeer.TOKEN;
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
        private static final String PEER = "/tmp/chameleon-peer";
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
            container.withExtraHost(ChameleonPeer.HOST, "127.0.0.1");
            container.withExtraHost("chameleon-trap.invalid", "127.0.0.1");
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
                stage(container, ChameleonPeer.class);
                stage(container, ChameleonRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java Chameleon fixture", failure);
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
                    ChameleonPeer.class.getName(),
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
                    ChameleonRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/chameleon-" + name;
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

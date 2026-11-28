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
package org.apache.seatunnel.e2e.connector.chartmogul;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.chartmogul.ChartMogulRecoveryProbe;
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
import java.util.Arrays;
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
 * Verifies native ChartMogul exports through the real Java 11 Zeta engine and HTTPS.
 *
 * <p>Independent literal provider fixtures validate every request and compare committed full rows,
 * including native UUIDs, calendar metric dates and exact activity windows. No fixture reads
 * production resource definitions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ChartMogulIT extends TestSuiteBase {
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
        trust = identity("trusted", ChartMogulPeer.HOST);
        wrongTrust = identity("untrusted", ChartMogulPeer.HOST);
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

    private static final String START = "2024-02-28T00:00:00Z";
    private static final String END = "2024-04-01T00:00:00Z";

    @Test
    void exportsAllSixResourcesWithNativeIdentityAndDistinctCalendarIntervals() throws Exception {
        Settings s = new Settings("all-six");
        String customer =
                customer(
                        "9223372036854775808",
                        "cus-native",
                        START,
                        ",\"external_id\":\"same-external\",\"attributes\":{\"custom\":{\"string\":\"value\"}}");
        String activity =
                activity(
                        "a-native",
                        START,
                        ",\"activity-mrr\":9223372036854775808,\"activity-mrr-movement\":-1234567890123456789,\"currency\":\"USD\"");
        List<ObjectNode> steps =
                new ArrayList<>(
                        List.of(
                                list("customers", 2, null, false, "unused-terminal", customer),
                                list("activities", 2, null, false, "unused-terminal", activity)));
        List<JsonNode> wanted =
                new ArrayList<>(
                        List.of(
                                expected(
                                        "customers",
                                        "9223372036854775808",
                                        "cus-native",
                                        START,
                                        customer),
                                expected("activities", "a-native", "a-native", START, activity)));
        String[][] metrics = {
            {"customer_daily_count", "day", "2024-02-29"},
            {"customer_weekly_count", "week", "2024-03-03"},
            {"customer_monthly_count", "month", "2024-02-29"},
            {"customer_quarterly_count", "quarter", "2024-03-31"}
        };
        for (String[] metric : metrics) {
            String row = metric(metric[2], "9007199254740993");
            steps.add(metricPage(metric[1], row));
            wanted.add(expected(metric[0], metric[2], null, metric[2], row));
        }
        program(s.name, steps);
        success(run(s));
        consumed(s.name, 6);
        assertRows(s.name, wanted);
    }

    @Test
    void permitsCustomersOnlyWithoutAnyDateAndRetainsAnAbsentUuid() throws Exception {
        Settings s = new Settings("customer-only", "customers");
        String row =
                customer(
                        "100",
                        null,
                        null,
                        ",\"attributes\":{\"tags\":[\"海豚🐬\"],\"custom\":{\"n\":null}}");
        program(s.name, List.of(list("customers", 2, null, false, null, row)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("customers", "100", null, null, row)));
    }

    @Test
    void includesBothActivityBoundsAndPreservesTiedUuidsAndOriginalOffsetStrings()
            throws Exception {
        Settings s = new Settings("activity-bounds", "activities");
        s.pageSize = 5;
        String before = activity("before", "2024-02-27T23:59:59.999999999Z", ""),
                first = activity("first", "2024-02-28T08:00:00+08:00", ""),
                tied = activity("tied", START, ""),
                last = activity("last", END, ""),
                after = activity("after", "2024-04-01T00:00:00.000000001Z", "");
        program(
                s.name,
                List.of(
                        list(
                                "activities",
                                5,
                                null,
                                false,
                                null,
                                before,
                                first,
                                tied,
                                last,
                                after)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(
                s.name,
                List.of(
                        expected("activities", "first", "first", START, first),
                        expected("activities", "tied", "tied", START, tied),
                        expected("activities", "last", "last", END, last)));
    }

    @Test
    void continuesEmptyAndShortPagesOnlyWhileHasMoreIsTrue() throws Exception {
        Settings s = new Settings("cursor", "customers");
        String cursor = "https://chartmogul-trap.invalid/?x=+/=%中文";
        String a = customer("1", "cus-A", null, ""), b = customer("2", "cus-B", null, "");
        program(
                s.name,
                List.of(
                        list("customers", 2, null, true, cursor),
                        list("customers", 2, cursor, true, "last", a),
                        list("customers", 2, "last", false, "terminal-unused", b)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name,
                List.of(
                        expected("customers", "1", "cus-A", null, a),
                        expected("customers", "2", "cus-B", null, b)));
    }

    @Test
    void ignoresDeprecatedNumericPageMetadataAndStopsWithANonemptyTerminalCursor()
            throws Exception {
        Settings s = new Settings("modern-only", "customers");
        ObjectNode step = list("customers", 2, null, false, "ignored");
        ObjectNode body = (ObjectNode) JSON.readTree(step.path("response").asText());
        body.put("page", 1).put("current_page", 1).put("total_pages", 9999);
        step.put("response", body.toString());
        program(s.name, List.of(step));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of());
    }

    @Test
    void retainsOnlyMetricEntriesWithLeapDayAndServerSuppliedIntervalEndDates() throws Exception {
        Settings s =
                new Settings(
                        "calendar",
                        "customer_daily_count",
                        "customer_weekly_count",
                        "customer_monthly_count",
                        "customer_quarterly_count");
        String day = metric("2024-02-29", "0"),
                week = metric("2024-03-03", "11"),
                month = metric("2024-02-29", "12"),
                quarter = metric("2024-03-31", "13");
        program(
                s.name,
                List.of(
                        metricPage("day", day),
                        metricPage("week", week),
                        metricPage("month", month),
                        metricPage("quarter", quarter)));
        success(run(s));
        consumed(s.name, 4);
        assertRows(
                s.name,
                List.of(
                        expected("customer_daily_count", "2024-02-29", null, "2024-02-29", day),
                        expected("customer_weekly_count", "2024-03-03", null, "2024-03-03", week),
                        expected("customer_monthly_count", "2024-02-29", null, "2024-02-29", month),
                        expected(
                                "customer_quarterly_count",
                                "2024-03-31",
                                null,
                                "2024-03-31",
                                quarter)));
    }

    @Test
    void rejectsMalformedMetricDatesCountsAndUnexpectedPaging() throws Exception {
        int i = 0;
        for (String row :
                List.of(
                        metric("2023-02-29", "1"),
                        metric("2024-02-29", "-1"),
                        metric("2024-02-29", "1.5"),
                        "{\"date\":\"2024-02-29\"}")) {
            fails(
                    new Settings("bad-metric-" + i++, "customer_daily_count"),
                    List.of(metricPage("day", row)),
                    "ChartMogul");
        }
        ObjectNode step = metricPage("day");
        ObjectNode body = (ObjectNode) JSON.readTree(step.path("response").asText());
        body.put("has_more", true).put("cursor", "more");
        step.put("response", body.toString());
        fails(new Settings("metric-paging", "customer_daily_count"), List.of(step), "ChartMogul");
    }

    @Test
    void rejectsMissingBooleanContinuationCursorCyclesAndFiniteReadBudgets() throws Exception {
        int i = 0;
        for (String response :
                List.of(
                        "{\"entries\":[],\"has_more\":true}",
                        "{\"entries\":[],\"has_more\":\"false\"}",
                        "{\"entries\":[]}",
                        "{\"entries\":[],\"has_more\":true,\"cursor\":\"\"}")) {
            fails(
                    new Settings("bad-cursor-" + i++, "customers"),
                    List.of(get("/v1/customers", response).put("query", "per_page=2")),
                    "ChartMogul");
        }
        fails(
                new Settings("cycle", "customers"),
                List.of(
                        list("customers", 2, null, true, "same"),
                        list("customers", 2, "same", true, "same")),
                "ChartMogul");
        Settings pages = new Settings("page-budget", "customers");
        pages.pages = 1;
        fails(pages, List.of(list("customers", 2, null, true, "next")), "max_pages_per_collection");
        Settings scan = new Settings("scan-budget", "customers", "activities");
        scan.scanPages = 1;
        fails(scan, List.of(list("customers", 2, null, false, null)), "max_pages_per_scan");
    }

    @Test
    void retainsTwoHundredTiedActivitiesAndTheirFollowingCursorSuffix() throws Exception {
        Settings s = new Settings("two-hundred", "activities");
        s.pageSize = 200;
        List<String> rows = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (int i = 0; i < 201; i++) {
            String row = activity("A" + i, START, "");
            rows.add(row);
            wanted.add(expected("activities", "A" + i, "A" + i, START, row));
        }
        program(
                s.name,
                List.of(
                        list(
                                "activities",
                                200,
                                null,
                                true,
                                "next",
                                rows.subList(0, 200).toArray(new String[0])),
                        list("activities", 200, "next", false, null, rows.get(200))));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, wanted);
    }

    @Test
    void boundsMetricEntryCountsAndRejectsDuplicateIdentities() throws Exception {
        Settings small = new Settings("metric-limit", "customer_daily_count");
        small.items = 1;
        fails(
                small,
                List.of(metricPage("day", metric("2024-02-28", "1"), metric("2024-02-29", "2"))),
                "ChartMogul");
        String row = activity("same", START, "");
        fails(
                new Settings("duplicate-id", "activities"),
                List.of(list("activities", 2, null, false, null, row, row)),
                "duplicate");
    }

    @Test
    void reportsPermissionAndProviderErrorsWithoutLeakingTheBasicCredential() throws Exception {
        for (int status : List.of(401, 403, 404, 500)) {
            fails(
                    new Settings("http-" + status, "customers"),
                    List.of(
                            get("/v1/customers", "{\"error\":\"private-provider-message\"}")
                                    .put("query", "per_page=2")
                                    .put("status", status)),
                    "HTTP " + status);
        }
        fails(
                new Settings("error-object", "customers"),
                List.of(
                        get(
                                        "/v1/customers",
                                        "{\"errors\":[\"private-provider-message\"],\"entries\":[],\"has_more\":false}")
                                .put("query", "per_page=2")),
                "ChartMogul");
    }

    @Test
    void rejectsInvalidIdentifiersAndActivityDatesRatherThanInventingKeys() throws Exception {
        fails(
                new Settings("fractional-id", "customers"),
                List.of(list("customers", 2, null, false, null, customer("1.5", null, null, ""))),
                "ChartMogul");
        fails(
                new Settings("missing-uuid", "activities"),
                List.of(
                        list(
                                "activities",
                                2,
                                null,
                                false,
                                null,
                                "{\"date\":\"" + START + "\",\"id\":12}")),
                "ChartMogul");
        fails(
                new Settings("bad-date", "activities"),
                List.of(list("activities", 2, null, false, null, activity("A1", "not-a-date", ""))),
                "ChartMogul");
        fails(
                new Settings("missing-date", "activities"),
                List.of(list("activities", 2, null, false, null, activity("A1", null, ""))),
                "ChartMogul");
    }

    @Test
    void validatesRequiredActivityDatesWhileAllowingAnInclusiveSingleInstantWindow()
            throws Exception {
        Settings missing = new Settings("missing-start", "activities");
        missing.dated = false;
        fails(missing, List.of(), "start_date");
        Settings reversed = new Settings("reversed", "activities");
        reversed.start = END;
        reversed.end = START;
        fails(reversed, List.of(), "ChartMogul");
        Settings equal = new Settings("equal", "activities");
        equal.end = START;
        String row = activity("same", START, "");
        program(
                equal.name,
                List.of(
                        list("activities", 2, null, false, null, row)
                                .put(
                                        "query",
                                        "per-page=2&start-date="
                                                + encode(START)
                                                + "&end-date="
                                                + encode(START))));
        success(run(equal));
        consumed(equal.name, 1);
        assertRows(equal.name, List.of(expected("activities", "same", "same", START, row)));
    }

    @Test
    void verifiesTrustAndHostnameAndRefusesRedirectCredentialForwarding() throws Exception {
        Settings ca = new Settings("wrong-ca", "customers");
        ca.trust = wrongTrust;
        fails(ca, List.of(), "certificate");
        selectIdentity("wrong-name");
        try {
            Settings san = new Settings("wrong-san", "customers");
            san.trust = wrongNameTrust;
            fails(san, List.of(), "certificate");
        } finally {
            selectIdentity("trusted");
        }
        fails(
                new Settings("redirect", "customers"),
                List.of(
                        list("customers", 2, null, false, null)
                                .put("status", 302)
                                .put("mode", "redirect")),
                "HTTP 302");
    }

    @Test
    void acceptsGzipAndBoundsBothCompressedAndExpandedResponses() throws Exception {
        Settings s = new Settings("gzip", "customers");
        String row = customer("1", null, null, "");
        program(s.name, List.of(list("customers", 2, null, false, null, row).put("mode", "gzip")));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("customers", "1", null, null, row)));
        for (String mode : List.of("json", "gzip")) {
            Settings small = new Settings("body-" + mode, "customers");
            small.bytes = 256;
            fails(
                    small,
                    List.of(
                            get(
                                            "/v1/customers",
                                            "{\"entries\":[],\"has_more\":false,\"padding\":\""
                                                    + "x".repeat(5000)
                                                    + "\"}")
                                    .put("query", "per_page=2")
                                    .put("mode", mode)),
                    "max_response_bytes");
        }
        fails(
                new Settings("encoding", "customers"),
                List.of(list("customers", 2, null, false, null).put("encoding", "br")),
                "encoding");
    }

    @Test
    void rejectsMalformedJsonUtf8AndNonrecordElementsBeforePublication() throws Exception {
        int i = 0;
        for (String body :
                List.of(
                        "null",
                        "{\"entries\":null,\"has_more\":false}",
                        "{\"entries\":[null],\"has_more\":false}",
                        "{\"entries\":[],\"has_more\":false}{}",
                        "{\"entries\":[],\"entries\":[],\"has_more\":false}")) {
            Settings s = new Settings("json-" + i++, "customers");
            fails(s, List.of(get("/v1/customers", body).put("query", "per_page=2")), "ChartMogul");
            assertRows(s.name, List.of());
        }
        fails(
                new Settings("utf8", "customers"),
                List.of(
                        get("/v1/customers", "")
                                .put("query", "per_page=2")
                                .put(
                                        "bytes",
                                        Base64.getEncoder()
                                                .encodeToString(new byte[] {(byte) 0xc3, 0x28}))),
                "ChartMogul");
    }

    @Test
    void honorsRetryAfterAndTheConfiguredRetryAndDelayCeilings() throws Exception {
        Settings s = new Settings("retry", "customers");
        s.retries = 1;
        program(
                s.name,
                List.of(
                        list("customers", 2, null, false, null)
                                .put("status", 429)
                                .put("Retry-After", "2"),
                        list("customers", 2, null, false, null)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 2000L);
        Settings exhausted = new Settings("retry-exhausted", "customers");
        exhausted.retries = 1;
        ObjectNode unavailable = list("customers", 2, null, false, null).put("status", 503);
        fails(exhausted, List.of(unavailable, unavailable), "2 attempts");
        Settings capped = new Settings("retry-cap", "customers");
        capped.retries = 1;
        capped.retryDelay = 250;
        fails(
                capped,
                List.of(
                        list("customers", 2, null, false, null)
                                .put("status", 429)
                                .put("Retry-After", "3")),
                "max_retry_delay_ms");
    }

    @Test
    void pacesDifferentCollectionsWithoutDroppingSuccessfulPages() throws Exception {
        Settings s = new Settings("pace", "customers", "activities");
        s.interval = 600;
        program(
                s.name,
                List.of(
                        list("customers", 2, null, false, null),
                        list("activities", 2, null, false, null)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 600L);
    }

    @Test
    void rejectsInvalidSelectionsAndPageSizesBeforeSendingTheSecret() throws Exception {
        fails(new Settings("unknown", "unknown"), List.of(), "resources");
        fails(new Settings("duplicate", "customers", "customers"), List.of(), "resources");
        Settings large = new Settings("bad-size", "customers");
        large.pageSize = 201;
        fails(large, List.of(), "page_size");
        Settings token = new Settings("bad-token", "customers");
        token.token = "bad:secret";
        fails(token, List.of(), "token");
    }

    @Test
    void boundsContinuousBodyTrafficAndClosesItsSocketAtTheDeadline() throws Exception {
        Settings s = new Settings("deadline", "customers");
        s.request = 2000;
        s.operation = 3000;
        fails(
                s,
                List.of(list("customers", 2, null, false, null).put("mode", "trickle")),
                "ChartMogul");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(s.name + ".disconnect"));
        long start = Long.parseLong(engine.lines(s.name + ".started").get(0)),
                end = Long.parseLong(engine.lines(s.name + ".disconnect").get(0));
        assertTrue(end - start < 6000L);
        assertRows(s.name, List.of());
    }

    @Test
    void cancelsTheActualEngineJobWithoutPublishingAPartialResponse() throws Exception {
        Settings s = new Settings("cancel", "customers");
        program(s.name, List.of(list("customers", 2, null, false, null).put("mode", "trickle")));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/chartmogul.conf", id, s.variables()));
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
    void restoresAnUnfinishedCursorWithoutReissuingTheCommittedFirstPage() throws Exception {
        Settings s = new Settings("cursor-savepoint", "customers");
        String first = customer("1", "cus-one", null, ""),
                second = customer("2", "cus-two", null, "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                list("customers", 2, null, true, "next", first),
                                list("customers", 2, "next", false, null, second)
                                        .put("mode", "partial")));
        program("cursor-restored", List.of(list("customers", 2, "next", false, null, second)));
        success(engine.restoreJob("/chartmogul.conf", id, s.variables()));
        consumed("cursor-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expected("customers", "1", "cus-one", null, first),
                        expected("customers", "2", "cus-two", null, second)));
    }

    @Test
    void restoresBetweenResourcesWithTheOriginalInclusiveActivityWindow() throws Exception {
        Settings s = new Settings("between-savepoint", "customers", "activities");
        String first = customer("1", null, null, ""), second = activity("activity-one", END, "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                list("customers", 2, null, false, null, first),
                                list("activities", 2, null, false, null, second)
                                        .put("mode", "partial")));
        program("between-restored", List.of(list("activities", 2, null, false, null, second)));
        success(engine.restoreJob("/chartmogul-window.conf", id, s.variables()));
        consumed("between-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expected("customers", "1", null, null, first),
                        expected("activities", "activity-one", "activity-one", END, second)));
    }

    @Test
    void restoresATerminalSourceWithoutAuthenticatedTrafficOrDuplicateCommittedRows()
            throws Exception {
        Settings s = new Settings("terminal-savepoint", "customers");
        String row = customer("1", "terminal", null, "");
        List<JsonNode> wanted = List.of(expected("customers", "1", "terminal", null, row));
        program(s.name, List.of(list("customers", 2, null, false, "unused", row)));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/chartmogul-terminal.conf", id, s.variables()));
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
                        () -> engine.restoreJob("/chartmogul-terminal.conf", id, s.variables()));
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
    void serializes32Of33ActivitiesAndRejectsChangedFilteredAndPublishedRecords() throws Exception {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < 33; i++) {
            rows.add(activity("A" + i, START, ",\"description\":\"synthetic-response-record\""));
        }
        rows.add(activity("filtered", "2024-02-27T23:59:59Z", ""));
        for (String mode : List.of("resume", "changed", "changed-filtered")) {
            List<String> replay = new ArrayList<>(rows);
            if (mode.equals("changed")) {
                replay.set(0, replay.get(0).replace("A0", "CHANGED"));
            }
            if (mode.equals("changed-filtered")) {
                replay.set(33, replay.get(33).replace("filtered", "CHANGED"));
            }
            List<ObjectNode> steps =
                    List.of(
                            list("activities", 50, null, false, null, rows.toArray(new String[0])),
                            list(
                                    "activities",
                                    50,
                                    null,
                                    false,
                                    null,
                                    replay.toArray(new String[0])));
            String name = "reader-" + mode;
            program(name, steps);
            success(engine.probe(mode));
            consumed(name, 2);
        }
        program("reader-zero", List.of());
        success(engine.probe("zero"));
        consumed("reader-zero", 0);
    }

    private String saveBlocked(Settings settings, List<ObjectNode> steps) throws Exception {
        program(settings.name, steps);
        String id = jobId();
        String config = settings.dated ? "/chartmogul-window.conf" : "/chartmogul.conf";
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

    private void delayAtLeast(String name, long millis) throws Exception {
        List<String> times = engine.lines(name + ".times");
        assertTrue(
                Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= millis,
                times.toString());
    }

    private static String customer(String id, String uuid, String since, String extra) {
        return "{\"id\":"
                + id
                + ",\"uuid\":"
                + (uuid == null ? "null" : "\"" + uuid + "\"")
                + ",\"customer-since\":"
                + (since == null ? "null" : "\"" + since + "\"")
                + extra
                + EXTRA
                + "}";
    }

    private static String activity(String uuid, String date, String extra) {
        return "{\"uuid\":\""
                + uuid
                + "\",\"date\":"
                + (date == null ? "null" : "\"" + date + "\"")
                + extra
                + EXTRA
                + "}";
    }

    private static String metric(String date, String count) {
        return "{\"date\":\""
                + date
                + "\",\"customers\":"
                + count
                + ",\"percentage-change\":-1.23456789012345678900"
                + EXTRA
                + "}";
    }

    private static ObjectNode list(
            String resource,
            int limit,
            String cursor,
            boolean more,
            String next,
            String... records) {
        ObjectNode body = JSON.createObjectNode();
        ArrayNode entries = JSON.createArrayNode();
        for (String row : records) {
            try {
                entries.add(JSON.readTree(row));
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid independent fixture", e);
            }
        }
        body.set("entries", entries);
        body.put("has_more", more);
        if (next != null) {
            body.put("cursor", next);
        } else {
            body.putNull("cursor");
        }
        String query = (resource.equals("customers") ? "per_page=" : "per-page=") + limit;
        if (resource.equals("activities")) {
            query += "&start-date=" + encode(START) + "&end-date=" + encode(END);
        }
        if (cursor != null) {
            query += "&cursor=" + encode(cursor);
        }
        return get("/v1/" + resource, body.toString()).put("query", query);
    }

    private static ObjectNode metricPage(String interval, String... records) {
        return get(
                        "/v1/metrics/customer-count",
                        "{\"entries\":"
                                + array(records)
                                + ",\"summary\":{\"current\":999999999,\"previous\":1}}")
                .put("query", "start-date=2024-02-28&end-date=2024-04-01&interval=" + interval);
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
            String resource, String id, String uuid, String eventTime, String payload)
            throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put("id", id)
                        .put("uuid", uuid)
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
                settings.dated ? "/chartmogul-window.conf" : "/chartmogul.conf",
                jobId(),
                settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(ChartMogulPeer.TOKEN), "API key leaked");
        assertFalse(
                output.contains(
                        "Basic "
                                + Base64.getEncoder()
                                        .encodeToString(
                                                (ChartMogulPeer.TOKEN + ":")
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
                                ChartMogulPeer.PASSWORD,
                                "-keypass",
                                ChartMogulPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, ChartMogulPeer.PASSWORD.toCharArray());
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
        String trust = ChartMogulIT.trust;
        String token = ChartMogulPeer.TOKEN;
        int request = 60000;
        int operation = 70000;
        int retries;
        int interval;
        int retryDelay = 60000;
        int bytes = 8388608;
        int pages = 10000;
        int scanPages = 100000;
        String start = START, end = END;
        int items = 10000;
        boolean dated;
        int pageSize = 2;

        Settings(String name, String... resources) {
            this.name = name;
            this.resources = resources;
            this.dated =
                    resources.length == 0
                            || Arrays.stream(resources)
                                    .anyMatch(resource -> !resource.equals("customers"));
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
                                    "api.items=" + items,
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
        private static final String PEER = "/tmp/chartmogul-peer";
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
            container.withExtraHost(ChartMogulPeer.HOST, "127.0.0.1");
            container.withExtraHost("chartmogul-trap.invalid", "127.0.0.1");
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
                stage(container, ChartMogulPeer.class);
                stage(container, ChartMogulRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java ChartMogul fixture", failure);
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
                    ChartMogulPeer.class.getName(),
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
                    ChartMogulRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/chartmogul-" + name;
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

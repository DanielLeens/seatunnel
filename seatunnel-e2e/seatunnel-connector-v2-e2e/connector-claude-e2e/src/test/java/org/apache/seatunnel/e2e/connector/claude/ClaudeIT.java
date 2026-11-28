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
package org.apache.seatunnel.e2e.connector.claude;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.claude.ClaudeRecoveryProbe;
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
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
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
 * Verifies native Claude reads through the real Java 11 Zeta engine and HTTPS.
 *
 * <p>Independent provider fixtures assert the Enterprise key and API version, native adoption
 * dates, grouped report windows, organization metadata and complete committed records.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ClaudeIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String BASE = "/v1/organizations/analytics/";
    private static final LocalDate TODAY = LocalDate.now(ZoneOffset.UTC);
    private static final LocalDate START =
            YearMonth.from(TODAY).minusMonths(2).atEndOfMonth().minusDays(1);
    private static final String REFRESH = "2026-11-01T06:00:00.123+00:00";
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
        trust = identity("trusted", ClaudePeer.HOST);
        wrongTrust = identity("untrusted", ClaudePeer.HOST);
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

    @Test
    void exportsAllNineNativeResourcesWithExactOrganizationAndWindowColumns() throws Exception {
        Settings s = new Settings("all");
        String user = user("user-one", EXTRA);
        String project = "{\"project_id\":\"project-one\",\"project_name\":null" + EXTRA + "}";
        String skill =
                "{\"skill_name\":\"skill-one\",\"distinct_conversation_skill_used_count\":0"
                        + EXTRA
                        + "}";
        String connector =
                "{\"connector_name\":\"connector-one\",\"distinct_conversation_connector_used_count\":1"
                        + EXTRA
                        + "}";
        String summary = summary(START);
        String usage = actor("user-one", "usage");
        String cost = actor("user-one", "tokens");
        String usageBucket =
                bucket(
                        START,
                        "[{\"product\":\"claude_code\",\"model\":null,\"cache_creation\":{\"ephemeral_1h_input_tokens\":0,\"ephemeral_5m_input_tokens\":1},\"cache_read_input_tokens\":0,\"uncached_input_tokens\":1,\"output_tokens\":9223372036854775808,\"requests\":1}]");
        String costBucket =
                bucket(
                        START,
                        "[{\"product\":\"claude_code\",\"model\":null,\"cost_type\":\"tokens\",\"currency\":\"USD\",\"amount\":\"0.00000000000001\"}]");
        program(
                s.name,
                List.of(
                        daily("users", START, 2, null, null, user),
                        daily("apps/chat/projects", START, 2, null, null, project),
                        daily("skills", START, 2, null, null, skill),
                        daily("connectors", START, 2, null, null, connector),
                        summaryStep(START, summary),
                        report(
                                "user_usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                REFRESH,
                                usage),
                        report(
                                "user_cost_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                REFRESH,
                                cost),
                        report(
                                "usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                REFRESH,
                                usageBucket),
                        report(
                                "cost_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                REFRESH,
                                costBucket)));
        success(run(s));
        consumed(s.name, 9);
        assertRows(
                s.name,
                List.of(
                        expected("users", "user-one", null, START, START.plusDays(1), null, user),
                        expected(
                                "chat_projects",
                                "project-one",
                                null,
                                START,
                                START.plusDays(1),
                                null,
                                project),
                        expected(
                                "skills", "skill-one", null, START, START.plusDays(1), null, skill),
                        expected(
                                "connectors",
                                "connector-one",
                                null,
                                START,
                                START.plusDays(1),
                                null,
                                connector),
                        expected("summaries", null, null, START, START.plusDays(1), null, summary),
                        expected(
                                "user_usage_report",
                                "user-one",
                                "org-one",
                                START,
                                START.plusDays(1),
                                REFRESH,
                                usage),
                        expected(
                                "user_cost_report",
                                "user-one",
                                "org-one",
                                START,
                                START.plusDays(1),
                                REFRESH,
                                cost),
                        expected(
                                "usage_report",
                                null,
                                "org-one",
                                START,
                                START.plusDays(1),
                                REFRESH,
                                usageBucket),
                        expected(
                                "cost_report",
                                null,
                                "org-one",
                                START,
                                START.plusDays(1),
                                REFRESH,
                                costBucket)));
    }

    @Test
    void continuesEmptyAndShortAdoptionPagesUsingEncodedOpaqueCursors() throws Exception {
        Settings s = new Settings("daily-cursor", "users");
        String first = user("same", ""), second = user("same", ",\"deleted\":true");
        String cursor = "https://unrelated.invalid/?secret=+ /&页";
        program(
                s.name,
                List.of(
                        daily("users", START, 2, null, cursor),
                        daily("users", START, 2, cursor, "two", first),
                        daily("users", START, 2, "two", null, second)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name,
                List.of(
                        expectedDaily("users", "same", START, first),
                        expectedDaily("users", "same", START, second)));
    }

    @Test
    void preservesDeletedUserNullAndUnknownNestedFieldsWithoutInventingAnId() throws Exception {
        Settings s = new Settings("deleted-user", "users");
        String row = "{\"user\":null,\"chat_metrics\":{\"messages_count\":0}" + EXTRA + "}";
        program(s.name, List.of(daily("users", START, 2, null, null, row)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expectedDaily("users", null, START, row)));
    }

    @Test
    void iteratesInclusiveAdoptionDatesAcrossAMonthBoundaryWithoutTimestampFiltering()
            throws Exception {
        Settings s = new Settings("date-slices", "users", "summaries");
        s.end = START.plusDays(2).toString();
        List<ObjectNode> steps = new ArrayList<>();
        List<JsonNode> expected = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            LocalDate day = START.plusDays(i);
            String row = user("U" + i, ",\"last_active_at\":null");
            steps.add(daily("users", day, 2, null, null, row));
            expected.add(expectedDaily("users", "U" + i, day, row));
        }
        for (int i = 0; i < 3; i++) {
            LocalDate day = START.plusDays(i);
            String row = summary(day);
            steps.add(summaryStep(day, row));
            expected.add(expectedDaily("summaries", null, day, row));
        }
        program(s.name, steps);
        success(run(s));
        consumed(s.name, 6);
        assertRows(s.name, expected);
    }

    @Test
    void preservesPerUserCostFanoutAboveTheActorLimitAndRepeatedNativeUserIds() throws Exception {
        Settings s = new Settings("cost-fanout", "user_cost_report");
        s.pageSize = 1;
        String a = actor("same-user", "tokens"),
                b = actor("same-user", "web_search"),
                c = actor("same-user", "code_execution");
        program(
                s.name,
                List.of(
                        report(
                                "user_cost_report",
                                START,
                                START.plusDays(1),
                                1,
                                null,
                                null,
                                REFRESH,
                                a,
                                b,
                                c)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(
                s.name,
                List.of(
                        expectedReport("user_cost_report", "same-user", a),
                        expectedReport("user_cost_report", "same-user", b),
                        expectedReport("user_cost_report", "same-user", c)));
    }

    @Test
    void chunksAggregateReportsIntoSevenDaysAndAPartialTailAndCapsTheirLimitAt31()
            throws Exception {
        Settings s = new Settings("bucket-slices", "usage_report");
        s.pageSize = 1000;
        s.end = START.plusDays(7).toString();
        String first = bucket(START, "[]"),
                seventh = bucket(START.plusDays(6), "[]"),
                last = bucket(START.plusDays(7), "[]");
        program(
                s.name,
                List.of(
                        report(
                                "usage_report",
                                START,
                                START.plusDays(7),
                                31,
                                null,
                                null,
                                null,
                                first,
                                seventh),
                        report(
                                "usage_report",
                                START.plusDays(7),
                                START.plusDays(8),
                                31,
                                null,
                                null,
                                null,
                                last)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(
                s.name,
                List.of(
                        expected(
                                "usage_report",
                                null,
                                "org-one",
                                START,
                                START.plusDays(7),
                                null,
                                first),
                        expected(
                                "usage_report",
                                null,
                                "org-one",
                                START,
                                START.plusDays(7),
                                null,
                                seventh),
                        expected(
                                "usage_report",
                                null,
                                "org-one",
                                START.plusDays(7),
                                START.plusDays(8),
                                null,
                                last)));
    }

    @Test
    void continuesAnEmptyReportPageAndAcceptsAFirstNullRefreshFollowedByAnExport()
            throws Exception {
        Settings s = new Settings("report-pages", "user_usage_report");
        String row = actor("U", "usage");
        program(
                s.name,
                List.of(
                        report(
                                "user_usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                "opaque+/=",
                                null),
                        report(
                                "user_usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                "opaque+/=",
                                null,
                                REFRESH,
                                row)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expectedReport("user_usage_report", "U", row)));
    }

    @Test
    void preservesValidEmptyReportsAndRejectsRowsWithoutAFreshnessWatermark() throws Exception {
        Settings s =
                new Settings(
                        "no-export",
                        "user_usage_report",
                        "user_cost_report",
                        "usage_report",
                        "cost_report");
        String empty = bucket(START, "[]");
        program(
                s.name,
                List.of(
                        report("user_usage_report", START, START.plusDays(1), 2, null, null, null),
                        report("user_cost_report", START, START.plusDays(1), 2, null, null, null),
                        report(
                                "usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                null,
                                empty),
                        report(
                                "cost_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                null,
                                empty)));
        success(run(s));
        consumed(s.name, 4);
        assertRows(
                s.name,
                List.of(
                        expected(
                                "usage_report",
                                null,
                                "org-one",
                                START,
                                START.plusDays(1),
                                null,
                                empty),
                        expected(
                                "cost_report",
                                null,
                                "org-one",
                                START,
                                START.plusDays(1),
                                null,
                                empty)));
        fails(
                new Settings("missing-user-refresh", "user_usage_report"),
                List.of(
                        report(
                                "user_usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                null,
                                actor("U", "usage"))),
                "freshness watermark");
        fails(
                new Settings("missing-bucket-refresh", "usage_report"),
                List.of(
                        report(
                                "usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                null,
                                bucket(START, "[{\"amount\":\"0\"}]"))),
                "freshness watermark");
    }

    @Test
    void pinsTheReportOrganizationAcrossPagesAndAcrossDifferentReportResources() throws Exception {
        ObjectNode second =
                report("user_usage_report", START, START.plusDays(1), 2, "next", null, REFRESH);
        second.put("response", second.path("response").asText().replace("org-one", "org-two"));
        fails(
                new Settings("org-page", "user_usage_report"),
                List.of(
                        report(
                                "user_usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                "next",
                                REFRESH),
                        second),
                "organization changed");
        second = report("user_cost_report", START, START.plusDays(1), 2, null, null, REFRESH);
        second.put("response", second.path("response").asText().replace("org-one", "org-two"));
        fails(
                new Settings("org-resource", "user_usage_report", "user_cost_report"),
                List.of(
                        report(
                                "user_usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                REFRESH),
                        second),
                "organization changed");
    }

    @Test
    void rejectsMalformedReportMetadataAndContradictoryContinuationBeforeEmission()
            throws Exception {
        String base = envelope(true, null, REFRESH, actor("U", "usage"));
        List<String> bad =
                List.of(
                        base.replace("\"org-one\"", "null"),
                        base.replace("\"organization_id\":\"org-one\",", ""),
                        base.replace("\"data_refreshed_at\":\"" + REFRESH + "\",", ""),
                        base.replace(REFRESH, "not-a-timestamp"),
                        base.replace("\"has_more\":false", "\"has_more\":true"),
                        base.replace("\"has_more\":false,", ""),
                        base.replace("\"next_page\":null", "\"next_page\":\"dangling\""));
        for (int i = 0; i < bad.size(); i++) {
            Settings s = new Settings("bad-meta-" + i, "user_usage_report");
            fails(
                    s,
                    List.of(
                            report(
                                            "user_usage_report",
                                            START,
                                            START.plusDays(1),
                                            2,
                                            null,
                                            null,
                                            REFRESH)
                                    .put("response", bad.get(i))),
                    "Claude");
            assertRows(s.name, List.of());
        }
    }

    @Test
    void rejectsLegacySummaryAliasesWrongDayBoundsAndNonterminalSummaries() throws Exception {
        String valid = envelope(false, null, null, summary(START));
        List<String> bad =
                List.of(
                        valid.replace("\"data\"", "\"summaries\""),
                        valid.replace("\"next_page\":null", "\"next_page\":\"unexpected\""),
                        valid.replace(utc(START), utc(START.plusDays(4))),
                        envelope(false, null, null),
                        envelope(false, null, null, summary(START), summary(START)));
        for (int i = 0; i < bad.size(); i++) {
            Settings s = new Settings("bad-summary-" + i, "summaries");
            fails(
                    s,
                    List.of(summaryStep(START, summary(START)).put("response", bad.get(i))),
                    "Claude");
            assertRows(s.name, List.of());
        }
    }

    @Test
    void rejectsOutOfOrderRepeatedAndOutOfRangeAggregateBuckets() throws Exception {
        for (int i = 0; i < 3; i++) {
            Settings s = new Settings("bad-bucket-" + i, "usage_report");
            s.end = START.plusDays(2).toString();
            String[] rows =
                    i == 0
                            ? new String[] {bucket(START.plusDays(1), "[]"), bucket(START, "[]")}
                            : i == 1
                                    ? new String[] {bucket(START, "[]"), bucket(START, "[]")}
                                    : new String[] {bucket(START.plusDays(3), "[]")};
            fails(
                    s,
                    List.of(
                            report(
                                    "usage_report",
                                    START,
                                    START.plusDays(3),
                                    2,
                                    null,
                                    null,
                                    REFRESH,
                                    rows)),
                    "bucket");
            assertRows(s.name, List.of());
        }
    }

    @Test
    void rejectsRepeatedCursorsAndFiniteCollectionAndScanBudgetExhaustion() throws Exception {
        fails(
                new Settings("cursor-repeat", "users"),
                List.of(daily("users", START, 2, null, "A"), daily("users", START, 2, "A", "A")),
                "repeated");
        fails(
                new Settings("cursor-cycle", "users"),
                List.of(
                        daily("users", START, 2, null, "A"),
                        daily("users", START, 2, "A", "B"),
                        daily("users", START, 2, "B", "A")),
                "repeated");
        Settings pages = new Settings("page-budget", "users");
        pages.pages = 1;
        fails(pages, List.of(daily("users", START, 2, null, "A")), "max_pages_per_collection");
        Settings scan = new Settings("scan-budget", "users", "skills");
        scan.scanPages = 1;
        fails(scan, List.of(daily("users", START, 2, null, null)), "max_pages_per_scan");
    }

    @Test
    void rejectsOversizedNativePagesAndSeparatelyBoundsPerUserCostFanout() throws Exception {
        fails(
                new Settings("over-page", "users"),
                List.of(
                        daily(
                                "users",
                                START,
                                2,
                                null,
                                null,
                                user("1", ""),
                                user("2", ""),
                                user("3", ""))),
                "page size limit");
        Settings s = new Settings("fanout-budget", "user_cost_report");
        s.items = 2;
        s.pageSize = 1;
        fails(
                s,
                List.of(
                        report(
                                "user_cost_report",
                                START,
                                START.plusDays(1),
                                1,
                                null,
                                null,
                                REFRESH,
                                actor("U", "tokens"),
                                actor("U", "web_search"),
                                actor("U", "code_execution"))),
                "record or page size limit");
    }

    @Test
    void rejectsMalformedCanonicalDataAndMissingOrInvalidNativeIdentities() throws Exception {
        List<String> invalid =
                List.of(
                        "[]",
                        "{\"data\":[],\"next_page\":7}",
                        "{\"data\":[]}",
                        "{\"data\":{},\"next_page\":null}",
                        envelope(false, null, null, "null"),
                        envelope(false, null, null, "{\"user\":{\"id\":7}}"),
                        envelope(false, null, null, "{}"));
        for (int i = 0; i < invalid.size(); i++) {
            Settings s = new Settings("bad-user-" + i, "users");
            fails(
                    s,
                    List.of(daily("users", START, 2, null, null).put("response", invalid.get(i))),
                    "Claude");
            assertRows(s.name, List.of());
        }
        fails(
                new Settings("wrong-actor-id", "user_usage_report"),
                List.of(
                        report(
                                "user_usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                REFRESH,
                                "{\"actor\":{\"id\":\"not-user-id\"}}")),
                "native identity");
    }

    @Test
    void failsExpiredReportCursorsWithoutSilentlyRestartingTheDateRange() throws Exception {
        Settings s = new Settings("expired-cursor", "user_usage_report");
        s.retries = 2;
        fails(
                s,
                List.of(
                        report(
                                "user_usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                "expired",
                                REFRESH),
                        report(
                                        "user_usage_report",
                                        START,
                                        START.plusDays(1),
                                        2,
                                        "expired",
                                        null,
                                        REFRESH)
                                .put("status", 410)
                                .put("response", "private-provider-message")),
                "cursor expired");
    }

    @Test
    void respectsRetryAfterAndPacesDifferentResourceFamiliesWithTheSameEnterpriseKey()
            throws Exception {
        Settings retry = new Settings("rate-limit", "users");
        retry.retries = 1;
        program(
                retry.name,
                List.of(
                        daily("users", START, 2, null, null)
                                .put("status", 429)
                                .put("Retry-After", "1"),
                        daily("users", START, 2, null, null)));
        success(run(retry));
        consumed(retry.name, 2);
        delayAtLeast(retry.name, 1000);
        Settings pace = new Settings("cross-resource-pace", "users", "user_usage_report");
        pace.interval = 600;
        program(
                pace.name,
                List.of(
                        daily("users", START, 2, null, null),
                        report(
                                "user_usage_report",
                                START,
                                START.plusDays(1),
                                2,
                                null,
                                null,
                                null)));
        success(run(pace));
        consumed(pace.name, 2);
        delayAtLeast(pace.name, 600);
    }

    @Test
    void boundsRetryExhaustionAndRefusesExcessiveServerDelays() throws Exception {
        Settings s = new Settings("retry-exhausted", "users");
        s.retries = 1;
        fails(
                s,
                List.of(
                        daily("users", START, 2, null, null).put("status", 503),
                        daily("users", START, 2, null, null).put("status", 503)),
                "HTTP 503");
        s = new Settings("delay-budget", "users");
        s.retries = 1;
        s.retryDelay = 250;
        fails(
                s,
                List.of(
                        daily("users", START, 2, null, null)
                                .put("status", 429)
                                .put("Retry-After", "2")),
                "max_retry_delay_ms");
        for (int status : new int[] {400, 401, 403, 404}) {
            Settings denied = new Settings("denied-" + status, "users");
            denied.retries = 2;
            fails(
                    denied,
                    List.of(
                            daily("users", START, 2, null, null)
                                    .put("status", status)
                                    .put("response", "private-provider-message")),
                    "HTTP " + status);
        }
    }

    @Test
    void preservesCompressedDataAndSeparatelyBoundsWireAndDecodedResponses() throws Exception {
        Settings good = new Settings("gzip", "users");
        String row = user("gzip", EXTRA);
        program(good.name, List.of(daily("users", START, 2, null, null, row).put("mode", "gzip")));
        success(run(good));
        consumed(good.name, 1);
        assertRows(good.name, List.of(expectedDaily("users", "gzip", START, row)));
        for (String mode : List.of("json", "gzip")) {
            Settings s = new Settings("body-budget-" + mode, "users");
            s.bytes = 256;
            fails(
                    s,
                    List.of(
                            daily(
                                            "users",
                                            START,
                                            2,
                                            null,
                                            null,
                                            user("large", ",\"text\":\"" + "x".repeat(2048) + "\""))
                                    .put("mode", mode)),
                    "max_response_bytes");
            assertRows(s.name, List.of());
        }
        fails(
                new Settings("bad-encoding", "users"),
                List.of(daily("users", START, 2, null, null).put("encoding", "br")),
                "content encoding");
        fails(
                new Settings("bad-json", "users"),
                List.of(daily("users", START, 2, null, null).put("response", "{broken")),
                "JSON");
    }

    @Test
    void validatesSelectionsCredentialsAndDateBoundariesBeforeAuthenticatedTraffic()
            throws Exception {
        fails(new Settings("unknown", "unknown"), List.of(), "resources");
        fails(new Settings("duplicate", "users", "users"), List.of(), "resources");
        Settings token = new Settings("bad-token", "users");
        token.token = " ";
        fails(token, List.of(), "token");
        for (String date :
                List.of(
                        "2025-12-31",
                        "2026-02-30",
                        START + "T00:00:00Z",
                        TODAY.plusDays(5).toString())) {
            Settings s = new Settings("bad-date-" + Math.abs(date.hashCode()), "users");
            s.start = date;
            s.end = date;
            fails(s, List.of(), "date");
        }
        Settings reversed = new Settings("reversed", "users");
        reversed.end = START.minusDays(1).toString();
        fails(reversed, List.of(), "end_date");
        Settings future = new Settings("future-end", "users");
        future.end = TODAY.plusDays(5).toString();
        fails(future, List.of(), "end_date");
    }

    @Test
    void verifiesCertificateTrustAndHostnameAndRefusesRedirectCredentialForwarding()
            throws Exception {
        Settings ca = new Settings("wrong-ca", "users");
        ca.trust = wrongTrust;
        fails(ca, List.of(), "certificate");
        selectIdentity("wrong-name");
        try {
            Settings san = new Settings("wrong-san", "users");
            san.trust = wrongNameTrust;
            fails(san, List.of(), "certificate");
        } finally {
            selectIdentity("trusted");
        }
        fails(
                new Settings("redirect", "users"),
                List.of(
                        daily("users", START, 2, null, null)
                                .put("status", 302)
                                .put("mode", "redirect")),
                "HTTP 302");
    }

    @Test
    void boundsContinuousBodyTrafficAndClosesItsSocketAtTheDeadline() throws Exception {
        Settings s = new Settings("deadline", "users");
        s.request = 2000;
        s.operation = 3000;
        fails(s, List.of(daily("users", START, 2, null, null).put("mode", "trickle")), "Claude");
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
        Settings s = new Settings("cancel", "users");
        program(s.name, List.of(daily("users", START, 2, null, null).put("mode", "trickle")));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/claude.conf", id, s.variables()));
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
    void restoresAnUnfinishedDailyPageWithoutReissuingTheCommittedPage() throws Exception {
        Settings s = new Settings("cursor-savepoint", "users");
        String first = user("first", ""), second = user("second", "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                daily("users", START, 2, null, "next", first),
                                daily("users", START, 2, "next", null, second)
                                        .put("mode", "partial")));
        program("cursor-restored", List.of(daily("users", START, 2, "next", null, second)));
        success(engine.restoreJob("/claude.conf", id, s.variables()));
        consumed("cursor-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expectedDaily("users", "first", START, first),
                        expectedDaily("users", "second", START, second)));
    }

    @Test
    void restoresBetweenDaysWithoutReopeningTheCompletedDateWindow() throws Exception {
        Settings s = new Settings("day-savepoint", "users");
        s.end = START.plusDays(1).toString();
        String first = user("first", ""), second = user("second", "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                daily("users", START, 2, null, null, first),
                                daily("users", START.plusDays(1), 2, null, null, second)
                                        .put("mode", "partial")));
        program("day-restored", List.of(daily("users", START.plusDays(1), 2, null, null, second)));
        success(engine.restoreJob("/claude.conf", id, s.variables()));
        consumed("day-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expectedDaily("users", "first", START, first),
                        expectedDaily("users", "second", START.plusDays(1), second)));
    }

    @Test
    void restoresReportMetadataAcrossResourcesAndRejectsAnOrganizationSwitch() throws Exception {
        Settings s = new Settings("report-savepoint", "user_usage_report", "user_cost_report");
        String usage = actor("U", "usage"), cost = actor("U", "tokens");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                report(
                                        "user_usage_report",
                                        START,
                                        START.plusDays(1),
                                        2,
                                        null,
                                        null,
                                        REFRESH,
                                        usage),
                                report(
                                                "user_cost_report",
                                                START,
                                                START.plusDays(1),
                                                2,
                                                null,
                                                null,
                                                REFRESH,
                                                cost)
                                        .put("mode", "partial")));
        ObjectNode bad =
                report("user_cost_report", START, START.plusDays(1), 2, null, null, REFRESH, cost);
        bad.put("response", bad.path("response").asText().replace("org-one", "org-two"));
        program("report-restored", List.of(bad));
        Container.ExecResult result = engine.restoreJob("/claude.conf", id, s.variables());
        assertNotEquals(0, result.getExitCode());
        assertTrue((result.getStdout() + result.getStderr()).contains("organization changed"));
        consumed("report-restored", 1);
        assertRows(s.name, List.of(expectedReport("user_usage_report", "U", usage)));
    }

    @Test
    void restoresATerminalSourceWithoutAuthenticatedTrafficOrDuplicateCommittedRows()
            throws Exception {
        Settings s = new Settings("terminal-savepoint", "summaries");
        String row = summary(START);
        List<JsonNode> wanted = List.of(expectedDaily("summaries", null, START, row));
        program(s.name, List.of(summaryStep(START, row)));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/claude-terminal.conf", id, s.variables()));
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
                tasks.submit(() -> engine.restoreJob("/claude-terminal.conf", id, s.variables()));
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
    void serializesAPublishedPrefixAndRejectsRawPageChangesAndScopeChanges() throws Exception {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < 33; i++) {
            rows.add(user("U" + i, ",\"description\":\"synthetic-response-record\""));
        }
        for (String mode : List.of("resume", "changed-prefix", "changed-suffix")) {
            List<String> replay = new ArrayList<>(rows);
            int changed = mode.equals("changed-suffix") ? 32 : 0;
            if (!mode.equals("resume")) {
                replay.set(changed, replay.get(changed).replace("U" + changed, "CHANGED"));
            }
            String name = "reader-" + mode;
            program(
                    name,
                    List.of(
                            daily("users", START, 50, null, null, rows.toArray(new String[0])),
                            daily("users", START, 50, null, null, replay.toArray(new String[0]))));
            success(engine.probe(mode));
            consumed(name, 2);
        }
        program("reader-zero", List.of());
        success(engine.probe("zero"));
        consumed("reader-zero", 0);
    }

    private String saveBlocked(Settings s, List<ObjectNode> steps) throws Exception {
        program(s.name, steps);
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/claude.conf", id, s.variables()));
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

    private static String utc(LocalDate date) {
        return date + "T00:00:00Z";
    }

    private static String user(String id, String extra) {
        return "{\"user\":{\"id\":\"" + id + "\",\"email_address\":null}" + extra + "}";
    }

    private static String actor(String id, String dimension) {
        String dimensions =
                dimension.equals("usage")
                        ? ",\"cache_creation\":{\"ephemeral_1h_input_tokens\":0,\"ephemeral_5m_input_tokens\":1},\"cache_read_input_tokens\":0,\"uncached_input_tokens\":1,\"output_tokens\":9223372036854775808,\"total_tokens\":9223372036854775810,\"requests\":1"
                        : ",\"cost_type\":\""
                                + dimension
                                + "\",\"currency\":\"USD\",\"amount\":\"0.00000000000001\"";
        return "{\"actor\":{\"user_id\":\""
                + id
                + "\",\"email_address\":null,\"deleted\":false},\"product\":\"claude_code\",\"model\":null,\"starting_at\":null,\"ending_at\":null"
                + dimensions
                + EXTRA
                + "}";
    }

    private static String summary(LocalDate day) {
        return "{\"starting_at\":\""
                + utc(day)
                + "\",\"ending_at\":\""
                + utc(day.plusDays(1))
                + "\",\"daily_active_user_count\":0,\"weekly_active_user_count\":1,\"monthly_active_user_count\":1"
                + EXTRA
                + "}";
    }

    private static String bucket(LocalDate day, String results) {
        return "{\"starting_at\":\""
                + utc(day)
                + "\",\"ending_at\":\""
                + utc(day.plusDays(1))
                + "\",\"results\":"
                + results
                + EXTRA
                + "}";
    }

    private static String quoted(String text) {
        return text == null ? "null" : JSON.getNodeFactory().textNode(text).toString();
    }

    private static String envelope(boolean report, String next, String refresh, String... rows) {
        return "{"
                + (report
                        ? "\"organization_id\":\"org-one\",\"data_refreshed_at\":"
                                + quoted(refresh)
                                + ",\"has_more\":"
                                + (next != null)
                                + ","
                        : "")
                + "\"data\":["
                + String.join(",", rows)
                + "],\"next_page\":"
                + quoted(next)
                + "}";
    }

    private static ObjectNode daily(
            String route, LocalDate day, int limit, String page, String next, String... rows) {
        return get(
                route,
                "date=" + day + "&limit=" + limit + pageQuery(page),
                envelope(false, next, null, rows));
    }

    private static ObjectNode summaryStep(LocalDate day, String row) {
        return get(
                "summaries",
                "starting_date=" + day + "&ending_date=" + day.plusDays(1),
                envelope(false, null, null, row));
    }

    private static ObjectNode report(
            String route,
            LocalDate start,
            LocalDate end,
            int limit,
            String page,
            String next,
            String refresh,
            String... rows) {
        String query =
                "starting_at="
                        + encode(utc(start))
                        + "&ending_at="
                        + encode(utc(end))
                        + "&limit="
                        + limit;
        if (route.equals("usage_report") || route.equals("cost_report")) {
            query += "&bucket_width=1d";
        }
        query += "&group_by%5B%5D=product&group_by%5B%5D=model";
        if (route.equals("user_cost_report") || route.equals("cost_report")) {
            query += "&group_by%5B%5D=cost_type";
        }
        return get(route, query + pageQuery(page), envelope(true, next, refresh, rows));
    }

    private static String pageQuery(String page) {
        return page == null ? "" : "&page=" + encode(page);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static ObjectNode get(String route, String query, String response) {
        return JSON.createObjectNode()
                .put("path", BASE + route)
                .put("query", query)
                .put("response", response);
    }

    private static JsonNode expectedDaily(String resource, String id, LocalDate day, String payload)
            throws IOException {
        return expected(resource, id, null, day, day.plusDays(1), null, payload);
    }

    private static JsonNode expectedReport(String resource, String id, String payload)
            throws IOException {
        return expected(resource, id, "org-one", START, START.plusDays(1), REFRESH, payload);
    }

    private static JsonNode expected(
            String resource,
            String id,
            String organization,
            LocalDate start,
            LocalDate end,
            String refresh,
            String payload)
            throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put("id", id)
                        .put("organization_id", organization)
                        .put("window_start", utc(start))
                        .put("window_end", utc(end))
                        .put("data_refreshed_at", refresh);
        row.set("record", JSON.readTree(payload));
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>(), wanted = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(parsed.isObject() && parsed.size() == 7, line);
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
        return engine.executeJob("/claude.conf", jobId(), settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(ClaudePeer.TOKEN), "data API key leaked");
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
                                ClaudePeer.PASSWORD,
                                "-keypass",
                                ClaudePeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, ClaudePeer.PASSWORD.toCharArray());
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
        String trust = ClaudeIT.trust;
        String token = ClaudePeer.TOKEN;
        int request = 60000;
        int operation = 70000;
        int retries;
        int interval;
        int retryDelay = 60000;
        int bytes = 8388608;
        int pages = 10000;
        int scanPages = 100000;
        String start = START.toString();
        String end = START.toString();
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
        private static final String PEER = "/tmp/claude-peer";
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
            container.withExtraHost(ClaudePeer.HOST, "127.0.0.1");
            container.withExtraHost("claude-trap.invalid", "127.0.0.1");
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
                stage(container, ClaudePeer.class);
                stage(container, ClaudeRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java Claude fixture", failure);
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
                    ClaudePeer.class.getName(),
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
                    ClaudeRecoveryProbe.class.getName(),
                    PEER + "/trusted.pem",
                    mode,
                    START.toString());
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
            String path = "/tmp/seatunnel_mnt/claude-" + name;
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

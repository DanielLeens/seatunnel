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
package org.apache.seatunnel.e2e.connector.clazar;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.clazar.ClazarRecoveryProbe;
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
import java.util.Locale;
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
 * Verifies published Clazar entity and dataset contracts through the real Java 11 Zeta engine.
 *
 * <p>Independent literal HTTPS fixtures validate each route, token exchange and continuation before
 * committed full rows are compared as a multiset. No production resource table is used to build
 * fixtures.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ClazarIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
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
        trust = identity("trusted", ClazarPeer.HOST);
        wrongTrust = identity("untrusted", ClazarPeer.HOST);
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

    private static final String FIRST = "30000000-0000-4000-8000-000000000001";
    private static final String SECOND = "30000000-0000-4000-8000-000000000002";
    private static final String THIRD = "30000000-0000-4000-8000-000000000003";
    private static final String TIME = "2024-02-29T00:00:00Z";
    private static final String AWS_REVENUE = "analytics_aws_marketplace_revenue";
    private static final String AWS_REVENUE_PATH = "/analytics/datasets/aws_marketplace_revenue";

    @Test
    void readsAllTwentyCollectionsAndDatasetsWithOneTokenAndExactPayloads() throws Exception {
        String[][] routes = {
            {"buyers", "/buyers/"},
            {"listings", "/listings/"},
            {"contracts", "/contracts/"},
            {"opportunities", "/opportunities/"},
            {"private_offers", "/private_offers/"},
            {"analytics_aws_cosell_opportunities", "/analytics/datasets/aws_cosell_opportunities"},
            {
                "analytics_aws_marketplace_disbursements",
                "/analytics/datasets/aws_marketplace_disbursements"
            },
            {"analytics_aws_marketplace_revenue", "/analytics/datasets/aws_marketplace_revenue"},
            {
                "analytics_azure_cosell_opportunities",
                "/analytics/datasets/azure_cosell_opportunities"
            },
            {
                "analytics_azure_marketplace_customers",
                "/analytics/datasets/azure_marketplace_customers"
            },
            {
                "analytics_azure_marketplace_metered_usage",
                "/analytics/datasets/azure_marketplace_metered_usage"
            },
            {"analytics_azure_marketplace_orders", "/analytics/datasets/azure_marketplace_orders"},
            {
                "analytics_azure_marketplace_revenue",
                "/analytics/datasets/azure_marketplace_revenue"
            },
            {
                "analytics_gcp_marketplace_charges_and_usage",
                "/analytics/datasets/gcp_marketplace_charges_and_usage"
            },
            {
                "analytics_gcp_marketplace_daily_insights",
                "/analytics/datasets/gcp_marketplace_daily_insights"
            },
            {
                "analytics_gcp_marketplace_disbursements",
                "/analytics/datasets/gcp_marketplace_disbursements"
            },
            {
                "analytics_gcp_marketplace_disbursements_summary",
                "/analytics/datasets/gcp_marketplace_disbursements_summary"
            },
            {
                "analytics_gcp_marketplace_incremental_daily_insights",
                "/analytics/datasets/gcp_marketplace_incremental_daily_insights"
            },
            {
                "analytics_gcp_marketplace_incremental_monthly_insights",
                "/analytics/datasets/gcp_marketplace_incremental_monthly_insights"
            },
            {
                "analytics_gcp_marketplace_monthly_insights",
                "/analytics/datasets/gcp_marketplace_monthly_insights"
            }
        };
        String[] names = new String[routes.length];
        List<ObjectNode> steps = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        steps.add(auth());
        for (int i = 0; i < routes.length; i++) {
            String name = routes[i][0];
            names[i] = name;
            boolean entity = i < 5;
            String row =
                    entity
                            ? entity(FIRST, TIME)
                            : object(
                                    "\"currency\":\"EUR\",\"decimal_string\":\"9876543210.10000000000\"");
            steps.add(page(routes[i][1], 1, 100, 1, null, row));
            wanted.add(expected(name, entity ? FIRST : null, entity ? TIME : null, row));
        }
        Settings settings = new Settings("all-resources", names);
        program(settings.name, steps);
        success(run(settings));
        consumed(settings.name, 21);
        assertRows(settings.name, wanted);
    }

    @Test
    void followsSafeEmptyAndShortContinuationsAndPreservesDuplicateAnalyticsRows()
            throws Exception {
        Settings s = new Settings("analytics-paging", AWS_REVENUE);
        s.pageSize = 3;
        String amount = object("\"currency\":\"USD\",\"net\":\"19.9900\"");
        String second = object("\"currency\":\"JPY\",\"net\":141421356237309.50488");
        program(
                s.name,
                List.of(
                        auth(),
                        page(AWS_REVENUE_PATH, 1, 3, 3, "?page=2&page_size=3"),
                        page(
                                AWS_REVENUE_PATH,
                                2,
                                3,
                                3,
                                "https://api.clazar.io" + AWS_REVENUE_PATH + "?page=3",
                                amount),
                        page(AWS_REVENUE_PATH, 3, 3, 3, null, amount, second)));
        success(run(s));
        consumed(s.name, 4);
        assertRows(
                s.name,
                List.of(
                        expected(AWS_REVENUE, null, null, amount),
                        expected(AWS_REVENUE, null, null, amount),
                        expected(AWS_REVENUE, null, null, second)));
    }

    @Test
    void filtersOnlyEntityDatesAcrossPagesAndKeepsBothRowsOnTheStartBoundary() throws Exception {
        Settings s = new Settings("local-window", "contracts", AWS_REVENUE);
        s.config = "/clazar-window.conf";
        s.pageSize = 3;
        String start = entity(FIRST, "2024-02-29T02:00:00+02:00");
        String tie = entity(SECOND, TIME);
        String last = entity(THIRD, "2024-02-29T23:59:59.999999999Z");
        String old = entity(id(4), "2024-02-28T23:59:59.999999999Z");
        String end = entity(id(5), "2024-03-01T00:00:00Z");
        String amount = object("\"report_day\":\"2022-01-01\",\"currency\":\"CAD\"");
        program(
                s.name,
                List.of(
                        auth(),
                        page("/contracts/", 1, 3, 5, "/contracts/?page=2&page_size=3", old, start),
                        page("/contracts/", 2, 3, 5, null, end, tie, last),
                        page(AWS_REVENUE_PATH, 1, 3, 1, null, amount)));
        success(run(s));
        consumed(s.name, 4);
        assertRows(
                s.name,
                List.of(
                        expected("contracts", FIRST, TIME, start),
                        expected("contracts", SECOND, TIME, tie),
                        expected("contracts", THIRD, "2024-02-29T23:59:59.999999999Z", last),
                        expected(AWS_REVENUE, null, null, amount)));
    }

    @Test
    void obtainsANewTokenAfterMonotonicExpiryWithoutARejectedGet() throws Exception {
        Settings s = new Settings("token-expiry", "buyers");
        String first = entity(FIRST, TIME), second = entity(SECOND, TIME);
        program(
                s.name,
                List.of(
                        auth(ClazarPeer.TOKEN, 1),
                        page("/buyers/", 1, 100, 2, "?page=2", first).put("delay", 1250),
                        auth(ClazarPeer.NEXT_TOKEN, 3600),
                        page("/buyers/", 2, 100, 2, null, second)
                                .put("bearer", ClazarPeer.NEXT_TOKEN)));
        success(run(s));
        consumed(s.name, 4);
        assertRows(
                s.name,
                List.of(
                        expected("buyers", FIRST, TIME, first),
                        expected("buyers", SECOND, TIME, second)));
    }

    @Test
    void renewsOnceAfterExplicitUnauthorizedWithoutDroppingTheFailedPage() throws Exception {
        Settings s = new Settings("one-renewal", "listings");
        s.attempts = 2;
        String row = entity(FIRST, TIME);
        program(
                s.name,
                List.of(
                        auth(),
                        error("/listings/", 401),
                        auth(ClazarPeer.NEXT_TOKEN, 3600),
                        page("/listings/", 1, 100, 1, null, row)
                                .put("bearer", ClazarPeer.NEXT_TOKEN)));
        success(run(s));
        consumed(s.name, 4);
        assertRows(s.name, List.of(expected("listings", FIRST, TIME, row)));
    }

    @Test
    void failsASecondUnauthorizedWithoutMintingAThirdToken() throws Exception {
        Settings s = new Settings("repeat-unauthorized", "buyers");
        s.attempts = 4;
        fails(
                s,
                List.of(
                        auth(),
                        error("/buyers/", 401),
                        auth(ClazarPeer.NEXT_TOKEN, 3600),
                        error("/buyers/", 401).put("bearer", ClazarPeer.NEXT_TOKEN)),
                "HTTP 401");
        assertRows(s.name, List.of());
    }

    @Test
    void doesNotReplayRejectedOrAmbiguouslyFailedAuthentication() throws Exception {
        for (int code : List.of(401, 429, 503)) {
            Settings s = new Settings("auth-status-" + code, "buyers");
            s.attempts = 4;
            fails(
                    s,
                    List.of(
                            auth().put("status", code)
                                    .put("response", "{\"message\":\"private-provider-message\"}")
                                    .put("Retry-After", "0")),
                    "HTTP " + code);
        }
        Settings s = new Settings("auth-truncated", "buyers");
        s.attempts = 3;
        fails(s, List.of(auth().put("mode", "truncated")), "token exchange");
    }

    @Test
    void rejectsMalformedCredentialsReturnedByTheProviderBeforeDataReads() throws Exception {
        for (String response :
                List.of(
                        "{\"access_token\":\"no-lifetime\",\"token_type\":\"Bearer\"}",
                        "{\"access_token\":\"no token\",\"expires_in\":100,\"token_type\":\"Bearer\"}",
                        "{\"access_token\":\"wrong-type\",\"expires_in\":100,\"token_type\":\"Basic\"}",
                        "{\"access_token\":\"expired\",\"expires_in\":0,\"token_type\":\"Bearer\"}")) {
            Settings s =
                    new Settings(
                            "bad-auth-" + Integer.toUnsignedString(response.hashCode()), "buyers");
            s.attempts = 4;
            fails(s, List.of(auth().put("response", response)), "invalid bearer token response");
        }
    }

    @Test
    void rejectsForbiddenAndMissingResourcesWithoutRenewing() throws Exception {
        for (int code : List.of(403, 404)) {
            Settings s = new Settings("data-http-" + code, "private_offers");
            s.attempts = 4;
            fails(s, List.of(auth(), error("/private_offers/", code)), "HTTP " + code);
            assertRows(s.name, List.of());
        }
    }

    @Test
    void retriesOnlyDataReadsAndHonorsTheProviderRetryDelay() throws Exception {
        Settings s = new Settings("data-retry", "opportunities");
        s.attempts = 3;
        String row = entity(FIRST, TIME);
        program(
                s.name,
                List.of(
                        auth(),
                        error("/opportunities/", 503).put("Retry-After", "0"),
                        error("/opportunities/", 429).put("Retry-After", "2"),
                        page("/opportunities/", 1, 100, 1, null, row)));
        success(run(s));
        consumed(s.name, 4);
        List<String> times = engine.lines(s.name + ".times");
        assertTrue(
                Long.parseLong(times.get(3)) - Long.parseLong(times.get(2)) >= 2000,
                times.toString());
        assertRows(s.name, List.of(expected("opportunities", FIRST, TIME, row)));
        Settings exhausted = new Settings("data-exhausted", "opportunities");
        exhausted.attempts = 2;
        fails(
                exhausted,
                List.of(auth(), error("/opportunities/", 500), error("/opportunities/", 500)),
                "HTTP 500");
    }

    @Test
    void failsLinksThatChangeOriginResourceParametersOrPageOrderWithoutFollowingThem()
            throws Exception {
        List<String> bad =
                List.of(
                        "https://clazar-trap.invalid/buyers/?page=2",
                        "http://api.clazar.io/buyers/?page=2",
                        "https://api.clazar.io:444/buyers/?page=2",
                        "https://user:secret@api.clazar.io/buyers/?page=2",
                        "/listings/?page=2",
                        "?page=1",
                        "?page=3",
                        "?page=2&page=2",
                        "?page=2&page_size=1",
                        "?page=2#unexpected",
                        "?page=2&unsupported=value");
        int i = 0;
        for (String link : bad) {
            Settings s = new Settings("foreign-link-" + i++, "buyers");
            fails(
                    s,
                    List.of(auth(), page("/buyers/", 1, 100, 2, link, entity(FIRST, TIME))),
                    "next page link");
            assertRows(s.name, List.of());
        }
    }

    @Test
    void failsCountDriftPrematureTerminationOrDataBeyondTheTotal() throws Exception {
        String row = entity(FIRST, TIME);
        Settings drift = new Settings("count-drift", "buyers");
        fails(
                drift,
                List.of(
                        auth(),
                        page("/buyers/", 1, 100, 2, "?page=2", row),
                        page("/buyers/", 2, 100, 3, null, entity(SECOND, TIME))),
                "count changed");
        Settings shortPage = new Settings("short-terminal", "buyers");
        fails(shortPage, List.of(auth(), page("/buyers/", 1, 100, 2, null, row)), "does not agree");
        Settings overflow = new Settings("count-overflow", "buyers");
        fails(overflow, List.of(auth(), page("/buyers/", 1, 100, 0, null, row)), "does not agree");
        Settings badNext = new Settings("next-after-count", "buyers");
        fails(
                badNext,
                List.of(auth(), page("/buyers/", 1, 100, 1, "?page=2", row)),
                "does not agree");
    }

    @Test
    void rejectsRepeatedEntityIdsButAllowsAnEmptyTerminalPage() throws Exception {
        Settings same = new Settings("duplicate-on-page", "buyers");
        String row = entity(FIRST, TIME);
        fails(
                same,
                List.of(auth(), page("/buyers/", 1, 100, 2, null, row, row)),
                "identity repeated");
        Settings adjacent = new Settings("duplicate-across-pages", "buyers");
        fails(
                adjacent,
                List.of(
                        auth(),
                        page("/buyers/", 1, 100, 2, "?page=2", row),
                        page("/buyers/", 2, 100, 2, null, row)),
                "identity repeated");
        Settings empty = new Settings("empty-collection", "buyers");
        program(empty.name, List.of(auth(), page("/buyers/", 1, 100, 0, null)));
        success(run(empty));
        consumed(empty.name, 2);
        assertRows(empty.name, List.of());
    }

    @Test
    void enforcesPageAndResponseBudgetsBeforeAdditionalRequestsOrOutput() throws Exception {
        Settings pages = new Settings("page-budget", "buyers");
        pages.requests = 1;
        fails(
                pages,
                List.of(auth(), page("/buyers/", 1, 100, 2, "?page=2", entity(FIRST, TIME))),
                "max_requests");
        Settings oversize = new Settings("page-width", "buyers");
        oversize.pageSize = 1;
        fails(
                oversize,
                List.of(
                        auth(),
                        page("/buyers/", 1, 1, 2, null, entity(FIRST, TIME), entity(SECOND, TIME))),
                "within page_size");
        assertRows(oversize.name, List.of());
    }

    @Test
    void rejectsMalformedEnvelopesEntitiesUtf8AndTrailingJson() throws Exception {
        List<String> bodies =
                List.of(
                        "{\"count\":1,\"next\":null,\"results\":{}}",
                        "{\"count\":0,\"results\":[]}",
                        "{\"count\":-1,\"next\":null,\"results\":[]}",
                        "{\"count\":0,\"count\":0,\"next\":null,\"results\":[]}",
                        response(
                                1,
                                null,
                                "{\"id\":\"not-a-uuid\",\"last_modified_at\":\"2024-01-01T00:00:00Z\"}"),
                        response(1, null, entity(FIRST, "timezone-missing")),
                        response(1, null, "true"),
                        response(0, null) + " null");
        int i = 0;
        for (String body : bodies) {
            Settings s = new Settings("malformed-" + i++, "buyers");
            fails(s, List.of(auth(), request("/buyers/", 1, 100, body)), "Clazar");
            assertRows(s.name, List.of());
        }
        Settings utf = new Settings("invalid-utf8", "buyers");
        fails(
                utf,
                List.of(
                        auth(),
                        request("/buyers/", 1, 100, "")
                                .put(
                                        "bytes",
                                        Base64.getEncoder()
                                                .encodeToString(new byte[] {(byte) 0xc3, 0x28}))),
                "invalid UTF-8");
    }

    @Test
    void validatesMediaAndBoundsDecodedGzipWhileKeepingAnOrdinaryGzipPage() throws Exception {
        Settings gzip = new Settings("gzip-page", "buyers");
        String row = entity(FIRST, TIME);
        program(
                gzip.name,
                List.of(auth(), page("/buyers/", 1, 100, 1, null, row).put("mode", "gzip")));
        success(run(gzip));
        consumed(gzip.name, 2);
        assertRows(gzip.name, List.of(expected("buyers", FIRST, TIME, row)));
        Settings oversize = new Settings("gzip-decoded-budget", AWS_REVENUE);
        oversize.bytes = 1024;
        fails(
                oversize,
                List.of(
                        auth(),
                        page(
                                        AWS_REVENUE_PATH,
                                        1,
                                        100,
                                        1,
                                        null,
                                        "{\"detail\":\"" + "x".repeat(2048) + "\"}")
                                .put("mode", "gzip")),
                "max_response_bytes");
        Settings mime = new Settings("bad-mime", "buyers");
        fails(
                mime,
                List.of(auth(), page("/buyers/", 1, 100, 0, null).put("mime", "text/plain")),
                "Content-Type");
        Settings encoding = new Settings("bad-encoding", "buyers");
        fails(
                encoding,
                List.of(auth(), page("/buyers/", 1, 100, 0, null).put("encoding", "br")),
                "compression");
    }

    @Test
    void validatesTlsBeforeSendingCredentialsAndRejectsAuthenticationAndDataRedirects()
            throws Exception {
        Settings ca = new Settings("wrong-ca", "buyers");
        ca.certificate = wrongTrust;
        fails(ca, List.of(), "TLS certificate");
        selectIdentity("wrong-name");
        Settings hostname = new Settings("wrong-host", "buyers");
        hostname.certificate = wrongNameTrust;
        fails(hostname, List.of(), "TLS certificate");
        selectIdentity("trusted");
        Settings authRedirect = new Settings("auth-redirect", "buyers");
        fails(authRedirect, List.of(auth().put("status", 302).put("mode", "redirect")), "HTTP 302");
        Settings dataRedirect = new Settings("data-redirect", "buyers");
        fails(
                dataRedirect,
                List.of(auth(), error("/buyers/", 302).put("mode", "redirect")),
                "HTTP 302");
    }

    @Test
    void rejectsInvalidDateOptionsAndCredentialsBeforeSendingAnyRequest() throws Exception {
        Settings empty = new Settings("empty-resources");
        fails(empty, List.of(), "Clazar");
        Settings duplicate = new Settings("duplicate-resource", "buyers", "buyers");
        fails(duplicate, List.of(), "Clazar");
        Settings blank = new Settings("invalid-credentials", "buyers");
        blank.clientSecret = "   ";
        fails(blank, List.of(), "client_secret");
        Settings analyticsWindow = new Settings("analytics-only-window", AWS_REVENUE);
        analyticsWindow.config = "/clazar-window.conf";
        fails(analyticsWindow, List.of(), "date bounds");
        Settings width = new Settings("invalid-width", "buyers");
        width.pageSize = 5001;
        fails(width, List.of(), "page_size");
    }

    @Test
    void enforcesOneDeadlineForAuthenticationAndTheDataBody() throws Exception {
        Settings s = new Settings("total-deadline", "buyers");
        s.timeout = 2500;
        s.attempts = 2;
        fails(
                s,
                List.of(
                        auth().put("delay", 700),
                        page("/buyers/", 1, 100, 1, null, entity(FIRST, TIME))
                                .put("mode", "trickle")),
                "deadline");
        Awaitility.await()
                .atMost(8, TimeUnit.SECONDS)
                .until(() -> engine.exists(s.name + ".disconnect"));
        assertRows(s.name, List.of());
    }

    @Test
    void cancelsAnActiveZetaReadWithoutPublishingIncompleteRecords() throws Exception {
        Settings s = new Settings("cancel-data", "buyers");
        program(
                s.name,
                List.of(
                        auth(),
                        page("/buyers/", 1, 100, 1, null, entity(FIRST, TIME))
                                .put("mode", "trickle")));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(s.config, id, s.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".started"));
            success(engine.cancelJob(id));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(8, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".disconnect"));
        } finally {
            if (!running.isDone()) {
                running.cancel(true);
            }
        }
        consumed(s.name, 2);
        assertRows(s.name, List.of());
    }

    @Test
    void cancelsAnActiveTokenExchangeWithoutReplayingItOrReadingData() throws Exception {
        Settings s = new Settings("cancel-authentication", "buyers");
        s.attempts = 4;
        program(s.name, List.of(auth().put("mode", "trickle")));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(s.config, id, s.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".started"));
            success(engine.cancelJob(id));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(8, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".disconnect"));
        } finally {
            if (!running.isDone()) {
                running.cancel(true);
            }
        }
        consumed(s.name, 1);
        assertRows(s.name, List.of());
    }

    @Test
    void restoresAnUnfinishedPageWithoutResendingTheCommittedPage() throws Exception {
        Settings s = new Settings("page-savepoint", "buyers");
        String first = entity(FIRST, TIME), second = entity(SECOND, TIME);
        String id =
                saveBlocked(
                        s,
                        List.of(
                                auth(),
                                page("/buyers/", 1, 100, 2, "?page=2", first),
                                page("/buyers/", 2, 100, 2, null, second).put("mode", "partial")));
        program("page-restored", List.of(auth(), page("/buyers/", 2, 100, 2, null, second)));
        success(engine.restoreJob(s.config, id, s.variables()));
        consumed("page-restored", 2);
        assertRows(
                s.name,
                List.of(
                        expected("buyers", FIRST, TIME, first),
                        expected("buyers", SECOND, TIME, second)));
    }

    @Test
    void restoresAtTheEntityToAnalyticsBoundaryWithoutRescanningEntities() throws Exception {
        Settings s = new Settings("resource-savepoint", "buyers", AWS_REVENUE);
        String first = entity(FIRST, TIME), second = object("\"currency\":\"EUR\"");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                auth(),
                                page("/buyers/", 1, 100, 1, null, first),
                                page(AWS_REVENUE_PATH, 1, 100, 1, null, second)
                                        .put("mode", "partial")));
        program(
                "resource-restored",
                List.of(auth(), page(AWS_REVENUE_PATH, 1, 100, 1, null, second)));
        success(engine.restoreJob(s.config, id, s.variables()));
        consumed("resource-restored", 2);
        assertRows(
                s.name,
                List.of(
                        expected("buyers", FIRST, TIME, first),
                        expected(AWS_REVENUE, null, null, second)));
    }

    @Test
    void restoresATerminalSourceWithoutAnotherTokenExchangeOrDuplicateRows() throws Exception {
        Settings s = new Settings("terminal-savepoint", "buyers");
        String row = entity(FIRST, TIME);
        List<JsonNode> wanted = List.of(expected("buyers", FIRST, TIME, row));
        program(s.name, List.of(auth(), page("/buyers/", 1, 100, 1, null, row)));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/clazar-terminal.conf", id, s.variables()));
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
                tasks.submit(() -> engine.restoreJob("/clazar-terminal.conf", id, s.variables()));
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
    void replaysOnlyThePackagedReaderSuffixAndRejectsChangesInTheRawPage() throws Exception {
        List<String> rows = new ArrayList<>();
        for (int i = 1; i <= 33; i++) {
            rows.add(entity(id(i), TIME));
        }
        rows.add(entity(id(34), "2023-01-01T00:00:00Z"));
        for (String mode : List.of("resume", "changed", "changed-filtered")) {
            List<String> replay = new ArrayList<>(rows);
            if (mode.equals("changed")) {
                replay.set(0, entity(id(201), TIME));
            }
            if (mode.equals("changed-filtered")) {
                replay.set(33, entity(id(202), "2023-01-01T00:00:00Z"));
            }
            String name = "reader-" + mode;
            program(
                    name,
                    List.of(
                            auth(), page("/buyers/", 1, 100, 34, null, rows.toArray(new String[0])),
                            auth(),
                                    page(
                                            "/buyers/",
                                            1,
                                            100,
                                            34,
                                            null,
                                            replay.toArray(new String[0]))));
            success(engine.probe(mode));
            consumed(name, 4);
        }
        program("reader-zero", List.of());
        success(engine.probe("zero"));
        consumed("reader-zero", 0);
    }

    private String saveBlocked(Settings s, List<ObjectNode> steps) throws Exception {
        program(s.name, steps);
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(s.config, id, s.variables()));
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

    private static String object(String fields) {
        return "{" + fields + EXTRA + "}";
    }

    private static String id(int value) {
        return String.format(Locale.ROOT, "40000000-0000-4000-8000-%012d", value);
    }

    private static String entity(String id, String time) {
        return object(
                "\"id\":\""
                        + id
                        + "\",\"last_modified_at\":\""
                        + time
                        + "\",\"metadata\":{\"marketplace\":\"synthetic\"},\"registration_details\":[null,3]");
    }

    private static String response(long count, String next, String... rows) {
        return "{\"count\":"
                + count
                + ",\"next\":"
                + (next == null ? "null" : JSON.getNodeFactory().textNode(next).toString())
                + ",\"previous\":null,\"results\":"
                + array(rows)
                + "}";
    }

    private static ObjectNode page(
            String path, int page, int size, long count, String next, String... rows) {
        return request(path, page, size, response(count, next, rows));
    }

    private static ObjectNode request(String path, int page, int size, String body) {
        return get(path, body).put("query", "page=" + page + "&page_size=" + size);
    }

    private static ObjectNode auth() {
        return auth(ClazarPeer.TOKEN, 3600);
    }

    private static ObjectNode auth(String bearer, int seconds) {
        String payload =
                JSON.createObjectNode()
                        .put("access_token", bearer)
                        .put("expires_in", seconds)
                        .put("token_type", "Bearer")
                        .toString();
        return get("/authenticate/", payload).put("method", "POST");
    }

    private static ObjectNode error(String path, int status) {
        return request(path, 1, 100, "{\"message\":\"private-provider-message\"}")
                .put("status", status);
    }

    private static String array(String... records) {
        return "[" + String.join(",", records) + "]";
    }

    private static ObjectNode get(String path, String response) {
        return JSON.createObjectNode().put("path", path).put("query", "").put("response", response);
    }

    private static JsonNode expected(String resource, String id, String time, String payload)
            throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("record_time", time);
        row.set("payload", JSON.readTree(payload));
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>(), wanted = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(parsed.isObject() && parsed.size() == 4, line);
            assertTrue(parsed.path("payload").isTextual(), line);
            ObjectNode row = (ObjectNode) parsed;
            row.set("payload", JSON.readTree(parsed.path("payload").textValue()));
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
        return engine.executeJob(settings.config, jobId(), settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(ClazarPeer.TOKEN), "bearer token leaked");
        assertFalse(output.contains(ClazarPeer.SECRET), "client secret leaked");
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
                                ClazarPeer.PASSWORD,
                                "-keypass",
                                ClazarPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, ClazarPeer.PASSWORD.toCharArray());
        }
        Files.writeString(
                directory.resolve(name + ".pem"),
                "-----BEGIN CERTIFICATE-----\n"
                        + Base64.getMimeEncoder(64, new byte[] {'\n'})
                                .encodeToString(keys.getCertificate("api").getEncoded())
                        + "\n-----END CERTIFICATE-----\n");
        return "/tmp/clazar-peer/" + name + ".pem";
    }

    /**
     * Configures only synthetic tenant scopes and bounded reads for one real engine job.
     *
     * <p>The date-window fixture includes explicit endpoints. Ordinary jobs omit date options.
     */
    private static final class Settings {
        final String name;
        final String[] resources;
        String clientId = ClazarPeer.CLIENT_ID;
        String clientSecret = ClazarPeer.SECRET;
        String certificate = trust;
        String start = "2024-02-29T00:00:00Z", end = "2024-03-01T00:00:00Z";
        int requests = 10000, pageSize = 100, attempts = 1, timeout = 60000, bytes = 8388608;
        String config = "/clazar.conf";

        Settings(String name, String... resources) {
            this.name = name;
            this.resources = resources;
        }

        String[] variables() {
            ArrayNode selection = JSON.createArrayNode();
            for (String resource : resources) {
                selection.add(resource);
            }
            return new String[] {
                "api.output=" + name,
                "api.resources=" + selection,
                "api.client_id=" + clientId,
                "api.client_secret=" + clientSecret,
                "api.certificate=" + certificate,
                "api.requests=" + requests,
                "api.page_size=" + pageSize,
                "api.attempts=" + attempts,
                "api.timeout=" + timeout,
                "api.bytes=" + bytes,
                "api.start=" + start,
                "api.end=" + end
            };
        }
    }

    /**
     * Hosts normal packaged plugins and independent Java TLS fixtures in one owned worker.
     *
     * <p>DNS and certificate trust overrides apply only to this integration-test container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER = "/tmp/clazar-peer";
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
            container.withExtraHost(ClazarPeer.HOST, "127.0.0.1");
            container.withExtraHost("clazar-trap.invalid", "127.0.0.1");
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
                stage(container, ClazarPeer.class);
                stage(container, ClazarRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java Clazar fixture", failure);
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
                    ClazarPeer.class.getName(),
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
                    ClazarRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/clazar-" + name;
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

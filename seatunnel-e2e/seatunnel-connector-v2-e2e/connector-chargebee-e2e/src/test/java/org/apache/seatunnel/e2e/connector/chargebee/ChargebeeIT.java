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

package org.apache.seatunnel.e2e.connector.chargebee;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeRecoveryProbe;
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
import java.math.BigDecimal;
import java.math.BigInteger;
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
import java.time.Instant;
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
 * Drives the packaged Chargebee Source through real Zeta jobs and an independent HTTPS peer.
 *
 * <p>The literal resource matrix and request scripts establish the expected provider wire bytes.
 * Transactional LocalFile receipts are the result oracle.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ChargebeeIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String START = "2024-01-01T00:00:00Z";
    private static final String END = "2024-01-02T00:00:00Z";
    private static final long START_SEC = 1704067200L;
    private static final long END_SEC = 1704153600L;
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
        trust = identity("trusted", ChargebeePeer.HOST);
        wrongTrust = identity("untrusted", ChargebeePeer.HOST);
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
    void readsAll27ResourcesAcrossBothCatalogsAndPreservesProjectedValues() throws Exception {
        List<Contract> table = catalog();
        assertEquals(27, table.size());
        for (Contract row : table) {
            List<String> catalogs =
                    row.catalog.equals("both") && row.name.equals("customer")
                            ? List.of("1.0", "2.0")
                            : List.of(row.catalog.equals("1.0") ? "1.0" : "2.0");
            for (String version : catalogs) {
                Settings settings = new Settings(row.name + "-" + version, row.name);
                settings.catalog = version;
                String parentId = row.parent == null ? null : "parent-1";
                String id = row.mode.equals("singleton") ? parentId : "entity-1";
                ObjectNode wire = sample(row, id, row.cursor == null ? null : START_SEC);
                List<ObjectNode> requests = new ArrayList<>();
                if (parentId != null) {
                    requests.add(parent(settings, row, parentId));
                }
                requests.add(step(settings, row, parentId, null, false, envelope(row, null, wire)));
                program(settings.name, requests);
                success(run(settings));
                assertRows(settings.name, List.of(output(row, wire, parentId)));
                assertTrue(engine.rows(settings.name).get(0).contains("123456789012345678.012300"));
                consumed(settings.name, requests.size());
            }
        }
    }

    @Test
    void rejectsCatalogMismatchesAndInvalidCredentialsBeforeAnyHttpsRequest() throws Exception {
        Settings plan = new Settings("plan-in-pc2", "plan");
        fails(plan, List.of(), "product_catalog");
        Settings item = new Settings("item-in-pc1", "item");
        item.catalog = "1.0";
        fails(item, List.of(), "product_catalog");
        Settings invalidVersion = new Settings("invalid-catalog", "customer");
        invalidVersion.catalog = "2.1";
        fails(invalidVersion, List.of(), "product_catalog");
        Settings site = new Settings("invalid-site", "customer");
        site.site = "https://somewhere.invalid/api";
        fails(site, List.of(), "site");
        Settings key = new Settings("invalid-key", "customer");
        key.token = "non-provider-key:password";
        fails(key, List.of(), "token");
    }

    @Test
    void keepsOpaqueOffsetsVerbatimAcrossEmptyAndShortPages() throws Exception {
        Settings settings = new Settings("opaque", "invoice");
        Contract row = named(settings.resource);
        String first = "[\"/+&= 雪\",123]";
        String second = "https://chargebee-trap.invalid/not-a-route?a=b&token=untouched";
        ObjectNode one = sample(row, "invoice-1", START_SEC);
        ObjectNode two = sample(row, "invoice-2", START_SEC);
        program(
                settings.name,
                List.of(
                        step(settings, row, null, null, false, envelope(row, first)),
                        step(settings, row, null, first, false, envelope(row, second, one)),
                        step(settings, row, null, second, false, envelope(row, null, two))));
        success(run(settings));
        assertRows(settings.name, List.of(output(row, one, null), output(row, two, null)));
        consumed(settings.name, 3);
    }

    @Test
    void rejectsCursorLoopsLengthAndNonTextOffsetsWithAFinitePageBudget() throws Exception {
        Contract row = named("coupon");
        Settings repeated = new Settings("cursor-repeat", row.name);
        fails(
                repeated,
                List.of(
                        step(repeated, row, null, null, false, envelope(row, "A")),
                        step(repeated, row, null, "A", false, envelope(row, "B")),
                        step(repeated, row, null, "B", false, envelope(row, "A"))),
                "repeated next_offset");
        Settings longOffset = new Settings("cursor-long", row.name);
        fails(
                longOffset,
                List.of(step(longOffset, row, null, null, false, envelope(row, "x".repeat(1001)))),
                "invalid next_offset");
        Settings nonString = new Settings("cursor-shape", row.name);
        fails(
                nonString,
                List.of(
                        step(
                                nonString,
                                row,
                                null,
                                null,
                                false,
                                "{\"list\":[],\"next_offset\":[1,2]}")),
                "invalid next_offset");
        Settings budget = new Settings("page-budget", row.name);
        budget.pages = 1;
        fails(
                budget,
                List.of(step(budget, row, null, null, false, envelope(row, "one-more"))),
                "max_pages");
    }

    @Test
    void traversesClosedCalendarMonthWindowsAcrossLeapDay() throws Exception {
        Settings settings = new Settings("calendar-windows", "event");
        settings.start = "2024-01-31T00:00:00Z";
        settings.end = "2024-03-01T00:00:00Z";
        settings.from = 1706659200L;
        settings.through = 1709251200L;
        Contract row = named(settings.resource);
        ObjectNode before = sample(row, "before", 1709164799L);
        ObjectNode boundary = sample(row, "boundary", 1709164800L);
        ObjectNode finalTime = sample(row, "end", settings.through);
        program(
                settings.name,
                List.of(
                        step(
                                settings,
                                row,
                                null,
                                null,
                                false,
                                1706659200L,
                                1709164799L,
                                envelope(row, null, before)),
                        step(
                                settings,
                                row,
                                null,
                                null,
                                false,
                                1709164800L,
                                1709251200L,
                                envelope(row, null, boundary, finalTime))));
        success(run(settings));
        assertRows(
                settings.name,
                List.of(
                        output(row, before, null),
                        output(row, boundary, null),
                        output(row, finalTime, null)));
        consumed(settings.name, 2);
    }

    @Test
    void replaysTheFiveMinuteOverlapAndRetainsEqualSecondsAcrossPages() throws Exception {
        Settings settings = new Settings("five-minute-overlap", "customer");
        settings.incremental = "2024-01-01T00:10:00Z";
        settings.from = START_SEC + 300;
        Contract row = named(settings.resource);
        ObjectNode old = sample(row, "old", settings.from - 1);
        ObjectNode first = sample(row, "first", settings.from);
        ObjectNode second = sample(row, "second", settings.from);
        ObjectNode upper = sample(row, "upper", END_SEC);
        program(
                settings.name,
                List.of(
                        step(settings, row, null, null, false, envelope(row, "tie", old, first)),
                        step(
                                settings,
                                row,
                                null,
                                "tie",
                                false,
                                envelope(row, null, second, upper))));
        success(run(settings));
        assertRows(
                settings.name,
                List.of(
                        output(row, first, null),
                        output(row, second, null),
                        output(row, upper, null)));
        consumed(settings.name, 2);
    }

    @Test
    void scansLocalFilterResourcesWithoutInventedQueryParameters() throws Exception {
        for (String resource :
                List.of("gift", "unbilled_charge", "site_migration_detail", "differential_price")) {
            Settings settings = new Settings("local-" + resource, resource);
            settings.size = 3;
            Contract row = named(resource);
            ObjectNode tooOld = sample(row, "older", START_SEC - 1);
            ObjectNode relevant = sample(row, "recent", END_SEC);
            ObjectNode nullable = sample(row, "unknown", null);
            program(
                    settings.name,
                    List.of(
                            step(
                                    settings,
                                    row,
                                    null,
                                    null,
                                    false,
                                    envelope(row, null, tooOld, relevant, nullable))));
            success(run(settings));
            assertRows(
                    settings.name,
                    List.of(output(row, relevant, null), output(row, nullable, null)));
            consumed(settings.name, 1);
        }
    }

    @Test
    void encodesParentSegmentsAndRestartsOffsetHistoryForEachChildScope() throws Exception {
        Settings settings = new Settings("encoded-parents", "contact");
        Contract contact = named(settings.resource);
        Contract customers = named(contact.parent);
        String firstScope = "old user+/?雪", secondScope = "new user";
        ObjectNode p1 = sample(customers, firstScope, START_SEC - 3600);
        ObjectNode p2 = sample(customers, secondScope, START_SEC - 3600);
        ObjectNode c1 = sample(contact, "child", null);
        ObjectNode c2 = sample(contact, "child", null);
        program(
                settings.name,
                List.of(
                        step(
                                settings,
                                customers,
                                null,
                                null,
                                true,
                                envelope(customers, null, p1, p2)),
                        step(
                                settings,
                                contact,
                                firstScope,
                                null,
                                false,
                                envelope(contact, "same offset")),
                        step(
                                settings,
                                contact,
                                firstScope,
                                "same offset",
                                false,
                                envelope(contact, null, c1)),
                        step(
                                settings,
                                contact,
                                secondScope,
                                null,
                                false,
                                envelope(contact, "same offset")),
                        step(
                                settings,
                                contact,
                                secondScope,
                                "same offset",
                                false,
                                envelope(contact, null, c2))));
        success(run(settings));
        assertRows(
                settings.name,
                List.of(output(contact, c1, firstScope), output(contact, c2, secondScope)));
        consumed(settings.name, 5);
    }

    @Test
    void toleratesOnlyKnownUnavailableScheduledSubscriptionsAndKeepsOtherErrors() throws Exception {
        Contract changes = named("subscription_with_scheduled_changes");
        Contract subscriptions = named(changes.parent);
        Settings settings = new Settings("scheduled-gone", changes.name);
        settings.size = 3;
        List<ObjectNode> qs = new ArrayList<>();
        qs.add(
                step(
                        settings,
                        subscriptions,
                        null,
                        null,
                        true,
                        envelope(
                                subscriptions,
                                null,
                                sample(subscriptions, "gone", START_SEC - 1),
                                sample(subscriptions, "none", START_SEC - 1),
                                sample(subscriptions, "active", START_SEC - 1))));
        qs.add(
                step(settings, changes, "gone", null, false, "{\"message\":\"gone\"}")
                        .put("status", 404));
        qs.add(
                step(
                                settings,
                                changes,
                                "none",
                                null,
                                false,
                                "{\"message\":\"No changes are scheduled for this subscription.\"}")
                        .put("status", 400));
        ObjectNode live = sample(changes, "active", null);
        qs.add(step(settings, changes, "active", null, false, envelope(changes, null, live)));
        program(settings.name, qs);
        success(run(settings));
        assertRows(settings.name, List.of(output(changes, live, "active")));
        consumed(settings.name, 4);
        Settings rejected = new Settings("scheduled-bad", changes.name);
        fails(
                rejected,
                List.of(
                        parent(rejected, changes, "bad"),
                        step(
                                        rejected,
                                        changes,
                                        "bad",
                                        null,
                                        false,
                                        "{\"message\":\"private-provider-message\"}")
                                .put("status", 400)),
                "HTTP 400");
    }

    @Test
    void preservesExistingCustomFieldsAndRejectsMalformedOrCrossParentRecords() throws Exception {
        Contract customer = named("customer");
        Settings valid = new Settings("existing-custom", customer.name);
        ObjectNode body = sample(customer, "customer-1", START_SEC);
        body.putArray("custom_fields").addObject().put("name", "preexisting").put("value", "keep");
        body.putObject("cf_details").put("nested", "full").putNull("unchanged");
        program(
                valid.name,
                List.of(step(valid, customer, null, null, false, envelope(customer, null, body))));
        success(run(valid));
        assertRows(valid.name, List.of(output(customer, body, null)));
        consumed(valid.name, 1);
        Settings bad = new Settings("invalid-custom", customer.name);
        ObjectNode broken = sample(customer, "broken", START_SEC);
        broken.putObject("custom_fields").put("unexpected", 1);
        fails(
                bad,
                List.of(step(bad, customer, null, null, false, envelope(customer, null, broken))),
                "custom_fields");
        Settings shape = new Settings("invalid-timestamp", customer.name);
        ObjectNode badTime = sample(customer, "bad-time", START_SEC).put("updated_at", 1.5);
        fails(
                shape,
                List.of(
                        step(
                                shape,
                                customer,
                                null,
                                null,
                                false,
                                envelope(customer, null, badTime))),
                "timestamp");
        Contract contact = named("contact");
        Settings crossed = new Settings("cross-parent", contact.name);
        ObjectNode foreign = sample(contact, "child", null).put("customer_id", "other");
        fails(
                crossed,
                List.of(
                        parent(crossed, contact, "parent"),
                        step(
                                crossed,
                                contact,
                                "parent",
                                null,
                                false,
                                envelope(contact, null, foreign))),
                "requested parent");
    }

    @Test
    void handlesInsufficientAccessAndProviderRetryAfterWithinBudget() throws Exception {
        Contract row = named("invoice");
        for (int status : List.of(401, 403)) {
            Settings denied = new Settings("permission-" + status, row.name);
            denied.attempts = 3;
            fails(
                    denied,
                    List.of(
                            step(
                                            denied,
                                            row,
                                            null,
                                            null,
                                            false,
                                            "{\"message\":\"private-provider-message\"}")
                                    .put("status", status)),
                    "HTTP " + status);
        }
        Settings retry = new Settings("rate-wait", row.name);
        retry.attempts = 2;
        ObjectNode value = sample(row, "invoice", START_SEC);
        program(
                retry.name,
                List.of(
                        step(retry, row, null, null, false, "")
                                .put("status", 429)
                                .put("delay_seconds", 3),
                        step(retry, row, null, null, false, envelope(row, null, value))));
        success(run(retry));
        List<String> times = engine.lines(retry.name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 1800L);
        assertRows(retry.name, List.of(output(row, value, null)));
        consumed(retry.name, 2);
        Settings budget = new Settings("retry-budget", row.name);
        budget.deadline = 5000;
        budget.attempts = 3;
        fails(
                budget,
                List.of(
                        step(budget, row, null, null, false, "")
                                .put("status", 429)
                                .put("Retry-After", "60")),
                "retry delay");
    }

    @Test
    void refusesRedirectsAndUntrustedOrWrongTlsNames() throws Exception {
        Contract row = named("customer");
        Settings redirect = new Settings("redirect", row.name);
        fails(
                redirect,
                List.of(
                        step(redirect, row, null, null, false, "")
                                .put("status", 302)
                                .put("mode", "redirect")),
                "HTTP 302");
        Settings trustFail = new Settings("untrusted", row.name);
        trustFail.certificate = wrongTrust;
        fails(trustFail, List.of(), "TLS");
        selectIdentity("wrong-name");
        Settings wrong = new Settings("wrong-name", row.name);
        wrong.certificate = wrongNameTrust;
        fails(wrong, List.of(), "TLS");
    }

    @Test
    void rejectsMalformedJsonUtf8AndOversizedCompressedOrRawBodies() throws Exception {
        Contract row = named("customer");
        Settings json = new Settings("malformed", row.name);
        fails(json, List.of(step(json, row, null, null, false, "{\"list\":123}")), "list response");
        Settings utf = new Settings("bad-utf8", row.name);
        byte[] invalid =
                new byte[] {'{', '"', 'x', '"', ':', '"', (byte) 0xc3, (byte) 0x28, '"', '}'};
        fails(
                utf,
                List.of(
                        step(utf, row, null, null, false, "")
                                .put("bytes", Base64.getEncoder().encodeToString(invalid))),
                "UTF-8");
        for (String mode : List.of("json", "gzip")) {
            Settings cap = new Settings("body-cap-" + mode, row.name);
            cap.bytes = 1024;
            ObjectNode data = sample(row, "cap", START_SEC).put("oversize", "x".repeat(4096));
            fails(
                    cap,
                    List.of(
                            step(cap, row, null, null, false, envelope(row, null, data))
                                    .put("mode", mode)),
                    "max_response_bytes");
        }
        Settings gzip = new Settings("gzip-ok", row.name);
        ObjectNode data = sample(row, "gzip", START_SEC);
        program(
                gzip.name,
                List.of(
                        step(gzip, row, null, null, false, envelope(row, null, data))
                                .put("mode", "gzip")));
        success(run(gzip));
        assertRows(gzip.name, List.of(output(row, data, null)));
        consumed(gzip.name, 1);
    }

    @Test
    void stopsATricklingResponseAtTheAbsoluteRequestDeadline() throws Exception {
        Contract row = named("order");
        Settings settings = new Settings("absolute-deadline", row.name);
        settings.deadline = 1500;
        settings.readTimeout = 15000;
        fails(
                settings,
                List.of(step(settings, row, null, null, false, "{").put("mode", "trickle")),
                "deadline");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(settings.name + ".disconnect"));
        assertRows(settings.name, List.of());
    }

    @Test
    void cancelsAnInFlightZetaRequestWithoutPublishingPartialData() throws Exception {
        Contract row = named("order");
        Settings settings = new Settings("active-cancel", row.name);
        program(
                settings.name,
                List.of(step(settings, row, null, null, false, "{").put("mode", "trickle")));
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
    void resumesAtTheUnfinishedChildAfterAnActualParentSavepoint() throws Exception {
        Settings settings = new Settings("parent-first-run", "contact");
        Contract child = named(settings.resource), parents = named(child.parent);
        ObjectNode a = sample(parents, "parent-A", START_SEC - 1);
        ObjectNode b = sample(parents, "parent-B", START_SEC - 1);
        ObjectNode one = sample(child, "child-A", null);
        ObjectNode two = sample(child, "child-B", null);
        ObjectNode inventory =
                step(settings, parents, null, null, true, envelope(parents, null, a, b));
        String job =
                saveWhileWaiting(
                        settings,
                        List.of(
                                inventory,
                                step(
                                        settings,
                                        child,
                                        "parent-A",
                                        null,
                                        false,
                                        envelope(child, null, one)),
                                step(
                                                settings,
                                                child,
                                                "parent-B",
                                                null,
                                                false,
                                                envelope(child, null, two))
                                        .put("mode", "partial")));
        program(
                "parent-restored",
                List.of(
                        inventory,
                        step(
                                settings,
                                child,
                                "parent-B",
                                null,
                                false,
                                envelope(child, null, two))));
        success(engine.restoreJob(jobConfig(settings), job, settings.variables()));
        assertRows(
                settings.name,
                List.of(output(child, one, "parent-A"), output(child, two, "parent-B")));
        consumed("parent-restored", 2);
    }

    @Test
    void resumesTheOpaqueCursorInTheCurrentCalendarWindowAfterAnActualSavepoint() throws Exception {
        Settings settings = new Settings("monthly-first-run", "event");
        settings.start = "2024-01-31T00:00:00Z";
        settings.end = "2024-03-01T00:00:00Z";
        settings.from = 1706659200L;
        settings.through = 1709251200L;
        Contract row = named(settings.resource);
        ObjectNode one = sample(row, "before", 1709164799L);
        ObjectNode two = sample(row, "middle", 1709164800L);
        ObjectNode three = sample(row, "last", settings.through);
        String cursor = "[\"saved + signed\",21]";
        String job =
                saveWhileWaiting(
                        settings,
                        List.of(
                                step(
                                        settings,
                                        row,
                                        null,
                                        null,
                                        false,
                                        settings.from,
                                        1709164799L,
                                        envelope(row, null, one)),
                                step(
                                        settings,
                                        row,
                                        null,
                                        null,
                                        false,
                                        1709164800L,
                                        settings.through,
                                        envelope(row, cursor, two)),
                                step(
                                                settings,
                                                row,
                                                null,
                                                cursor,
                                                false,
                                                1709164800L,
                                                settings.through,
                                                envelope(row, null, three))
                                        .put("mode", "partial")));
        program(
                "monthly-restored",
                List.of(
                        step(
                                settings,
                                row,
                                null,
                                cursor,
                                false,
                                1709164800L,
                                settings.through,
                                envelope(row, null, three))));
        success(engine.restoreJob(jobConfig(settings), job, settings.variables()));
        assertRows(
                settings.name,
                List.of(output(row, one, null), output(row, two, null), output(row, three, null)));
        consumed("monthly-restored", 1);
    }

    @Test
    void restoresTheTerminalPipelineWithoutAdditionalRequests() throws Exception {
        Settings settings = new Settings("terminal-first-run", "gift");
        Contract row = named(settings.resource);
        ObjectNode one = sample(row, "gift", START_SEC);
        List<JsonNode> expected = List.of(output(row, one, null));
        program(
                settings.name,
                List.of(step(settings, row, null, null, false, envelope(row, null, one))));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/chargebee-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(settings.name, expected));
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(settings.name, 1);
        program("terminal-restored", List.of());
        Future<Container.ExecResult> restored =
                tasks.submit(
                        () ->
                                engine.restoreJob(
                                        "/chargebee-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(job)));
            long baseline = engine.completedAcrossPipelines(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(job) > baseline);
            assertRows(settings.name, expected);
            consumed("terminal-restored", 0);
        } finally {
            if (!restored.isDone()) {
                engine.cancelJob(job);
            }
            restored.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void packagedReaderResumesTheExactRawOffsetOrRejectsEitherChangedRecord() throws Exception {
        Contract row = named("item");
        Settings settings = new Settings("probe", row.name);
        settings.size = 34;
        List<ObjectNode> seeded = new ArrayList<>();
        for (int index = 1; index <= 33; index++) {
            seeded.add(sample(row, Integer.toString(index), START_SEC));
        }
        seeded.add(sample(row, "out-of-window", START_SEC - 1));
        String first = envelope(row, null, seeded.toArray(new ObjectNode[0]));
        for (String mode : List.of("resume", "changed-selected", "changed-filtered")) {
            List<ObjectNode> second = new ArrayList<>(seeded);
            if (mode.equals("changed-selected")) {
                second.set(32, sample(row, "changed-last-selected", START_SEC));
            } else if (mode.equals("changed-filtered")) {
                second.set(33, sample(row, "changed-filtered", START_SEC - 1));
            }
            program(
                    mode,
                    List.of(
                            step(settings, row, null, null, false, first),
                            step(
                                    settings,
                                    row,
                                    null,
                                    null,
                                    false,
                                    envelope(row, null, second.toArray(new ObjectNode[0])))));
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
        try (InputStream stream =
                ChargebeeIT.class.getResourceAsStream("/chargebee-contracts.tsv")) {
            if (stream == null) {
                throw new IOException("missing independent Chargebee route table");
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
        for (Contract value : catalog()) {
            if (value.name.equals(resource)) {
                return value;
            }
        }
        throw new IOException("missing provider route fixture " + resource);
    }

    private static ObjectNode sample(Contract row, String id, Long timestamp) {
        ObjectNode value =
                JSON.createObjectNode()
                        .put(row.name.equals("site_migration_detail") ? "entity_id" : "id", id)
                        .put("deleted", true)
                        .put("resource_version", 1_704_067_200_123L)
                        .put("cf_priority", "priority-雪")
                        .putNull("cf_reference");
        if (row.cursor != null) {
            if (timestamp == null) {
                value.putNull(row.cursor);
            } else {
                value.put(row.cursor, timestamp);
            }
        }
        value.putObject("unmodeled")
                .put("precision", new BigDecimal("123456789012345678.012300"))
                .put("huge", new BigInteger("9223372036854775808"))
                .put("cf_keep_nested", "retain")
                .putArray("items")
                .addNull()
                .add(false)
                .add("项目🧾");
        return value;
    }

    private static String envelope(Contract row, String next, ObjectNode... values) {
        ObjectNode wrapper = JSON.createObjectNode();
        if (row.mode.equals("singleton")) {
            if (values.length != 1 || next != null) {
                throw new IllegalArgumentException("incorrect singleton fixture");
            }
            wrapper.set(row.wrapper, values[0]);
        } else {
            ArrayNode list = wrapper.putArray("list");
            for (ObjectNode value : values) {
                list.addObject().set(row.wrapper, value);
            }
            wrapper.put("next_offset", next);
        }
        return wrapper.toString();
    }

    private static ObjectNode parent(Settings settings, Contract child, String id)
            throws IOException {
        Contract parent = named(child.parent);
        return step(
                settings,
                parent,
                null,
                null,
                true,
                envelope(parent, null, sample(parent, id, settings.from - 1)));
    }

    private static ObjectNode step(
            Settings settings,
            Contract row,
            String parent,
            String offset,
            boolean hidden,
            String response) {
        return step(
                settings, row, parent, offset, hidden, settings.from, settings.through, response);
    }

    private static ObjectNode step(
            Settings settings,
            Contract row,
            String parent,
            String offset,
            boolean hidden,
            long from,
            long through,
            String body) {
        String route = "/api/v2" + row.path;
        if (parent != null) {
            route = route.replace("{parent}", enc(parent).replace("+", "%20").replace("*", "%2A"));
        }
        if (!row.mode.equals("singleton")) {
            route += "?limit=" + settings.size;
            if (offset != null) {
                route += "&offset=" + enc(offset);
            }
            if (row.sort != null) {
                route += "&sort_by%5Basc%5D=" + row.sort;
            }
            if (row.deleted) {
                route += "&include_deleted=true";
            }
            if (hidden && settings.resource.equals("subscription_with_scheduled_changes")) {
                route += "&has_scheduled_changes%5Bis%5D=true";
            }
            if (!hidden && (row.mode.equals("month") || row.mode.equals("single"))) {
                route +=
                        "&" + row.cursor + "%5Bbetween%5D=" + enc("[" + from + "," + through + "]");
            }
        }
        return JSON.createObjectNode()
                .put("host", ChargebeePeer.HOST)
                .put("target", route)
                .put("response", body);
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static JsonNode output(Contract row, ObjectNode wire, String parent) {
        ObjectNode data = wire.deepCopy();
        String cursor =
                row.cursor == null || data.path(row.cursor).isNull()
                        ? null
                        : Instant.ofEpochSecond(data.path(row.cursor).longValue()).toString();
        if (row.name.equals("contact")) {
            data.put("customer_id", parent);
        } else if (row.name.equals("quote_line_group")) {
            data.put("quote_id", parent);
        } else if (row.name.equals("subscription_with_scheduled_changes")) {
            data.put("subscription_id", parent);
        }
        ArrayNode fields =
                data.has("custom_fields")
                        ? (ArrayNode) data.get("custom_fields")
                        : data.putArray("custom_fields");
        for (String key : List.of("cf_priority", "cf_reference", "cf_details")) {
            JsonNode value = data.remove(key);
            if (value != null) {
                fields.addObject().put("name", key).set("value", value);
            }
        }
        String id =
                row.mode.equals("singleton")
                        ? parent
                        : wire.path(row.name.equals("site_migration_detail") ? "entity_id" : "id")
                                .textValue();
        return JSON.createObjectNode()
                .put("id", id)
                .put("parent_id", parent)
                .put("cursor_timestamp", cursor)
                .set("data", data);
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
        return settings.incremental == null ? "/chargebee.conf" : "/chargebee-incremental.conf";
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        for (String secret :
                List.of(ChargebeePeer.TOKEN, settings.token, "private-provider-message")) {
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
        String san = "SAN=dns:" + hostname;
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
                                ChargebeePeer.PASSWORD,
                                "-keypass",
                                ChargebeePeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, ChargebeePeer.PASSWORD.toCharArray());
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
     * Holds literal HTTPS paths and parameters from the two provider API specifications.
     *
     * <p>The integration expectations are independent of the production resource registry.
     */
    private static final class Contract {
        final String name;
        final String path;
        final String wrapper;
        final String catalog;
        final String cursor;
        final String mode;
        final String sort;
        final boolean deleted;
        final String parent;

        Contract(String[] row) {
            name = row[0];
            path = row[1];
            wrapper = row[2];
            catalog = row[3];
            cursor = optional(row[4]);
            mode = row[5];
            sort = optional(row[6]);
            deleted = row[7].equals("yes");
            parent = optional(row[8]);
        }

        private static String optional(String value) {
            return "-".equals(value) ? null : value;
        }
    }

    /**
     * Holds immutable fixture identity and adjustable per-job options for one test case.
     *
     * <p>Program names distinguish every job's request trace and output receipts.
     */
    private static final class Settings {
        final String name;
        final String resource;
        String site = ChargebeePeer.SITE;
        String token = ChargebeePeer.TOKEN;
        String catalog = "2.0";
        String certificate = trust;
        String start = START;
        String end = END;
        String incremental;
        long from = START_SEC;
        long through = END_SEC;
        int size = DEFAULT_SIZE;
        int pages = 100;
        int bytes = 8388608;
        int readTimeout = 120000;
        int deadline = 180000;
        int attempts = 1;
        int lookback = 300;

        Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        String[] variables() {
            return new String[] {
                "api.output=" + name,
                "api.resource=" + resource,
                "api.site=" + site,
                "api.token=" + token,
                "api.catalog=" + catalog,
                "api.certificate=" + certificate,
                "api.start=" + start,
                "api.end=" + end,
                "api.incremental=" + (incremental == null ? start : incremental),
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
        private static final String PEER = "/tmp/chargebee-peer";
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
            container.withExtraHost(ChargebeePeer.HOST, "127.0.0.1");
            container.withExtraHost("chargebee-trap.invalid", "127.0.0.1");
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
                stage(container, ChargebeePeer.class);
                stage(container, ChargebeeRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java Chargebee fixture", failure);
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
                    ChargebeePeer.class.getName(),
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
                    ChargebeeRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/chargebee-" + name;
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

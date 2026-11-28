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

package org.apache.seatunnel.e2e.connector.circa;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.circa.CircaRecoveryProbe;
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
 * Drives the packaged Circa Source through real Zeta jobs and an independent HTTPS peer.
 *
 * <p>The literal resource matrix and request scripts establish the expected provider wire bytes.
 * Transactional LocalFile receipts are the result oracle.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class CircaIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String START = "2024-01-01T00:00:00Z";
    private static final String END = "2024-01-02T00:00:00Z";
    private static final String AFTER = "2024-01-02T00:00:01Z";
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
        trust = identity("trusted", CircaPeer.HOST);
        wrongTrust = identity("untrusted", CircaPeer.HOST);
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
    void readsAllNineNativeResourcesAndKeepsRelationshipAndFieldIdentities() throws Exception {
        String[][] matrix = {
            {"events", "/events", "true", "", "", ""},
            {"contacts", "/contacts", "true", "", "", ""},
            {"teams", "/teams", "false", "", "", ""},
            {"companies", "/companies", "true", "", "", ""},
            {
                "company_contacts",
                "/companies/company-1/contacts",
                "true",
                "",
                "/companies",
                "company-1"
            },
            {"event_fields", "/fields", "false", "Event", "", ""},
            {"contact_fields", "/fields", "false", "Contact", "", ""},
            {"company_fields", "/fields", "false", "Company", "", ""},
            {"event_contacts", "/events/event-1/contacts", "true", "", "/events", "event-1"}
        };
        for (String[] row : matrix) {
            String name = row[0];
            String route = row[1];
            boolean paged = Boolean.parseBoolean(row[2]);
            Settings config = new Settings("all-" + name.replace('_', '-'), name);
            ObjectNode value =
                    sample(
                            "id-" + name,
                            name.equals("events")
                                            || name.equals("contacts")
                                            || name.equals("companies")
                                    ? START
                                    : null);
            List<ObjectNode> requests = new ArrayList<>();
            String parent = row[5].isEmpty() ? null : row[5];
            if (parent != null) {
                requests.add(
                        step(
                                row[4],
                                requestPage(row[4], 1),
                                page(1, 1, 1, sample(parent, "2001-01-02T03:04:05Z"))));
            }
            JsonNode body =
                    !row[3].isEmpty()
                            ? JSON.createObjectNode().put("fields_for", row[3])
                            : paged ? requestPage(route, 1) : null;
            if (!row[3].isEmpty()) {
                value.put("field_for", row[3].equals("Contact") ? "EventContact" : row[3]);
                value.putArray("options").addNull().add("电子邀请");
            }
            ObjectNode expectedRequest =
                    step(route, body, paged ? page(1, 1, 1, value) : envelope(value));
            if ("events".equals(name)) {
                expectedRequest.put("status", 202);
            }
            requests.add(expectedRequest);
            program(config.name, requests);
            success(run(config));
            assertRows(
                    config.name,
                    List.of(output(value, parent, value.path("updated_at").textValue())));
            consumed(config.name, requests.size());
        }
    }

    @Test
    void followsExplicitMetadataThroughAFullShortEmptyAndFinalPage() throws Exception {
        Settings config = new Settings("pagination", "contacts");
        List<ObjectNode> input = new ArrayList<>();
        for (int index = 1; index <= 25; index++) {
            input.add(sample("person-" + index, START));
        }
        ObjectNode extra = sample("person-26", START);
        program(
                config.name,
                List.of(
                        step(
                                "/contacts",
                                requestPage("/contacts", 1),
                                page(1, 4, 26, input.toArray(new ObjectNode[0]))),
                        step("/contacts", requestPage("/contacts", 2), page(2, 4, 26)),
                        step("/contacts", requestPage("/contacts", 3), page(3, 4, 26, extra)),
                        step("/contacts", requestPage("/contacts", 4), page(4, 4, 26))));
        success(run(config));
        input.add(extra);
        List<JsonNode> expected = new ArrayList<>();
        for (ObjectNode row : input) {
            expected.add(output(row, null, START));
        }
        assertRows(config.name, expected);
        consumed(config.name, 4);
    }

    @Test
    void usesFixedPageSizeWithoutMetadataAndRejectsDuplicatesOrPageBudgetExhaustion()
            throws Exception {
        List<ObjectNode> rows = new ArrayList<>();
        for (int index = 0; index < 25; index++) {
            rows.add(sample("company-" + index, START));
        }
        ObjectNode[] all = rows.toArray(new ObjectNode[0]);
        Settings fallback = new Settings("pagination-fallback", "companies");
        program(
                fallback.name,
                List.of(
                        step("/companies", requestPage("/companies", 1), envelope(all)),
                        step("/companies", requestPage("/companies", 2), envelope())));
        success(run(fallback));
        List<JsonNode> expected = new ArrayList<>();
        for (ObjectNode row : rows) {
            expected.add(output(row, null, START));
        }
        assertRows(fallback.name, expected);
        consumed(fallback.name, 2);

        Settings repeated = new Settings("pagination-repeated", "companies");
        fails(
                repeated,
                List.of(
                        step("/companies", requestPage("/companies", 1), envelope(all)),
                        step("/companies", requestPage("/companies", 2), envelope(all))),
                "provider repeated the preceding page");
        Settings inconsistent = new Settings("pagination-position", "companies");
        fails(
                inconsistent,
                List.of(step("/companies", requestPage("/companies", 1), page(2, 4, 100, all))),
                "invalid or repeated pagination position");
        Settings limited = new Settings("pagination-limit", "companies");
        limited.pages = 1;
        fails(
                limited,
                List.of(step("/companies", requestPage("/companies", 1), page(1, 2, 30, all))),
                "max_pages exhausted");
    }

    @Test
    void sendsNormalizedTimeRangeInTheGetBodyAndKeepsSameSecondRecords() throws Exception {
        for (String resource : List.of("events", "contacts", "companies")) {
            String route = "/" + resource;
            Settings config = new Settings("bounds-" + resource, resource);
            config.start = "2024-01-01T07:59:59+08:00";
            config.incremental = "2024-01-01T08:00:01+08:00";
            config.end = "2024-01-02T01:00:00+01:00";
            config.lookback = 1;
            ObjectNode first = sample("same-first", "2023-12-31T19:00:00-05:00");
            ObjectNode second = sample("same-second", START);
            ObjectNode last = sample("through", END);
            ObjectNode early = sample("old", "2023-12-31T23:59:59Z");
            ObjectNode late = sample("late", AFTER);
            ObjectNode date1 = requestPage(route, 1);
            date1.putObject("updated_at").put("min", START).put("max", END);
            ObjectNode date2 = requestPage(route, 2);
            date2.putObject("updated_at").put("min", START).put("max", END);
            program(
                    config.name,
                    List.of(
                            step(route, date1, page(1, 2, 5, early, first)),
                            step(route, date2, page(2, 2, 5, second, last, late))));
            success(run(config));
            assertRows(
                    config.name,
                    List.of(
                            output(first, null, START),
                            output(second, null, START),
                            output(last, null, END)));
            consumed(config.name, 2);
        }
    }

    @Test
    void discoversOldParentsAndRetainsTheSameContactUnderTwoOpaquePaths() throws Exception {
        for (String root : List.of("/events", "/companies")) {
            String resource = root.equals("/events") ? "event_contacts" : "company_contacts";
            Settings config = new Settings("children-" + root.substring(1), resource);
            String firstId = "stale/one +?%";
            String secondId = "stale-two";
            String first = root + "/stale%2Fone%20%2B%3F%25/contacts";
            String second = root + "/stale-two/contacts";
            ObjectNode contact =
                    sample("matching-id", END).put("registration_status", "REGISTERED");
            ObjectNode extra = sample("linked-other", END).put("check_in_status", "CHECKED_IN");
            program(
                    config.name,
                    List.of(
                            step(
                                    root,
                                    requestPage(root, 1),
                                    page(1, 2, 2, sample(firstId, "2001-01-01T00:00:00Z"))),
                            step(first, requestPage(first, 1), page(1, 2, 2, contact)),
                            step(first, requestPage(first, 2), page(2, 2, 2, extra)),
                            step(
                                    root,
                                    requestPage(root, 2),
                                    page(2, 2, 2, sample(secondId, "2002-01-01T00:00:00Z"))),
                            step(second, requestPage(second, 1), page(1, 1, 1, contact))));
            success(run(config));
            assertRows(
                    config.name,
                    List.of(
                            output(contact, firstId, null),
                            output(extra, firstId, null),
                            output(contact, secondId, null)));
            consumed(config.name, 5);
        }
    }

    @Test
    void rejectsInvalidCredentialsDatesAndResourceBeforeOpeningTheApi() throws Exception {
        Settings bad = new Settings("invalid-api-key", "teams");
        bad.token = "bad key";
        fails(bad, List.of(), "api_key must be a valid Bearer credential");
        Settings dateOnly = new Settings("invalid-date-kind", "events");
        dateOnly.start = "2024-01-01";
        dateOnly.bounded = true;
        fails(dateOnly, List.of(), "start_date must be a whole-second");
        Settings fullDate = new Settings("invalid-full-date", "contact_fields");
        fullDate.bounded = true;
        fails(fullDate, List.of(), "date bounds require start_date");
        Settings wrong = new Settings("invalid-resource", "other");
        fails(wrong, List.of(), "unsupported resource");
    }

    @Test
    void surfacesDeniedAndDisabledFiltersAndValidatesTheAcceptedEventsEnvelope() throws Exception {
        for (int status : List.of(401, 402, 403, 404, 422)) {
            Settings config = new Settings("error-" + status, "contact_fields");
            config.attempts = 3;
            ObjectNode body = JSON.createObjectNode().put("fields_for", "Contact");
            fails(
                    config,
                    List.of(
                            step(
                                            "/fields",
                                            body,
                                            "{\"errors\":{\"private-provider-message\":\"disabled\"}}")
                                    .put("status", status)),
                    "HTTP " + status);
        }
        Settings missing = new Settings("accepted-without-data", "events");
        fails(
                missing,
                List.of(
                        step(
                                        "/events",
                                        requestPage("/events", 1),
                                        "{\"job_id\":\"private-provider-message\"}")
                                .put("status", 202)),
                "invalid data array");
        Settings other = new Settings("unexpected-accepted", "contacts");
        fails(
                other,
                List.of(
                        step(
                                        "/contacts",
                                        requestPage("/contacts", 1),
                                        page(1, 1, 1, sample("wrong-status", START)))
                                .put("status", 202)),
                "HTTP 202");
    }

    @Test
    void retriesIdenticalGetWithRateAndTransientResponsesWithinFiniteBudget() throws Exception {
        Settings config = new Settings("rate-backoff", "events");
        config.attempts = 3;
        ObjectNode row = sample("rate-result", START);
        ObjectNode first = step("/events", requestPage("/events", 1), "{}").put("status", 429);
        first.put("Retry-After", "1");
        ObjectNode second = step("/events", requestPage("/events", 1), "{}").put("status", 503);
        program(
                config.name,
                List.of(
                        first,
                        second,
                        step("/events", requestPage("/events", 1), page(1, 1, 1, row))));
        success(run(config));
        assertRows(config.name, List.of(output(row, null, START)));
        consumed(config.name, 3);
        List<String> times = engine.lines(config.name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 850L);

        Settings excessive = new Settings("rate-deadline", "teams");
        excessive.attempts = 3;
        excessive.deadline = 1500;
        ObjectNode rate = step("/teams", null, "{}").put("status", 429).put("Retry-After", "20");
        fails(excessive, List.of(rate), "retry delay exceeds request deadline");

        Settings exhausted = new Settings("rate-attempts", "teams");
        exhausted.attempts = 2;
        ObjectNode failure = step("/teams", null, "{}").put("status", 503);
        fails(
                exhausted,
                List.of(failure, failure.deepCopy()),
                "retry limit reached after HTTP 503");
    }

    @Test
    void preservesExactNestedValuesFromGzipAndRejectsInvalidOrExpandedBodies() throws Exception {
        Settings config = new Settings("json-gzip", "company_fields");
        ObjectNode row = sample("field-1", null);
        row.put("field_for", "Company");
        program(
                config.name,
                List.of(
                        step(
                                        "/fields",
                                        JSON.createObjectNode().put("fields_for", "Company"),
                                        envelope(row))
                                .put("mode", "gzip")));
        success(run(config));
        assertRows(config.name, List.of(output(row, null, null)));
        consumed(config.name, 1);

        Settings raw = new Settings("json-raw-limit", "teams");
        raw.bytes = 1024;
        Settings zipped = new Settings("json-decompressed-limit", "teams");
        zipped.bytes = 1024;
        ObjectNode large = sample("oversize", null).put("large", "x".repeat(4096));
        fails(
                raw,
                List.of(step("/teams", null, envelope(large))),
                "response exceeds max_response_bytes");
        fails(
                zipped,
                List.of(step("/teams", null, envelope(large)).put("mode", "gzip")),
                "response exceeds max_response_bytes");
        Settings malformed = new Settings("json-duplicate", "teams");
        fails(
                malformed,
                List.of(step("/teams", null, "{\"data\":[{\"id\":\"x\",\"id\":\"y\"}]}")),
                "invalid JSON response");
        Settings badUtf8 = new Settings("json-invalid-utf", "teams");
        ObjectNode step = step("/teams", null, "{}");
        step.put(
                "bytes",
                Base64.getEncoder().encodeToString(new byte[] {123, 34, -64, -81, 34, 125}));
        fails(badUtf8, List.of(step), "response is not valid UTF-8 JSON");
    }

    @Test
    void refusesRedirectsAndBothCertificateFailureKinds() throws Exception {
        Settings redirect = new Settings("redirect", "teams");
        fails(
                redirect,
                List.of(step("/teams", null, "{}").put("status", 307).put("mode", "redirect")),
                "HTTP 307");
        Settings untrusted = new Settings("untrusted", "teams");
        untrusted.certificate = wrongTrust;
        fails(untrusted, List.of(), "HTTPS or TLS exchange failed");
        Settings wrong = new Settings("wrong-san", "teams");
        wrong.certificate = wrongNameTrust;
        selectIdentity("wrong-name");
        fails(wrong, List.of(), "HTTPS or TLS exchange failed");
    }

    @Test
    void enforcesAbsoluteTimeoutAndClosesCanceledInFlightRequests() throws Exception {
        Settings deadline = new Settings("absolute-deadline", "teams");
        deadline.deadline = 2000;
        fails(
                deadline,
                List.of(step("/teams", null, "{").put("mode", "trickle")),
                "request deadline exceeded");
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> engine.exists(deadline.name + ".disconnect"));

        Settings config = new Settings("in-flight-cancel", "teams");
        program(config.name, List.of(step("/teams", null, "{").put("mode", "trickle")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(jobConfig(config), job, config.variables()));
        try {
            Awaitility.await()
                    .atMost(25, TimeUnit.SECONDS)
                    .until(() -> engine.exists(config.name + ".started"));
            success(engine.cancelJob(job));
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .until(() -> engine.exists(config.name + ".disconnect"));
            assertRows(config.name, List.of());
            consumed(config.name, 1);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void resumesThePendingSecondPageAndTheUnfinishedParentInActualZetaJobs() throws Exception {
        Settings list = new Settings("savepoint-page", "contacts");
        list.bounded = true;
        ObjectNode first = sample("first-page", START);
        ObjectNode second = sample("second-page", END);
        ObjectNode bounds1 = requestPage("/contacts", 1);
        bounds1.putObject("updated_at").put("min", START).put("max", END);
        ObjectNode bounds2 = requestPage("/contacts", 2);
        bounds2.putObject("updated_at").put("min", START).put("max", END);
        String listJob =
                saveWhileWaiting(
                        list,
                        List.of(
                                step("/contacts", bounds1, page(1, 2, 2, first)),
                                step("/contacts", bounds2, page(2, 2, 2, second))
                                        .put("mode", "partial")));
        assertRows(list.name, List.of(output(first, null, START)));
        program("resume-page", List.of(step("/contacts", bounds2, page(2, 2, 2, second))));
        success(engine.restoreJob(jobConfig(list), listJob, list.variables()));
        assertRows(list.name, List.of(output(first, null, START), output(second, null, END)));
        consumed("resume-page", 1);

        Settings children = new Settings("savepoint-parent", "event_contacts");
        ObjectNode parentA = sample("old-a", "2001-01-01T00:00:00Z");
        ObjectNode parentB = sample("old-b", "2002-01-01T00:00:00Z");
        ObjectNode contactA = sample("same-contact", START);
        ObjectNode contactB = sample("same-contact", END);
        ObjectNode parents =
                step("/events", requestPage("/events", 1), page(1, 1, 2, parentA, parentB))
                        .put("status", 202);
        String a = "/events/old-a/contacts";
        String b = "/events/old-b/contacts";
        ObjectNode pending = step(b, requestPage(b, 1), page(1, 1, 1, contactB));
        String childJob =
                saveWhileWaiting(
                        children,
                        List.of(
                                parents,
                                step(a, requestPage(a, 1), page(1, 1, 1, contactA)),
                                pending.deepCopy().put("mode", "partial")));
        assertRows(children.name, List.of(output(contactA, "old-a", null)));
        program("resume-parent", List.of(parents, pending));
        success(engine.restoreJob(jobConfig(children), childJob, children.variables()));
        assertRows(
                children.name,
                List.of(output(contactA, "old-a", null), output(contactB, "old-b", null)));
        consumed("resume-parent", 2);
    }

    @Test
    void restoresTheTerminalPipelineWithoutAdditionalRequests() throws Exception {
        Settings settings = new Settings("terminal-first-run", "teams");
        ObjectNode one = sample("team-1", null);
        List<JsonNode> expected = List.of(output(one, null, null));
        program(settings.name, List.of(step("/teams", null, envelope(one))));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/circa-terminal.conf", job, settings.variables()));
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
                        () -> engine.restoreJob("/circa-terminal.conf", job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(job)));
            long checks = engine.completedAcrossPipelines(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(job) > checks);
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
    void replaysTheExactRawPageOffsetAndRejectsChangedFilteredOrSelectedRows() throws Exception {
        List<ObjectNode> original = new ArrayList<>();
        for (int index = 1; index <= 17; index++) {
            original.add(sample("raw-" + index, index == 5 ? "2023-12-31T23:59:59Z" : START));
        }
        ObjectNode range = requestPage("/contacts", 1);
        range.putObject("updated_at").put("min", START).put("max", END);
        String same = page(1, 1, 17, original.toArray(new ObjectNode[0]));
        for (String mode : List.of("resume", "changed-selected", "changed-filtered")) {
            List<ObjectNode> restoredData = new ArrayList<>();
            for (ObjectNode input : original) {
                restoredData.add(input.deepCopy());
            }
            if (mode.startsWith("changed")) {
                restoredData.get(mode.endsWith("filtered") ? 4 : 8).put("drift", true);
            }
            String name = "probe-" + mode;
            program(
                    name,
                    List.of(
                            step("/contacts", range, same),
                            step(
                                    "/contacts",
                                    range,
                                    page(1, 1, 17, restoredData.toArray(new ObjectNode[0])))));
            success(engine.probe(mode));
            consumed(name, 2);
        }

        program("probe-zero", List.of());
        success(engine.probe("zero"));
        consumed("probe-zero", 0);
    }

    /**
     * Takes an actual savepoint while a new response is deliberately incomplete.
     *
     * <p>The blocked request is never adopted, and previously committed rows stay visible.
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

    private static ObjectNode sample(String id, String cursor) throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("id", id)
                        .put("start_date", "2030-12-24")
                        .put("label", "活动 Zoë")
                        .putNull("optional");
        if (cursor != null) {
            row.put("updated_at", cursor);
        }
        row.set(
                "custom_fields",
                JSON.readTree(
                        "{\"future field\":[null,true,123456789012345678901234567890,1.00000000000000000009],\"nested\":{\"a\":null}}"));
        return row;
    }

    private static String envelope(ObjectNode... rows) {
        ObjectNode body = JSON.createObjectNode();
        ArrayNode data = body.putArray("data");
        for (ObjectNode row : rows) {
            data.add(row);
        }
        return body.toString();
    }

    private static String page(int current, int total, int entries, ObjectNode... rows)
            throws IOException {
        ObjectNode body = (ObjectNode) JSON.readTree(envelope(rows));
        body.putObject("pagination")
                .put("current_page", current)
                .put("total_pages", total)
                .put("per_page", 25)
                .put("total_entries", entries);
        return body.toString();
    }

    private static ObjectNode requestPage(String route, int number) {
        ObjectNode request = JSON.createObjectNode().put("page", number);
        if (route.equals("/events")) {
            request.put("include_undated", true);
        }
        return request;
    }

    private static ObjectNode step(String route, JsonNode body, String response) {
        ObjectNode step =
                JSON.createObjectNode().put("target", "/api/v1" + route).put("response", response);
        if (body != null) {
            step.set("request", body);
        }
        return step;
    }

    private static JsonNode output(ObjectNode payload, String parent, String cursor) {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("id", payload.path("id").textValue())
                        .put("parent_id", parent)
                        .put("cursor_timestamp", cursor);
        row.set("data", payload);
        return row;
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
        if (settings.incremental != null) {
            return "/circa-incremental.conf";
        }
        return settings.bounded ? "/circa-bounded.conf" : "/circa.conf";
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        for (String secret : List.of(CircaPeer.TOKEN, settings.token, "private-provider-message")) {
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
                                CircaPeer.PASSWORD,
                                "-keypass",
                                CircaPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, CircaPeer.PASSWORD.toCharArray());
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
     * Holds immutable fixture identity and adjustable per-job options for one test case.
     *
     * <p>Program names distinguish every job's request trace and output receipts.
     */
    private static final class Settings {
        final String name;
        final String resource;
        String token = CircaPeer.TOKEN;
        String certificate = trust;
        String start = START;
        String end = END;
        String incremental;
        boolean bounded;
        int pages = 100;
        int bytes = 8388608;
        int readTimeout = 120000;
        int deadline = 180000;
        int attempts = 1;
        int lookback = 1;

        Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        String[] variables() {
            return new String[] {
                "api.output=" + name,
                "api.resource=" + resource,
                "api.token=" + token,
                "api.certificate=" + certificate,
                "api.start=" + start,
                "api.end=" + end,
                "api.incremental=" + (incremental == null ? start : incremental),
                "api.lookback=" + lookback,
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
        private static final String PEER = "/tmp/circa-peer";
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
            container.withExtraHost(CircaPeer.HOST, "127.0.0.1");
            container.withExtraHost("circa-trap.invalid", "127.0.0.1");
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
                stage(container, CircaPeer.class);
                stage(container, CircaRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java Circa fixture", failure);
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
                    CircaPeer.class.getName(),
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
                    CircaRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/circa-" + name;
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

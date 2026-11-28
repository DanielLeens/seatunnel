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

package org.apache.seatunnel.e2e.connector.clarifai;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.clarifai.ClarifaiRecoveryProbe;
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
 * Drives the packaged Clarifai Source through real Zeta jobs and an independent HTTPS peer.
 *
 * <p>The literal resource matrix and request scripts establish the expected provider wire bytes.
 * Transactional LocalFile receipts are the result oracle.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ClarifaiIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String USER = "owner";
    private static final String ROOT = "/v2/users/owner/apps";
    private static final String START = "2024-01-01T00:00:00.123456789Z";
    private static final String END = "2024-01-02T00:00:00Z";
    private static final String AFTER = "2024-01-02T00:00:00.000000001Z";
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
        trust = identity("trusted", ClarifaiPeer.HOST);
        wrongTrust = identity("untrusted", ClarifaiPeer.HOST);
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
    void readsAllEightNativeResourcesAndTheHistoricalWorkflowAlias() throws Exception {
        String[][] matrix = {
            {"applications", "", "apps", "20"},
            {"datasets", "/datasets", "datasets", "100"},
            {"models", "/models", "models", "100"},
            {"model_versions", "/models/model-1/versions", "model_versions", "100"},
            {"workflows", "/workflows", "workflows", "100"},
            {"worklows", "/workflows", "workflows", "100"},
            {"app_inputs", "/inputs", "inputs", "20"},
            {"app_inputs_jobs", "/inputs/jobs/add", "inputs_add_jobs", "20"},
            {"app_concepts", "/concepts", "concepts", "20"}
        };
        for (String[] entry : matrix) {
            String resource = entry[0];
            Settings config = new Settings("all-" + resource.replace('_', '-'), resource);
            String app = resource.equals("applications") ? null : "app-1";
            String model = resource.equals("model_versions") ? "model-1" : null;
            ObjectNode raw = sample("result-" + resource, START);
            if ("app_inputs_jobs".equals(resource)) {
                raw.putObject("progress").put("success_count", "18446744073709551615");
                raw.putObject("status").put("code", 30001);
            }
            String cursor =
                    List.of("applications", "datasets", "models").contains(resource) ? START : null;
            List<ObjectNode> script = new ArrayList<>();
            if (app != null) {
                script.add(
                        step(ROOT, 1, 20, envelope("apps", sample(app, "1999-01-01T00:00:00Z"))));
            }
            if (model != null) {
                script.add(
                        step(
                                ROOT + "/app-1/models",
                                1,
                                100,
                                envelope("models", sample(model, "2000-01-01T00:00:00Z"))));
            }
            script.add(
                    step(
                            ROOT + (app == null ? "" : "/" + app) + entry[1],
                            1,
                            Integer.parseInt(entry[3]),
                            envelope(entry[2], raw)));
            program(config.name, script);
            success(run(config));
            assertRows(config.name, List.of(output(raw, USER, app, model, cursor)));
            consumed(config.name, script.size());
        }
    }

    @Test
    void handlesTwentyPlusOneAndStopsOnAShortOrSuccessfulOmittedList() throws Exception {
        Settings config = new Settings("pages-20-21", "applications");
        List<ObjectNode> raw = new ArrayList<>();
        for (int index = 0; index < 21; index++) {
            raw.add(sample("app-" + index, START));
        }
        program(
                config.name,
                List.of(
                        step(
                                ROOT,
                                1,
                                20,
                                envelope("apps", raw.subList(0, 20).toArray(new ObjectNode[0]))),
                        step(ROOT, 2, 20, envelope("apps", raw.get(20)))));
        success(run(config));
        List<JsonNode> projected = new ArrayList<>();
        for (ObjectNode row : raw) {
            projected.add(output(row, USER, null, null, START));
        }
        assertRows(config.name, projected);
        consumed(config.name, 2);

        Settings exact = new Settings("pages-exact", "applications");
        exact.size = 2;
        program(
                exact.name,
                List.of(
                        step(ROOT, 1, 2, envelope("apps", raw.get(0), raw.get(1))),
                        step(ROOT, 2, 2, status(10000))));
        success(run(exact));
        assertRows(exact.name, projected.subList(0, 2));
        consumed(exact.name, 2);
    }

    @Test
    void rejectsRepeatedOrOversizedPagesAndBoundsTheWalkAcrossHiddenScopes() throws Exception {
        Settings repeated = new Settings("repeated", "applications");
        repeated.size = 2;
        String full = envelope("apps", sample("first", START), sample("second", START));
        fails(
                repeated,
                List.of(step(ROOT, 1, 2, full), step(ROOT, 2, 2, full)),
                "provider repeated the preceding page");
        Settings oversized = new Settings("oversized-page", "applications");
        oversized.size = 1;
        fails(oversized, List.of(step(ROOT, 1, 1, full)), "invalid list envelope");
        Settings budget = new Settings("hidden-and-leaf-budget", "model_versions");
        budget.pages = 2;
        fails(
                budget,
                List.of(
                        step(ROOT, 1, 20, envelope("apps", sample("app-1", null))),
                        step(
                                ROOT + "/app-1/models",
                                1,
                                100,
                                envelope("models", sample("m", null)))),
                "max_pages exhausted");
    }

    @Test
    void filtersOnlySelectedModifiedAtAndPublishesNullCursorsWithoutDroppingRows()
            throws Exception {
        for (String resource : List.of("applications", "datasets", "models")) {
            Settings config = new Settings("range-" + resource, resource);
            config.start = "2024-01-01T07:59:58+08:00";
            config.incremental = "2024-01-01T08:00:01.123456789+08:00";
            config.end = "2024-01-02T01:00:00+01:00";
            config.lookback = 1;
            String app = "applications".equals(resource) ? null : "old-app";
            int size = app == null ? 20 : 100;
            String route = app == null ? ROOT : ROOT + "/old-app/" + resource;
            String key = app == null ? "apps" : resource;
            ObjectNode one = sample("equal-a", "2023-12-31T19:00:00.123456789-05:00");
            ObjectNode two = sample("equal-b", START);
            ObjectNode last = sample("last", END);
            ObjectNode oldest = sample("filtered", "2024-01-01T00:00:00.123456788Z");
            ObjectNode latest = sample("filtered-late", AFTER);
            ObjectNode noCursor = sample("missing", null);
            ObjectNode nullCursor = sample("explicit-null", null).putNull("modified_at");
            List<ObjectNode> script = new ArrayList<>();
            if (app != null) {
                script.add(
                        step(ROOT, 1, 20, envelope("apps", sample(app, "2001-01-01T00:00:00Z"))));
            }
            script.add(
                    step(
                            route,
                            1,
                            size,
                            envelope(key, oldest, one, noCursor, two, last, latest, nullCursor)));
            program(config.name, script);
            success(run(config));
            assertRows(
                    config.name,
                    List.of(
                            output(one, USER, app, null, START),
                            output(two, USER, app, null, START),
                            output(last, USER, app, null, END),
                            output(noCursor, USER, app, null, null),
                            output(nullCursor, USER, app, null, null)));
            consumed(config.name, script.size());
        }
    }

    @Test
    void preservesRepeatedModelAndVersionIdsAcrossOldAppsAndEncodesPathSegments() throws Exception {
        Settings config = new Settings("parent-scope", "model_versions");
        config.user = "owner/α +%";
        config.size = 2;
        String base = "/v2/users/owner%2F%CE%B1%20%2B%25/apps";
        String first = "old/app +?";
        String second = "old-other";
        String model = "m/one%";
        String a = base + "/old%2Fapp%20%2B%3F/models";
        String b = base + "/old-other/models";
        ObjectNode one = sample("same-version", START);
        ObjectNode two = sample("same-version", END);
        program(
                config.name,
                List.of(
                        step(
                                base,
                                1,
                                2,
                                envelope(
                                        "apps",
                                        sample(first, "2000-01-01T00:00:00Z"),
                                        sample(second, "2001-01-01T00:00:00Z"))),
                        step(a, 1, 2, envelope("models", sample(model, "1990-01-01T00:00:00Z"))),
                        step(a + "/m%2Fone%25/versions", 1, 2, envelope("model_versions", one)),
                        step(b, 1, 2, envelope("models", sample(model, "1991-01-01T00:00:00Z"))),
                        step(b + "/m%2Fone%25/versions", 1, 2, envelope("model_versions", two)),
                        step(base, 2, 2, status(10000))));
        success(run(config));
        assertRows(
                config.name,
                List.of(
                        output(one, config.user, first, model, null),
                        output(two, config.user, second, model, null)));
        consumed(config.name, 6);
    }

    @Test
    void validatesTokenOwnerPageSizeAndApplicableDateBoundsBeforeAnyRequest() throws Exception {
        Settings token = new Settings("invalid-token", "applications");
        token.token = "bad key";
        fails(token, List.of(), "token must be a valid personal access token");
        Settings owner = new Settings("invalid-owner", "applications");
        owner.user = "..";
        fails(owner, List.of(), "user_id must be a nonempty path identifier");
        Settings limit = new Settings("invalid-page-size", "applications");
        limit.size = 1001;
        fails(limit, List.of(), "page_size must be between 1 and 1000");
        Settings date = new Settings("invalid-date", "models");
        date.bounded = true;
        date.start = "2024-01-01";
        fails(date, List.of(), "start_date must be an ISO 8601 timestamp");
        Settings unsupported = new Settings("date-on-full", "model_versions");
        unsupported.bounded = true;
        fails(unsupported, List.of(), "date bounds require start_date");
    }

    @Test
    void distinguishesSuccessWarningsMixedErrorsAndTheNestedInputStatus() throws Exception {
        Settings warning = new Settings("warning", "app_inputs");
        warning.size = 1;
        ObjectNode raw = sample("input-id", null);
        raw.putObject("status").put("code", 30001).put("description", "provider-owned state");
        raw.putObject("data").putObject("image").put("url", "https://api.clarifai.com/not-fetched");
        String parent = ROOT + "/app-1/inputs";
        program(
                warning.name,
                List.of(
                        step(ROOT, 1, 1, envelope("apps", sample("app-1", null))),
                        step(parent, 1, 1, envelope(10001, "inputs", raw)),
                        step(parent, 2, 1, envelope(10002, "inputs")),
                        step(ROOT, 2, 1, status(10000))));
        success(run(warning));
        assertRows(warning.name, List.of(output(raw, USER, "app-1", null, null)));
        consumed(warning.name, 4);

        Settings missing = new Settings("missing-warning-array", "applications");
        fails(missing, List.of(step(ROOT, 1, 20, status(10001))), "invalid list envelope");
        for (int code : List.of(10010, 11008, 11007, 11101, 19999)) {
            Settings error = new Settings("native-error-" + code, "applications");
            fails(
                    error,
                    List.of(step(ROOT, 1, 20, envelope(code, "apps", sample("would-leak", START)))),
                    "Clarifai status " + code);
        }
        Settings badCode = new Settings("not-numeric-native-status", "applications");
        fails(
                badCode,
                List.of(step(ROOT, 1, 20, "{\"status\":{\"code\":\"10000\"},\"apps\":[]}")),
                "missing or invalid numeric Clarifai status code");
        Settings absent = new Settings("missing-native-status", "applications");
        fails(
                absent,
                List.of(step(ROOT, 1, 20, "{\"apps\":[]}")),
                "missing or invalid numeric Clarifai status code");
        for (int response : List.of(401, 403, 404)) {
            Settings http = new Settings("http-error-" + response, "applications");
            fails(
                    http,
                    List.of(step(ROOT, 1, 20, status(11007)).put("status", response)),
                    "HTTP " + response);
        }
    }

    @Test
    void retriesOnlyEligibleHttpAndNativeStatusesUnderTheSameAttemptAndTimeBudget()
            throws Exception {
        Settings config = new Settings("retry-native-and-http", "applications");
        config.attempts = 5;
        ObjectNode one = sample("ack", START);
        program(
                config.name,
                List.of(
                        step(ROOT, 1, 20, status(11005)).put("Retry-After", "1"),
                        step(ROOT, 1, 20, status(10030)),
                        step(ROOT, 1, 20, "{}").put("status", 429),
                        step(ROOT, 1, 20, "{}").put("status", 503),
                        step(ROOT, 1, 20, envelope("apps", one))));
        success(run(config));
        assertRows(config.name, List.of(output(one, USER, null, null, START)));
        consumed(config.name, 5);
        List<String> times = engine.lines(config.name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 850L);

        Settings deadline = new Settings("long-rate-delay", "applications");
        deadline.deadline = 1500;
        deadline.attempts = 3;
        fails(
                deadline,
                List.of(step(ROOT, 1, 20, status(11005)).put("Retry-After", "20")),
                "retry delay exceeds request deadline");
        Settings attempts = new Settings("native-attempts", "applications");
        attempts.attempts = 2;
        String unavailable = status(10030);
        fails(
                attempts,
                List.of(step(ROOT, 1, 20, unavailable), step(ROOT, 1, 20, unavailable)),
                "retry limit reached after Clarifai status 10030");
    }

    @Test
    void preservesNestedNumbersAndUrlsButRejectsInvalidCursorUtf8AndOversizeBodies()
            throws Exception {
        Settings config = new Settings("gzip-exact", "applications");
        ObjectNode row = sample("precision", START);
        program(config.name, List.of(step(ROOT, 1, 20, envelope("apps", row)).put("mode", "gzip")));
        success(run(config));
        assertRows(config.name, List.of(output(row, USER, null, null, START)));
        consumed(config.name, 1);

        Settings invalid = new Settings("invalid-epoch-cursor", "applications");
        ObjectNode cursor = sample("bad", null).put("modified_at", 1704067200);
        fails(
                invalid,
                List.of(step(ROOT, 1, 20, envelope("apps", cursor))),
                "modified_at is not an ISO 8601 timestamp");

        for (String coding : List.of("json", "gzip")) {
            Settings limit = new Settings("oversized-" + coding, "applications");
            limit.bytes = 1024;
            ObjectNode huge = sample("overflow", START).put("oversized", "x".repeat(4096));
            fails(
                    limit,
                    List.of(step(ROOT, 1, 20, envelope("apps", huge)).put("mode", coding)),
                    "response exceeds max_response_bytes");
        }
        Settings duplicate = new Settings("duplicate-json", "applications");
        fails(
                duplicate,
                List.of(
                        step(
                                ROOT,
                                1,
                                20,
                                "{\"status\":{\"code\":10000,\"code\":10002},\"apps\":[]}")),
                "invalid JSON response");
        Settings utf = new Settings("invalid-utf8", "applications");
        ObjectNode malformed = step(ROOT, 1, 20, "{}");
        malformed.put(
                "bytes",
                Base64.getEncoder().encodeToString(new byte[] {123, 34, -64, -81, 34, 125}));
        fails(utf, List.of(malformed), "response is not valid UTF-8 JSON");
    }

    @Test
    void rejectsRedirectsUntrustedChainsAndWrongCertificateNames() throws Exception {
        Settings redirected = new Settings("redirect", "applications");
        fails(
                redirected,
                List.of(step(ROOT, 1, 20, "{}").put("status", 302).put("mode", "redirect")),
                "HTTP 302");
        Settings untrusted = new Settings("untrusted", "applications");
        untrusted.certificate = wrongTrust;
        fails(untrusted, List.of(), "HTTPS or TLS exchange failed");
        Settings wrong = new Settings("wrong-san", "applications");
        wrong.certificate = wrongNameTrust;
        selectIdentity("wrong-name");
        fails(wrong, List.of(), "HTTPS or TLS exchange failed");
    }

    @Test
    void closesTricklingResponsesOnAbsoluteDeadlineAndCancellation() throws Exception {
        Settings deadline = new Settings("absolute-deadline", "applications");
        deadline.deadline = 2000;
        fails(
                deadline,
                List.of(step(ROOT, 1, 20, "{").put("mode", "trickle")),
                "request deadline exceeded");
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> engine.exists(deadline.name + ".disconnect"));

        Settings cancel = new Settings("cancel", "applications");
        program(cancel.name, List.of(step(ROOT, 1, 20, "{").put("mode", "trickle")));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(jobConfig(cancel), job, cancel.variables()));
        try {
            Awaitility.await()
                    .atMost(25, TimeUnit.SECONDS)
                    .until(() -> engine.exists(cancel.name + ".started"));
            success(engine.cancelJob(job));
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .until(() -> engine.exists(cancel.name + ".disconnect"));
            assertRows(cancel.name, List.of());
            consumed(cancel.name, 1);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void resumesASecondPageAndExactAppModelOwnershipFromRealZetaSavepoints() throws Exception {
        Settings list = new Settings("page-savepoint", "applications");
        list.size = 2;
        ObjectNode first = sample("p1", START);
        ObjectNode second = sample("p2", START);
        ObjectNode third = sample("p3", END);
        ObjectNode pending = step(ROOT, 2, 2, envelope("apps", third));
        String listJob =
                saveWhileWaiting(
                        list,
                        List.of(
                                step(ROOT, 1, 2, envelope("apps", first, second)),
                                pending.deepCopy().put("mode", "partial")));
        assertRows(
                list.name,
                List.of(
                        output(first, USER, null, null, START),
                        output(second, USER, null, null, START)));
        program("resumed-page", List.of(pending));
        success(engine.restoreJob(jobConfig(list), listJob, list.variables()));
        assertRows(
                list.name,
                List.of(
                        output(first, USER, null, null, START),
                        output(second, USER, null, null, START),
                        output(third, USER, null, null, END)));
        consumed("resumed-page", 1);

        Settings models = new Settings("model-savepoint", "model_versions");
        String root = ROOT + "/old-app/models";
        String next = ROOT + "/other-app/models";
        ObjectNode apps =
                step(
                        ROOT,
                        1,
                        20,
                        envelope(
                                "apps",
                                sample("old-app", "2001-01-01T00:00:00Z"),
                                sample("other-app", null)));
        ObjectNode parent =
                step(
                        root,
                        1,
                        100,
                        envelope(
                                "models",
                                sample("done-model", null),
                                sample("active-model", null)));
        ObjectNode before = sample("common-version", START);
        ObjectNode after = sample("common-version", END);
        ObjectNode nextApp = sample("common-version", AFTER);
        ObjectNode active =
                step(root + "/active-model/versions", 1, 100, envelope("model_versions", after));
        String job =
                saveWhileWaiting(
                        models,
                        List.of(
                                apps,
                                parent,
                                step(
                                        root + "/done-model/versions",
                                        1,
                                        100,
                                        envelope("model_versions", before)),
                                active.deepCopy().put("mode", "partial")));
        assertRows(models.name, List.of(output(before, USER, "old-app", "done-model", null)));
        program(
                "resumed-model",
                List.of(
                        apps,
                        parent,
                        active,
                        step(next, 1, 100, envelope("models", sample("active-model", null))),
                        step(
                                next + "/active-model/versions",
                                1,
                                100,
                                envelope("model_versions", nextApp))));
        success(engine.restoreJob(jobConfig(models), job, models.variables()));
        assertRows(
                models.name,
                List.of(
                        output(before, USER, "old-app", "done-model", null),
                        output(after, USER, "old-app", "active-model", null),
                        output(nextApp, USER, "other-app", "active-model", null)));
        consumed("resumed-model", 5);
    }

    @Test
    void restoresTerminalStageWithoutRenewedProviderTraffic() throws Exception {
        Settings config = new Settings("terminal-original", "applications");
        ObjectNode only = sample("finished", START);
        List<JsonNode> expected = List.of(output(only, USER, null, null, START));
        program(config.name, List.of(step(ROOT, 1, 20, envelope("apps", only))));
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/clarifai-terminal.conf", job, config.variables()));
        try {
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(config.name, expected));
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(config.name, 1);
        program("terminal-restored", List.of());
        Future<Container.ExecResult> resumed =
                tasks.submit(
                        () ->
                                engine.restoreJob(
                                        "/clarifai-terminal.conf", job, config.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(job)));
            long baseline = engine.completedAcrossPipelines(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(job) > baseline);
            assertRows(config.name, expected);
            consumed("terminal-restored", 0);
        } finally {
            if (!resumed.isDone()) {
                engine.cancelJob(job);
            }
            resumed.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void bindsThePackagedReaderReplayToItsExactRawPositionAndFullPageDigest() throws Exception {
        List<ObjectNode> raw = new ArrayList<>();
        for (int index = 1; index <= 17; index++) {
            raw.add(sample("raw-" + index, index == 5 ? "2024-01-01T00:00:00.123456788Z" : START));
        }
        String initial = envelope("apps", raw.toArray(new ObjectNode[0]));
        for (String mode : List.of("resume", "changed-selected", "changed-filtered")) {
            List<ObjectNode> restored = new ArrayList<>();
            for (ObjectNode row : raw) {
                restored.add(row.deepCopy());
            }
            if (mode.startsWith("changed")) {
                restored.get(mode.endsWith("filtered") ? 4 : 8).put("different", true);
            }
            String name = "offset-" + mode;
            program(
                    name,
                    List.of(
                            step(ROOT, 1, 20, initial),
                            step(
                                    ROOT,
                                    1,
                                    20,
                                    envelope("apps", restored.toArray(new ObjectNode[0])))));
            success(engine.probe(mode));
            consumed(name, 2);
        }
        program("probe-zero", List.of());
        success(engine.probe("zero"));
        consumed("probe-zero", 0);
    }

    /**
     * Captures a genuine checkpoint while the provider holds an incomplete HTTP response.
     *
     * <p>The independent LocalFile output proves the retained prefix was committed.
     */
    private String saveWhileWaiting(Settings config, List<ObjectNode> steps) throws Exception {
        program(config.name, steps);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(jobConfig(config), job, config.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(config.name + ".started"));
            long baseline = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(config.name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(config.name, steps.size());
        return job;
    }

    private static ObjectNode sample(String id, String cursor) throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("id", id)
                        .put("name", "图像 Zoë")
                        .putNull("optional")
                        .put("raw_timestamp", 1704067200);
        if (cursor != null) {
            row.put("modified_at", cursor);
        }
        row.set(
                "metadata",
                JSON.readTree(
                        "{\"opaque\":[null,true,123456789012345678901234567890,1.00000000000000000009],\"nested\":{\"value\":null}}"));
        return row;
    }

    private static String status(int code) {
        return response(code).toString();
    }

    private static ObjectNode response(int code) {
        ObjectNode root = JSON.createObjectNode();
        root.putObject("status").put("code", code).put("description", "private-provider-message");
        return root;
    }

    private static String envelope(String key, ObjectNode... rows) {
        return envelope(10000, key, rows);
    }

    private static String envelope(int code, String key, ObjectNode... rows) {
        ObjectNode root = response(code);
        ArrayNode data = root.putArray(key);
        for (ObjectNode row : rows) {
            data.add(row);
        }
        return root.toString();
    }

    private static ObjectNode step(String route, int page, int size, String response) {
        return JSON.createObjectNode()
                .put("target", route + "?page=" + page + "&per_page=" + size)
                .put("response", response);
    }

    private static JsonNode output(
            ObjectNode payload, String user, String app, String model, String cursor) {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("id", payload.path("id").textValue())
                        .put("user_id", user)
                        .put("app_id", app)
                        .put("model_id", model)
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
            return "/clarifai-incremental.conf";
        }
        if (settings.bounded) {
            return "/clarifai-bounded.conf";
        }
        return settings.size == 0 ? "/clarifai.conf" : "/clarifai-small.conf";
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        for (String secret :
                List.of(ClarifaiPeer.TOKEN, settings.token, "private-provider-message")) {
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
                                ClarifaiPeer.PASSWORD,
                                "-keypass",
                                ClarifaiPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, ClarifaiPeer.PASSWORD.toCharArray());
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
        String user = USER;
        int size;
        String token = ClarifaiPeer.TOKEN;
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
                "api.user=" + user,
                "api.page_size=" + size,
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
        private static final String PEER = "/tmp/clarifai-peer";
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
            container.withExtraHost(ClarifaiPeer.HOST, "127.0.0.1");
            container.withExtraHost("clarifai-trap.invalid", "127.0.0.1");
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
                stage(container, ClarifaiPeer.class);
                stage(container, ClarifaiRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java Clarifai fixture", failure);
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
                    ClarifaiPeer.class.getName(),
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
                    ClarifaiRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/clarifai-" + name;
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

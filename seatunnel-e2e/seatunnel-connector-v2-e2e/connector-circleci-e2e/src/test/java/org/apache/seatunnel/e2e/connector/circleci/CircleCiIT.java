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
package org.apache.seatunnel.e2e.connector.circleci;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.circleci.CircleCiRecoveryProbe;
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
 * Verifies CircleCI reads through both native API versions and the real Java 11 Zeta engine.
 *
 * <p>Independent literal HTTPS fixtures validate each route, token and parent scope before
 * committed full rows are compared as a multiset. No production resource table is used to build
 * fixtures.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class CircleCiIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String BASE = "/api";
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
        trust = identity("trusted", CircleCiPeer.HOST);
        wrongTrust = identity("untrusted", CircleCiPeer.HOST);
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

    private static final String ORG = "10000000-0000-4000-8000-000000000001";
    private static final String WORKFLOW = "20000000-0000-4000-8000-000000000001";
    private static final String SECOND_WORKFLOW = "20000000-0000-4000-8000-000000000002";
    private static final String NATIVE = "30000000-0000-4000-8000-000000000001";
    private static final String SECOND = "30000000-0000-4000-8000-000000000002";
    private static final String PROJECT = "gh/seatunnel-team/example";
    private static final String TIME = "2024-02-29T00:00:00Z";

    @Test
    void exportsEveryResourceUsingItsActualApiVersionEnvelopeAndNativeIdentity() throws Exception {
        String[][] cases = {
            {
                "context",
                "/api/v2/context",
                "owner-id=" + ORG,
                object("\"id\":\"" + NATIVE + "\",\"created_at\":\"" + TIME + "\""),
                NATIVE,
                ORG,
                TIME
            },
            {
                "self_ids",
                "/api/v2/me",
                "",
                object("\"id\":\"" + NATIVE + "\",\"login\":\"synthetic\""),
                NATIVE,
                null,
                null
            },
            {
                "self_collaborations",
                "/api/v2/me/collaborations",
                "",
                object(
                        "\"id\":null,\"vcs-type\":\"github\",\"slug\":\"seatunnel-team\",\"name\":\"Synthetic\""),
                "github:seatunnel-team",
                null,
                null
            },
            {
                "me",
                "/api/v1.1/me",
                "",
                object("\"analytics_id\":\"legacy-analytics\",\"login\":\"synthetic\""),
                "legacy-analytics",
                null,
                null
            },
            {
                "projects",
                "/api/v1.1/projects",
                "",
                object(
                        "\"vcs_url\":\"https://github.com/seatunnel-team/example\",\"branches\":{\"main\":{\"pusher_logins\":[\"synthetic\"]}}"),
                "https://github.com/seatunnel-team/example",
                null,
                null
            },
            {
                "pipelines",
                "/api/v2/pipeline",
                "org-slug=gh%2Fseatunnel-team",
                object(
                        "\"id\":\""
                                + NATIVE
                                + "\",\"project_slug\":\""
                                + PROJECT
                                + "\",\"created_at\":\""
                                + TIME
                                + "\",\"number\":9223372036854775807"),
                NATIVE,
                "gh/seatunnel-team",
                TIME
            },
            {
                "specific_project",
                "/api/v2/project/" + PROJECT,
                "",
                object("\"id\":\"" + NATIVE + "\",\"slug\":\"" + PROJECT + "\""),
                NATIVE,
                null,
                null
            },
            {
                "workflow",
                "/api/v2/workflow/" + WORKFLOW,
                "",
                object("\"id\":\"" + WORKFLOW + "\",\"created_at\":\"" + TIME + "\""),
                WORKFLOW,
                null,
                TIME
            },
            {
                "workflow_jobs",
                "/api/v2/workflow/" + WORKFLOW + "/job",
                "",
                object(
                        "\"id\":\""
                                + NATIVE
                                + "\",\"type\":\"approval\",\"started_at\":null,\"project_slug\":\""
                                + PROJECT
                                + "\""),
                NATIVE,
                WORKFLOW,
                null
            },
            {
                "jobs",
                "/api/v2/project/" + PROJECT + "/job/7",
                "",
                job(7, PROJECT),
                "7",
                PROJECT,
                null
            },
            {
                "insights_metrics",
                "/api/v2/insights/pages/" + PROJECT + "/summary",
                "",
                object(
                        "\"project_id\":\""
                                + NATIVE
                                + "\",\"project_data\":{\"metrics\":{\"total_credits_used\":9223372036854775808,\"success_rate\":0.123456789012345678900}}"),
                NATIVE,
                PROJECT,
                null
            },
            {
                "insights_branches",
                "/api/v2/insights/" + PROJECT + "/branches",
                "",
                object(
                        "\"project_id\":\""
                                + NATIVE
                                + "\",\"org_id\":\""
                                + ORG
                                + "\",\"branches\":[\"main\",\"feature/中文🌏\"]"),
                NATIVE,
                PROJECT,
                null
            }
        };
        for (String[] c : cases) {
            Settings s = new Settings("resource-" + c[0], c[0]);
            if (c[0].equals("jobs")) {
                s.config = "/circleci-direct.conf";
            }
            String body = c[3];
            if (List.of("context", "pipelines", "workflow_jobs").contains(c[0])) {
                body = envelope(null, body);
            } else if (List.of("projects", "self_collaborations").contains(c[0])) {
                body = array(body);
            }
            program(s.name, List.of(get(c[1], body).put("query", c[2])));
            success(run(s));
            consumed(s.name, 1);
            assertRows(s.name, List.of(expected(c[0], c[4], c[5], c[6], c[3])));
        }
    }

    @Test
    void continuesEmptyAndShortContextPagesWithOpaqueTokensAsQueryValues() throws Exception {
        Settings s = new Settings("context-pages", "context");
        String token = "https://circleci-trap.invalid/?x=+/=%中文", row = context(NATIVE, TIME);
        program(
                s.name,
                List.of(
                        contextPage(null, token),
                        contextPage(token, "last", row),
                        contextPage("last", null)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(s.name, List.of(expected("context", NATIVE, ORG, TIME, row)));
    }

    @Test
    void usesOrgSlugOnlyOnPipelinePagesAndNeverAddsAnInventedPageSize() throws Exception {
        Settings s = new Settings("pipeline-pages", "pipelines");
        String row = object("\"id\":\"" + NATIVE + "\",\"created_at\":\"" + TIME + "\"");
        program(
                s.name,
                List.of(
                        get("/api/v2/pipeline", envelope("next", row))
                                .put("query", "org-slug=gh%2Fseatunnel-team"),
                        get("/api/v2/pipeline", envelope(null))
                                .put("query", "org-slug=gh%2Fseatunnel-team&page-token=next")));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expected("pipelines", NATIVE, "gh/seatunnel-team", TIME, row)));
    }

    @Test
    void discoversAllMatchingJobsAcrossWorkflowPagesWithoutDuplicatingNumbersOrPruningParents()
            throws Exception {
        Settings s = new Settings("discovery", "jobs");
        s.workflows = new String[] {WORKFLOW, SECOND_WORKFLOW};
        String a = workflowJob(NATIVE, 7L, PROJECT, null),
                approval = object("\"id\":\"approval\",\"started_at\":null,\"type\":\"approval\""),
                b = workflowJob(SECOND, 8L, "github/seatunnel-team/example", TIME),
                last = workflowJob("long", Long.MAX_VALUE, PROJECT, TIME),
                wrong = workflowJob("wrong", 9L, "gh/elsewhere/other", TIME);
        String seven = job(7, PROJECT),
                eight = job(8, PROJECT),
                maximum = job(Long.MAX_VALUE, PROJECT);
        program(
                s.name,
                List.of(
                        workflowPage(WORKFLOW, null, "next", a, approval),
                        workflowPage(WORKFLOW, "next", null, a, b),
                        workflowPage(SECOND_WORKFLOW, null, null, b, last, wrong),
                        get("/api/v2/project/" + PROJECT + "/job/7", seven),
                        get("/api/v2/project/" + PROJECT + "/job/8", eight),
                        get("/api/v2/project/" + PROJECT + "/job/" + Long.MAX_VALUE, maximum)));
        success(run(s));
        consumed(s.name, 6);
        assertRows(
                s.name,
                List.of(
                        expected("jobs", "7", PROJECT, null, seven),
                        expected("jobs", "8", PROJECT, null, eight),
                        expected("jobs", Long.toString(Long.MAX_VALUE), PROJECT, null, maximum)));
    }

    @Test
    void keepsExplicitWorkflowJobRowsWithoutUsableJobNumbersOrAMatchingProject() throws Exception {
        Settings s = new Settings("workflow-jobs", "workflow_jobs");
        String wrong = workflowJob(NATIVE, 9L, "gh/elsewhere/other", TIME),
                nil =
                        object(
                                "\"id\":\""
                                        + SECOND
                                        + "\",\"job_number\":null,\"started_at\":null,\"type\":\"approval\"");
        program(s.name, List.of(workflowPage(WORKFLOW, null, null, wrong, nil)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(
                s.name,
                List.of(
                        expected("workflow_jobs", NATIVE, WORKFLOW, TIME, wrong),
                        expected("workflow_jobs", SECOND, WORKFLOW, null, nil)));
    }

    @Test
    void readsAnExplicitInt64JobDirectlyWithoutWalkingAnyWorkflow() throws Exception {
        Settings s = new Settings("direct", "jobs");
        s.config = "/circleci-direct.conf";
        s.job = Long.MAX_VALUE;
        String row = job(s.job, "github/seatunnel-team/example");
        program(s.name, List.of(get("/api/v2/project/" + PROJECT + "/job/" + s.job, row)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("jobs", Long.toString(s.job), PROJECT, null, row)));
    }

    @Test
    void rejectsMismatchedTargetedProjectWorkflowNumberAndJobProjectResponses() throws Exception {
        fails(
                new Settings("wrong-project", "specific_project"),
                List.of(
                        get(
                                "/api/v2/project/" + PROJECT,
                                object(
                                        "\"id\":\""
                                                + NATIVE
                                                + "\",\"slug\":\"gh/elsewhere/other\""))),
                "CircleCI");
        fails(
                new Settings("wrong-workflow", "workflow"),
                List.of(
                        get(
                                "/api/v2/workflow/" + WORKFLOW,
                                object(
                                        "\"id\":\""
                                                + SECOND_WORKFLOW
                                                + "\",\"created_at\":\""
                                                + TIME
                                                + "\""))),
                "CircleCI");
        for (boolean number : List.of(false, true)) {
            Settings s = new Settings("wrong-job-" + number, "jobs");
            s.config = "/circleci-direct.conf";
            fails(
                    s,
                    List.of(
                            get(
                                    "/api/v2/project/" + PROJECT + "/job/7",
                                    job(number ? 8 : 7, number ? PROJECT : "gh/elsewhere/other"))),
                    "CircleCI");
        }
    }

    @Test
    void filtersTemporalRowsWithNanosecondBoundsAndNormalizesOffsetTimes() throws Exception {
        Settings s = new Settings("date-bounds", "context");
        s.config = "/circleci-window.conf";
        String before = context("before", "2024-02-28T23:59:59.999999999Z"),
                first = context("first", "2024-02-29T08:00:00+08:00"),
                last = context("last", "2024-02-29T23:59:59.999999999Z"),
                end = context("end", s.end);
        program(s.name, List.of(contextPage(null, null, before, first, last, end)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(
                s.name,
                List.of(
                        expected("context", "first", ORG, TIME, first),
                        expected("context", "last", ORG, "2024-02-29T23:59:59.999999999Z", last)));
    }

    @Test
    void retainsUndatedLegacyMeAndWorkflowJobsInsideAnOutputTimeWindow() throws Exception {
        Settings legacy = new Settings("legacy-window", "me");
        legacy.config = "/circleci-window.conf";
        String me = object("\"login\":\"synthetic-legacy\"");
        program(legacy.name, List.of(get("/api/v1.1/me", me)));
        success(run(legacy));
        consumed(legacy.name, 1);
        assertRows(legacy.name, List.of(expected("me", "synthetic-legacy", null, null, me)));
        Settings s = new Settings("undated-job-window", "workflow_jobs");
        s.config = "/circleci-window.conf";
        String row = object("\"id\":\"" + NATIVE + "\",\"started_at\":null");
        program(s.name, List.of(workflowPage(WORKFLOW, null, null, row)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("workflow_jobs", NATIVE, WORKFLOW, null, row)));
    }

    @Test
    void sendsAnExplicitReportingWindowOnlyForTheSummaryAndKeepsAggregatePrecision()
            throws Exception {
        Settings s = new Settings("reporting", "insights_metrics");
        s.config = "/circleci-report.conf";
        String row =
                object(
                        "\"project_data\":{\"metrics\":{\"success_rate\":0.123456789012345678900,\"total_duration_secs\":9223372036854775808}}");
        program(
                s.name,
                List.of(
                        get("/api/v2/insights/pages/" + PROJECT + "/summary", row)
                                .put("query", "reporting-window=last-7-days")));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("insights_metrics", PROJECT, PROJECT, null, row)));
    }

    @Test
    void preservesFiveThousandInsightsBranchNamesAsOneNativeResponse() throws Exception {
        Settings s = new Settings("branches-limit", "insights_branches");
        ObjectNode body = JSON.createObjectNode().put("org_id", ORG).put("project_id", NATIVE);
        ArrayNode branches = JSON.createArrayNode();
        for (int i = 0; i < 5000; i++) {
            branches.add("feature/分支-" + i);
        }
        body.set("branches", branches);
        String row = body.toString();
        program(s.name, List.of(get("/api/v2/insights/" + PROJECT + "/branches", row)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("insights_branches", NATIVE, PROJECT, null, row)));
        branches.add("beyond-the-documented-limit");
        Settings tooMany = new Settings("branches-limit-plus-one", "insights_branches");
        fails(
                tooMany,
                List.of(get("/api/v2/insights/" + PROJECT + "/branches", body.toString())),
                "5000");
        assertRows(tooMany.name, List.of());
    }

    @Test
    void supportsModernUuidProjectSlugsAndRejectsAnInsightsProjectMismatch() throws Exception {
        Settings s = new Settings("modern-project", "specific_project");
        s.project = "circleci/" + ORG + "/" + NATIVE;
        String row = object("\"id\":\"" + NATIVE + "\",\"slug\":\"" + s.project + "\"");
        program(s.name, List.of(get("/api/v2/project/" + s.project, row)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("specific_project", NATIVE, null, null, row)));
        Settings wrong = new Settings("modern-insights-mismatch", "insights_branches");
        wrong.project = s.project;
        fails(
                wrong,
                List.of(
                        get(
                                "/api/v2/insights/" + s.project + "/branches",
                                object("\"project_id\":\"" + SECOND + "\",\"branches\":[]"))),
                "CircleCI");
    }

    @Test
    void rejectsMoreThanTwentyThousandNativeObjectsBeforeAnyRowsArePublished() throws Exception {
        StringBuilder body = new StringBuilder("{\"items\":[");
        for (int i = 0; i < 20001; i++) {
            if (i > 0) {
                body.append(',');
            }
            body.append("{\"id\":\"native-")
                    .append(i)
                    .append("\",\"created_at\":\"")
                    .append(TIME)
                    .append("\"}");
        }
        body.append("],\"next_page_token\":null}");
        Settings s = new Settings("object-budget", "context");
        fails(
                s,
                List.of(get("/api/v2/context", body.toString()).put("query", "owner-id=" + ORG)),
                "20000");
        assertRows(s.name, List.of());
    }

    @Test
    void rejectsMissingOrCyclicContinuationAndFiniteRequestAndDiscoveryBudgets() throws Exception {
        fails(
                new Settings("missing-token", "context"),
                List.of(get("/api/v2/context", "{\"items\":[]}").put("query", "owner-id=" + ORG)),
                "CircleCI");
        fails(
                new Settings("cycle", "context"),
                List.of(contextPage(null, "same"), contextPage("same", "same")),
                "CircleCI");
        Settings budget = new Settings("budget", "context");
        budget.requests = 1;
        fails(budget, List.of(contextPage(null, "next")), "CircleCI");
        Settings jobs = new Settings("job-budget", "jobs");
        jobs.jobs = 1;
        fails(
                jobs,
                List.of(
                        workflowPage(
                                WORKFLOW,
                                null,
                                null,
                                workflowJob(NATIVE, 7L, PROJECT, null),
                                workflowJob(SECOND, 8L, PROJECT, null))),
                "CircleCI");
    }

    @Test
    void rejectsInvalidNativeShapesIdsDatesAndDuplicateRecordsBeforePublication() throws Exception {
        int i = 0;
        for (String row :
                List.of(
                        "{}",
                        "{\"id\":12,\"created_at\":\"" + TIME + "\"}",
                        "{\"id\":\"one\"}",
                        context("bad", "not-a-date"))) {
            fails(
                    new Settings("bad-record-" + i++, "context"),
                    List.of(contextPage(null, null, row)),
                    "CircleCI");
        }
        String row = context(NATIVE, TIME);
        fails(
                new Settings("duplicate-id", "context"),
                List.of(contextPage(null, null, row, row)),
                "CircleCI");
        fails(
                new Settings("wrong-array", "context"),
                List.of(
                        get("/api/v2/context", "{\"items\":{},\"next_page_token\":null}")
                                .put("query", "owner-id=" + ORG)),
                "CircleCI");
    }

    @Test
    void reportsHttpAndProviderFailuresWithoutLeakingThePersonalTokenOrErrorBody()
            throws Exception {
        for (int status : List.of(401, 403, 404, 500)) {
            fails(
                    new Settings("http-" + status, "self_ids"),
                    List.of(
                            get("/api/v2/me", "{\"message\":\"private-provider-message\"}")
                                    .put("status", status)),
                    "CircleCI");
        }
        fails(
                new Settings("error-object", "self_ids"),
                List.of(get("/api/v2/me", "{\"message\":\"private-provider-message\"}")),
                "CircleCI");
    }

    @Test
    void verifiesCertificateTrustAndHostnameAndRefusesTokenForwardingOnRedirects()
            throws Exception {
        Settings ca = new Settings("wrong-ca", "self_ids");
        ca.certificate = wrongTrust;
        fails(ca, List.of(), "CircleCI");
        selectIdentity("wrong-name");
        try {
            Settings san = new Settings("wrong-san", "self_ids");
            san.certificate = wrongNameTrust;
            fails(san, List.of(), "CircleCI");
        } finally {
            selectIdentity("trusted");
        }
        fails(
                new Settings("redirect", "self_ids"),
                List.of(get("/api/v2/me", "{}").put("status", 302).put("mode", "redirect")),
                "CircleCI");
    }

    @Test
    void acceptsGzipAndBoundsDecodedBodiesAndRejectsOtherEncodingsAndMedia() throws Exception {
        Settings s = new Settings("gzip", "self_ids");
        String row = object("\"id\":\"" + NATIVE + "\"");
        program(s.name, List.of(get("/api/v2/me", row).put("mode", "gzip")));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("self_ids", NATIVE, null, null, row)));
        for (String mode : List.of("json", "gzip")) {
            Settings small = new Settings("body-" + mode, "self_ids");
            small.bytes = 1024;
            fails(
                    small,
                    List.of(
                            get(
                                            "/api/v2/me",
                                            object(
                                                    "\"id\":\""
                                                            + NATIVE
                                                            + "\",\"padding\":\""
                                                            + "x".repeat(5000)
                                                            + "\""))
                                    .put("mode", mode)),
                    "CircleCI");
        }
        fails(
                new Settings("encoding", "self_ids"),
                List.of(get("/api/v2/me", row).put("encoding", "br")),
                "CircleCI");
        fails(
                new Settings("media", "self_ids"),
                List.of(get("/api/v2/me", row).put("mime", "text/plain")),
                "CircleCI");
    }

    @Test
    void rejectsMalformedUtf8JsonTrailingTokensDuplicateKeysAndNonrecordValues() throws Exception {
        int i = 0;
        for (String body : List.of("null", "[]", "{}{}", "{\"id\":\"one\",\"id\":\"two\"}")) {
            Settings s = new Settings("json-" + i++, "self_ids");
            fails(s, List.of(get("/api/v2/me", body)), "CircleCI");
            assertRows(s.name, List.of());
        }
        fails(
                new Settings("utf8", "self_ids"),
                List.of(
                        get("/api/v2/me", "")
                                .put(
                                        "bytes",
                                        Base64.getEncoder()
                                                .encodeToString(new byte[] {(byte) 0xc3, 0x28}))),
                "CircleCI");
    }

    @Test
    void honorsRetryAfterAndTheLargerResetTimeWithoutExceedingAttemptBudgets() throws Exception {
        String row = object("\"id\":\"" + NATIVE + "\"");
        Settings retry = new Settings("retry", "self_ids");
        retry.attempts = 2;
        program(
                retry.name,
                List.of(
                        get("/api/v2/me", "{}").put("status", 429).put("Retry-After", "2"),
                        get("/api/v2/me", row)));
        success(run(retry));
        consumed(retry.name, 2);
        delayAtLeast(retry.name, 2000);
        Settings reset = new Settings("reset", "self_ids");
        reset.attempts = 2;
        program(
                reset.name,
                List.of(
                        get("/api/v2/me", "{}")
                                .put("status", 429)
                                .put("Retry-After", "1")
                                .put("X-RateLimit-Reset", "4"),
                        get("/api/v2/me", row)));
        success(run(reset));
        consumed(reset.name, 2);
        delayAtLeast(reset.name, 4000);
        Settings exhausted = new Settings("exhausted", "self_ids");
        exhausted.attempts = 2;
        ObjectNode unavailable = get("/api/v2/me", "{}").put("status", 503);
        fails(exhausted, List.of(unavailable, unavailable), "CircleCI");
    }

    @Test
    void rejectsInvalidScopeSelectionsAndBudgetsBeforeSendingAnyToken() throws Exception {
        fails(new Settings("empty"), List.of(), "CircleCI");
        fails(new Settings("unknown", "unknown"), List.of(), "CircleCI");
        fails(new Settings("duplicate", "self_ids", "self_ids"), List.of(), "CircleCI");
        Settings org = new Settings("bad-org", "context");
        org.org = "invalid";
        fails(org, List.of(), "CircleCI");
        Settings project = new Settings("bad-project", "specific_project");
        project.project = "https://circleci-trap.invalid/x";
        fails(project, List.of(), "CircleCI");
        Settings number = new Settings("negative-job", "jobs");
        number.config = "/circleci-direct.conf";
        number.job = -1;
        fails(number, List.of(), "CircleCI");
        Settings token = new Settings("bad-token", "self_ids");
        token.token = " ";
        fails(token, List.of(), "CircleCI");
        Settings attempts = new Settings("bad-attempts", "self_ids");
        attempts.attempts = 7;
        fails(attempts, List.of(), "CircleCI");
    }

    @Test
    void boundsContinuousBodyTrafficAndClosesTheSocketAtTheRequestDeadline() throws Exception {
        Settings s = new Settings("deadline", "self_ids");
        s.timeout = 2000;
        fails(
                s,
                List.of(
                        get("/api/v2/me", object("\"id\":\"" + NATIVE + "\""))
                                .put("mode", "trickle")),
                "CircleCI");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(s.name + ".disconnect"));
        long first = Long.parseLong(engine.lines(s.name + ".started").get(0)),
                last = Long.parseLong(engine.lines(s.name + ".disconnect").get(0));
        assertTrue(last - first < 6000);
        assertRows(s.name, List.of());
    }

    @Test
    void cancelsTheActualEngineJobWithoutPublishingAPartialProviderResponse() throws Exception {
        Settings s = new Settings("cancel", "self_ids");
        program(
                s.name,
                List.of(
                        get("/api/v2/me", object("\"id\":\"" + NATIVE + "\""))
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
    void doesNotPruneJobDiscoveryWhenTheSelectedParentOutputIsOutsideTheTimeWindow()
            throws Exception {
        Settings s = new Settings("filtered-parent", "workflow_jobs", "jobs");
        s.config = "/circleci-window.conf";
        String parent = workflowJob(NATIVE, 7L, PROJECT, "2023-01-01T00:00:00Z"),
                row = job(7, PROJECT);
        program(
                s.name,
                List.of(
                        workflowPage(WORKFLOW, null, null, parent),
                        get("/api/v2/project/" + PROJECT + "/job/7", row)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expected("jobs", "7", PROJECT, null, row)));
    }

    @Test
    void restoresTheUnfinishedPageTokenWithoutReissuingTheCommittedFirstPage() throws Exception {
        Settings s = new Settings("page-savepoint", "context");
        String first = context(NATIVE, TIME), last = context(SECOND, TIME);
        String id =
                saveBlocked(
                        s,
                        List.of(
                                contextPage(null, "next", first),
                                contextPage("next", null, last).put("mode", "partial")));
        program("page-restored", List.of(contextPage("next", null, last)));
        success(engine.restoreJob(s.config, id, s.variables()));
        consumed("page-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expected("context", NATIVE, ORG, TIME, first),
                        expected("context", SECOND, ORG, TIME, last)));
    }

    @Test
    void restoresBetweenApiVersionsWithoutReissuingACompletedResource() throws Exception {
        Settings s = new Settings("between-savepoint", "self_ids", "projects");
        String first = object("\"id\":\"" + NATIVE + "\""),
                project = object("\"vcs_url\":\"https://github.com/seatunnel-team/example\"");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                get("/api/v2/me", first),
                                get("/api/v1.1/projects", array(project)).put("mode", "partial")));
        program("between-restored", List.of(get("/api/v1.1/projects", array(project))));
        success(engine.restoreJob(s.config, id, s.variables()));
        consumed("between-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expected("self_ids", NATIVE, null, null, first),
                        expected(
                                "projects",
                                "https://github.com/seatunnel-team/example",
                                null,
                                null,
                                project)));
    }

    @Test
    void restoresCommittedJobDiscoveriesWithoutRepeatingTheWorkflowParentScan() throws Exception {
        Settings s = new Settings("discovery-savepoint", "jobs");
        String a = job(7, PROJECT), b = job(8, PROJECT);
        String id =
                saveBlocked(
                        s,
                        List.of(
                                workflowPage(
                                        WORKFLOW,
                                        null,
                                        null,
                                        workflowJob(NATIVE, 7L, PROJECT, null),
                                        workflowJob(SECOND, 8L, PROJECT, null)),
                                get("/api/v2/project/" + PROJECT + "/job/7", a)
                                        .put("mode", "partial")));
        program(
                "discovery-restored",
                List.of(
                        get("/api/v2/project/" + PROJECT + "/job/7", a),
                        get("/api/v2/project/" + PROJECT + "/job/8", b)));
        success(engine.restoreJob(s.config, id, s.variables()));
        consumed("discovery-restored", 2);
        assertRows(
                s.name,
                List.of(
                        expected("jobs", "7", PROJECT, null, a),
                        expected("jobs", "8", PROJECT, null, b)));
    }

    @Test
    void restoresATerminalSourceWithZeroHttpAndNoDuplicateCommittedRows() throws Exception {
        Settings s = new Settings("terminal-savepoint", "self_ids");
        String row = object("\"id\":\"" + NATIVE + "\"");
        List<JsonNode> wanted = List.of(expected("self_ids", NATIVE, null, null, row));
        program(s.name, List.of(get("/api/v2/me", row)));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/circleci-terminal.conf", id, s.variables()));
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
                tasks.submit(() -> engine.restoreJob("/circleci-terminal.conf", id, s.variables()));
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
    void serializes32Of33RowsAndRejectsChangesToPublishedAndFilteredRecords() throws Exception {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < 33; i++) {
            rows.add(
                    object(
                            "\"id\":\"C"
                                    + i
                                    + "\",\"created_at\":\""
                                    + TIME
                                    + "\",\"description\":\"synthetic-response-record\""));
        }
        rows.add(context("filtered", "2023-01-01T00:00:00Z"));
        for (String mode : List.of("resume", "changed", "changed-filtered")) {
            List<String> replay = new ArrayList<>(rows);
            if (mode.equals("changed")) {
                replay.set(0, replay.get(0).replace("C0", "CHANGED"));
            }
            if (mode.equals("changed-filtered")) {
                replay.set(33, replay.get(33).replace("filtered", "CHANGED"));
            }
            String name = "reader-" + mode;
            program(
                    name,
                    List.of(
                            contextPage(null, null, rows.toArray(new String[0])),
                            contextPage(null, null, replay.toArray(new String[0]))));
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

    private void delayAtLeast(String name, long millis) throws Exception {
        List<String> times = engine.lines(name + ".times");
        assertTrue(
                Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= millis,
                times.toString());
    }

    private static String object(String fields) {
        return "{" + fields + EXTRA + "}";
    }

    private static String context(String id, String time) {
        return object("\"id\":\"" + id + "\",\"created_at\":\"" + time + "\"");
    }

    private static String job(long number, String project) {
        return object(
                "\"number\":"
                        + number
                        + ",\"project\":{\"slug\":\""
                        + project
                        + "\"},\"executor\":{\"resource_class\":\"large\"}");
    }

    private static String workflowJob(String id, Long number, String project, String time) {
        return object(
                "\"id\":\""
                        + id
                        + "\",\"job_number\":"
                        + (number == null ? "null" : number)
                        + ",\"project_slug\":\""
                        + project
                        + "\",\"started_at\":"
                        + (time == null ? "null" : "\"" + time + "\""));
    }

    private static String envelope(String token, String... rows) {
        return "{\"items\":"
                + array(rows)
                + ",\"next_page_token\":"
                + (token == null ? "null" : JSON.getNodeFactory().textNode(token).toString())
                + "}";
    }

    private static ObjectNode contextPage(String cursor, String next, String... rows) {
        return get("/api/v2/context", envelope(next, rows))
                .put(
                        "query",
                        "owner-id="
                                + ORG
                                + (cursor == null ? "" : "&page-token=" + encode(cursor)));
    }

    private static ObjectNode workflowPage(
            String workflow, String cursor, String next, String... rows) {
        return get("/api/v2/workflow/" + workflow + "/job", envelope(next, rows))
                .put("query", cursor == null ? "" : "page-token=" + encode(cursor));
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
            String resource, String id, String parent, String time, String payload)
            throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("parent_id", parent)
                        .put("record_time", time);
        row.set("payload", JSON.readTree(payload));
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>(), wanted = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(parsed.isObject() && parsed.size() == 5, line);
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
        assertFalse(output.contains(CircleCiPeer.TOKEN), "data API key leaked");
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
                                CircleCiPeer.PASSWORD,
                                "-keypass",
                                CircleCiPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, CircleCiPeer.PASSWORD.toCharArray());
        }
        Files.writeString(
                directory.resolve(name + ".pem"),
                "-----BEGIN CERTIFICATE-----\n"
                        + Base64.getMimeEncoder(64, new byte[] {'\n'})
                                .encodeToString(keys.getCertificate("api").getEncoded())
                        + "\n-----END CERTIFICATE-----\n");
        return "/tmp/circleci-peer/" + name + ".pem";
    }

    /**
     * Configures only synthetic tenant scopes and bounded reads for one real engine job.
     *
     * <p>Optional job numbers, output windows and reporting filters are supplied by separate HOCON
     * fixtures so an omitted option really is absent from the source configuration.
     */
    private static final class Settings {
        final String name;
        final String[] resources;
        String token = CircleCiPeer.TOKEN, certificate = trust;
        String org = ORG, orgSlug = "gh/seatunnel-team", project = "gh/seatunnel-team/example";
        String[] workflows = {WORKFLOW};
        String start = "2024-02-29T00:00:00Z",
                end = "2024-03-01T00:00:00Z",
                reporting = "last-7-days";
        long job = 7;
        int requests = 10000, jobs = 4096, attempts = 1, timeout = 60000, bytes = 8388608;
        String config = "/circleci.conf";

        Settings(String name, String... resources) {
            this.name = name;
            this.resources = resources;
        }

        String[] variables() {
            ArrayNode selection = JSON.createArrayNode(), flow = JSON.createArrayNode();
            for (String r : resources) {
                selection.add(r);
            }
            for (String w : workflows) {
                flow.add(w);
            }
            return new String[] {
                "api.output=" + name,
                "api.resources=" + selection,
                "api.token=" + token,
                "api.certificate=" + certificate,
                "api.org=" + org,
                "api.org_slug=" + orgSlug,
                "api.project=" + project,
                "api.workflows=" + flow,
                "api.requests=" + requests,
                "api.jobs=" + jobs,
                "api.attempts=" + attempts,
                "api.timeout=" + timeout,
                "api.bytes=" + bytes,
                "api.job=" + job,
                "api.start=" + start,
                "api.end=" + end,
                "api.reporting=" + reporting
            };
        }
    }

    /**
     * Hosts normal packaged plugins and independent Java TLS fixtures in one owned worker.
     *
     * <p>DNS and certificate trust overrides apply only to this integration-test container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER = "/tmp/circleci-peer";
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
            container.withExtraHost(CircleCiPeer.HOST, "127.0.0.1");
            container.withExtraHost("circleci-trap.invalid", "127.0.0.1");
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
                stage(container, CircleCiPeer.class);
                stage(container, CircleCiRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java CircleCi fixture", failure);
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
                    CircleCiPeer.class.getName(),
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
                    CircleCiRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/circleci-" + name;
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

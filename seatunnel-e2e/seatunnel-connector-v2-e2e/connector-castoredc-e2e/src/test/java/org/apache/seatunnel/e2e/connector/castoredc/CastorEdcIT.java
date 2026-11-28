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
package org.apache.seatunnel.e2e.connector.castoredc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.castoredc.CastorEdcRecoveryProbe;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs CastorEdc through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class CastorEdcIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String TOKEN = "fixture-access-first";
    private static final String NEXT_TOKEN = "fixture-access-second";
    private static final String CLIENT_ID = "synthetic-client-id+&=";
    private static final String CLIENT_SECRET = "synthetic-client-secret+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "castoredc-peer";
    private static final String OTHER_ORIGIN = "castoredc-secondary";
    private static final String PEM = "/tmp/castoredc.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-castoredc.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String OAUTH = "/oauth/token";
    private static final String STUDY = "/api/study";
    private static final String USER = "/api/user";
    private static final String COUNTRY = "/api/country";
    private static final String STUDY_A = "00000000-0000-4000-A000-00000000000A";
    private static final String STUDY_B = "00000000-0000-4000-A000-00000000000B";
    private static final String START = "2025-01-02T00:00:00Z";
    private static final String END = "2025-01-04T00:00:00Z";
    private static final List<String> ALL =
            List.of(
                    "user",
                    "study",
                    "audit_trial",
                    "country",
                    "study_survey",
                    "study_survey_package",
                    "study_site",
                    "study_fields",
                    "study_field_dependency",
                    "study_field_validation",
                    "study_form",
                    "study_role",
                    "study_fieldoption_groups",
                    "study_statistics",
                    "study_user",
                    "study_visit");
    private final Engine engine = new Engine();
    private final ExecutorService submissions = Executors.newSingleThreadExecutor();
    private final HttpClient testControl =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private GenericContainer<?> peer;
    private Path certificate;

    /**
     * Provisions independent certificates, the protocol peer and the engine.
     *
     * <p>The unrelated certificate lets a negative test use a genuine trust failure with the
     * expected HTTPS endpoint still running.
     */
    @BeforeAll
    void boot(@TempDir Path testFiles) throws Exception {
        String runner = DockerClientFactory.instance().dockerHostIpAddress();
        assertTrue(runner.matches("[A-Za-z0-9.:-]+"));
        String runnerType = runner.matches("[0-9.]+") || runner.contains(":") ? "ip:" : "dns:";
        String create =
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass castoredc-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:castoredc-redirect-host,dns:castoredc-secondary,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass castoredc-fixture -keystore ";
        String entry =
                create
                        + "/tmp/provider.p12 && "
                        + export
                        + "/tmp/provider.p12 -file "
                        + PEM
                        + " && "
                        + create
                        + "/tmp/unrelated.p12 && "
                        + export
                        + "/tmp/unrelated.p12 -file "
                        + OTHER_PEM
                        + " && exec java --add-modules jdk.httpserver -cp /tmp "
                        + CastorEdcPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + CastorEdcPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = CastorEdcPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "castoredc-other-host", "castoredc-redirect-host")
                        .withExposedPorts(18080, 18443)
                        .withCopyToContainer(Transferable.of(bytecode), "/tmp" + providerClass)
                        .withCommand("sh", "-c", entry)
                        .waitingFor(
                                Wait.forHttp("/requests")
                                        .forPort(18080)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        peer.start();
        engine.startUp();
        String testedCertificate = copyCertificate(PEM);
        copyCertificate(OTHER_PEM);
        certificate = Files.writeString(testFiles.resolve("fixture.pem"), testedCertificate);
    }

    /**
     * Stops both owned containers and async submissions after the suite.
     *
     * <p>The provider is still released if teardown of Zeta reports an error.
     */
    @AfterAll
    void stop() throws Exception {
        submissions.shutdownNow();
        try {
            engine.tearDown();
        } finally {
            if (peer != null) {
                peer.stop();
            }
        }
    }

    @Test
    void persistsAllSixteenResourcesWithActualOAuthHalAndStudyWire() throws Exception {
        ObjectNode user = record(1);
        user.set("last_login", clock("2025-01-02 02:00:00.123456", "+02:00"));
        ObjectNode parent = study(STUDY_A, START).put("name", "研究 🧭");
        parent.put("large", new BigInteger("9223372036854775808"));
        ObjectNode event = options().put("event_type", "synthetic").putNull("event_details");
        event.set("datetime", clock("2025-01-02 01:00:00.000000", "UTC"));
        ObjectNode survey = record(7).put("description", "定义");
        survey.put("numeric_value", new BigDecimal("123456789012345678.123456789000"));
        survey.putObject("_links")
                .putObject("self")
                .put("href", "https://external.invalid/never-follow");
        ObjectNode role = options().put("name", "Investigator");
        ObjectNode stats =
                options().put("study_id", STUDY_A).set("records", options().put("count", 12));
        ObjectNode member = record(8).put("last_login", "2025-01-03T01:00:00+01:00");
        reset(
                oauth(TOKEN, 18000),
                ok(USER, rows(user)),
                ok(STUDY, hal("study", parent)),
                ok(child(STUDY_A, "audit-trail"), audit(1, event)),
                ok(COUNTRY, envelope("results", record(5)).put("totals", 1)),
                ok(child(STUDY_A, "survey"), hal("surveys", survey)),
                ok(child(STUDY_A, "survey-package"), hal("survey_packages", record(9))),
                ok(child(STUDY_A, "site"), hal("sites", record(10))),
                ok(child(STUDY_A, "field"), hal("fields", record(11))),
                ok(child(STUDY_A, "field-dependency"), hal("fieldDependencies", record(12))),
                ok(child(STUDY_A, "field-validation"), hal("fieldValidations", record(13))),
                ok(child(STUDY_A, "form"), hal("forms", record(14))),
                ok(child(STUDY_A, "role"), hal("roles", role).put("total_items", 1)),
                ok(child(STUDY_A, "field-optiongroup"), hal("fieldOptionGroups", record(15))),
                ok(child(STUDY_A, "statistics"), stats),
                ok(child(STUDY_A, "user"), rows(member)),
                ok(child(STUDY_A, "visit"), hal("visits", record(17))));
        try (Job job =
                job(
                        ALL,
                        options()
                                .put("start_date", START)
                                .put("end_date", END)
                                .put("audit_page_size", 2),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("user", "1", null, "2025-01-02T00:00:00.123456Z", user),
                            out("study", STUDY_A, STUDY_A, START, parent),
                            out(
                                    "audit_trial",
                                    fingerprintId(event),
                                    STUDY_A,
                                    "2025-01-02T01:00:00Z",
                                    event),
                            out("country", "5", null, null, record(5)),
                            out("study_survey", "7", STUDY_A, null, survey),
                            out("study_survey_package", "9", STUDY_A, null, record(9)),
                            out("study_site", "10", STUDY_A, null, record(10)),
                            out("study_fields", "11", STUDY_A, null, record(11)),
                            out("study_field_dependency", "12", STUDY_A, null, record(12)),
                            out("study_field_validation", "13", STUDY_A, null, record(13)),
                            out("study_form", "14", STUDY_A, null, record(14)),
                            out("study_role", "Investigator", STUDY_A, null, role),
                            out("study_fieldoption_groups", "15", STUDY_A, null, record(15)),
                            out("study_statistics", STUDY_A, STUDY_A, null, stats),
                            out("study_user", "8", STUDY_A, "2025-01-03T00:00:00Z", member),
                            out("study_visit", "17", STUDY_A, null, record(17))));
        }
        assertTrace(
                wire(OAUTH),
                wire(USER),
                wire(STUDY),
                auditWire(child(STUDY_A, "audit-trail"), 1),
                wire(COUNTRY),
                page(child(STUDY_A, "survey"), 1),
                page(child(STUDY_A, "survey-package"), 1),
                page(child(STUDY_A, "site"), 1),
                page(child(STUDY_A, "field"), 1),
                page(child(STUDY_A, "field-dependency"), 1),
                page(child(STUDY_A, "field-validation"), 1),
                page(child(STUDY_A, "form"), 1),
                wire(child(STUDY_A, "role")),
                page(child(STUDY_A, "field-optiongroup"), 1),
                wire(child(STUDY_A, "statistics")),
                wire(child(STUDY_A, "user")),
                page(child(STUDY_A, "visit"), 1));
        List<Seen> calls = seen();
        for (int i = 1; i < calls.size(); i++) {
            assertTrue(
                    TimeUnit.NANOSECONDS.toMillis(calls.get(i).at - calls.get(i - 1).at) >= 950,
                    "paced token and GET " + i);
        }
    }

    @Test
    void discoversOlderStudiesAndPaginatesPerParentWithoutDroppingEmptyPages() throws Exception {
        ObjectNode old = study(STUDY_A, "2024-01-01T00:00:00Z");
        ObjectNode current =
                options().put("crf_id", STUDY_B.toLowerCase(Locale.ROOT)).put("created_on", START);
        String visitA = child(STUDY_A, "visit");
        String visitB = child(STUDY_B, "visit");
        reset(
                oauth(TOKEN, 18000),
                ok(
                        STUDY,
                        next(
                                hal("study", old).put("total_items", 2),
                                ENDPOINT + STUDY + "?page=2")),
                ok(STUDY, next(hal("study"), "?page=3")),
                ok(STUDY, hal("study", current).put("total_items", 2)),
                ok(visitA, hal("visits").put("page", 1).put("page_count", 2)),
                ok(
                        visitA,
                        hal("visits", record(1))
                                .put("page", 2)
                                .put("page_count", 2)
                                .put("total_items", 1)),
                ok(visitB, hal("visits", record(1))));
        try (Job job =
                job(
                        List.of("study", "study_visit"),
                        options().put("start_date", START).put("end_date", END),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("study", STUDY_B, STUDY_B, START, current),
                            out("study_visit", "1", STUDY_A, null, record(1)),
                            out("study_visit", "1", STUDY_B, null, record(1))));
        }
        assertTrace(
                wire(OAUTH),
                wire(STUDY),
                page(STUDY, 2),
                page(STUDY, 3),
                page(visitA, 1),
                page(visitA, 2),
                page(visitB, 1));
    }

    @Test
    void respectsNullLoginsAndAcceptsAStudyUserHalContinuationWhenAvailable() throws Exception {
        String route = child(STUDY_A, "user");
        ObjectNode late = record(1).putNull("last_login");
        ObjectNode match = record(2);
        match.set("last_login", clock("2025-01-02 02:00:00.000000001", "Europe/Athens"));
        ObjectNode end = record(3).put("last_login", END);
        reset(
                oauth(TOKEN, 18000),
                ok(USER, hal("user", late)),
                ok(STUDY, hal("study", study(STUDY_A, "2020-01-01T00:00:00Z"))),
                ok(route, next(hal("studyUsers", late, end), "?page=2")),
                ok(route, rows(match)));
        try (Job job =
                job(
                        List.of("user", "study_user"),
                        options().put("start_date", START).put("end_date", END),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out(
                                    "study_user",
                                    "2",
                                    STUDY_A,
                                    "2025-01-02T00:00:00.000000001Z",
                                    match)));
        }
        assertTrace(wire(OAUTH), wire(USER), wire(STUDY), wire(route), page(route, 2));
    }

    @Test
    void preservesAuditTiesAcrossPagesAndLocalBoundsWithoutDroppingIdenticalEvents()
            throws Exception {
        String route = child(STUDY_A, "audit-trail");
        ObjectNode old =
                options().put("uuid", "old").put("datetime", "2025-01-01T23:59:59.999999Z");
        ObjectNode exact = options().put("uuid", "exact").put("datetime", START);
        ObjectNode identical = options().put("event_type", "synthetic").put("datetime", START);
        ObjectNode end = options().put("uuid", "end").put("datetime", "2025-01-04T02:00:00+02:00");
        reset(
                oauth(TOKEN, 18000),
                ok(STUDY, hal("study", study(STUDY_A, START))),
                ok(route, audit(3, end, exact)),
                ok(route, audit(3, old, identical)),
                ok(route, audit(3, identical)));
        try (Job job =
                job(
                        List.of("audit_trial"),
                        options()
                                .put("start_date", START)
                                .put("end_date", END)
                                .put("audit_page_size", 2),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            ObjectNode duplicate =
                    out("audit_trial", fingerprintId(identical), STUDY_A, START, identical);
            assertOutput(
                    job,
                    List.of(
                            out("audit_trial", "exact", STUDY_A, START, exact),
                            duplicate,
                            duplicate));
        }
        assertTrace(
                wire(OAUTH),
                wire(STUDY),
                auditWire(route, 1),
                auditWire(route, 2),
                auditWire(route, 3));
    }

    @Test
    void skipsStudyChildrenOnEmptyDiscoveryAndReadsTheIndependentCountryDocument()
            throws Exception {
        reset(
                oauth(TOKEN, 18000),
                ok(STUDY, hal("study").put("total_items", 0)),
                ok(COUNTRY, envelope("results", record(3)).put("totals", 1)));
        try (Job job =
                job(
                        List.of("study_site", "study_statistics", "country"),
                        options(),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("country", "3", null, null, record(3))));
        }
        assertTrace(wire(OAUTH), wire(STUDY), wire(COUNTRY));
    }

    @Test
    void isolatesCredentialBearingRequestsByOriginAndRejectsForeignLinks() throws Exception {
        reset(
                oauth(TOKEN, 18000),
                ok(COUNTRY, envelope("results", record(1))),
                oauth(NEXT_TOKEN, 18000, "secondary"),
                reply(COUNTRY, 200, "plain", "", envelope("results", record(2)), NEXT_TOKEN));
        for (boolean secondary : List.of(false, true)) {
            ObjectNode settings = options();
            if (secondary) {
                settings.put("endpoint", "https://" + OTHER_ORIGIN + ":18443")
                        .put("url_region", "au")
                        .put("auth_client_id", "secondary-test-id")
                        .put("auth_client_secret", "secondary-test-secret");
            }
            int id = secondary ? 2 : 1;
            try (Job job = job(List.of("country"), settings, false, false)) {
                good(engine.executeJob(job.configName));
                assertOutput(
                        job, List.of(out("country", Integer.toString(id), null, null, record(id))));
            }
        }
        assertTrace(wire(OAUTH), wire(COUNTRY), wire(OAUTH), wire(COUNTRY));
        assertEquals(
                List.of(
                        HOST + ":18443",
                        HOST + ":18443",
                        OTHER_ORIGIN + ":18443",
                        OTHER_ORIGIN + ":18443"),
                seen().stream().map(item -> item.host).collect(Collectors.toList()));
        rejectOne(
                ok(
                        STUDY,
                        next(
                                hal("study"),
                                "https://" + OTHER_ORIGIN + ":18443" + STUDY + "?page=2")),
                List.of("study"),
                options(),
                wire(STUDY),
                "HAL link leaves the requested API scope");
    }

    @Test
    void refreshesARejectedOrNearlyExpiredTokenWithoutReplayingCompletedRows() throws Exception {
        for (boolean unauthorized : List.of(false, true)) {
            ObjectNode first = record(1);
            ObjectNode second = record(2);
            String next = child(STUDY_A, "field");
            List<String> plan =
                    new ArrayList<>(
                            List.of(
                                    oauth(TOKEN, unauthorized ? 18000 : 2),
                                    reply(
                                            STUDY,
                                            200,
                                            unauthorized ? "hal" : "delay",
                                            "",
                                            hal("study", study(STUDY_A, START)))));
            if (unauthorized) {
                plan.add(reply(next, 401, "plain", "", options().put("detail", HIDDEN_MESSAGE)));
            }
            plan.add(oauth(NEXT_TOKEN, 18000));
            plan.add(
                    reply(next, 200, "hal", "", next(hal("fields", first), "?page=2"), NEXT_TOKEN));
            plan.add(reply(next, 200, "hal", "", hal("fields", second), NEXT_TOKEN));
            reset(plan.toArray(String[]::new));
            try (Job job = job(List.of("study_fields"), options(), false, false)) {
                good(engine.executeJob(job.configName));
                assertOutput(
                        job,
                        List.of(
                                out("study_fields", "1", STUDY_A, null, first),
                                out("study_fields", "2", STUDY_A, null, second)));
            }
            if (unauthorized) {
                assertTrace(
                        wire(OAUTH),
                        wire(STUDY),
                        page(next, 1),
                        wire(OAUTH),
                        page(next, 1),
                        page(next, 2));
            } else {
                assertTrace(wire(OAUTH), wire(STUDY), wire(OAUTH), page(next, 1), page(next, 2));
            }
        }
    }

    private static String fingerprintId(ObjectNode value) throws Exception {
        byte[] hash =
                MessageDigest.getInstance("SHA-256")
                        .digest(value.toString().getBytes(StandardCharsets.UTF_8));
        return "sha256:" + Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    }

    @Test
    void boundsRefreshAtOneAndStopsOnAnExplicitPermissionDenial() throws Exception {
        reset(
                oauth(TOKEN, 18000),
                reply(COUNTRY, 401, "plain", "", options()),
                oauth(NEXT_TOKEN, 18000),
                reply(COUNTRY, 401, "plain", "", options(), NEXT_TOKEN));
        failJob(List.of("country"), options(), "access denied (HTTP 401)");
        assertTrace(wire(OAUTH), wire(COUNTRY), wire(OAUTH), wire(COUNTRY));
        String child = child(STUDY_A, "field");
        reset(
                oauth(TOKEN, 18000),
                ok(STUDY, hal("study", study(STUDY_A, START), study(STUDY_B, START))),
                reply(child, 403, "plain", "", options().put("detail", HIDDEN_MESSAGE)));
        failJob(List.of("study_fields"), options(), "access denied (HTTP 403)");
        assertTrace(wire(OAUTH), wire(STUDY), page(child, 1));
    }

    @Test
    void treatsOAuthPostFailuresSeparatelyFromReplayableDataRequests() throws Exception {
        for (int status : List.of(400, 401, 503)) {
            reset(
                    reply(
                            OAUTH,
                            status,
                            "plain",
                            "0",
                            options().put("detail", HIDDEN_MESSAGE),
                            "primary"));
            failJob(
                    List.of("country"),
                    options(),
                    "OAuth token request failed (HTTP " + status + ")");
            assertTrace(wire(OAUTH));
        }
        reset(reply(OAUTH, 200, "truncated", "", options().put("access_token", TOKEN), "primary"));
        failJob(List.of("country"), options(), "OAuth token request outcome unknown");
        assertTrace(wire(OAUTH));
        for (ObjectNode invalid :
                List.of(
                        options().put("access_token", "contains space"),
                        options().put("access_token", TOKEN).put("token_type", "Basic"),
                        options().put("access_token", TOKEN).put("expires_in", 0))) {
            reset(reply(OAUTH, 200, "plain", "", invalid, "primary"));
            failJob(List.of("country"), options(), "OAuth token");
            assertTrace(wire(OAUTH));
        }
    }

    @Test
    void honorsRetryAfterAndTheDocumentedFallbackWithoutHidden503Replay() throws Exception {
        reset(
                oauth(TOKEN, 18000),
                reply(COUNTRY, 429, "plain", "2", options()),
                ok(COUNTRY, envelope("results")));
        try (Job job = job(List.of("country"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(OAUTH), wire(COUNTRY), wire(COUNTRY));
        List<Seen> attempts = seen();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(attempts.get(2).at - attempts.get(1).at) >= 1980);
        for (String retry : List.of("", "90")) {
            rejectOne(
                    reply(COUNTRY, 429, "plain", retry, options()),
                    List.of("country"),
                    options(),
                    wire(COUNTRY),
                    "retry delay exceeds request deadline");
        }
        rejectOne(
                reply(COUNTRY, 503, "plain", "0", options()),
                List.of("country"),
                options().put("max_attempts", 1),
                wire(COUNTRY),
                "HTTP 503 retry budget exhausted");
        reset(
                reply(OAUTH, 429, "plain", "2", options(), "primary"),
                oauth(TOKEN, 18000),
                ok(COUNTRY, envelope("results")));
        try (Job job = job(List.of("country"), options(), false, false)) {
            good(engine.executeJob(job.configName));
        }
        assertTrace(wire(OAUTH), wire(OAUTH), wire(COUNTRY));
        attempts = seen();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(attempts.get(1).at - attempts.get(0).at) >= 1980);
    }

    @Test
    void rejectsUnprovenPaginationTotalsConflictingCountsAndForeignParentTargets()
            throws Exception {
        for (String link :
                List.of(
                        "?page=1",
                        "?page=3",
                        "?page=2&page=2",
                        "?page=2&page_size=99",
                        "../country?page=2")) {
            rejectOne(
                    ok(STUDY, next(hal("study", study(STUDY_A, START)), link)),
                    List.of("study"),
                    options(),
                    wire(STUDY),
                    "HAL link");
        }
        rejectOne(
                ok(STUDY, hal("study", study(STUDY_A, START)).put("total_items", 2)),
                List.of("study"),
                options(),
                wire(STUDY),
                "record count disagrees");
        rejectOne(
                ok(STUDY, hal("study").put("page_count", 2)),
                List.of("study"),
                options(),
                wire(STUDY),
                "without a supported continuation");
        String route = child(STUDY_A, "visit");
        reset(
                oauth(TOKEN, 18000),
                ok(STUDY, hal("study", study(STUDY_A, START))),
                ok(route, next(hal("visits"), child(STUDY_B, "visit") + "?page=2")));
        failJob(List.of("study_visit"), options(), "HAL link leaves the requested API scope");
        assertTrace(wire(OAUTH), wire(STUDY), page(route, 1));
        reset(
                oauth(TOKEN, 18000),
                ok(STUDY, hal("study", study(STUDY_A, START))),
                ok(route, next(hal("visits").put("page_count", 1), "?page=2")));
        failJob(List.of("study_visit"), options(), "page count disagrees");
        assertTrace(wire(OAUTH), wire(STUDY), page(route, 1));
        reset(
                oauth(TOKEN, 18000),
                ok(STUDY, hal("study", study(STUDY_A, START))),
                ok(route, hal("visits", record(1)).put("page_size", 1)));
        failJob(List.of("study_visit"), options(), "full page lacks terminal metadata");
        assertTrace(wire(OAUTH), wire(STUDY), page(route, 1));
    }

    @Test
    void boundsStudyFanoutAndPagesAndRejectsForeignStatistics() throws Exception {
        reset(
                oauth(TOKEN, 18000),
                ok(STUDY, hal("study", study(STUDY_A, START), study(STUDY_B, START))));
        failJob(List.of("study_visit"), options().put("max_studies", 1), "max_studies");
        assertTrace(wire(OAUTH), wire(STUDY));
        reset(oauth(TOKEN, 18000), ok(STUDY, next(hal("study"), "?page=2")));
        failJob(List.of("study"), options().put("max_requests", 1), "max_requests exhausted");
        assertTrace(wire(OAUTH), wire(STUDY));
        String route = child(STUDY_A, "statistics");
        reset(
                oauth(TOKEN, 18000),
                ok(STUDY, hal("study", study(STUDY_A, START))),
                ok(route, options().put("study_id", STUDY_B)));
        failJob(List.of("study_statistics"), options(), "record belongs to a different study");
        assertTrace(wire(OAUTH), wire(STUDY), wire(route));
    }

    @Test
    void validatesDatesRegionStudyIdentifiersAndTrustBeforeUnsafeRequests() throws Exception {
        reset();
        failJob(ALL, options(), "is required for audit_trial");
        failJob(
                List.of("study"),
                options().put("url_region", "au.castoredc.com"),
                "expected uk, eu, nl, us or au");
        failJob(
                List.of("study"),
                options().put("auth_client_secret", "bad value"),
                "credential without whitespace");
        failJob(
                List.of("study"),
                options().put("start_date", "2025-01-02"),
                "timestamp with a UTC offset");
        failJob(
                List.of("study"),
                options().put("start_date", END).put("end_date", START),
                "must be later than start_date");
        failJob(
                List.of("study"),
                options().put("legacy_timezone", "not-a-zone"),
                "expected an IANA zone");
        failJob(List.of("study"), options().put("request_interval_ms", 999), "request_interval_ms");
        failJob(
                List.of("study"),
                options().put("endpoint", "http://castoredc-peer:18443"),
                "expected a bare HTTPS origin");
        assertTrace();
        rejectOne(
                ok(STUDY, hal("study", study("bad/id", START))),
                List.of("study_visit"),
                options(),
                wire(STUDY),
                "expected a UUID study identifier");
        rejectOne(
                ok(STUDY, hal("study", study(STUDY_A, START).put("crf_id", STUDY_B))),
                List.of("study_visit"),
                options(),
                wire(STUDY),
                "conflicting study identifier");
        rejectOne(
                ok(USER, rows(record(1).put("last_login", "2025-11-02 01:30:00"))),
                List.of("user"),
                options().put("legacy_timezone", "America/New_York"),
                wire(USER),
                "ambiguous in its time zone");
    }

    @Test
    void validatesDecodedResponseMimeUtf8EnvelopeAndDuplicateContent() throws Exception {
        ObjectNode value = record(1).put("name", "苏");
        reset(oauth(TOKEN, 18000), reply(COUNTRY, 200, "gzip", "", envelope("results", value)));
        try (Job job = job(List.of("country"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("country", "1", null, null, value)));
        }
        assertTrace(wire(OAUTH), wire(COUNTRY));
        rejectOne(
                reply(
                        COUNTRY,
                        200,
                        "gzip",
                        "",
                        envelope("results", record(1).put("name", "x".repeat(8192)))),
                List.of("country"),
                options().put("max_response_bytes", 1024),
                wire(COUNTRY),
                "response exceeds max_response_bytes");
        rejectOne(
                reply(COUNTRY, 200, "unsupported", "", envelope("results", value)),
                List.of("country"),
                options(),
                wire(COUNTRY),
                "unsupported response compression");
        rejectOne(
                reply(COUNTRY, 200, "binary", "", envelope("results", value)),
                List.of("country"),
                options(),
                wire(COUNTRY),
                "expected a UTF-8 JSON or HAL JSON Content-Type");
        rejectOne(
                frame(COUNTRY, 200, "plain", "", new byte[] {'{', (byte) 0xFF, '}'}, TOKEN),
                List.of("country"),
                options(),
                wire(COUNTRY),
                "single valid UTF-8 JSON document");
        rejectOne(
                ok(COUNTRY, envelope("results", value, value)),
                List.of("country"),
                options(),
                wire(COUNTRY),
                "duplicate provider id");
        rejectOne(
                ok(COUNTRY, envelope("results", value, rows())),
                List.of("country"),
                options(),
                wire(COUNTRY),
                "a resource record must be a JSON object");
    }

    @Test
    void retriesAReadThatClosesEarlyWithoutPublishingUnvalidatedData() throws Exception {
        reset(
                oauth(TOKEN, 18000),
                reply(COUNTRY, 200, "truncated", "", envelope("results", record(1))),
                ok(COUNTRY, envelope("results", record(2))));
        try (Job job = job(List.of("country"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("country", "2", null, null, record(2))));
        }
        assertTrace(wire(OAUTH), wire(COUNTRY), wire(COUNTRY));
    }

    @Test
    void verifiesBothTrustAndHostnameAndNeverRedirectsCredentials() throws Exception {
        reset();
        failJob(
                List.of("country"),
                options().put("trust_certificate", OTHER_PEM),
                "TLS certificate or hostname");
        failJob(
                List.of("country"),
                options().put("endpoint", "https://castoredc-other-host:18443"),
                "TLS certificate or hostname");
        assertTrace();
        reset(reply(OAUTH, 302, "redirect", "", options(), "primary"));
        failJob(List.of("country"), options(), "OAuth token request failed (HTTP 302)");
        assertTrace(wire(OAUTH));
        rejectOne(
                reply(COUNTRY, 302, "redirect", "", options()),
                List.of("country"),
                options(),
                wire(COUNTRY),
                "HTTP 302");
    }

    @Test
    void boundsAnActiveGetAndCancelsWithoutReachingOtherStudies() throws Exception {
        rejectOne(
                stall(COUNTRY),
                List.of("country"),
                options().put("request_timeout_ms", 4000),
                wire(COUNTRY),
                "request deadline exceeded");
        await(() -> trace().contains("disconnect\t" + COUNTRY), "deadline closed provider request");
        String route = child(STUDY_A, "visit");
        reset(
                oauth(TOKEN, 18000),
                ok(STUDY, hal("study", study(STUDY_A, START), study(STUDY_B, START))),
                stall(route));
        try (Job job =
                job(
                        List.of("study_visit"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 3, "active study visit before cancellation");
            } finally {
                cancel(job.id, running);
            }
            await(() -> trace().contains("disconnect\t" + route), "cancel closed visit");
            assertOutput(job, List.of());
        }
        assertTrace(wire(OAUTH), wire(STUDY), page(route, 1));
    }

    @Test
    void restoresTheCurrentStudyPageWithoutRepeatingEarlierPagesOrTokenState() throws Exception {
        String first = child(STUDY_A, "visit");
        String second = child(STUDY_B, "visit");
        ObjectNode a = study(STUDY_A, START);
        ObjectNode b = study(STUDY_B, START);
        List<ObjectNode> committed =
                List.of(
                        out("study", STUDY_A, STUDY_A, START, a),
                        out("study", STUDY_B, STUDY_B, START, b),
                        out("study_visit", "6", STUDY_A, null, record(6)));
        reset(
                oauth(TOKEN, 18000),
                ok(STUDY, next(hal("study", a), "?page=2")),
                ok(STUDY, hal("study", b)),
                ok(first, next(hal("visits", record(6)), "?page=2")),
                stall(first));
        try (Job job =
                job(
                        List.of("study", "study_visit"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            saveActive(job, 5, committed, first);
            queue(
                    oauth(NEXT_TOKEN, 18000),
                    reply(first, 200, "hal", "", hal("visits"), NEXT_TOKEN),
                    reply(second, 200, "hal", "", hal("visits", record(7)), NEXT_TOKEN));
            good(engine.restoreJob(job.configName, job.id));
            List<ObjectNode> expected = new ArrayList<>(committed);
            expected.add(out("study_visit", "7", STUDY_B, null, record(7)));
            assertOutput(job, expected);
        }
        assertTrace(
                wire(OAUTH),
                wire(STUDY),
                page(STUDY, 2),
                page(first, 1),
                page(first, 2),
                wire(OAUTH),
                page(first, 2),
                page(second, 1));
    }

    @Test
    void refusesChangedRegionalScopeBeforeRestoredNetworkAndKeepsTerminalRestoresTerminal()
            throws Exception {
        reset(oauth(TOKEN, 18000), stall(USER));
        try (Job original =
                        job(
                                List.of("user"),
                                options().put("request_timeout_ms", 60000),
                                true,
                                false);
                Job changed =
                        job(List.of("user"), options().put("url_region", "au"), true, false)) {
            saveActive(original, 2, List.of(), USER);
            bad(engine.restoreJob(changed.configName, original.id), "read scope changed");
        }
        assertTrace(wire(OAUTH), wire(USER));
        ObjectNode expected = out("country", "3", null, null, record(3));
        reset(oauth(TOKEN, 18000), ok(COUNTRY, envelope("results", record(3))));
        try (Job job = job(List.of("country"), options(), true, true)) {
            saveActive(job, 2, List.of(expected), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(job.id)),
                        "active restored anchor");
                long before = engine.checkpointCount(job.id);
                await(() -> engine.checkpointCount(job.id) > before, "restored anchor checkpoint");
                assertOutput(job, List.of(expected));
                assertTrace(wire(OAUTH), wire(COUNTRY));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void serializesTheUnpublishedSuffixAndRejectsChangesEvenToFilteredRecords() throws Exception {
        ArrayNode data = rows(record(100).putNull("last_login"));
        List<String> ids = new ArrayList<>();
        for (int i = 1; i <= 33; i++) {
            data.add(record(i).put("last_login", START));
            ids.add(Integer.toString(i));
        }
        ArrayNode changed = data.deepCopy();
        ((ObjectNode) changed.get(0)).put("name", "changed but filtered");
        ArrayNode reordered = data.deepCopy();
        reordered.set(1, data.get(2));
        reordered.set(2, data.get(1));
        reset(
                oauth(TOKEN, 18000),
                ok(USER, data),
                oauth(TOKEN, 18000),
                ok(USER, data),
                oauth(TOKEN, 18000),
                ok(USER, changed),
                oauth(TOKEN, 18000),
                ok(USER, reordered));
        CastorEdcRecoveryProbe probe = recovery(List.of("user"));
        List<String> initial = new ArrayList<>();
        byte[] checkpoint = probe.firstChunk(initial);
        assertEquals(ids.subList(0, 32), initial);
        String serialized = new String(checkpoint, StandardCharsets.ISO_8859_1);
        assertFalse(
                serialized.contains(TOKEN)
                        || serialized.contains(CLIENT_SECRET)
                        || serialized.contains("last_login"));
        List<String> suffix = new ArrayList<>();
        probe.consume(checkpoint, suffix);
        assertEquals(ids.subList(32, 33), suffix);
        for (int i = 0; i < 2; i++) {
            List<String> invalid = new ArrayList<>();
            Exception failure =
                    assertThrows(Exception.class, () -> probe.consume(checkpoint, invalid));
            assertTrue(failure.getMessage().contains("provider page changed since checkpoint"));
            assertTrue(invalid.isEmpty());
        }
        assertTrace(
                wire(OAUTH),
                wire(USER),
                wire(OAUTH),
                wire(USER),
                wire(OAUTH),
                wire(USER),
                wire(OAUTH),
                wire(USER));
    }

    @Test
    void restoresPendingAssignmentAndFinishesEmptyOrCancelledReadersWithoutTransport()
            throws Exception {
        reset();
        CastorEdcRecoveryProbe probe = recovery(List.of("country"));
        byte[] assigned = probe.recoverUnassigned();
        probe.finishWithoutAssignment();
        List<String> result = new ArrayList<>();
        probe.stopBeforeAssignment(result);
        assertTrue(result.isEmpty());
        assertTrace();
        queue(oauth(TOKEN, 18000), ok(COUNTRY, envelope("results", record(2))));
        probe.consume(assigned, result);
        assertEquals(List.of("2"), result);
        assertTrace(wire(OAUTH), wire(COUNTRY));
    }

    private void rejectOne(
            String scripted,
            List<String> resources,
            ObjectNode settings,
            Seen expected,
            String diagnostic)
            throws Exception {
        reset(oauth(TOKEN, 18000), scripted);
        try (Job job = job(resources, settings, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
            assertOutput(job, List.of());
        }
        assertTrace(wire(OAUTH), expected);
    }

    private void failJob(List<String> resources, ObjectNode values, String diagnostic)
            throws Exception {
        try (Job job = job(resources, values, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
        }
    }

    private void saveActive(Job job, int calls, List<ObjectNode> committed, String stalledPath)
            throws Exception {
        Future<Container.ExecResult> running = start(job, job.id);
        try {
            await(() -> seen().size() == calls, "expected provider request");
            awaitOutput(job, committed);
            good(engine.savepointJob(job.id));
            running.get(45, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                cancel(job.id, running);
            }
        }
        if (stalledPath != null) {
            await(() -> trace().contains("disconnect\t" + stalledPath), "savepoint closed request");
        }
    }

    private CastorEdcRecoveryProbe recovery(List<String> resources) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new CastorEdcRecoveryProbe(
                url.toString(), certificate.toString(), CLIENT_ID, CLIENT_SECRET, resources);
    }

    private Future<Container.ExecResult> start(Job job, String id) {
        return submissions.submit(() -> engine.start(job.configName, id));
    }

    private void cancel(String id, Future<Container.ExecResult> running) throws Exception {
        try {
            if (!running.isDone()) {
                engine.stopJob(id);
            }
            running.get(45, TimeUnit.SECONDS);
        } finally {
            if (!running.isDone()) {
                running.cancel(true);
            }
        }
    }

    private static String id() {
        return Long.toString(UUID.randomUUID().getLeastSignificantBits() & Long.MAX_VALUE);
    }

    private String copyCertificate(String path) throws Exception {
        Container.ExecResult certificate = peer.execInContainer("cat", path);
        assertEquals(0, certificate.getExitCode(), certificate.getStderr());
        engine.putCertificate(path, certificate.getStdout());
        return certificate.getStdout();
    }

    private static Job job(
            List<String> selected, ObjectNode overrides, boolean stream, boolean anchor)
            throws Exception {
        String name = "castoredc-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "castoredc_data")
                        .put("auth_client_id", CLIENT_ID)
                        .put("auth_client_secret", CLIENT_SECRET)
                        .put("endpoint", ENDPOINT)
                        .put("trust_certificate", PEM);
        ArrayNode resources = settings.putArray("resources");
        selected.forEach(resources::add);
        settings.setAll(overrides);
        String anchorInput =
                anchor
                        ? " FakeSource {\n plugin_output=anchor\n row.num=1000\n split.num=1000\n"
                                + " split.read-interval=250\n schema.fields.name=\"string\"\n}\n"
                        : "";
        String anchorOutput = anchor ? " Console { plugin_input=anchor }\n" : "";
        String contents =
                "env {\n job.mode="
                        + (stream ? "STREAMING" : "BATCH")
                        + "\n parallelism=1\n checkpoint.interval=350\n}\n"
                        + "source {\n CastorEdc "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=castoredc_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"castoredc_$"
                        + "{transactionId}\"\n}\n"
                        + anchorOutput
                        + "}\n";
        Path directory = ContainerUtil.getCurrentModulePath().resolve("src/test/resources");
        Files.createDirectories(directory);
        return new Job(
                Files.writeString(
                        directory.resolve(name + ".conf"), contents, StandardCharsets.UTF_8),
                path);
    }

    private static ObjectNode options() {
        return JSON.createObjectNode();
    }

    private static ObjectNode record(int id) {
        return options().put("id", id);
    }

    private static ObjectNode study(String id, String created) {
        return options().put("study_id", id).put("created_on", created);
    }

    private static String child(String id, String resource) {
        return STUDY + "/" + id + "/" + resource;
    }

    private static ObjectNode hal(String field, JsonNode... records) {
        return object("_embedded", envelope(field, records));
    }

    private static ObjectNode next(ObjectNode response, String link) {
        response.putObject("_links").putObject("next").put("href", link);
        return response;
    }

    private static ObjectNode audit(int totalPages, JsonNode... records) {
        ObjectNode payload = envelope("items", records);
        payload.putObject("extra").put("total_pages", totalPages);
        return payload;
    }

    private static ObjectNode clock(String date, String zone) {
        return options().put("date", date).put("timezone", zone);
    }

    private static ObjectNode object(String field, JsonNode value) {
        return options().set(field, value);
    }

    private static ObjectNode envelope(String field, JsonNode... records) {
        return object(field, rows(records));
    }

    private static ArrayNode rows(JsonNode... records) {
        ArrayNode array = JSON.createArrayNode();
        for (JsonNode record : records) {
            array.add(record);
        }
        return array;
    }

    private static ObjectNode out(
            String resource, String id, String study, String time, ObjectNode payload) {
        ObjectNode row =
                options()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("study_id", study)
                        .put("record_time", time);
        row.set("payload", payload);
        return row;
    }

    private static String oauth(String token, int expiresIn) {
        return oauth(token, expiresIn, "primary");
    }

    private static String oauth(String token, int expiresIn, String slot) {
        return reply(
                OAUTH,
                200,
                "plain",
                "",
                options()
                        .put("access_token", token)
                        .put("token_type", "Bearer")
                        .put("expires_in", expiresIn),
                slot);
    }

    private static String reply(String path, int status, String mode, String retry, JsonNode body) {
        return reply(path, status, mode, retry, body, TOKEN);
    }

    private static String reply(
            String path,
            int status,
            String mode,
            String retry,
            JsonNode body,
            String expectedCredential) {
        return frame(
                path,
                status,
                mode,
                retry,
                body.toString().getBytes(StandardCharsets.UTF_8),
                expectedCredential);
    }

    private static String frame(
            String path,
            int status,
            String mode,
            String retry,
            byte[] bytes,
            String expectedCredential) {
        return String.join(
                "\t",
                Integer.toString(status),
                mode,
                retry,
                Base64.getEncoder().encodeToString(bytes),
                path,
                expectedCredential);
    }

    private static String ok(String path, JsonNode body) {
        return reply(path, 200, "hal", "", body);
    }

    private static String stall(String path) {
        return reply(path, 200, "chunked", "", rows());
    }

    private void reset(String... replies) throws Exception {
        prepare("/reset", replies);
    }

    private void queue(String... replies) throws Exception {
        prepare("/queue", replies);
    }

    private URI fixture(String path) throws Exception {
        return new URI("http", null, peer.getHost(), peer.getMappedPort(18080), path, null, null);
    }

    private void prepare(String path, String[] replies) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(fixture(path))
                        .timeout(Duration.ofSeconds(5))
                        .POST(HttpRequest.BodyPublishers.ofString(String.join("\n", replies)))
                        .build();
        assertEquals(
                200,
                testControl.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
    }

    private String trace() throws Exception {
        HttpRequest get =
                HttpRequest.newBuilder(fixture("/requests")).timeout(Duration.ofSeconds(5)).build();
        HttpResponse<String> result = testControl.send(get, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, result.statusCode());
        return result.body();
    }

    private List<Seen> seen() throws Exception {
        List<Seen> requests = new ArrayList<>();
        for (String frame : trace().split("\n")) {
            if (!frame.startsWith("call\t")) {
                continue;
            }
            String[] parts = frame.split("\t", -1);
            assertEquals(
                    OAUTH.equals(URI.create(parts[2]).getRawPath()) ? "POST" : "GET", parts[1]);
            assertEquals("true", parts[4], "expected scoped Bearer or OAuth form");
            assertEquals("true", parts[5], "the connector must ask for JSON");
            URI request = URI.create(parts[2]);
            assertEquals("true", parts[6], "expected the connector application User-Agent");
            Map<String, String> query = new TreeMap<>();
            if (request.getRawQuery() != null) {
                for (String param : request.getRawQuery().split("&")) {
                    String[] pair = param.split("=", 2);
                    assertEquals(2, pair.length);
                    assertNull(
                            query.put(pair[0], URLDecoder.decode(pair[1], StandardCharsets.UTF_8)));
                }
            }
            requests.add(new Seen(request.getRawPath(), query, Long.parseLong(parts[3]), parts[7]));
        }
        return requests;
    }

    private static Seen wire(String path) {
        return new Seen(path, Map.of(), 0L);
    }

    private static Seen page(String path, int page) {
        return new Seen(path, Map.of("page", Integer.toString(page)), 0L);
    }

    private static Seen auditWire(String path, int page) {
        return new Seen(
                path,
                Map.of(
                        "page",
                        Integer.toString(page),
                        "date_from",
                        "2025-01-01",
                        "date_to",
                        "2025-01-05",
                        "page_size",
                        "2"),
                0L);
    }

    private void assertTrace(Seen... expected) throws Exception {
        List<Seen> actual = seen();
        assertEquals(
                expected.length,
                actual.size(),
                () ->
                        actual.stream()
                                .map(item -> item.route + item.query)
                                .collect(Collectors.joining(", ")));
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i].route, actual.get(i).route, "route at index " + i);
            assertEquals(expected[i].query, actual.get(i).query, "query at index " + i);
        }
    }

    private void assertOutput(Job job, List<ObjectNode> expected) throws Exception {
        assertEquals(frequencies(expected), frequencies(engine.outputs(job.path)));
    }

    private void awaitOutput(Job job, List<ObjectNode> expected) throws Exception {
        await(
                () -> frequencies(engine.outputs(job.path)).equals(frequencies(expected)),
                "committed local output");
    }

    private static Map<ObjectNode, Long> frequencies(List<ObjectNode> values) {
        return values.stream()
                .collect(Collectors.groupingBy(value -> value, Collectors.counting()));
    }

    private static void good(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> result.getStdout() + result.getStderr());
    }

    private void bad(Container.ExecResult result, String diagnostic) {
        assertNotEquals(0, result.getExitCode(), diagnostic);
        String visible = result.getStdout() + result.getStderr() + engine.getServerLogs();
        assertTrue(visible.contains(diagnostic), "Missing diagnostic " + diagnostic);
        assertFalse(
                visible.contains(TOKEN)
                        || visible.contains(NEXT_TOKEN)
                        || visible.contains(CLIENT_ID)
                        || visible.contains(CLIENT_SECRET),
                "credentials leaked");
        assertFalse(visible.contains(HIDDEN_MESSAGE), "provider error text leaked");
    }

    private static void await(Probe check, String purpose) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        do {
            if (check.ready()) {
                return;
            }
            Thread.sleep(100L);
        } while (System.nanoTime() < until);
        throw new AssertionError("Timed out waiting for " + purpose);
    }

    /**
     * An observed wire request with a provider-side timestamp.
     *
     * <p>The timing is comparable within this fixture process and never uses wall-clock time.
     */
    private static final class Seen {
        private final String route;
        private final Map<String, String> query;
        private final long at;

        private final String host;

        private Seen(String route, Map<String, String> query, long at) {
            this(route, query, at, "");
        }

        private Seen(String route, Map<String, String> query, long at, String host) {
            this.route = route;
            this.query = query;
            this.at = at;
            this.host = host;
        }
    }

    /**
     * Files for a job submitted via the regular Zeta CLI.
     *
     * <p>The same output folder survives restoration so tests can detect duplicated writes.
     */
    private static final class Job implements AutoCloseable {
        private final Path config;
        private final String configName;
        private final String path;
        private final String id = CastorEdcIT.id();

        private Job(Path config, String path) {
            this.config = config;
            this.configName = "/" + config.getFileName();
            this.path = path;
        }

        @Override
        public void close() throws Exception {
            Files.deleteIfExists(config);
        }
    }

    /**
     * Checked condition against the active fixture or engine.
     *
     * <p>Unexpected read failures are exposed immediately rather than hidden behind a wait timeout.
     */
    @FunctionalInterface
    private interface Probe {
        boolean ready() throws Exception;
    }

    /**
     * Exposes the committed LocalFile results and live checkpoint counters for this suite.
     *
     * <p>Fixture tests use the actual starter, LocalFile committer and Zeta REST status.
     */
    private static final class Engine extends SeaTunnelContainer {
        private final HttpClient rest = HttpClient.newHttpClient();

        private void putCertificate(String path, String contents) {
            server.copyFileToContainer(Transferable.of(contents), path);
        }

        private Container.ExecResult start(String config, String id) throws Exception {
            return executeJob(server, config, id, List.of());
        }

        private List<ObjectNode> outputs(String directory) throws Exception {
            Container.ExecResult files =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d '"
                                    + directory
                                    + "' ]; then find '"
                                    + directory
                                    + "' -maxdepth 1 -type f -name 'castoredc_*' -exec cat {} +; fi");
            assertEquals(0, files.getExitCode(), files.getStderr());
            List<ObjectNode> output = new ArrayList<>();
            for (String line : files.getStdout().split("\\R")) {
                if (!line.isBlank()) {
                    ObjectNode row = (ObjectNode) JSON.readTree(line);
                    assertTrue(row.path("payload").isTextual());
                    row.set("payload", JSON.readTree(row.path("payload").textValue()));
                    // LocalFile can omit null columns. Compare the logical fixed schema.
                    for (String nullable : List.of("study_id", "record_time")) {
                        if (!row.has(nullable)) {
                            row.putNull(nullable);
                        }
                    }
                    output.add(row);
                }
            }
            return output;
        }

        private long checkpointCount(String id) throws Exception {
            URI address =
                    new URI(
                            "http",
                            null,
                            server.getHost(),
                            server.getMappedPort(8080),
                            "/jobs/checkpoints/" + id,
                            null,
                            null);
            HttpRequest query =
                    HttpRequest.newBuilder(address).timeout(Duration.ofSeconds(5)).build();
            HttpResponse<String> response = rest.send(query, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return 0;
            }
            // The CastorEdc pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}

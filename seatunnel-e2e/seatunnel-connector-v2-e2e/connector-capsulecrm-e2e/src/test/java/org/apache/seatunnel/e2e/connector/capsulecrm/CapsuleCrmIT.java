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
package org.apache.seatunnel.e2e.connector.capsulecrm;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.capsulecrm.CapsuleCrmRecoveryProbe;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
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
 * Runs CapsuleCrm through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class CapsuleCrmIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String TOKEN = "capsulecrm-fixture-private+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "capsulecrm-peer";
    private static final String PEM = "/tmp/capsulecrm.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-capsulecrm.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String ROOT = "/api/v2/";
    private static final String PARTIES = ROOT + "parties";
    private static final String USERS = ROOT + "users";
    private static final String SITE = ROOT + "site";
    private static final String TASKS = ROOT + "tasks";
    private static final String OPPORTUNITIES = ROOT + "opportunities";
    private static final String START = "2025-01-02T02:00:00.123456789+02:00";
    private static final String SINCE = "2025-01-01T23:59:59Z";
    private static final String END = "2025-01-03T00:00:00Z";
    private static final List<String> ALL =
            List.of(
                    "users",
                    "parties",
                    "tasks",
                    "employees",
                    "projects",
                    "opportunities",
                    "pipelines",
                    "milestones",
                    "site",
                    "tags",
                    "custom_fields",
                    "lost_reasons",
                    "board",
                    "categories",
                    "activity_types",
                    "stages");
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
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass capsulecrm-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:capsulecrm-redirect-host,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass capsulecrm-fixture -keystore ";
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
                        + CapsuleCrmPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + CapsuleCrmPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = CapsuleCrmPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(
                                HOST, "capsulecrm-other-host", "capsulecrm-redirect-host")
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
    void readsEveryResourceThroughTheOfficialEnvelopeAndScope() throws Exception {
        ObjectNode company = organisation(14).put("name", "星空 🧭").putNull("note");
        company.put("largeValue", new BigInteger("9223372036854775808"));
        ObjectNode another = organisation(15);
        ObjectNode individual = person(16);
        ObjectNode employeeA = person(70).putNull("organisation");
        ObjectNode employeeB = person(70);
        employeeB.putObject("organisation").put("id", 15);
        ObjectNode opp = record(3).put("updatedAt", "2025-01-02T00:00:00Z");
        opp.put("value", new BigDecimal("123456789012345678.123456789000"));
        opp.putArray("nested").addObject().putNull("extra");
        ObjectNode user = record(1).put("pictureURL", "https://other.invalid/never-fetch");
        ObjectNode site =
                options().put("subdomain", "test-org").put("url", "https://test-org.invalid");
        ObjectNode note = options().put("id", "NOTE").put("name", "Note");
        ObjectNode builtin = record(-1).put("name", "Legacy");
        ObjectNode stage = record(17);
        stage.putObject("board").put("id", 13);

        reset(
                ok(USERS, envelope("users", user)),
                ok(PARTIES, envelope("parties", company, another, individual)),
                ok(TASKS, envelope("tasks", record(2))),
                ok(employee(14), envelope("parties", employeeA)),
                ok(employee(15), envelope("parties", employeeB)),
                ok(ROOT + "kases", envelope("kases", record(4))),
                ok(OPPORTUNITIES, envelope("opportunities", opp)),
                ok(ROOT + "pipelines", envelope("pipelines", record(5))),
                ok(ROOT + "milestones", envelope("milestones", record(6))),
                ok(SITE, object("site", site)),
                ok(ROOT + "kases/tags", envelope("tags", record(10))),
                ok(ROOT + "kases/fields/definitions", envelope("definitions", record(11))),
                ok(ROOT + "lostreasons", envelope("lostReasons", record(12))),
                ok(ROOT + "boards", envelope("boards", record(13))),
                ok(ROOT + "categories", envelope("categories", record(18))),
                ok(ROOT + "activitytypes", envelope("activityTypes", note, builtin)),
                ok(ROOT + "stages", envelope("stages", stage)));
        try (Job job = job(ALL, options().put("entity", "kases"), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("users", "1", null, user), out("parties", "14", null, company),
                            out("parties", "15", null, another),
                                    out("parties", "16", null, individual),
                            out("tasks", "2", null, record(2)),
                                    out("employees", "70", "14", employeeA),
                            out("employees", "70", "15", employeeB),
                                    out("projects", "4", null, record(4)),
                            out("opportunities", "3", null, opp),
                                    out("pipelines", "5", null, record(5)),
                            out("milestones", "6", null, record(6)),
                                    out("site", "test-org", null, site),
                            out("tags", "10", "kases", record(10)),
                                    out("custom_fields", "11", "kases", record(11)),
                            out("lost_reasons", "12", null, record(12)),
                                    out("board", "13", null, record(13)),
                            out("categories", "18", null, record(18)),
                                    out("activity_types", "NOTE", null, note),
                            out("activity_types", "-1", null, builtin),
                                    out("stages", "17", null, stage)));
        }
        assertTrace(
                wire(USERS),
                wire(PARTIES, 100, 1),
                wire(TASKS, 100, 1),
                wire(employee(14), 100, 1),
                wire(employee(15), 100, 1),
                wire(ROOT + "kases", 100, 1),
                wire(OPPORTUNITIES, 100, 1),
                wire(ROOT + "pipelines", 100, 1),
                wire(ROOT + "milestones", 100, 1),
                wire(SITE),
                wire(ROOT + "kases/tags", 100, 1),
                wire(ROOT + "kases/fields/definitions", 100, 1),
                wire(ROOT + "lostreasons"),
                wire(ROOT + "boards", 100, 1),
                wire(ROOT + "categories", 100, 1),
                wire(ROOT + "activitytypes", 100, 1),
                wire(ROOT + "stages", 100, 1));
    }

    @Test
    void followsScopedLinksThroughEmptyPagesAndPreservesLaterOrganisationChildren()
            throws Exception {
        ObjectNode worker = person(98);
        reset(
                ok(
                        PARTIES,
                        envelope("parties", organisation(10)),
                        "<"
                                + ENDPOINT
                                + PARTIES
                                + "?page=2>; title=\"comma, next is a label\"; rel=\"next alternate\"",
                        "<" + PARTIES + "?page=1>; rel=prev"),
                ok(PARTIES, envelope("parties"), "<?page=3>; rel=\"next\""),
                ok(PARTIES, envelope("parties", organisation(10), organisation(20))),
                ok(employee(10), envelope("parties"), "<?page=2&perPage=2>; rel=next"),
                ok(employee(10), envelope("parties")),
                ok(employee(20), envelope("parties", worker)));
        try (Job job = job(List.of("employees"), options().put("page_size", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("employees", "98", "20", worker)));
        }
        assertTrace(
                wire(PARTIES, 2, 1),
                wire(PARTIES, 2, 2),
                wire(PARTIES, 2, 3),
                wire(employee(10), 2, 1),
                wire(employee(10), 2, 2),
                wire(employee(20), 2, 1));
    }

    @Test
    void skipsEmployeesWhenNoOrganisationWasDiscovered() throws Exception {
        reset(
                ok(PARTIES, envelope("parties", person(1))),
                ok(ROOT + "stages", envelope("stages", record(2))));
        try (Job job = job(List.of("employees", "stages"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("stages", "2", null, record(2))));
        }
        assertTrace(wire(PARTIES, 100, 1), wire(ROOT + "stages", 100, 1));
    }

    @Test
    void supportsEachDefinitionEntityWithoutChangingAnyOtherResource() throws Exception {
        for (String entity : List.of("opportunities", "parties")) {
            String tags = ROOT + entity + "/tags";
            String fields = ROOT + entity + "/fields/definitions";
            reset(
                    ok(tags, envelope("tags", record(2))),
                    ok(fields, envelope("definitions", record(2))));
            try (Job job =
                    job(
                            List.of("tags", "custom_fields"),
                            options().put("entity", entity),
                            false,
                            false)) {
                good(engine.executeJob(job.configName));
                assertOutput(
                        job,
                        List.of(
                                out("tags", "2", entity, record(2)),
                                out("custom_fields", "2", entity, record(2))));
            }
            assertTrace(wire(tags, 100, 1), wire(fields, 100, 1));
        }
    }

    @Test
    void filtersOpportunityTiesAndFractionalOffsetsAcrossUnsortedPages() throws Exception {
        ObjectNode exact = record(1).put("updatedAt", START);
        ObjectNode tie = record(2).put("updatedAt", "2025-01-02T00:00:00.123456789Z");
        ObjectNode old = record(3).put("updatedAt", "2025-01-02T00:00:00.123456788Z");
        ObjectNode upper = record(4).put("updatedAt", "2025-01-03T01:00:00+01:00");
        ObjectNode last = record(5).put("updatedAt", "2025-01-02T23:59:59.999999999Z");
        reset(
                ok(OPPORTUNITIES, envelope("opportunities", upper, exact), "<?page=2>; rel=next"),
                ok(OPPORTUNITIES, envelope("opportunities", old, last), "<?page=3>; rel=next"),
                ok(OPPORTUNITIES, envelope("opportunities", tie)));
        try (Job job =
                job(
                        List.of("opportunities"),
                        options().put("page_size", 2).put("start_date", START).put("end_date", END),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("opportunities", "1", null, exact),
                            out("opportunities", "2", null, tie),
                            out("opportunities", "5", null, last)));
        }
        assertTrace(since(2, 1, SINCE), since(2, 2, SINCE), since(2, 3, SINCE));
    }

    @Test
    void omitsUnsupportedSitePagingAndRejectsAnyUnexpectedContinuation() throws Exception {
        rejectOne(
                ok(SITE, object("site", options().put("subdomain", "demo")), "<?page=2>; rel=next"),
                List.of("site"),
                options(),
                wire(SITE),
                "unexpected or excessive Link metadata");
        rejectOne(
                ok(SITE, envelope("site", record(1))),
                List.of("site"),
                options(),
                wire(SITE),
                "expected the site object envelope");
    }

    @Test
    void refusesCrossOriginCrossParentCyclicAndAmbiguousPagination() throws Exception {
        for (String link :
                List.of(
                        "<https://capsulecrm-redirect-host:18443" + TASKS + "?page=2>; rel=next",
                        "<" + PARTIES + "?page=2>; rel=next",
                        "<?page=1>; rel=next",
                        "<?page=3>; rel=next",
                        "<?page=2&page=2>; rel=next",
                        "<?page=2&perPage=1>; rel=next",
                        "<?page=2&since=unexpected>; rel=next",
                        "<?page=2>; rel=next, <?page=2>; rel=next",
                        "<?page=2>; rel=\"next",
                        "<?page=2>; rel=next,")) {
            rejectOne(
                    ok(TASKS, envelope("tasks", record(2)), link),
                    List.of("tasks"),
                    options(),
                    wire(TASKS, 100, 1),
                    "Capsule CRM tasks");
        }
        reset(
                ok(PARTIES, envelope("parties", organisation(1))),
                ok(employee(1), envelope("parties"), "<" + employee(2) + "?page=2>; rel=next"));
        try (Job job = job(List.of("employees"), options(), false, false)) {
            bad(engine.executeJob(job.configName), "next-page link leaves the requested API scope");
            assertOutput(job, List.of());
        }
        assertTrace(wire(PARTIES, 100, 1), wire(employee(1), 100, 1));
    }

    @Test
    void boundsTotalPagesAndOrganisationFanoutBeforeTheNextRequest() throws Exception {
        reset(ok(TASKS, envelope("tasks"), "<?page=2>; rel=next"));
        failJob(List.of("tasks"), options().put("max_requests", 1), "max_requests exhausted");
        assertTrace(wire(TASKS, 100, 1));
        reset(
                ok(PARTIES, envelope("parties", organisation(1)), "<?page=2>; rel=next"),
                ok(PARTIES, envelope("parties", organisation(2))));
        failJob(
                List.of("employees"),
                options().put("max_parents", 1),
                "discovery exceeds max_parents");
        assertTrace(wire(PARTIES, 100, 1), wire(PARTIES, 100, 2));
    }

    @Test
    void rejectsInvalidEnvelopesRecordsKeysAndOpportunityTimestampsBeforePublishing()
            throws Exception {
        for (JsonNode invalid :
                List.of(
                        rows(record(1)),
                        object("other", rows(record(1))),
                        envelope("tasks", record(1), record(1)),
                        envelope("tasks", record(1), options()),
                        envelope("tasks", record(1), rows(record(2))))) {
            rejectOne(
                    ok(TASKS, invalid),
                    List.of("tasks"),
                    options(),
                    wire(TASKS, 100, 1),
                    "Capsule CRM tasks");
        }
        rejectOne(
                ok(TASKS, envelope("tasks", record(1), record(2))),
                List.of("tasks"),
                options().put("page_size", 1),
                wire(TASKS, 1, 1),
                "within its response limit");
        for (String invalid :
                List.of("{\"tasks\":[{\"id\":1,\"id\":2}]}", "{\"tasks\":[]} {\"extra\":true}")) {
            rejectOne(
                    frame(TASKS, 200, "plain", "", invalid.getBytes(StandardCharsets.UTF_8)),
                    List.of("tasks"),
                    options(),
                    wire(TASKS, 100, 1),
                    "single valid UTF-8 JSON");
        }
        for (ObjectNode invalid : List.of(record(1), record(1).put("updatedAt", "yesterday"))) {
            rejectOne(
                    ok(OPPORTUNITIES, envelope("opportunities", invalid)),
                    List.of("opportunities"),
                    options().put("start_date", START).put("end_date", END),
                    since(100, 1, SINCE),
                    "updatedAt");
        }
    }

    @Test
    void rejectsEmployeeParentMisattributionAndDoesNotSilentlySkipDeniedChildren()
            throws Exception {
        ObjectNode error = person(9);
        error.putObject("organisation").put("id", 2);
        for (int status : List.of(200, 403)) {
            reset(
                    ok(PARTIES, envelope("parties", organisation(1), organisation(2))),
                    reply(employee(1), status, "plain", "", envelope("parties", error)));
            try (Job job = job(List.of("employees"), options(), false, false)) {
                bad(
                        engine.executeJob(job.configName),
                        status == 200 ? "employee organisation differs" : "HTTP 403");
                assertOutput(job, List.of());
            }
            assertTrace(wire(PARTIES, 100, 1), wire(employee(1), 100, 1));
        }
    }

    @Test
    void validatesConfigurationBeforeOpeningAnyConnection() throws Exception {
        reset();
        failJob(
                ALL,
                options().put("entity", "../users"),
                "expected parties, opportunities or kases");
        failJob(ALL, options().put("token", "Bearer " + TOKEN), "without a prefix");
        failJob(
                ALL,
                options().put("endpoint", "http://capsulecrm-peer:18443"),
                "expected a bare HTTPS origin");
        failJob(ALL, options().put("page_size", 101), "page_size");
        failJob(ALL, options().put("start_date", "2025-01-02"), "timestamp with a UTC offset");
        failJob(
                ALL,
                options().put("start_date", END).put("end_date", END),
                "must be later than start_date");
        failJob(List.of("tasks"), options().put("start_date", START), "date filters apply only");
        failJob(List.of("parties", "parties"), options(), "choose each resource only once");
        failJob(List.of("people"), options(), "unsupported resource name");
        assertTrace();
    }

    @Test
    void doesNotExposeErrorBodiesOrCredentialsAndNeverAcceptsPartialStatus() throws Exception {
        for (int status : List.of(400, 401, 403, 404, 204, 206)) {
            rejectOne(
                    frame(
                            USERS,
                            status,
                            "binary",
                            "",
                            HIDDEN_MESSAGE.getBytes(StandardCharsets.UTF_8)),
                    List.of("users"),
                    options(),
                    wire(USERS),
                    "HTTP " + status);
        }
    }

    @Test
    void honorsProviderResetEpochAndRetryAfterWithoutImplicit503Replay() throws Exception {
        reset(reply(USERS, 429, "plain", "1;+2", options()), ok(USERS, envelope("users")));
        try (Job job = job(List.of("users"), options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(USERS), wire(USERS));
        long resetEpoch =
                trace().lines()
                        .filter(item -> item.startsWith("reset_epoch\t"))
                        .mapToLong(item -> Long.parseLong(item.substring("reset_epoch\t".length())))
                        .findFirst()
                        .orElseThrow();
        assertTrue(seen().get(1).epochMillis >= resetEpoch * 1000L);
        rejectOne(
                reply(USERS, 429, "plain", "2;+90", options()),
                List.of("users"),
                options(),
                wire(USERS),
                "retry delay exceeds request deadline");
        rejectOne(
                reply(USERS, 429, "plain", ";not-an-epoch", options()),
                List.of("users"),
                options(),
                wire(USERS),
                "invalid X-RateLimit-Reset epoch seconds");
        rejectOne(
                reply(USERS, 503, "plain", "000", options()),
                List.of("users"),
                options().put("max_attempts", 1),
                wire(USERS),
                "HTTP 503 retry budget exhausted");
    }

    @Test
    void boundsDecodedGzipAndRejectsInvalidMediaAndUtf8() throws Exception {
        List<String> selected = List.of("users");
        ObjectNode user = record(32).put("name", "约瑟");
        reset(reply(USERS, 200, "gzip", "", envelope("users", user)));
        try (Job job = job(selected, options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("users", "32", null, user)));
        }
        assertTrace(wire(USERS));
        rejectOne(
                reply(
                        USERS,
                        200,
                        "gzip",
                        "",
                        envelope("users", record(1).put("detail", "x".repeat(8192)))),
                selected,
                options().put("max_response_bytes", 1024),
                wire(USERS),
                "response exceeds max_response_bytes");
        rejectOne(
                reply(USERS, 200, "unsupported", "", envelope("users", user)),
                selected,
                options(),
                wire(USERS),
                "unsupported response compression");
        rejectOne(
                reply(USERS, 200, "binary", "", envelope("users", user)),
                selected,
                options(),
                wire(USERS),
                "expected Content-Type application/json");
        rejectOne(
                frame(USERS, 200, "plain", "", new byte[] {'{', (byte) 0xFF, '}'}),
                selected,
                options(),
                wire(USERS),
                "single valid UTF-8 JSON");
    }

    @Test
    void retriesTheSameGetAfterTruncationWithoutPublishingItsPartialResponse() throws Exception {
        reset(
                reply(
                        TASKS,
                        200,
                        "truncated",
                        "",
                        envelope("tasks", record(1)),
                        "<?page=2>; rel=next"),
                ok(TASKS, envelope("tasks", record(2))));
        try (Job job = job(List.of("tasks"), options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("tasks", "2", null, record(2))));
        }
        assertTrace(wire(TASKS, 100, 1), wire(TASKS, 100, 1));
    }

    @Test
    void verifiesTrustAndHostnameAndNeverFollowsRedirects() throws Exception {
        reset();
        failJob(
                List.of("users"),
                options().put("trust_certificate", OTHER_PEM),
                "TLS certificate or hostname");
        failJob(
                List.of("users"),
                options().put("endpoint", "https://capsulecrm-other-host:18443"),
                "TLS certificate or hostname");
        assertTrace();
        rejectOne(
                reply(USERS, 302, "redirect", "", options()),
                List.of("users"),
                options(),
                wire(USERS),
                "HTTP 302");
    }

    @Test
    void stopsAnActiveBodyByItsOverallDeadline() throws Exception {
        rejectOne(
                stall(USERS),
                List.of("users"),
                options().put("request_timeout_ms", 2500),
                wire(USERS),
                "request deadline exceeded");
        await(
                () -> trace().contains("disconnect\t" + USERS),
                "deadline closed provider connection");
    }

    @Test
    void cancelsAnActiveEmployeePageBeforeAnotherParentCanStart() throws Exception {
        String route = employee(1);
        reset(ok(PARTIES, envelope("parties", organisation(1), organisation(2))), stall(route));
        try (Job job =
                job(
                        List.of("employees", "stages"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 2, "stalled employee page");
            } finally {
                cancel(job.id, running);
            }
            await(() -> trace().contains("disconnect\t" + route), "cancel closed employee page");
            assertOutput(job, List.of());
        }
        assertTrace(wire(PARTIES, 100, 1), wire(route, 100, 1));
    }

    @Test
    void restoresADiscoveredParentAndChildPageWithoutReplayingCommittedRows() throws Exception {
        String first = employee(1);
        String second = employee(2);
        ObjectNode parentA = organisation(1);
        ObjectNode parentB = organisation(2);
        ObjectNode childA = person(7);
        ObjectNode childB = person(8);
        List<ObjectNode> committed =
                List.of(
                        out("parties", "1", null, parentA),
                        out("parties", "2", null, parentB),
                        out("employees", "7", "1", childA));
        reset(
                ok(PARTIES, envelope("parties", parentA), "<?page=2>; rel=next"),
                ok(PARTIES, envelope("parties", parentB)),
                ok(first, envelope("parties", childA), "<?page=2>; rel=next"),
                stall(first));
        try (Job job =
                job(
                        List.of("parties", "employees"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            saveActive(job, 4, committed, first);
            queue(ok(first, envelope("parties")), ok(second, envelope("parties", childB)));
            good(engine.restoreJob(job.configName, job.id));
            List<ObjectNode> expected = new ArrayList<>(committed);
            expected.add(out("employees", "8", "2", childB));
            assertOutput(job, expected);
        }
        assertTrace(
                wire(PARTIES, 100, 1),
                wire(PARTIES, 100, 2),
                wire(first, 100, 1),
                wire(first, 100, 2),
                wire(first, 100, 2),
                wire(second, 100, 1));
    }

    @Test
    void rejectsRestoredScopeChangesBeforeCallingCapsule() throws Exception {
        reset(stall(OPPORTUNITIES));
        try (Job original =
                        job(
                                List.of("opportunities"),
                                options().put("request_timeout_ms", 60000).put("start_date", START),
                                true,
                                false);
                Job updated =
                        job(
                                List.of("opportunities"),
                                options().put("page_size", 42).put("start_date", START),
                                true,
                                false)) {
            saveActive(original, 1, List.of(), OPPORTUNITIES);
            bad(engine.restoreJob(updated.configName, original.id), "read scope changed");
        }
        assertTrace(since(100, 1, SINCE));
    }

    @Test
    void restoresTerminalCapsuleStateAlongsideAnIndependentActivePipeline() throws Exception {
        ObjectNode value = record(6);
        ObjectNode expected = out("users", "6", null, value);
        reset(ok(USERS, envelope("users", value)));
        try (Job job = job(List.of("users"), options(), true, true)) {
            saveActive(job, 1, List.of(expected), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(job.id)),
                        "active restored pipeline");
                long before = engine.checkpointCount(job.id);
                await(
                        () -> engine.checkpointCount(job.id) > before,
                        "restored pipeline checkpoint");
                assertOutput(job, List.of(expected));
                assertTrace(wire(USERS));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void restoresOnlyUnpublishedRowsAndRejectsAnAlteredOpportunityPage() throws Exception {
        List<String> ids = new ArrayList<>();
        ArrayNode data = rows(record(100).put("updatedAt", "2025-01-01T23:59:59.999999999Z"));
        for (int n = 1; n <= 33; n++) {
            ids.add(Integer.toString(n));
            data.add(record(n).put("updatedAt", "2025-01-02T09:00:00+09:00"));
        }
        ArrayNode changedExcluded = data.deepCopy();
        ((ObjectNode) changedExcluded.get(0)).put("content", "filtered but altered");
        ArrayNode changedIncluded = data.deepCopy();
        ((ObjectNode) changedIncluded.get(33)).putNull("newField");
        ArrayNode reordered = data.deepCopy();
        reordered.set(1, data.get(2));
        reordered.set(2, data.get(1));
        ObjectNode stable = object("opportunities", data);
        reset(
                ok(OPPORTUNITIES, stable),
                ok(OPPORTUNITIES, stable),
                ok(OPPORTUNITIES, object("opportunities", changedExcluded)),
                ok(OPPORTUNITIES, object("opportunities", changedIncluded)),
                ok(OPPORTUNITIES, object("opportunities", reordered)),
                ok(OPPORTUNITIES, stable, "<?page=2>; rel=next"));
        CapsuleCrmRecoveryProbe reader = recovery(List.of("opportunities"));
        List<String> initial = new ArrayList<>();
        byte[] snapshot = reader.firstChunk(initial);
        assertEquals(ids.subList(0, 32), initial);
        String serialized = new String(snapshot, StandardCharsets.ISO_8859_1);
        assertFalse(serialized.contains(TOKEN) || serialized.contains("updatedAt"));
        List<String> suffix = new ArrayList<>();
        reader.consume(snapshot, suffix);
        assertEquals(ids.subList(32, 33), suffix);
        for (int n = 0; n < 4; n++) {
            List<String> invalid = new ArrayList<>();
            Exception refusal =
                    assertThrows(Exception.class, () -> reader.consume(snapshot, invalid));
            assertTrue(refusal.getMessage().contains("provider page changed since checkpoint"));
            assertTrue(invalid.isEmpty());
        }
        Seen wire = since(100, 1, SINCE);
        assertTrace(wire, wire, wire, wire, wire, wire);
    }

    @Test
    void restoresPendingCoordinatorAndClosesEmptyOrCancelledReadersWithoutNetwork()
            throws Exception {
        reset();
        CapsuleCrmRecoveryProbe reader = recovery(List.of("users"));
        byte[] assigned = reader.recoverUnassigned();
        reader.finishWithoutAssignment();
        List<String> rows = new ArrayList<>();
        reader.stopBeforeAssignment(rows);
        assertTrue(rows.isEmpty());
        assertTrace();
        queue(ok(USERS, envelope("users", record(88))));
        reader.consume(assigned, rows);
        assertEquals(List.of("88"), rows);
        assertTrace(wire(USERS));
    }

    private void rejectOne(
            String scripted,
            List<String> resources,
            ObjectNode settings,
            Seen expected,
            String diagnostic)
            throws Exception {
        reset(scripted);
        try (Job job = job(resources, settings, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
            assertOutput(job, List.of());
        }
        assertTrace(expected);
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

    private CapsuleCrmRecoveryProbe recovery(List<String> resources) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new CapsuleCrmRecoveryProbe(
                url.toString(), certificate.toString(), TOKEN, resources);
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
        String name = "capsulecrm-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "capsulecrm_data")
                        .put("token", TOKEN)
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
                        + "source {\n CapsuleCrm "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=capsulecrm_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"capsulecrm_$"
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

    private static ObjectNode organisation(int id) {
        return record(id).put("type", "organisation");
    }

    private static ObjectNode person(int id) {
        return record(id).put("type", "person");
    }

    private static String employee(int id) {
        return PARTIES + "/" + id + "/people";
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

    private static ObjectNode out(String resource, String id, String parent, ObjectNode payload) {
        ObjectNode row =
                options()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("parent_id", parent)
                        .put("updated_at", payload.path("updatedAt").textValue());
        row.set("payload", payload);
        return row;
    }

    private static String reply(
            String path,
            int status,
            String mode,
            String retryAfter,
            JsonNode body,
            String... links) {
        return frame(
                path,
                status,
                mode,
                retryAfter,
                body.toString().getBytes(StandardCharsets.UTF_8),
                links);
    }

    private static String frame(
            String path, int status, String mode, String retryAfter, byte[] body, String... links) {
        String encoded = Base64.getEncoder().encodeToString(body);
        String link =
                Base64.getEncoder()
                        .encodeToString(String.join("\n", links).getBytes(StandardCharsets.UTF_8));
        return String.join("\t", Integer.toString(status), mode, retryAfter, encoded, path, link);
    }

    private static String ok(String path, JsonNode body, String... links) {
        return reply(path, 200, "plain", "", body, links);
    }

    private static String stall(String path) {
        return reply(path, 200, "chunked", "", options());
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
            assertEquals("GET", parts[1]);
            assertEquals("true", parts[4], "expected Bearer authentication");
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
            requests.add(
                    new Seen(
                            request.getRawPath(),
                            query,
                            Long.parseLong(parts[3]),
                            Long.parseLong(parts[7])));
        }
        return requests;
    }

    private static Seen wire(String path) {
        return new Seen(path, Map.of(), 0L);
    }

    private static Seen wire(String path, int pageSize, int page) {
        return new Seen(
                path,
                Map.of("perPage", Integer.toString(pageSize), "page", Integer.toString(page)),
                0L);
    }

    private static Seen since(int pageSize, int page, String since) {
        return new Seen(
                OPPORTUNITIES,
                Map.of(
                        "since",
                        since,
                        "perPage",
                        Integer.toString(pageSize),
                        "page",
                        Integer.toString(page)),
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
        assertFalse(visible.contains(TOKEN), "access key leaked");
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

        private final long epochMillis;

        private Seen(String route, Map<String, String> query, long at) {
            this(route, query, at, 0L);
        }

        private Seen(String route, Map<String, String> query, long at, long epochMillis) {
            this.route = route;
            this.query = query;
            this.at = at;
            this.epochMillis = epochMillis;
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
        private final String id = CapsuleCrmIT.id();

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
                                    + "' -maxdepth 1 -type f -name 'capsulecrm_*' -exec cat {} +; fi");
            assertEquals(0, files.getExitCode(), files.getStderr());
            List<ObjectNode> output = new ArrayList<>();
            for (String line : files.getStdout().split("\\R")) {
                if (!line.isBlank()) {
                    ObjectNode row = (ObjectNode) JSON.readTree(line);
                    assertTrue(row.path("payload").isTextual());
                    row.set("payload", JSON.readTree(row.path("payload").textValue()));
                    // LocalFile can omit null columns. Compare the logical fixed schema.
                    for (String nullable : List.of("parent_id", "updated_at")) {
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
            // The CapsuleCrm pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}

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
package org.apache.seatunnel.e2e.connector.chift;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.chift.ChiftRecoveryProbe;
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
 * Runs Chift through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class ChiftIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String TOKEN = "fixture-chift-first-token";
    private static final String NEXT_TOKEN = "fixture-chift-second-token";
    private static final String CLIENT_ID = "client id+\"\\?";
    private static final String CLIENT_SECRET = "synthetic secret+\"\\/=?";
    private static final String ACCOUNT_ID = "00000000-0000-4000-A000-000000000001";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "chift-peer";
    private static final String OTHER_ORIGIN = "chift-secondary";
    private static final String PEM = "/tmp/chift.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-chift.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String AUTH = "/token";
    private static final String CONSUMERS = "/consumers";
    private static final String SYNCS = "/syncs";
    private static final String A = "00000000-0000-4000-A000-00000000000A";
    private static final String B = "00000000-0000-4000-A000-00000000000B";
    private static final String C = "00000000-0000-4000-A000-00000000000C";
    private static final String D = "00000000-0000-4000-A000-00000000000D";
    private static final String CONNECTION = "00000000-0000-4000-A000-00000000000E";
    private static final String START = "2025-01-02T00:00:00.123456789Z";
    private static final String END = "2025-01-03T00:00:00Z";
    private static final List<String> ALL =
            List.of("consumers", "connections", "syncs", "executions");
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
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass chift-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:chift-redirect-host,dns:chift-secondary,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass chift-fixture -keystore ";
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
                        + ChiftPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + ChiftPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = ChiftPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "chift-other-host", "chift-redirect-host")
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
    void persistsAllFourResourcesWithExactParentIdentityAndPrecision() throws Exception {
        ObjectNode consumer =
                consumer(A)
                        .put("name", "客户 🧭")
                        .putNull("email")
                        .put("redirect_url", "https://chift-redirect-host:18443/never-follow");
        ObjectNode connection = connection(CONNECTION);
        connection.put("integrationid", new BigInteger("9223372036854775808"));
        connection
                .putObject("data")
                .put("amount", new BigDecimal("123456789012345678.123456789000"));
        ObjectNode sync = sync(B);
        sync.putArray("mappings").addObject().putNull("value");
        ObjectNode execution =
                execution("opaque/数据-1", START)
                        .put("consumer_id", "outside-current-consumer-schema")
                        .putNull("parentexecutionid");
        reset(
                auth(TOKEN),
                ok(CONSUMERS, rows(consumer)),
                ok(connectionRoute(A), rows(connection)),
                ok(SYNCS, rows(sync)),
                ok(executionRoute(B), resultPage(1, 500, 1, execution)));
        try (Job job = job(ALL, options().put("page_size", 500), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("consumers", A, null, null, consumer),
                            out("connections", CONNECTION, A, null, connection),
                            out("syncs", B, null, null, sync),
                            out("executions", "opaque/数据-1", B, START, execution)));
        }
        assertTrace(
                wire(AUTH),
                wire(CONSUMERS),
                wire(connectionRoute(A)),
                wire(SYNCS),
                page(executionRoute(B), 1, 500));
    }

    @Test
    void discoversBothUnselectedParentFamiliesAndReadsAnIndependentChildAfterEmptyArrays()
            throws Exception {
        ObjectNode c = connection(CONNECTION).put("consumerid", C.toLowerCase(Locale.ROOT));
        ObjectNode a = execution("shared-run", START);
        reset(
                auth(TOKEN),
                ok(CONSUMERS, rows(consumer(A), consumer(C))),
                ok(connectionRoute(A), rows()),
                ok(connectionRoute(C), rows(c)),
                ok(SYNCS, rows(sync(B), sync(D))),
                ok(executionRoute(B), resultPage(1, 50, 0)),
                ok(executionRoute(D), resultPage(1, 50, 1, a)));
        try (Job job = job(List.of("connections", "executions"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("connections", CONNECTION, C, null, c),
                            out("executions", "shared-run", D, START, a)));
        }
        assertTrace(
                wire(AUTH),
                wire(CONSUMERS),
                wire(connectionRoute(A)),
                wire(connectionRoute(C)),
                wire(SYNCS),
                page(executionRoute(B), 1, 50),
                page(executionRoute(D), 1, 50));
    }

    @Test
    void appliesExactLocalTimeBoundsAndKeepsTiesOnBothPagesAndUnderDistinctSyncs()
            throws Exception {
        ObjectNode old = execution("old", "2025-01-02 00:00:00.123456788+0000");
        ObjectNode exact = execution("same", "2025-01-02T01:00:00.123456789+01:00");
        ObjectNode tie = execution("another", "2025-01-02 01:00:00.123456789");
        ObjectNode end = execution("end", "2025-01-03T01:00:00+01:00");
        ObjectNode newest = execution("same", "2025-01-02T23:59:59.999999999Z");
        reset(
                auth(TOKEN),
                ok(SYNCS, rows(sync(B).put("created_on", "2020-01-01T00:00:00Z"), sync(D))),
                ok(executionRoute(B), resultPage(1, 2, 4, old, exact)),
                ok(executionRoute(B), resultPage(2, 2, 4, tie, end)),
                ok(executionRoute(D), resultPage(1, 2, 1, newest)));
        try (Job job =
                job(
                        List.of("executions"),
                        options()
                                .put("page_size", 2)
                                .put("start_date", START)
                                .put("end_date", END)
                                .put("legacy_timezone", "Europe/Paris"),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("executions", "same", B, START, exact),
                            out("executions", "another", B, START, tie),
                            out(
                                    "executions",
                                    "same",
                                    D,
                                    "2025-01-02T23:59:59.999999999Z",
                                    newest)));
        }
        assertTrace(
                wire(AUTH),
                wire(SYNCS),
                page(executionRoute(B), 1, 2),
                page(executionRoute(B), 2, 2),
                page(executionRoute(D), 1, 2));
    }

    @Test
    void returnsNoImplicitChildrenWhenTheAccountHasNoParents() throws Exception {
        reset(auth(TOKEN), ok(CONSUMERS, rows()), ok(SYNCS, rows()));
        try (Job job =
                job(
                        List.of("executions", "connections"),
                        options().put("max_requests", 2),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(AUTH), wire(CONSUMERS), wire(SYNCS));
    }

    @Test
    void rejectsMissingOrContradictoryExecutionTotalsAndDoesNotReuseAdjacentIds() throws Exception {
        String route = executionRoute(B);
        ObjectNode run = execution("first", START);
        for (ObjectNode wrong :
                List.of(
                        resultPage(2, 2, 1, run),
                        resultPage(1, 9, 1, run),
                        resultPage(1, 2, 2, run),
                        resultPage(1, 2, 3),
                        resultPage(1, 2, -1),
                        resultPage(1, 2, 1, run).put("total", "1"),
                        object("items", rows(run)))) {
            reset(auth(TOKEN), ok(SYNCS, rows(sync(B))), ok(route, wrong));
            failJob(List.of("executions"), options().put("page_size", 2), "Chift executions");
            assertTrace(wire(AUTH), wire(SYNCS), page(route, 1, 2));
        }
        for (ObjectNode badNext :
                List.of(
                        resultPage(2, 2, 3, run),
                        resultPage(2, 2, 4, run, execution("third", START)))) {
            reset(
                    auth(TOKEN),
                    ok(SYNCS, rows(sync(B))),
                    ok(route, resultPage(1, 2, 3, run, execution("second", START))),
                    ok(route, badNext));
            failJob(List.of("executions"), options().put("page_size", 2), "Chift executions");
            assertTrace(wire(AUTH), wire(SYNCS), page(route, 1, 2), page(route, 2, 2));
        }
    }

    @Test
    void refusesForeignParentEchoesAndDoesNotSilentlySkipDeniedChildren() throws Exception {
        String route = connectionRoute(A);
        ObjectNode foreign = connection(CONNECTION).put("consumerid", C);
        for (int status : List.of(200, 403, 404)) {
            reset(
                    auth(TOKEN),
                    ok(CONSUMERS, rows(consumer(A), consumer(C))),
                    reply(route, status, "plain", "", rows(foreign)));
            failJob(
                    List.of("connections"),
                    options(),
                    status == 200
                            ? "provider record belongs to a different parent"
                            : "HTTP " + status);
            assertTrace(wire(AUTH), wire(CONSUMERS), wire(route));
        }
        String execution = executionRoute(B);
        reset(
                auth(TOKEN),
                ok(SYNCS, rows(sync(B))),
                ok(execution, resultPage(1, 50, 1, execution("wrong", START).put("syncid", D))));
        failJob(List.of("executions"), options(), "provider record belongs to a different parent");
        assertTrace(wire(AUTH), wire(SYNCS), page(execution, 1, 50));
    }

    @Test
    void checksParentPathIdentityStabilityAndBoundsDiscovery() throws Exception {
        for (JsonNode bad :
                List.of(
                        rows(consumer("wrong/path")),
                        rows(consumer(A), consumer(A.toLowerCase(Locale.ROOT))))) {
            rejectOne(
                    ok(CONSUMERS, bad),
                    List.of("connections"),
                    options(),
                    wire(CONSUMERS),
                    "Chift consumers");
        }
        rejectOne(
                ok(CONSUMERS, rows(consumer(A), consumer(C))),
                List.of("connections"),
                options().put("max_parents", 1),
                wire(CONSUMERS),
                "parent discovery exceeds max_parents");
        rejectOne(
                ok(CONSUMERS, rows(consumer(A))),
                List.of("connections"),
                options().put("max_requests", 1),
                wire(CONSUMERS),
                "max_requests exhausted");
        rejectOne(
                reply(CONSUMERS, 200, "link", "", rows(consumer(A))),
                List.of("consumers"),
                options(),
                wire(CONSUMERS),
                "unexpected Link metadata");
    }

    @Test
    void usesLocalLegacyZoneOnlyWithExplicitUnambiguousTimeAndValidConfiguration()
            throws Exception {
        reset();
        failJob(ALL, options().put("auth_account_id", "not-a-uuid"), "expected a UUID");
        failJob(ALL, options().put("auth_client_id", "   "), "nonempty credential");
        failJob(
                ALL,
                options().put("start_date", "2025-01-02T00:00:00"),
                "ISO-8601 timestamp with a UTC offset");
        failJob(
                ALL,
                options().put("start_date", END).put("end_date", START),
                "must be later than start_date");
        failJob(
                List.of("consumers"),
                options().put("end_date", END),
                "date filters apply only to executions");
        failJob(ALL, options().put("page_size", 501), "page_size");
        failJob(
                ALL,
                options().put("endpoint", "http://chift-peer:18443"),
                "expected a bare HTTPS origin");
        assertTrace();
        String route = executionRoute(B);
        for (String zone : List.of("", "America/New_York")) {
            reset(
                    auth(TOKEN),
                    ok(SYNCS, rows(sync(B))),
                    ok(route, resultPage(1, 50, 1, execution("overlap", "2025-11-02 01:30:00"))));
            failJob(
                    List.of("executions"),
                    zone.isEmpty() ? options() : options().put("legacy_timezone", zone),
                    zone.isEmpty()
                            ? "expected an offset timestamp"
                            : "execution start is ambiguous");
            assertTrace(wire(AUTH), wire(SYNCS), page(route, 1, 50));
        }
    }

    @Test
    void keepsTokenJsonAndBearerCredentialsInsideEachExplicitOrigin() throws Exception {
        reset(
                auth(TOKEN),
                ok(CONSUMERS, rows(consumer(A))),
                auth(NEXT_TOKEN, 1800, 1800, "secondary"),
                reply(CONSUMERS, 200, "plain", "", rows(consumer(C)), NEXT_TOKEN));
        try (Job original = job(List.of("consumers"), options(), false, false);
                Job secondary =
                        job(
                                List.of("consumers"),
                                options()
                                        .put("endpoint", "https://" + OTHER_ORIGIN + ":18443")
                                        .put("auth_client_id", "secondary-id")
                                        .put("auth_client_secret", "secondary-secret")
                                        .put(
                                                "auth_account_id",
                                                "00000000-0000-4000-A000-000000000002"),
                                false,
                                false)) {
            good(engine.executeJob(original.configName));
            good(engine.executeJob(secondary.configName));
            assertOutput(original, List.of(out("consumers", A, null, null, consumer(A))));
            assertOutput(secondary, List.of(out("consumers", C, null, null, consumer(C))));
        }
        assertTrace(wire(AUTH), wire(CONSUMERS), wire(AUTH), wire(CONSUMERS));
        assertEquals(
                List.of(
                        HOST + ":18443",
                        HOST + ":18443",
                        OTHER_ORIGIN + ":18443",
                        OTHER_ORIGIN + ":18443"),
                seen().stream().map(x -> x.host).collect(Collectors.toList()));
    }

    @Test
    void refreshesOnceAfter401AndUsesBothAbsoluteAndRelativeExpiry() throws Exception {
        for (int mode = 0; mode < 3; mode++) {
            List<String> traffic = new ArrayList<>();
            traffic.add(
                    mode == 0
                            ? auth(TOKEN)
                            : auth(TOKEN, mode == 1 ? 1800 : 1, mode == 1 ? 2 : 1800, "primary"));
            traffic.add(
                    reply(CONSUMERS, 200, mode == 0 ? "plain" : "delay", "", rows(consumer(A))));
            if (mode == 0) {
                traffic.add(reply(SYNCS, 401, "plain", "", options().put("error", HIDDEN_MESSAGE)));
            }
            traffic.add(auth(NEXT_TOKEN));
            traffic.add(reply(SYNCS, 200, "plain", "", rows(sync(B)), NEXT_TOKEN));
            reset(traffic.toArray(String[]::new));
            try (Job job = job(List.of("consumers", "syncs"), options(), false, false)) {
                good(engine.executeJob(job.configName));
                assertOutput(
                        job,
                        List.of(
                                out("consumers", A, null, null, consumer(A)),
                                out("syncs", B, null, null, sync(B))));
            }
            if (mode == 0) {
                assertTrace(wire(AUTH), wire(CONSUMERS), wire(SYNCS), wire(AUTH), wire(SYNCS));
            } else {
                assertTrace(wire(AUTH), wire(CONSUMERS), wire(AUTH), wire(SYNCS));
            }
        }
        reset(
                auth(TOKEN),
                reply(CONSUMERS, 401, "plain", "", options()),
                auth(NEXT_TOKEN),
                reply(CONSUMERS, 401, "plain", "", options(), NEXT_TOKEN));
        failJob(List.of("consumers"), options(), "access denied (HTTP 401)");
        assertTrace(wire(AUTH), wire(CONSUMERS), wire(AUTH), wire(CONSUMERS));
    }

    @Test
    void neverReplaysAnAmbiguousTokenPostAndRejectsInvalidExpiryMetadata() throws Exception {
        for (int status : List.of(400, 401, 503)) {
            reset(
                    reply(
                            AUTH,
                            status,
                            "plain",
                            "0",
                            options().put("error", HIDDEN_MESSAGE),
                            "primary"));
            failJob(List.of("consumers"), options(), "token request failed (HTTP " + status + ")");
            assertTrace(wire(AUTH));
        }
        reset(reply(AUTH, 200, "truncated", "", options().put("access_token", TOKEN), "primary"));
        failJob(List.of("consumers"), options(), "token request outcome unknown");
        assertTrace(wire(AUTH));
        reset(auth(TOKEN, 1800, -5, "primary"));
        failJob(List.of("consumers"), options(), "provider returned an expired token");
        assertTrace(wire(AUTH));
        for (ObjectNode bad :
                List.of(
                        options().put("access_token", TOKEN),
                        options().put("token_type", "Basic").put("access_token", TOKEN),
                        options()
                                .put("token_type", "Bearer")
                                .put("access_token", "spaces invalid")
                                .put("expires_in", 1800)
                                .put("expires_on", 0),
                        options()
                                .put("token_type", "Bearer")
                                .put("access_token", TOKEN)
                                .put("expires_in", 0)
                                .put("expires_on", 0))) {
            reset(reply(AUTH, 200, "token_1800", "", bad, "primary"));
            failJob(List.of("consumers"), options(), "invalid token response");
            assertTrace(wire(AUTH));
        }
    }

    @Test
    void honorsRetryAfterOnTokenAndDataRequestsAndSuppressesHidden503Replay() throws Exception {
        reset(
                reply(AUTH, 429, "plain", "2", options(), "primary"),
                auth(TOKEN),
                reply(CONSUMERS, 429, "plain", "2", options()),
                ok(CONSUMERS, rows()));
        try (Job job = job(List.of("consumers"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(AUTH), wire(AUTH), wire(CONSUMERS), wire(CONSUMERS));
        List<Seen> requests = seen();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(requests.get(1).at - requests.get(0).at) >= 1980L);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(requests.get(3).at - requests.get(2).at) >= 1980L);
        rejectOne(
                reply(CONSUMERS, 429, "plain", "90", options()),
                List.of("consumers"),
                options(),
                wire(CONSUMERS),
                "retry delay exceeds request deadline");
        rejectOne(
                reply(CONSUMERS, 503, "plain", "0", options()),
                List.of("consumers"),
                options().put("max_attempts", 1),
                wire(CONSUMERS),
                "HTTP 503 retry budget exhausted");
    }

    @Test
    void enforcesStrictUtf8MimeGzipAndDecodedResponseLimits() throws Exception {
        ObjectNode value = consumer(A).put("name", "徐");
        reset(auth(TOKEN), reply(CONSUMERS, 200, "gzip", "", rows(value)));
        try (Job job = job(List.of("consumers"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("consumers", A, null, null, value)));
        }
        assertTrace(wire(AUTH), wire(CONSUMERS));
        rejectOne(
                reply(CONSUMERS, 200, "gzip", "", rows(consumer(A).put("name", "x".repeat(8192)))),
                List.of("consumers"),
                options().put("max_response_bytes", 1024),
                wire(CONSUMERS),
                "response exceeds max_response_bytes");
        rejectOne(
                reply(CONSUMERS, 200, "unsupported", "", rows(value)),
                List.of("consumers"),
                options(),
                wire(CONSUMERS),
                "unsupported response compression");
        rejectOne(
                reply(CONSUMERS, 200, "binary", "", rows(value)),
                List.of("consumers"),
                options(),
                wire(CONSUMERS),
                "expected Content-Type application/json");
        for (byte[] raw :
                List.of(
                        new byte[] {'[', (byte) 0xFF, ']'},
                        ("[{\"consumerid\":\"" + A + "\",\"consumerid\":\"" + C + "\"}]")
                                .getBytes(StandardCharsets.UTF_8),
                        "[] {}".getBytes(StandardCharsets.UTF_8))) {
            rejectOne(
                    frame(CONSUMERS, 200, "plain", "", raw, TOKEN),
                    List.of("consumers"),
                    options(),
                    wire(CONSUMERS),
                    "single valid UTF-8 JSON document");
        }
    }

    @Test
    void retriesAnInterruptedReadWithoutPublishingInvalidOrPartialStatusBodies() throws Exception {
        reset(
                auth(TOKEN),
                reply(CONSUMERS, 200, "truncated", "", rows(consumer(A))),
                ok(CONSUMERS, rows(consumer(C))));
        try (Job job = job(List.of("consumers"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("consumers", C, null, null, consumer(C))));
        }
        assertTrace(wire(AUTH), wire(CONSUMERS), wire(CONSUMERS));
        for (int status : List.of(204, 206, 403, 422)) {
            rejectOne(
                    reply(CONSUMERS, status, "plain", "", options().put("detail", HIDDEN_MESSAGE)),
                    List.of("consumers"),
                    options(),
                    wire(CONSUMERS),
                    "HTTP " + status);
        }
    }

    @Test
    void verifiesTrustAndHostnameAndNeverFollowsCredentialBearingRedirects() throws Exception {
        reset();
        failJob(
                List.of("consumers"),
                options().put("trust_certificate", OTHER_PEM),
                "TLS certificate or hostname");
        failJob(
                List.of("consumers"),
                options().put("endpoint", "https://chift-other-host:18443"),
                "TLS certificate or hostname");
        assertTrace();
        reset(reply(AUTH, 302, "redirect", "", options(), "primary"));
        failJob(List.of("consumers"), options(), "token request failed (HTTP 302)");
        assertTrace(wire(AUTH));
        rejectOne(
                reply(CONSUMERS, 302, "redirect", "", options()),
                List.of("consumers"),
                options(),
                wire(CONSUMERS),
                "HTTP 302");
    }

    @Test
    void stopsAStalledRequestAtDeadlineAndCancelsBeforeReadingSiblingParents() throws Exception {
        rejectOne(
                stall(CONSUMERS),
                List.of("consumers"),
                options().put("request_timeout_ms", 2500),
                wire(CONSUMERS),
                "request deadline exceeded");
        await(
                () -> trace().contains("disconnect\t" + CONSUMERS),
                "deadline closes provider socket");
        String path = connectionRoute(A);
        reset(auth(TOKEN), ok(CONSUMERS, rows(consumer(A), consumer(C))), stall(path));
        try (Job job =
                job(
                        List.of("connections", "executions"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 3, "active connection request");
            } finally {
                cancel(job.id, running);
            }
            await(() -> trace().contains("disconnect\t" + path), "cancel closes provider socket");
            assertOutput(job, List.of());
        }
        assertTrace(wire(AUTH), wire(CONSUMERS), wire(path));
    }

    @Test
    void restoresBothParentFamiliesAndTheExecutionPageWithNoPriorOutputReplay() throws Exception {
        String target = executionRoute(B);
        ObjectNode connection = connection(CONNECTION);
        ObjectNode first = execution("first", START);
        ObjectNode second = execution("second", START);
        ObjectNode sibling = execution("first", END);
        List<ObjectNode> committed =
                List.of(
                        out("connections", CONNECTION, A, null, connection),
                        out("executions", "first", B, START, first));
        reset(
                auth(TOKEN),
                ok(CONSUMERS, rows(consumer(A))),
                ok(connectionRoute(A), rows(connection)),
                ok(SYNCS, rows(sync(B), sync(D))),
                ok(target, resultPage(1, 1, 2, first)),
                stall(target));
        try (Job job =
                job(
                        List.of("connections", "executions"),
                        options().put("page_size", 1).put("request_timeout_ms", 60000),
                        true,
                        false)) {
            saveActive(job, 6, committed, target);
            queue(
                    auth(NEXT_TOKEN),
                    reply(target, 200, "plain", "", resultPage(2, 1, 2, second), NEXT_TOKEN),
                    reply(
                            executionRoute(D),
                            200,
                            "plain",
                            "",
                            resultPage(1, 1, 1, sibling),
                            NEXT_TOKEN));
            good(engine.restoreJob(job.configName, job.id));
            List<ObjectNode> expected = new ArrayList<>(committed);
            expected.add(out("executions", "second", B, START, second));
            expected.add(out("executions", "first", D, END, sibling));
            assertOutput(job, expected);
        }
        assertTrace(
                wire(AUTH),
                wire(CONSUMERS),
                wire(connectionRoute(A)),
                wire(SYNCS),
                page(target, 1, 1),
                page(target, 2, 1),
                wire(AUTH),
                page(target, 2, 1),
                page(executionRoute(D), 1, 1));
    }

    @Test
    void refusesChangedAuthScopeOnRestoreAndUsesNoProviderRequestsForTerminalState()
            throws Exception {
        reset(auth(TOKEN), stall(CONSUMERS));
        try (Job original =
                        job(
                                List.of("consumers"),
                                options().put("request_timeout_ms", 60000),
                                true,
                                false);
                Job updated =
                        job(
                                List.of("consumers"),
                                options()
                                        .put(
                                                "auth_account_id",
                                                "00000000-0000-4000-A000-000000000002"),
                                true,
                                false)) {
            saveActive(original, 2, List.of(), CONSUMERS);
            bad(engine.restoreJob(updated.configName, original.id), "read scope changed");
        }
        assertTrace(wire(AUTH), wire(CONSUMERS));
        ObjectNode expected = out("consumers", A, null, null, consumer(A));
        reset(auth(TOKEN), ok(CONSUMERS, rows(consumer(A))));
        try (Job job = job(List.of("consumers"), options(), true, true)) {
            saveActive(job, 2, List.of(expected), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(job.id)),
                        "restored anchor remains active");
                long before = engine.checkpointCount(job.id);
                await(
                        () -> engine.checkpointCount(job.id) > before,
                        "restored anchor completes new checkpoint");
                assertOutput(job, List.of(expected));
                assertTrace(wire(AUTH), wire(CONSUMERS));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void recoversOnlyUnpublishedRowsAndRejectsChangesToFilteredInputsOrOrdering() throws Exception {
        String route = executionRoute(B);
        ArrayNode data = rows(execution("excluded", "2024-12-01T00:00:00Z"));
        List<String> ids = new ArrayList<>();
        for (int i = 1; i <= 33; i++) {
            ids.add("execution-" + i);
            data.add(execution(ids.get(i - 1), START));
        }
        ObjectNode stable = resultPage(1, 50, 34);
        stable.set("items", data);
        ObjectNode changed = stable.deepCopy();
        ((ObjectNode) changed.get("items").get(0)).put("detail", "filtered and changed");
        ObjectNode reordered = stable.deepCopy();
        ArrayNode items = (ArrayNode) reordered.get("items");
        items.set(1, data.get(2));
        items.set(2, data.get(1));
        reset(
                auth(TOKEN),
                ok(SYNCS, rows(sync(B))),
                ok(route, stable),
                auth(TOKEN),
                ok(route, stable),
                auth(TOKEN),
                ok(route, changed),
                auth(TOKEN),
                ok(route, reordered));
        ChiftRecoveryProbe reader = recovery(List.of("executions"));
        List<String> prefix = new ArrayList<>();
        byte[] state = reader.firstChunk(prefix);
        assertEquals(ids.subList(0, 32), prefix);
        String saved = new String(state, StandardCharsets.ISO_8859_1);
        assertFalse(
                saved.contains(TOKEN)
                        || saved.contains(CLIENT_SECRET)
                        || saved.contains("execution-1"));
        List<String> restored = new ArrayList<>();
        reader.consume(state, restored);
        assertEquals(ids.subList(32, 33), restored);
        for (int i = 0; i < 2; i++) {
            List<String> fresh = new ArrayList<>();
            Exception invalid = assertThrows(Exception.class, () -> reader.consume(state, fresh));
            assertTrue(invalid.getMessage().contains("provider page changed since checkpoint"));
            assertTrue(fresh.isEmpty());
        }
        assertTrace(
                wire(AUTH),
                wire(SYNCS),
                page(route, 1, 50),
                wire(AUTH),
                page(route, 1, 50),
                wire(AUTH),
                page(route, 1, 50),
                wire(AUTH),
                page(route, 1, 50));
    }

    @Test
    void restoresUnassignedOwnershipAndSeparatelyFinishesEmptyOrCancelledReaders()
            throws Exception {
        reset();
        ChiftRecoveryProbe reader = recovery(List.of("consumers"));
        byte[] owner = reader.recoverUnassigned();
        reader.finishWithoutAssignment();
        List<String> result = new ArrayList<>();
        reader.stopBeforeAssignment(result);
        assertTrue(result.isEmpty());
        assertTrace();
        queue(auth(TOKEN), ok(CONSUMERS, rows(consumer(A))));
        reader.consume(owner, result);
        assertEquals(List.of(A), result);
        assertTrace(wire(AUTH), wire(CONSUMERS));
    }

    private void rejectOne(
            String scripted,
            List<String> resources,
            ObjectNode settings,
            Seen expected,
            String diagnostic)
            throws Exception {
        reset(auth(TOKEN), scripted);
        try (Job job = job(resources, settings, false, false)) {
            bad(engine.executeJob(job.configName), diagnostic);
            assertOutput(job, List.of());
        }
        assertTrace(wire(AUTH), expected);
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

    private ChiftRecoveryProbe recovery(List<String> resources) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new ChiftRecoveryProbe(
                url.toString(),
                certificate.toString(),
                CLIENT_ID,
                CLIENT_SECRET,
                ACCOUNT_ID,
                resources);
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
        String name = "chift-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "chift_data")
                        .put("auth_client_id", CLIENT_ID)
                        .put("auth_client_secret", CLIENT_SECRET)
                        .put("auth_account_id", ACCOUNT_ID)
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
                        + "source {\n Chift "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=chift_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"chift_$"
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

    private static ObjectNode consumer(String id) {
        return options().put("consumerid", id).put("name", "consumer");
    }

    private static ObjectNode connection(String id) {
        return options().put("connectionid", id).put("status", "ACTIVE");
    }

    private static ObjectNode sync(String id) {
        return options().put("syncid", id).put("name", "sync");
    }

    private static ObjectNode execution(String id, String started) {
        return options().put("id", id).put("start", started).putNull("end");
    }

    private static String connectionRoute(String id) {
        return CONSUMERS + "/" + id + "/connections";
    }

    private static String executionRoute(String id) {
        return SYNCS + "/" + id + "/executions";
    }

    private static ObjectNode resultPage(int number, int size, long total, JsonNode... records) {
        return object("items", rows(records))
                .put("page", number)
                .put("size", size)
                .put("total", total);
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
            String resource, String id, String parent, String time, ObjectNode payload) {
        ObjectNode row =
                options()
                        .put("resource", resource)
                        .put("record_id", id)
                        .put("parent_id", parent)
                        .put("started_at", time);
        row.set("payload", payload);
        return row;
    }

    private static String auth(String value) {
        return auth(value, 1800, 1800, "primary");
    }

    private static String auth(String value, int relative, int absolute, String slot) {
        return reply(
                AUTH,
                200,
                "token_" + absolute,
                "",
                options()
                        .put("access_token", value)
                        .put("token_type", "bearer")
                        .put("expires_in", relative)
                        .put("expires_on", 0),
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
        return reply(path, 200, "plain", "", body);
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
            assertEquals(AUTH.equals(URI.create(parts[2]).getRawPath()) ? "POST" : "GET", parts[1]);
            assertEquals("true", parts[4], "expected scoped Bearer or exact token JSON body");
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

    private static Seen page(String path, int number, int size) {
        return new Seen(
                path, Map.of("page", Integer.toString(number), "size", Integer.toString(size)), 0L);
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
                        || visible.contains(CLIENT_SECRET)
                        || visible.contains(ACCOUNT_ID),
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
        private final String id = ChiftIT.id();

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
                                    + "' -maxdepth 1 -type f -name 'chift_*' -exec cat {} +; fi");
            assertEquals(0, files.getExitCode(), files.getStderr());
            List<ObjectNode> output = new ArrayList<>();
            for (String line : files.getStdout().split("\\R")) {
                if (!line.isBlank()) {
                    ObjectNode row = (ObjectNode) JSON.readTree(line);
                    assertTrue(row.path("payload").isTextual());
                    row.set("payload", JSON.readTree(row.path("payload").textValue()));
                    // LocalFile can omit null columns. Compare the logical fixed schema.
                    for (String nullable : List.of("parent_id", "started_at")) {
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
            // The Chift pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}

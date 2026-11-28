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
package org.apache.seatunnel.e2e.connector.chargify;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.chargify.ChargifyRecoveryProbe;
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
 * Runs Chargify through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class ChargifyIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String KEY = "synthetic-chargify-key+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "chargify-peer";
    private static final String PEM = "/tmp/chargify.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-chargify.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String CUSTOMERS = "/customers.json";
    private static final String SUBSCRIPTIONS = "/subscriptions.json";
    private static final String INVOICES = "/invoices.json";
    private static final String COUPONS = "/coupons.json";
    private static final String TRANSACTIONS = "/transactions.json";
    private static final List<String> ALL =
            List.of("customers", "subscriptions", "invoices", "coupons", "transactions");
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
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass chargify-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:chargify-redirect-host,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass chargify-fixture -keystore ";
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
                        + ChargifyPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + ChargifyPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = ChargifyPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "chargify-other-host", "chargify-redirect-host")
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
    void persistsAllFiveNativeWrappersWithExactBillingValues() throws Exception {
        ObjectNode customer =
                record(12)
                        .put("name", "订阅者 🧭")
                        .putNull("cc_emails")
                        .put("created_at", "2024-07-01T23:59:59-04:00");
        ObjectNode subscription = record(13).putNull("product").put("state", "active");
        subscription.set("customer", customer.deepCopy());
        subscription.put("balance_in_cents", new BigInteger("9223372036854775808"));
        ObjectNode invoice =
                options()
                        .put("uid", "inv_unique-_42")
                        .put("total_amount", "48.002")
                        .put("memo", "原样")
                        .putNull("paid_date");
        invoice.putObject("shipping_address").putNull("line2");
        ObjectNode coupon = record(14).put("percentage", "12.3456").putNull("archived_at");
        coupon.put("amount", new BigDecimal("123456789012345678.123456789000"));
        coupon.putArray("coupon_restrictions").addObject().put("item_type", "Component");
        ObjectNode transaction =
                record(15)
                        .put("transaction_type", "payment")
                        .put("card_number", "XXXX-XXXX-XXXX-1")
                        .put("amount_in_cents", 555)
                        .put("created_at", "2026-10-01T10:00:00+05:30")
                        .putNull("parent_id");
        reset(
                ok(CUSTOMERS, wrapped("customer", customer)),
                ok(CUSTOMERS, rows()),
                ok(SUBSCRIPTIONS, wrapped("subscription", subscription)),
                ok(SUBSCRIPTIONS, rows()),
                ok(INVOICES, envelope("invoices", invoice)),
                ok(INVOICES, envelope("invoices")),
                ok(COUPONS, wrapped("coupon", coupon)),
                ok(COUPONS, rows()),
                ok(TRANSACTIONS, wrapped("transaction", transaction)),
                ok(TRANSACTIONS, rows()));
        try (Job job = job(ALL, options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("customers", "12", customer),
                            out("subscriptions", "13", subscription),
                            out("invoices", "inv_unique-_42", invoice),
                            out("coupons", "14", coupon),
                            out("transactions", "15", transaction)));
        }
        assertTrace(
                wire(CUSTOMERS, 1),
                wire(CUSTOMERS, 2),
                wire(SUBSCRIPTIONS, 1),
                wire(SUBSCRIPTIONS, 2),
                wire(INVOICES, 1),
                wire(INVOICES, 2),
                wire(COUPONS, 1),
                wire(COUPONS, 2),
                wire(TRANSACTIONS, 1),
                wire(TRANSACTIONS, 2));
    }

    @Test
    void continuesAfterShortPagesAndIgnoresProviderDestinationLinks() throws Exception {
        ObjectNode first = record(1);
        ObjectNode second =
                record(2).put("reference", "https://chargify-redirect-host:18443/never-fetch");
        ObjectNode third = record(3);
        ObjectNode fourth = record(4);
        reset(
                ok(
                        CUSTOMERS,
                        wrapped("customer", first, second),
                        "<https://chargify-redirect-host:18443/never-follow>; rel=\"next\""),
                ok(CUSTOMERS, wrapped("customer", third)),
                ok(CUSTOMERS, wrapped("customer", fourth)),
                ok(CUSTOMERS, rows()));
        try (Job job = job(List.of("customers"), options().put("page_size", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("customers", "1", first),
                            out("customers", "2", second),
                            out("customers", "3", third),
                            out("customers", "4", fourth)));
        }
        assertTrace(
                wire(CUSTOMERS, 1, 2),
                wire(CUSTOMERS, 2, 2),
                wire(CUSTOMERS, 3, 2),
                wire(CUSTOMERS, 4, 2));
    }

    @Test
    void selectsOnlyRequestedResourcesAndKeepsRegionAndSiteIdentity() throws Exception {
        ObjectNode invoice = options().put("uid", "uid-1");
        reset(ok(INVOICES, envelope("invoices", invoice)), ok(INVOICES, envelope("invoices")));
        try (Job job =
                job(
                        List.of("invoices"),
                        options().put("region", "eu").put("site", "EU-Sandbox"),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            out("invoices", "uid-1", invoice)
                                    .put("region", "eu")
                                    .put("site", "eu-sandbox")));
        }
        assertTrace(wire(INVOICES, 1), wire(INVOICES, 2));
    }

    @Test
    void handlesEmptyResourcesIndependentlyAndPreservesTheConfiguredBudget() throws Exception {
        reset(ok(CUSTOMERS, rows()), ok(INVOICES, envelope("invoices")), ok(COUPONS, rows()));
        try (Job job =
                job(
                        List.of("invoices", "customers", "coupons"),
                        options().put("max_requests", 3),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(CUSTOMERS, 1), wire(INVOICES, 1), wire(COUPONS, 1));
        reset(ok(CUSTOMERS, wrapped("customer", record(1))));
        failJob(List.of("customers"), options().put("max_requests", 1), "max_requests exhausted");
        assertTrace(wire(CUSTOMERS, 1));
    }

    @Test
    void rejectsAdjacentRepeatedIdCohortsEvenWithNewPayloadAndReversedOrdering() throws Exception {
        reset(
                ok(CUSTOMERS, wrapped("customer", record(31), record(32))),
                ok(CUSTOMERS, wrapped("customer", record(32).put("name", "changed"), record(31))));
        failJob(
                List.of("customers"),
                options().put("page_size", 2),
                "adjacent pages repeat the same provider identifiers");
        assertTrace(wire(CUSTOMERS, 1, 2), wire(CUSTOMERS, 2, 2));
    }

    @Test
    void refusesUnsupportedLegacyBillingResponsesInsteadOfSubstitutingOtherPayments()
            throws Exception {
        for (int status : List.of(404, 422)) {
            rejectOne(
                    reply(
                            TRANSACTIONS,
                            status,
                            "plain",
                            "",
                            options().put("error", HIDDEN_MESSAGE)),
                    List.of("transactions"),
                    options(),
                    wire(TRANSACTIONS, 1),
                    "HTTP " + status + " from the Legacy Statement Billing route");
        }
        rejectOne(
                ok(TRANSACTIONS, envelope("transactions", record(8))),
                List.of("transactions"),
                options(),
                wire(TRANSACTIONS, 1),
                "expected the native collection array");
    }

    @Test
    void validatesEachPageBeforeEmissionAndRejectsOversizedOrAmbiguousRecords() throws Exception {
        for (JsonNode invalid :
                List.of(
                        wrapped("customer", record(1), record(1)),
                        wrapped("customer", record(1), record(0)),
                        rows(object("customer", record(1)), object("transactions", record(2))),
                        wrapped("customer", record(1), rows(record(2))),
                        wrapped("customer", record(1), options().put("id", "2")),
                        envelope("customer", record(1)))) {
            rejectOne(
                    ok(CUSTOMERS, invalid),
                    List.of("customers"),
                    options(),
                    wire(CUSTOMERS, 1),
                    "Chargify customers");
        }
        rejectOne(
                ok(CUSTOMERS, wrapped("customer", record(1), record(2))),
                List.of("customers"),
                options().put("page_size", 1),
                wire(CUSTOMERS, 1, 1),
                "expected the native collection array within page_size");
        for (JsonNode wrong :
                List.of(
                        rows(options().put("uid", "valid")),
                        envelope("invoices", options().putNull("uid")),
                        envelope("invoices", options().put("uid", "  ")),
                        envelope("invoices", object("invoice", options().put("uid", "wrapped"))))) {
            rejectOne(
                    ok(INVOICES, wrong),
                    List.of("invoices"),
                    options(),
                    wire(INVOICES, 1),
                    "Chargify invoices");
        }
    }

    @Test
    void rejectsUnsafeSiteKeysAndConfigurationsBeforeOpeningAnyConnection() throws Exception {
        reset();
        for (String value :
                List.of(
                        "user@host",
                        "https://merchant",
                        "merchant.chargify.com",
                        "merchant:18443",
                        "merchant/other",
                        "-lead",
                        "a".repeat(64))) {
            failJob(ALL, options().put("site", value), "expected one DNS site label");
        }
        failJob(ALL, options().put("access_key", "first:second"), "without whitespace or a colon");
        failJob(ALL, options().put("region", "other"), "expected us or eu");
        failJob(ALL, options().put("page_size", 201), "page_size");
        failJob(
                ALL,
                options().put("endpoint", "http://chargify-peer:18443"),
                "expected a bare HTTPS origin");
        failJob(List.of("customers", "customers"), options(), "choose each resource only once");
        failJob(List.of("usage"), options(), "unsupported resource name");
        assertTrace();
    }

    @Test
    void refusesProviderErrorsAndPartialStatusesWithoutLeakingBasicCredentials() throws Exception {
        for (int status : List.of(400, 401, 403, 404, 204, 206, 422)) {
            rejectOne(
                    frame(
                            CUSTOMERS,
                            status,
                            "binary",
                            "",
                            HIDDEN_MESSAGE.getBytes(StandardCharsets.UTF_8)),
                    List.of("customers"),
                    options(),
                    wire(CUSTOMERS, 1),
                    "HTTP " + status);
        }
    }

    @Test
    void honorsRetryAfterAndDoesNotImmediatelyRetryAnUnspecifiedRateLimit() throws Exception {
        reset(reply(CUSTOMERS, 429, "plain", "2", options()), ok(CUSTOMERS, rows()));
        try (Job job = job(List.of("customers"), options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(CUSTOMERS, 1), wire(CUSTOMERS, 1));
        List<Seen> attempts = seen();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(attempts.get(1).at - attempts.get(0).at) >= 1980);
        for (String retry : List.of("", "10")) {
            rejectOne(
                    reply(CUSTOMERS, 429, "plain", retry, options()),
                    List.of("customers"),
                    options().put("request_timeout_ms", 3000),
                    wire(CUSTOMERS, 1),
                    "retry delay exceeds request deadline");
        }
        rejectOne(
                reply(CUSTOMERS, 503, "plain", "0", options()),
                List.of("customers"),
                options().put("max_attempts", 1),
                wire(CUSTOMERS, 1),
                "HTTP 503 retry budget exhausted");
    }

    @Test
    void verifiesJsonMediaUtf8CompressionByteLimitAndDuplicateFields() throws Exception {
        ObjectNode value = record(8).put("name", "华北");
        reset(reply(CUSTOMERS, 200, "gzip", "", wrapped("customer", value)), ok(CUSTOMERS, rows()));
        try (Job job = job(List.of("customers"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("customers", "8", value)));
        }
        assertTrace(wire(CUSTOMERS, 1), wire(CUSTOMERS, 2));
        rejectOne(
                reply(
                        CUSTOMERS,
                        200,
                        "gzip",
                        "",
                        wrapped("customer", record(1).put("name", "x".repeat(8192)))),
                List.of("customers"),
                options().put("max_response_bytes", 1024),
                wire(CUSTOMERS, 1),
                "response exceeds max_response_bytes");
        rejectOne(
                reply(CUSTOMERS, 200, "unsupported", "", wrapped("customer", value)),
                List.of("customers"),
                options(),
                wire(CUSTOMERS, 1),
                "unsupported response compression");
        rejectOne(
                reply(CUSTOMERS, 200, "binary", "", wrapped("customer", value)),
                List.of("customers"),
                options(),
                wire(CUSTOMERS, 1),
                "expected Content-Type application/json");
        for (byte[] bytes :
                List.of(
                        new byte[] {'[', (byte) 0xFF, ']'},
                        "[{\"customer\":{\"id\":1,\"id\":2}}]".getBytes(StandardCharsets.UTF_8),
                        "[] {}".getBytes(StandardCharsets.UTF_8))) {
            rejectOne(
                    frame(CUSTOMERS, 200, "plain", "", bytes),
                    List.of("customers"),
                    options(),
                    wire(CUSTOMERS, 1),
                    "single valid UTF-8 JSON response");
        }
    }

    @Test
    void retriesAClosedBodyAgainstTheSameNumberedPageBeforeEmission() throws Exception {
        reset(
                reply(CUSTOMERS, 200, "truncated", "", wrapped("customer", record(1))),
                ok(CUSTOMERS, wrapped("customer", record(2))),
                ok(CUSTOMERS, rows()));
        try (Job job = job(List.of("customers"), options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(out("customers", "2", record(2))));
        }
        assertTrace(wire(CUSTOMERS, 1), wire(CUSTOMERS, 1), wire(CUSTOMERS, 2));
    }

    @Test
    void enforcesTlsTrustAndHostnameAndDoesNotFollowARedirect() throws Exception {
        reset();
        failJob(
                List.of("customers"),
                options().put("trust_certificate", OTHER_PEM),
                "TLS certificate or hostname");
        failJob(
                List.of("customers"),
                options().put("endpoint", "https://chargify-other-host:18443"),
                "TLS certificate or hostname");
        assertTrace();
        rejectOne(
                reply(CUSTOMERS, 302, "redirect", "", options()),
                List.of("customers"),
                options(),
                wire(CUSTOMERS, 1),
                "HTTP 302");
    }

    @Test
    void stopsAStalledPageAtDeadlineAndCancellationDoesNotStartTheNextResource() throws Exception {
        rejectOne(
                stall(CUSTOMERS),
                List.of("customers"),
                options().put("request_timeout_ms", 2500),
                wire(CUSTOMERS, 1),
                "request deadline exceeded");
        await(
                () -> trace().contains("disconnect\t" + CUSTOMERS),
                "deadline closed active HTTP body");
        reset(stall(CUSTOMERS));
        try (Job job =
                job(
                        List.of("customers", "invoices"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 1, "active customer request");
            } finally {
                cancel(job.id, running);
            }
            await(
                    () -> trace().contains("disconnect\t" + CUSTOMERS),
                    "cancellation closed HTTP body");
            assertOutput(job, List.of());
        }
        assertTrace(wire(CUSTOMERS, 1));
    }

    @Test
    void resumesTheCurrentPageAndSubsequentResourceWithoutReplayingCommittedData()
            throws Exception {
        ObjectNode first = record(1);
        ObjectNode second = record(2);
        ObjectNode invoice = options().put("uid", "resume-inv");
        List<ObjectNode> committed = List.of(out("customers", "1", first));
        reset(ok(CUSTOMERS, wrapped("customer", first)), stall(CUSTOMERS));
        try (Job job =
                job(
                        List.of("customers", "invoices"),
                        options().put("request_timeout_ms", 60000),
                        true,
                        false)) {
            saveActive(job, 2, committed, CUSTOMERS);
            queue(
                    ok(CUSTOMERS, wrapped("customer", second)),
                    ok(CUSTOMERS, rows()),
                    ok(INVOICES, envelope("invoices", invoice)),
                    ok(INVOICES, envelope("invoices")));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(
                    job,
                    List.of(
                            out("customers", "1", first),
                            out("customers", "2", second),
                            out("invoices", "resume-inv", invoice)));
        }
        assertTrace(
                wire(CUSTOMERS, 1),
                wire(CUSTOMERS, 2),
                wire(CUSTOMERS, 2),
                wire(CUSTOMERS, 3),
                wire(INVOICES, 1),
                wire(INVOICES, 2));
    }

    @Test
    void doesNotReadRestoredOffsetsUnderADifferentBillingScope() throws Exception {
        reset(stall(CUSTOMERS));
        try (Job original =
                        job(
                                List.of("customers"),
                                options().put("request_timeout_ms", 60000),
                                true,
                                false);
                Job change =
                        job(
                                List.of("customers"),
                                options().put("region", "eu").put("page_size", 15),
                                true,
                                false)) {
            saveActive(original, 1, List.of(), CUSTOMERS);
            bad(engine.restoreJob(change.configName, original.id), "read scope changed");
        }
        assertTrace(wire(CUSTOMERS, 1));
    }

    @Test
    void restoresTerminalStateWhileTheIndependentPipelineKeepsCheckpointing() throws Exception {
        ObjectNode result = out("customers", "1", record(1));
        reset(ok(CUSTOMERS, wrapped("customer", record(1))), ok(CUSTOMERS, rows()));
        try (Job job = job(List.of("customers"), options(), true, true)) {
            saveActive(job, 2, List.of(result), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(job.id)),
                        "restored independent pipeline");
                long committed = engine.checkpointCount(job.id);
                await(
                        () -> engine.checkpointCount(job.id) > committed,
                        "restored checkpoint completes");
                assertOutput(job, List.of(result));
                assertTrace(wire(CUSTOMERS, 1), wire(CUSTOMERS, 2));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void restoresOnlyTheUnpublishedPageSuffixAndRejectsEveryChangedReplay() throws Exception {
        ArrayNode page = rows();
        List<String> rawIds = new ArrayList<>();
        for (int i = 1; i <= 33; i++) {
            rawIds.add(Integer.toString(i));
            page.add(object("customer", record(i).put("note", "private saved row")));
        }
        ArrayNode modified = page.deepCopy();
        modified.set(32, object("customer", record(33).putNull("changed")));
        ArrayNode reordered = page.deepCopy();
        reordered.set(31, page.get(32));
        reordered.set(32, page.get(31));
        reset(
                ok(CUSTOMERS, page),
                ok(CUSTOMERS, page),
                ok(CUSTOMERS, rows()),
                ok(CUSTOMERS, modified),
                ok(CUSTOMERS, reordered),
                ok(CUSTOMERS, rows()));
        ChargifyRecoveryProbe probe = recovery(List.of("customers"));
        List<String> prefix = new ArrayList<>();
        byte[] state = probe.firstChunk(prefix);
        assertEquals(rawIds.subList(0, 32), prefix);
        String serialized = new String(state, StandardCharsets.ISO_8859_1);
        assertFalse(serialized.contains(KEY) || serialized.contains("private saved row"));
        List<String> suffix = new ArrayList<>();
        probe.consume(state, suffix);
        assertEquals(rawIds.subList(32, 33), suffix);
        for (int i = 0; i < 3; i++) {
            List<String> rejected = new ArrayList<>();
            Exception error = assertThrows(Exception.class, () -> probe.consume(state, rejected));
            assertTrue(error.getMessage().contains("provider page changed since checkpoint"));
            assertTrue(rejected.isEmpty());
        }
        assertTrace(
                wire(CUSTOMERS, 1),
                wire(CUSTOMERS, 1),
                wire(CUSTOMERS, 2),
                wire(CUSTOMERS, 1),
                wire(CUSTOMERS, 1),
                wire(CUSTOMERS, 1));
    }

    @Test
    void resolvesRegionalOriginsAndFinalizesUnassignedOrCancelledReadersWithoutNetwork()
            throws Exception {
        assertEquals(
                "https://sandbox-01.chargify.com",
                ChargifyRecoveryProbe.regionalOrigin("SANDBOX-01", "us"));
        assertEquals(
                "https://sandbox-01.ebilling.maxio.com",
                ChargifyRecoveryProbe.regionalOrigin("SANDBOX-01", "eu"));
        reset();
        ChargifyRecoveryProbe probe = recovery(List.of("coupons"));
        byte[] assigned = probe.recoverUnassigned();
        probe.finishWithoutAssignment();
        List<String> rows = new ArrayList<>();
        probe.stopBeforeAssignment(rows);
        assertTrue(rows.isEmpty());
        assertTrace();
        queue(ok(COUPONS, wrapped("coupon", record(7))), ok(COUPONS, rows()));
        probe.consume(assigned, rows);
        assertEquals(List.of("7"), rows);
        assertTrace(wire(COUPONS, 1), wire(COUPONS, 2));
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

    private ChargifyRecoveryProbe recovery(List<String> resources) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new ChargifyRecoveryProbe(url.toString(), certificate.toString(), KEY, resources);
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
        String name = "chargify-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "chargify_data")
                        .put("access_key", KEY)
                        .put("site", "Sandbox-01")
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
                        + "source {\n Chargify "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=chargify_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"chargify_$"
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

    private static ArrayNode wrapped(String field, JsonNode... values) {
        ArrayNode result = rows();
        for (JsonNode value : values) {
            result.add(object(field, value));
        }
        return result;
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

    private static ObjectNode out(String resource, String id, ObjectNode value) {
        return object("payload", value)
                .put("resource", resource)
                .put("record_id", id)
                .put("site", "sandbox-01")
                .put("region", "us");
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
            assertEquals("true", parts[4], "expected exact Basic API key and x password");
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
            requests.add(new Seen(request.getRawPath(), query, Long.parseLong(parts[3])));
        }
        return requests;
    }

    private static Seen wire(String path, int page) {
        return wire(path, page, 200);
    }

    private static Seen wire(String path, int page, int size) {
        return new Seen(
                path,
                Map.of("per_page", Integer.toString(size), "page", Integer.toString(page)),
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
        String basic =
                Base64.getEncoder().encodeToString((KEY + ":x").getBytes(StandardCharsets.UTF_8));
        assertFalse(visible.contains(KEY) || visible.contains(basic), "access key leaked");
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

        private Seen(String route, Map<String, String> query, long at) {
            this.route = route;
            this.query = query;
            this.at = at;
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
        private final String id = ChargifyIT.id();

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
                                    + "' -maxdepth 1 -type f -name 'chargify_*' -exec cat {} +; fi");
            assertEquals(0, files.getExitCode(), files.getStderr());
            List<ObjectNode> output = new ArrayList<>();
            for (String line : files.getStdout().split("\\R")) {
                if (!line.isBlank()) {
                    ObjectNode row = (ObjectNode) JSON.readTree(line);
                    assertTrue(row.path("payload").isTextual());
                    row.set("payload", JSON.readTree(row.path("payload").textValue()));
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
            // The Chargify pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}

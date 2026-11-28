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
package org.apache.seatunnel.e2e.connector.carequalitycommission;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.carequalitycommission.CareQualityCommissionRecoveryProbe;
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
 * Verifies native CareQualityCommission exports through the real Java 11 Zeta engine and HTTPS.
 *
 * <p>Independent literal provider fixtures validate every request and compare committed full rows,
 * including provider and location provenance. No fixture reads production resource definitions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class CareQualityCommissionIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String BASE = "/public/v1";
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
        trust = identity("trusted", CareQualityCommissionPeer.HOST);
        wrongTrust = identity("untrusted", CareQualityCommissionPeer.HOST);
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
    void exportsAllSixResourcesWithExactProvenanceAndCompleteSyntheticRecords() throws Exception {
        Settings s = new Settings("all-six");
        String area =
                record("inspectionAreaId", "SAFE", ",\"name\":\"Safe\",\"status\":\"Superseded\"");
        String location =
                record(
                        "locationId",
                        "1-100",
                        ",\"locationName\":\"Inactive service\",\"providerId\":\"RX1\"");
        String detailedLocation =
                record(
                        "locationId",
                        "1-100",
                        ",\"providerId\":\"RX1\",\"registrationStatus\":\"Deregistered\",\"currentRatings\":{\"overall\":{\"rating\":\"Not Rated\"}},\"relationships\":[{\"relatedProviderId\":\"OLD\"}]");
        String provider = record("providerId", "RX1", ",\"providerName\":\"Synthetic provider\"");
        String association =
                record("organisationId", "1-100", ",\"organisationName\":\"Historical service\"");
        String detailedProvider =
                record(
                        "providerId",
                        "RX1",
                        ",\"locationIds\":[\"1-100\"],\"registrationStatus\":\"Registered\",\"reports\":[{\"reportUri\":\"https://cqc-trap.invalid/report.pdf\"}]");
        program(
                s.name,
                List.of(
                        get(BASE + "/inspection-areas", collection("inspectionAreas", area)),
                        page("locations", 1, 2, 1, location),
                        get(BASE + "/locations/1-100", detailedLocation),
                        page("providers", 1, 2, 1, provider),
                        get(
                                BASE + "/providers/RX1/locations",
                                collection("locations", association)),
                        get(BASE + "/providers/RX1", detailedProvider)));
        success(run(s));
        consumed(s.name, 6);
        assertRows(
                s.name,
                List.of(
                        expected("inspection_areas", "SAFE", null, null, area),
                        expected("locations", "1-100", "RX1", "1-100", location),
                        expected("locations_detailed", "1-100", "RX1", "1-100", detailedLocation),
                        expected("providers", "RX1", "RX1", null, provider),
                        expected("provider_locations", "1-100", "RX1", "1-100", association),
                        expected("providers_detailed", "RX1", "RX1", null, detailedProvider)));
    }

    @Test
    void readsOnlySelectedChildrenWhilePreservingDistinctProviderAssociations() throws Exception {
        Settings s = new Settings("child-only", "provider_locations");
        String a = record("providerId", "A", ""), b = record("providerId", "B", "");
        String child = record("organisationId", "same-location", "");
        program(
                s.name,
                List.of(
                        page("providers", 1, 2, 1, a, b),
                        get(BASE + "/providers/A/locations", collection("locations", child)),
                        get(BASE + "/providers/B/locations", collection("locations", child))));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name,
                List.of(
                        expected(
                                "provider_locations", "same-location", "A", "same-location", child),
                        expected(
                                "provider_locations",
                                "same-location",
                                "B",
                                "same-location",
                                child)));
    }

    @Test
    void honorsEmptyAndShortPagesWhenTotalPagesStillAdvertisesContinuation() throws Exception {
        Settings s = new Settings("empty-short", "locations");
        String a = record("locationId", "A", ""), b = record("locationId", "B", "");
        program(
                s.name,
                List.of(
                        page("locations", 1, 2, 3),
                        page("locations", 2, 2, 3, a),
                        page("locations", 3, 2, 3, b)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name,
                List.of(
                        expected("locations", "A", null, "A", a),
                        expected("locations", "B", null, "B", b)));
    }

    @Test
    void preservesAllThousandRecordsAtTheConfiguredMaximumPageSize() throws Exception {
        Settings s = new Settings("thousand", "providers");
        s.pageSize = 1000;
        List<String> records = new ArrayList<>();
        List<JsonNode> expected = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            String id = "R-" + i, row = record("providerId", id, "");
            records.add(row);
            expected.add(expected("providers", id, id, null, row));
        }
        program(s.name, List.of(page("providers", 1, 1000, 1, records.toArray(new String[0]))));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, expected);
    }

    @Test
    void completesEmptyRootAndHiddenParentCollectionsWithoutInventingDetails() throws Exception {
        Settings s = new Settings("empty-all");
        program(
                s.name,
                List.of(
                        get(BASE + "/inspection-areas", collection("inspectionAreas")),
                        page("locations", 1, 2, 0),
                        page("providers", 1, 2, 0)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(s.name, List.of());
    }

    @Test
    void rejectsOffOriginOrMutatedNextLinksBeforeAnyFollowUp() throws Exception {
        int index = 0;
        for (String link :
                List.of(
                        "https://cqc-trap.invalid/locations?page=2&perPage=2",
                        "/providers?page=2&perPage=2",
                        "/locations?page=1&perPage=2",
                        "/locations?page=2&perPage=1",
                        "/locations?page=2&perPage=2&token=trap",
                        "/locations?page=2&page=3&perPage=2")) {
            Settings s = new Settings("bad-link-" + index++, "locations");
            ObjectNode step = page("locations", 1, 2, 2);
            ObjectNode response = (ObjectNode) JSON.readTree(step.path("response").asText());
            response.put("nextPageUri", link);
            step.put("response", response.toString());
            fails(s, List.of(step), "CareQualityCommission");
        }
    }

    @Test
    void validatesSameOriginNextLinkAndSynthesizesItsOwnFixedTarget() throws Exception {
        Settings s = new Settings("good-link", "locations");
        String a = record("locationId", "A", "");
        ObjectNode first = page("locations", 1, 2, 2);
        ObjectNode body = (ObjectNode) JSON.readTree(first.path("response").asText());
        body.put(
                "nextPageUri",
                "https://api.service.cqc.org.uk/public/v1/locations?page=2&perPage=2");
        first.put("response", body.toString());
        program(s.name, List.of(first, page("locations", 2, 2, 2, a)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expected("locations", "A", null, "A", a)));
    }

    @Test
    void rejectsInvalidPageMetadataRepeatedPagesAndExplicitBudgets() throws Exception {
        String row = record("providerId", "RX1", "");
        Settings repeated = new Settings("repeated", "providers");
        fails(
                repeated,
                List.of(page("providers", 1, 2, 2, row), page("providers", 2, 2, 2, row)),
                "repeated");
        int index = 0;
        for (String response :
                List.of(
                        "{\"providers\":[],\"page\":2,\"perPage\":2,\"totalPages\":2}",
                        "{\"providers\":[],\"page\":1,\"perPage\":1,\"totalPages\":1}",
                        "{\"providers\":[],\"page\":1,\"perPage\":2,\"totalPages\":-1}",
                        "{\"providers\":[],\"page\":1,\"perPage\":2,\"totalPages\":1.5}",
                        "{\"providers\":[],\"page\":1,\"perPage\":2,\"totalPages\":1,\"total\":-1}")) {
            fails(
                    new Settings("bad-metadata-" + index++, "providers"),
                    List.of(get(BASE + "/providers", response).put("query", "page=1&perPage=2")),
                    "CareQualityCommission");
        }
        Settings pages = new Settings("page-budget", "providers");
        pages.pages = 1;
        fails(pages, List.of(page("providers", 1, 2, 2, row)), "max_pages_per_collection");
        Settings scan = new Settings("scan-budget", "providers_detailed");
        scan.scanPages = 1;
        fails(scan, List.of(page("providers", 1, 2, 1, row)), "max_pages_per_scan");
        Settings items = new Settings("item-budget", "inspection_areas");
        items.items = 1;
        fails(
                items,
                List.of(
                        get(
                                BASE + "/inspection-areas",
                                collection(
                                        "inspectionAreas",
                                        record("inspectionAreaId", "A", ""),
                                        record("inspectionAreaId", "B", "")))),
                "configured record count");
    }

    @Test
    void rejectsUnexpectedCompleteListPaginationAndDuplicateNativeIdentities() throws Exception {
        String row = record("inspectionAreaId", "SAFE", "");
        fails(
                new Settings("duplicate-native", "inspection_areas"),
                List.of(get(BASE + "/inspection-areas", collection("inspectionAreas", row, row))),
                "duplicate ID");
        fails(
                new Settings("unexpected-complete-next", "inspection_areas"),
                List.of(
                        get(
                                BASE + "/inspection-areas",
                                "{\"inspectionAreas\":[],\"nextPageUri\":\"/inspection-areas?page=2\"}")),
                "unexpected pagination");
        String parent = record("providerId", "RX1", "");
        fails(
                new Settings("wrong-provider-envelope", "provider_locations"),
                List.of(
                        page("providers", 1, 2, 1, parent),
                        get(
                                BASE + "/providers/RX1/locations",
                                "{\"providerId\":\"OTHER\",\"locations\":[]}")),
                "different provider");
        String location = record("locationId", "L1", ",\"providerId\":\"RX1\"");
        fails(
                new Settings("changed-location-provider", "locations_detailed"),
                List.of(
                        page("locations", 1, 2, 1, location),
                        get(BASE + "/locations/L1", location.replace("RX1", "OTHER"))),
                "different provider");
    }

    @Test
    void rejectsWrongDetailIdentityUnsafeParentsAndContradictoryProviderAssociations()
            throws Exception {
        String parent = record("providerId", "RX1", "");
        fails(
                new Settings("wrong-detail", "providers_detailed"),
                List.of(
                        page("providers", 1, 2, 1, parent),
                        get(BASE + "/providers/RX1", record("providerId", "OTHER", ""))),
                "CareQualityCommission");
        fails(
                new Settings("wrong-association", "provider_locations"),
                List.of(
                        page("providers", 1, 2, 1, parent),
                        get(
                                BASE + "/providers/RX1/locations",
                                collection(
                                        "locations",
                                        record(
                                                "organisationId",
                                                "L1",
                                                ",\"providerId\":\"OTHER\"")))),
                "CareQualityCommission");
        fails(
                new Settings("ambiguous-association", "provider_locations"),
                List.of(
                        page("providers", 1, 2, 1, parent),
                        get(
                                BASE + "/providers/RX1/locations",
                                collection(
                                        "locations",
                                        record("organisationId", "L1", ",\"locationId\":\"L2\"")))),
                "CareQualityCommission");
        fails(
                new Settings("unsafe-parent", "providers_detailed"),
                List.of(page("providers", 1, 2, 1, record("providerId", "../escape", ""))),
                "CareQualityCommission");
    }

    @Test
    void doesNotMistakeAuthenticationPermissionsOrDisappearedDetailsForEmptyData()
            throws Exception {
        for (int status : List.of(401, 403, 404, 500)) {
            Settings s = new Settings("http-" + status, "providers_detailed");
            fails(
                    s,
                    List.of(
                            page("providers", 1, 2, 1, record("providerId", "RX1", "")),
                            get(
                                            BASE + "/providers/RX1",
                                            "{\"message\":\"private-provider-message\"}")
                                    .put("status", status)),
                    "HTTP " + status);
        }
    }

    @Test
    void retriesOnlyWithinTheBudgetAndHonorsTheFullRetryAfterFloor() throws Exception {
        Settings s = new Settings("retry-floor", "inspection_areas");
        s.retries = 2;
        ObjectNode unavailable =
                get(BASE + "/inspection-areas", "{\"private-provider-message\":true}")
                        .put("status", 503)
                        .put("Retry-After", "0");
        ObjectNode limited = unavailable.deepCopy().put("status", 429).put("Retry-After", "2");
        program(
                s.name,
                List.of(
                        unavailable,
                        limited,
                        get(BASE + "/inspection-areas", collection("inspectionAreas"))));
        success(run(s));
        consumed(s.name, 3);
        List<String> times = engine.lines(s.name + ".times");
        assertTrue(
                Long.parseLong(times.get(2)) - Long.parseLong(times.get(1)) >= 2000L,
                times.toString());
        Settings exhausted = new Settings("retry-exhausted", "inspection_areas");
        exhausted.retries = 1;
        fails(exhausted, List.of(unavailable, unavailable), "2 attempts");
        Settings tooLong = new Settings("retry-bound", "inspection_areas");
        tooLong.retries = 1;
        tooLong.retryDelay = 250;
        fails(
                tooLong,
                List.of(unavailable.deepCopy().put("Retry-After", "2")),
                "max_retry_delay_ms");
    }

    @Test
    void appliesPacingAcrossDifferentResourcesWithoutDiscardingTheFirstResponse() throws Exception {
        Settings s = new Settings("pace", "inspection_areas", "locations");
        s.interval = 600;
        program(
                s.name,
                List.of(
                        get(BASE + "/inspection-areas", collection("inspectionAreas")),
                        page("locations", 1, 2, 0)));
        success(run(s));
        consumed(s.name, 2);
        List<String> times = engine.lines(s.name + ".times");
        assertTrue(
                Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 600L,
                times.toString());
    }

    @Test
    void distinguishesUnknownTrustFromTrustedWrongHostnameAndNeverFollowsRedirects()
            throws Exception {
        Settings wrongCa = new Settings("bad-ca", "inspection_areas");
        wrongCa.trust = wrongTrust;
        fails(wrongCa, List.of(), "certificate");
        selectIdentity("wrong-name");
        try {
            Settings wrongSan = new Settings("bad-san", "inspection_areas");
            wrongSan.trust = wrongNameTrust;
            fails(wrongSan, List.of(), "certificate");
        } finally {
            selectIdentity("trusted");
        }
        fails(
                new Settings("redirect", "inspection_areas"),
                List.of(
                        get(BASE + "/inspection-areas", "")
                                .put("mode", "redirect")
                                .put("status", 302)),
                "HTTP 302");
    }

    @Test
    void acceptsGzipWhileRejectingDecodedAndWireLimitsAndUnsupportedEncodings() throws Exception {
        Settings s = new Settings("gzip-valid", "inspection_areas");
        String row = record("inspectionAreaId", "SAFE", "");
        program(
                s.name,
                List.of(
                        get(BASE + "/inspection-areas", collection("inspectionAreas", row))
                                .put("mode", "gzip")));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("inspection_areas", "SAFE", null, null, row)));
        for (String mode : List.of("json", "gzip")) {
            Settings small = new Settings("body-" + mode, "inspection_areas");
            small.bytes = 256;
            fails(
                    small,
                    List.of(
                            get(
                                            BASE + "/inspection-areas",
                                            "{\"inspectionAreas\":[],\"padding\":\""
                                                    + "x".repeat(5000)
                                                    + "\"}")
                                    .put("mode", mode)),
                    "max_response_bytes");
        }
        fails(
                new Settings("encoding", "inspection_areas"),
                List.of(get(BASE + "/inspection-areas", "{}").put("encoding", "br")),
                "encoding");
    }

    @Test
    void rejectsMalformedJsonUtf8AndWrongEnvelopesWithoutPublishingRows() throws Exception {
        int i = 0;
        for (String body :
                List.of(
                        "null",
                        "{\"inspectionAreas\":null}",
                        "{\"inspectionAreas\":[null]}",
                        "{\"inspectionAreas\":[{\"inspectionAreaId\":1}]}",
                        "{\"inspectionAreas\":[]}{}",
                        "{\"inspectionAreas\":[],\"inspectionAreas\":[]}",
                        "{\"error\":\"private-provider-message\"}")) {
            Settings s = new Settings("json-" + i++, "inspection_areas");
            fails(s, List.of(get(BASE + "/inspection-areas", body)), "CareQualityCommission");
            assertRows(s.name, List.of());
        }
        fails(
                new Settings("utf8", "inspection_areas"),
                List.of(
                        get(BASE + "/inspection-areas", "")
                                .put(
                                        "bytes",
                                        Base64.getEncoder()
                                                .encodeToString(new byte[] {(byte) 0xc3, 0x28}))),
                "CareQualityCommission");
    }

    @Test
    void rejectsInvalidConfigurationBeforeSendingAnyCredential() throws Exception {
        int i = 0;
        for (String resource : List.of("missing", "PROVIDERS")) {
            fails(new Settings("bad-resource-" + i++, resource), List.of(), "resources");
        }
        Settings duplicate = new Settings("duplicate-selection", "providers", "providers");
        fails(duplicate, List.of(), "resources");
        Settings size = new Settings("bad-size", "providers");
        size.pageSize = 1001;
        fails(size, List.of(), "page_size");
        Settings token = new Settings("bad-token", "providers");
        token.token = "contains space";
        fails(token, List.of(), "token");
    }

    @Test
    void boundsATricklingBodyByTheTotalAttemptDeadline() throws Exception {
        Settings s = new Settings("trickle-deadline", "inspection_areas");
        s.request = 2000;
        s.operation = 3000;
        fails(
                s,
                List.of(
                        get(BASE + "/inspection-areas", collection("inspectionAreas"))
                                .put("mode", "trickle")),
                "CareQualityCommission");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(s.name + ".disconnect"));
        long started = Long.parseLong(engine.lines(s.name + ".started").get(0));
        long closed = Long.parseLong(engine.lines(s.name + ".disconnect").get(0));
        assertTrue(
                closed - started < 6000L, "a continually active body must not extend its deadline");
        assertRows(s.name, List.of());
    }

    @Test
    void cancelsAnActiveRealJobAndClosesItsSocketWithoutPartialOutput() throws Exception {
        Settings s = new Settings("cancel-active", "inspection_areas");
        program(
                s.name,
                List.of(
                        get(BASE + "/inspection-areas", collection("inspectionAreas"))
                                .put("mode", "trickle")));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () -> engine.executeJob("/carequalitycommission.conf", id, s.variables()));
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
    void restoresTheNextPageWithoutRepeatingAlreadyCommittedProviderRows() throws Exception {
        Settings s = new Settings("page-savepoint", "providers");
        String first = record("providerId", "R1", ""),
                second = record("providerId", "R2", ""),
                third = record("providerId", "R3", "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                page("providers", 1, 2, 2, first, second),
                                page("providers", 2, 2, 2, third).put("mode", "partial")));
        program("page-restored", List.of(page("providers", 2, 2, 2, third)));
        success(engine.restoreJob("/carequalitycommission.conf", id, s.variables()));
        consumed("page-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expected("providers", "R1", "R1", null, first),
                        expected("providers", "R2", "R2", null, second),
                        expected("providers", "R3", "R3", null, third)));
    }

    @Test
    void restoresInsideAParentPageAndRequestsOnlyItsUnfinishedChild() throws Exception {
        Settings s = new Settings("parent-savepoint", "provider_locations");
        String first = record("providerId", "R1", ""), second = record("providerId", "R2", "");
        String one = record("organisationId", "L1", ""), two = record("organisationId", "L2", "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                page("providers", 1, 2, 1, first, second),
                                get(BASE + "/providers/R1/locations", collection("locations", one)),
                                get(BASE + "/providers/R2/locations", collection("locations", two))
                                        .put("mode", "partial")));
        program(
                "parent-restored",
                List.of(
                        page("providers", 1, 2, 1, first, second),
                        get(BASE + "/providers/R2/locations", collection("locations", two))));
        success(engine.restoreJob("/carequalitycommission.conf", id, s.variables()));
        consumed("parent-restored", 2);
        assertRows(
                s.name,
                List.of(
                        expected("provider_locations", "L1", "R1", "L1", one),
                        expected("provider_locations", "L2", "R2", "L2", two)));
    }

    @Test
    void rejectsChangedParentInventoryBeforeRequestingAnUnfinishedDetail() throws Exception {
        Settings s = new Settings("changed-parent-savepoint", "locations_detailed");
        String first = record("locationId", "L1", ",\"providerId\":\"R1\""),
                second = record("locationId", "L2", ",\"providerId\":\"R2\"");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                page("locations", 1, 2, 1, first, second),
                                get(BASE + "/locations/L1", first),
                                get(BASE + "/locations/L2", second).put("mode", "partial")));
        program(
                "changed-parent-restored",
                List.of(page("locations", 1, 2, 1, first, second.replace("R2", "OTHER"))));
        Container.ExecResult result =
                engine.restoreJob("/carequalitycommission.conf", id, s.variables());
        assertNotEquals(0, result.getExitCode());
        assertTrue((result.getStdout() + result.getStderr()).contains("changed before replay"));
        consumed("changed-parent-restored", 1);
        assertRows(s.name, List.of(expected("locations_detailed", "L1", "R1", "L1", first)));
    }

    @Test
    void restoresATerminalSourceWithoutAnotherRequestOrDuplicateTransactionalFile()
            throws Exception {
        Settings s = new Settings("terminal-savepoint", "inspection_areas");
        String row = record("inspectionAreaId", "SAFE", "");
        List<JsonNode> expected = List.of(expected("inspection_areas", "SAFE", null, null, row));
        program(
                s.name,
                List.of(get(BASE + "/inspection-areas", collection("inspectionAreas", row))));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/carequalitycommission-terminal.conf", id, s.variables()));
        try {
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertRows(s.name, expected));
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
                tasks.submit(
                        () ->
                                engine.restoreJob(
                                        "/carequalitycommission-terminal.conf", id, s.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(id)));
            long baseline = engine.completedAcrossPipelines(id);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.completedAcrossPipelines(id) > baseline);
            consumed("terminal-restored", 0);
            assertRows(s.name, expected);
        } finally {
            if (!resumed.isDone()) {
                engine.cancelJob(id);
            }
            resumed.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void serializesThe32RowNestedPositionAndFailsOnChangedParentOrChildBodies() throws Exception {
        List<String> associations = new ArrayList<>();
        for (int i = 0; i < 33; i++) {
            associations.add(
                    record("organisationId", "L" + i, ",\"name\":\"synthetic-provider-record\""));
        }
        String first = record("providerId", "RX1", ""), second = record("providerId", "RX2", "");
        String body = collection("locations", associations.toArray(new String[0]));
        for (String mode : List.of("resume", "changed-child", "changed-parent")) {
            String name = "reader-" + mode;
            List<ObjectNode> steps =
                    new ArrayList<>(
                            List.of(
                                    page("providers", 1, 2, 1, first, second),
                                    get(BASE + "/providers/RX1/locations", body),
                                    page(
                                            "providers",
                                            1,
                                            2,
                                            1,
                                            mode.equals("changed-parent")
                                                    ? first.replace(
                                                            "9223372036854775808",
                                                            "9223372036854775809")
                                                    : first,
                                            second)));
            if (!mode.equals("changed-parent")) {
                steps.add(
                        get(
                                BASE + "/providers/RX1/locations",
                                mode.equals("changed-child")
                                        ? body.replace("L0", "CHANGED")
                                        : body));
            }
            if (mode.equals("resume")) {
                steps.add(get(BASE + "/providers/RX2/locations", collection("locations")));
            }
            program(name, steps);
            success(engine.probe(mode));
            consumed(name, steps.size());
        }
        program("reader-zero", List.of());
        success(engine.probe("zero"));
        consumed("reader-zero", 0);
    }

    private String saveBlocked(Settings settings, List<ObjectNode> steps) throws Exception {
        program(settings.name, steps);
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/carequalitycommission.conf", id, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(settings.name + ".started"));
            long baseline = engine.getCompletedCheckpointCount(id);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(id) > baseline);
            success(engine.savepointJob(id));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(settings.name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(id);
                running.cancel(true);
            }
        }
        consumed(settings.name, steps.size());
        return id;
    }

    private static String record(String identity, String id, String extra) {
        return "{\"" + identity + "\":\"" + id + "\"" + extra + EXTRA + "}";
    }

    private static String collection(String field, String... records) {
        return "{\"" + field + "\":" + array(records) + "}";
    }

    private static ObjectNode page(
            String resource, int page, int size, int totalPages, String... records) {
        String response =
                "{\""
                        + resource
                        + "\":"
                        + array(records)
                        + ",\"page\":"
                        + page
                        + ",\"perPage\":"
                        + size
                        + ",\"totalPages\":"
                        + totalPages
                        + "}";
        return get(BASE + "/" + resource, response)
                .put("query", "page=" + page + "&perPage=" + size);
    }

    private static String array(String... records) {
        return "[" + String.join(",", records) + "]";
    }

    private static ObjectNode get(String path, String response) {
        return JSON.createObjectNode().put("path", path).put("query", "").put("response", response);
    }

    private static JsonNode expected(
            String resource, String id, String provider, String location, String payload)
            throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put("id", id)
                        .put("provider_id", provider)
                        .put("location_id", location);
        row.set("record", JSON.readTree(payload));
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>(), wanted = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(parsed.isObject() && parsed.size() == 5, line);
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
        return engine.executeJob("/carequalitycommission.conf", jobId(), settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(CareQualityCommissionPeer.TOKEN), "API key leaked");
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
                                CareQualityCommissionPeer.PASSWORD,
                                "-keypass",
                                CareQualityCommissionPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, CareQualityCommissionPeer.PASSWORD.toCharArray());
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
        String trust = CareQualityCommissionIT.trust;
        String token = CareQualityCommissionPeer.TOKEN;
        int request = 60000;
        int operation = 70000;
        int retries;
        int interval;
        int retryDelay = 60000;
        int bytes = 8388608;
        int pages = 10000;
        int scanPages = 100000;
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
        private static final String PEER = "/tmp/carequalitycommission-peer";
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
            container.withExtraHost(CareQualityCommissionPeer.HOST, "127.0.0.1");
            container.withExtraHost("carequalitycommission-trap.invalid", "127.0.0.1");
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
                stage(container, CareQualityCommissionPeer.class);
                stage(container, CareQualityCommissionRecoveryProbe.class);
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
                throw new IOException(
                        "Cannot stage the Java CareQualityCommission fixture", failure);
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
                    CareQualityCommissionPeer.class.getName(),
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
                    CareQualityCommissionRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/carequalitycommission-" + name;
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

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
package org.apache.seatunnel.e2e.connector.ciscomeraki;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiRecoveryProbe;
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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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
 * Verifies native Cisco Meraki reads through the real Java 11 Zeta engine and HTTPS.
 *
 * <p>Independent literal provider fixtures verify authentication per API, Link pagination, and
 * nested organization/network traversal before comparing committed records.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class CiscoMerakiIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String ROOT = "/api/v1/organizations";
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
        trust = identity("trusted", CiscoMerakiPeer.HOST);
        wrongTrust = identity("untrusted", CiscoMerakiPeer.HOST);
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

    private static final String O1 = "11000001";
    private static final String O2 = "11000002";
    private static final String N1 = "N_100001";
    private static final String N2 = "N_100002";
    private static final String DC = "datacenters";
    private static final String NET = "organization_networks";
    private static final String DEV = "organization_devices";
    private static final String AUDIT = "organization_apiRequests";
    private static final String ADMIN = "organization_admins";
    private static final String SAML = "organization_saml";
    private static final String SETTINGS = "organization_network_settings";
    private static final String ORG = "organizations";

    @Test
    void exportsAllEightNativeResourcesWithExactProvenanceAndShapes() throws Exception {
        Instant lower = recent(), upper = lower.plusSeconds(60);
        Settings s = new Settings("all-eight").window(lower, upper);
        String organization = nativeRow(O1, ",\"name\":\"Alpha\"");
        String dc =
                "{\"continents\":[{\"name\":\"Test continent\",\"cities\":[{\"dc\":\"synthetic-dc\",\"latitude\":\"35.12\",\"range\":\"192.0.2.0/24\"}]}]"
                        + EXTRA
                        + "}";
        String network = network(O1, N1);
        String settings =
                "{\"securePort\":{\"enabled\":false},\"localStatusPageEnabled\":false"
                        + EXTRA
                        + "}";
        String controller =
                "{\"items\":[{\"serial\":\"S-1\",\"network\":{\"id\":\"N_100001\"}}],\"meta\":{\"counts\":{\"total\":1}}"
                        + EXTRA
                        + "}";
        String log = audit(lower, "\"showType\"");
        String admin = nativeRow("A1", ",\"lastActive\":" + quote(upper.toString()));
        String saml = "{\"enabled\":true,\"spInitiated\":{\"enabled\":false}" + EXTRA + "}";
        List<ObjectNode> steps =
                List.of(
                        page(ROOT, 3, organization),
                        get(dataCenter(O1), dc),
                        page(organizationPath(O1, "networks"), 3, network),
                        get(networkSettings(N1), settings),
                        get(
                                        organizationPath(
                                                O1, "wirelessController/overview/byDevice"),
                                        controller)
                                .put("query", "perPage=3"),
                        auditPage(O1, s, log),
                        get(organizationPath(O1, "admins"), array(admin)),
                        get(organizationPath(O1, "saml"), saml));
        program(s.name, steps);
        success(run(s));
        consumed(s.name, steps.size());
        assertRows(
                s.name,
                List.of(
                        expected(ORG, O1, O1, null, organization),
                        expected(DC, null, O1, null, dc),
                        expected(NET, N1, O1, N1, network),
                        expected(SETTINGS, null, O1, N1, settings),
                        expected(DEV, null, O1, null, controller),
                        expected(AUDIT, null, O1, null, log),
                        expected(ADMIN, "A1", O1, null, admin),
                        expected(SAML, null, O1, null, saml)));
        assertTrue(
                engine.rows(s.name).stream()
                        .anyMatch(line -> line.contains("123456789012345678.12345678900")));
    }

    @Test
    void traversesHiddenOrganizationsAndNetworksWithoutPublishingUnselectedParents()
            throws Exception {
        Settings s = new Settings("hidden-two-orgs", SETTINGS);
        String settings = "{\"namedVlans\":{\"enabled\":true}}";
        List<ObjectNode> steps =
                List.of(
                        page(ROOT, 3, nativeRow(O1, ""), nativeRow(O2, "")),
                        page(organizationPath(O1, "networks"), 3, network(O1, N1), network(O1, N2)),
                        get(networkSettings(N1), settings),
                        get(networkSettings(N2), settings),
                        page(organizationPath(O2, "networks"), 3, network(O2, N1)),
                        get(networkSettings(N1), settings));
        program(s.name, steps);
        success(run(s));
        consumed(s.name, steps.size());
        assertRows(
                s.name,
                List.of(
                        expected(SETTINGS, null, O1, N1, settings),
                        expected(SETTINGS, null, O1, N2, settings),
                        expected(SETTINGS, null, O2, N1, settings)));
    }

    @Test
    void retainsWirelessOverviewMetadataAndFollowsAnEmptyItemsPage() throws Exception {
        Settings s = new Settings("wireless-envelope", DEV);
        String route = organizationPath(O1, "wirelessController/overview/byDevice");
        String first = "{\"items\":[],\"meta\":{\"counts\":{\"total\":5}}" + EXTRA + "}";
        String last =
                "{\"items\":[{\"serial\":\"S2\",\"firmware\":{\"version\":\"synthetic\"}}],\"meta\":{\"counts\":{\"total\":5}}}";
        List<ObjectNode> steps =
                List.of(
                        page(ROOT, 3, nativeRow(O1, "")),
                        get(route, first)
                                .put("query", "perPage=3")
                                .put("Link", "<?startingAfter=wireless-next>; rel=next"),
                        get(route, last).put("query", "perPage=3&startingAfter=wireless-next"));
        program(s.name, steps);
        success(run(s));
        consumed(s.name, steps.size());
        assertRows(
                s.name,
                List.of(expected(DEV, null, O1, null, first), expected(DEV, null, O1, null, last)));
    }

    @Test
    void followsShortAndEmptyListPagesUsingBothOpaqueCursorDirections() throws Exception {
        Settings s = new Settings("link-short-empty", ORG);
        String first = nativeRow(O1, ""), last = nativeRow(O2, "");
        ObjectNode initial = page(ROOT, 3, first);
        initial.putArray("links")
                .add(
                        "<https://api.meraki.com"
                                + ROOT
                                + "?endingBefore=a%2B%2F%3D%2C%20b>; title=\"a, b; \\\"quoted\\\"\"; rel=\"NeXT alternate\"")
                .add("<https://ignored.invalid/events?key=sample>; rel=prev");
        ObjectNode second =
                page(ROOT, 3)
                        .put("query", "perPage=3&endingBefore=a%2B%2F%3D%2C%20b")
                        .put(
                                "Link",
                                "</api/v1/organizations?startingAfter=d%26z&perPage=3>; rel=next");
        ObjectNode third = page(ROOT, 3, last).put("query", "perPage=3&startingAfter=d%26z");
        program(s.name, List.of(initial, second, third));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name,
                List.of(expected(ORG, O1, O1, null, first), expected(ORG, O2, O2, null, last)));
    }

    @Test
    void rejectsNextLinksThatChangeHostPathPageSizeOrCursorOwnership() throws Exception {
        int i = 0;
        for (String next :
                List.of(
                        "<https://meraki-trap.invalid/private?startingAfter=p>; rel=next",
                        "<http://api.meraki.com/api/v1/organizations?startingAfter=p>; rel=next",
                        "<https://other@api.meraki.com/api/v1/organizations?startingAfter=p>; rel=next",
                        "</api/v1/other?startingAfter=p>; rel=next",
                        "<?startingAfter=p&perPage=4>; rel=next",
                        "<?startingAfter=p&endingBefore=q>; rel=next",
                        "<?startingAfter=p&startingAfter=q>; rel=next",
                        "<?startingAfter=p&token=foreign>; rel=next",
                        "<?startingAfter=p>; anchor=\"/api/v1/other\"; rel=next",
                        "<?startingAfter=p>; rel=next, <?startingAfter=q>; rel=next",
                        "<?startingAfter=%0A>; rel=next",
                        "<?perPage=3>; rel=next",
                        "<?startingAfter=p>; rel=\"unterminated")) {
            fails(
                    new Settings("bad-link-" + i++, ORG),
                    List.of(page(ROOT, 3).put("Link", next)),
                    "Link");
        }
    }

    @Test
    void rejectsCrossOrganizationContinuationsBeforeIssuingTheNextRequest() throws Exception {
        Settings s = new Settings("wrong-linked-parent", NET);
        fails(
                s,
                List.of(
                        page(ROOT, 3, nativeRow(O1, "")),
                        page(organizationPath(O1, "networks"), 3)
                                .put(
                                        "Link",
                                        "<"
                                                + organizationPath(O2, "networks")
                                                + "?startingAfter=token>; rel=next")),
                "Link");
    }

    @Test
    void rejectsCyclicContinuationsAndBoundsBothPageBudgets() throws Exception {
        ObjectNode first = page(ROOT, 3).put("Link", "<?startingAfter=A>; rel=next");
        ObjectNode second =
                page(ROOT, 3)
                        .put("query", "perPage=3&startingAfter=A")
                        .put("Link", "<?startingAfter=B>; rel=next");
        ObjectNode third =
                page(ROOT, 3)
                        .put("query", "perPage=3&startingAfter=B")
                        .put("Link", "<?startingAfter=%41>; rel=next");
        fails(new Settings("cycle", ORG), List.of(first, second, third), "repeated Link");
        Settings pages = new Settings("page-limit", ORG);
        pages.pages = 1;
        fails(pages, List.of(first), "max_pages_per_collection");
        Settings scan = new Settings("scan-limit", SAML);
        scan.scanPages = 1;
        fails(scan, List.of(page(ROOT, 3, nativeRow(O1, ""))), "max_pages_per_scan");
    }

    @Test
    void readsOneThousandAndOneOrganizationsAcrossTheMaximumPageSize() throws Exception {
        Settings s = new Settings("max-page", ORG);
        s.pageSize = 1000;
        List<String> objects = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (int i = 0; i <= 1000; i++) {
            String id = "ORG-" + i;
            String row = nativeRow(id, "");
            objects.add(row);
            wanted.add(expected(ORG, id, id, null, row));
        }
        program(
                s.name,
                List.of(
                        page(ROOT, 1000, objects.subList(0, 1000).toArray(new String[0]))
                                .put("Link", "<?startingAfter=1000>; rel=next"),
                        page(ROOT, 1000, objects.get(1000))
                                .put("query", "perPage=1000&startingAfter=1000")));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, wanted);
    }

    @Test
    void sendsNativeInclusiveAuditWindowAndKeepsSameTimeDuplicates() throws Exception {
        Instant lower = recent(), upper = lower.plusSeconds(10);
        Settings s = new Settings("audit-inclusive", AUDIT).window(lower, upper);
        String first = audit(lower, "\"start\""), last = audit(upper, "\"end\"");
        String middle = audit(lower.plusSeconds(1), "\"same-time\"");
        String offset =
                "{\"ts\":"
                        + quote(lower.atOffset(ZoneOffset.ofHours(-4)).toString())
                        + ",\"method\":\"GET\""
                        + EXTRA
                        + "}";
        ObjectNode page1 =
                auditPage(O1, s, audit(lower.minusSeconds(1), "\"old\""), first, offset)
                        .put("Link", "<?endingBefore=prev-opaque>; rel=next");
        ObjectNode page2 =
                auditPage(O1, s, middle, middle, last)
                        .put("query", auditQuery(s) + "&endingBefore=prev-opaque")
                        .put("Link", "<?startingAfter=last-opaque>; rel=next");
        ObjectNode page3 =
                auditPage(O1, s, audit(upper.plusSeconds(1), "\"future\""))
                        .put("query", auditQuery(s) + "&startingAfter=last-opaque");
        program(s.name, List.of(page(ROOT, 3, nativeRow(O1, "")), page1, page2, page3));
        success(run(s));
        consumed(s.name, 4);
        assertRows(
                s.name,
                List.of(
                        expected(AUDIT, null, O1, null, first),
                        expected(AUDIT, null, O1, null, offset),
                        expected(AUDIT, null, O1, null, middle),
                        expected(AUDIT, null, O1, null, middle),
                        expected(AUDIT, null, O1, null, last)));
    }

    @Test
    void refusesAContinuationThatChangesTheFrozenLogWindow() throws Exception {
        Instant lower = recent();
        Settings s = new Settings("audit-link-drift", AUDIT).window(lower, lower.plusSeconds(30));
        fails(
                s,
                List.of(
                        page(ROOT, 3, nativeRow(O1, "")),
                        auditPage(O1, s)
                                .put(
                                        "Link",
                                        "<?startingAfter=more&t0="
                                                + query(lower.minusSeconds(1).toString())
                                                + ">; rel=next")),
                "Link");
    }

    @Test
    void filtersAdministratorActivityLocallyAndKeepsUntimedRowsForAnUnfilteredRequest()
            throws Exception {
        Instant lower = recent(), upper = lower.plusSeconds(30);
        String oldest =
                nativeRow("old", ",\"lastActive\":" + quote(lower.minusSeconds(1).toString()));
        String inclusive = nativeRow("on", ",\"lastActive\":" + quote(lower.toString()));
        String noTime = nativeRow("never", ",\"lastActive\":null");
        String latest = nativeRow("end", ",\"lastActive\":" + quote(upper.toString()));
        for (boolean filtered : new boolean[] {false, true}) {
            Settings s = new Settings("admin-filter-" + filtered, ADMIN);
            if (filtered) {
                s.window(lower, upper);
            }
            List<ObjectNode> steps =
                    List.of(
                            page(ROOT, 3, nativeRow(O1, ",\"updatedAt\":\"2000-01-01T00:00:00Z\"")),
                            get(
                                    organizationPath(O1, "admins"),
                                    array(oldest, inclusive, noTime, latest)));
            program(s.name, steps);
            success(run(s));
            consumed(s.name, 2);
            List<JsonNode> wanted =
                    new ArrayList<>(
                            List.of(
                                    expected(ADMIN, "on", O1, null, inclusive),
                                    expected(ADMIN, "end", O1, null, latest)));
            if (!filtered) {
                wanted.add(expected(ADMIN, "old", O1, null, oldest));
                wanted.add(expected(ADMIN, "never", O1, null, noTime));
            }
            assertRows(s.name, wanted);
        }
    }

    @Test
    void rejectsExpiredReversedAndInvalidDateSelectionsBeforeAuthenticating() throws Exception {
        Instant lower = recent();
        Settings stale =
                new Settings("stale-window", AUDIT)
                        .window(lower.minus(35, ChronoUnit.DAYS), lower.minus(34, ChronoUnit.DAYS));
        fails(stale, List.of(), "31 days");
        Settings reversed =
                new Settings("reversed-window", ADMIN).window(lower, lower.minusSeconds(1));
        fails(reversed, List.of(), "end_date");
        Settings malformed = new Settings("bad-date", ADMIN).window(lower, lower);
        malformed.start = "not-a-date";
        fails(malformed, List.of(), "start_date");
    }

    @Test
    void failsOnWrongParentIdentityAndWrongNativeRecordShapes() throws Exception {
        fails(
                new Settings("cross-org-record", NET),
                List.of(
                        page(ROOT, 3, nativeRow(O1, "")),
                        page(organizationPath(O1, "networks"), 3, network(O2, N1))),
                "different organization");
        fails(
                new Settings("duplicate-org", ORG),
                List.of(page(ROOT, 3, nativeRow(O1, ""), nativeRow(O1, ""))),
                "duplicate");
        fails(new Settings("missing-org-id", ORG), List.of(page(ROOT, 3, "{}")), "native identity");
        fails(
                new Settings("invalid-saml", SAML),
                List.of(
                        page(ROOT, 3, nativeRow(O1, "")),
                        get(organizationPath(O1, "saml"), "{\"enabled\":\"true\"}")),
                "SAML");
        fails(
                new Settings("invalid-datacenter", DC),
                List.of(page(ROOT, 3, nativeRow(O1, "")), get(dataCenter(O1), "[]")),
                "continents");
        fails(
                new Settings("invalid-overview", DEV),
                List.of(
                        page(ROOT, 3, nativeRow(O1, "")),
                        get(
                                        organizationPath(
                                                O1, "wirelessController/overview/byDevice"),
                                        "{\"items\":[7]}")
                                .put("query", "perPage=3")),
                "nonrecord");
    }

    @Test
    void usesTheDefaultThirtyDayAuditWindowWhileDiscoveringOrganizationsIndependently()
            throws Exception {
        Settings s = new Settings("default-audit-window", AUDIT);
        String data = audit(recent(), "\"within-default\"");
        List<ObjectNode> steps =
                List.of(
                        page(ROOT, 3, nativeRow(O1, "")),
                        get(organizationPath(O1, "apiRequests"), array(data))
                                .put("query", "perPage=3")
                                .put("windowDays", 30));
        program(s.name, steps);
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expected(AUDIT, null, O1, null, data)));
    }

    @Test
    void completesAnEmptyOrganizationListWithoutCallingAnyChild() throws Exception {
        Settings s = new Settings("empty-organization-list");
        program(s.name, List.of(page(ROOT, 3)));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of());
    }

    @Test
    void limitsUnpaginatedAdministratorsAndRejectsUnexpectedPageSizes() throws Exception {
        Settings admins = new Settings("admin-limit", ADMIN);
        admins.items = 1;
        fails(
                admins,
                List.of(
                        page(ROOT, 3, nativeRow(O1, "")),
                        get(
                                organizationPath(O1, "admins"),
                                array(nativeRow("A1", ""), nativeRow("A2", "")))),
                "item limit");
        for (int value : new int[] {2, 1001}) {
            Settings bad = new Settings("invalid-page-size-" + value, ORG);
            bad.pageSize = value;
            fails(bad, List.of(), "page_size");
        }
    }

    @Test
    void reportsPermanentAndTransientStatusesWithoutExposingCredentialsOrBodies() throws Exception {
        for (int status : new int[] {401, 403, 404, 422, 500, 502, 503}) {
            fails(
                    new Settings("http-" + status, ORG),
                    List.of(
                            page(ROOT, 3)
                                    .put("status", status)
                                    .put(
                                            "response",
                                            "{\"errors\":[\"private-provider-message "
                                                    + CiscoMerakiPeer.TOKEN
                                                    + "\"]}")),
                    "HTTP " + status);
        }
        fails(
                new Settings("payload-error", SETTINGS),
                List.of(
                        page(ROOT, 3, nativeRow(O1, "")),
                        page(organizationPath(O1, "networks"), 3, network(O1, N1)),
                        get(networkSettings(N1), "{\"errors\":[\"private-provider-message\"]}")),
                "error object");
    }

    @Test
    void verifiesTlsTrustAndHostnamesAndNeverFollowsAnUnapprovedRedirect() throws Exception {
        Settings ca = new Settings("wrong-ca", ORG);
        ca.trust = wrongTrust;
        fails(ca, List.of(), "certificate");
        selectIdentity("wrong-name");
        Settings hostname = new Settings("wrong-host", ORG);
        hostname.trust = wrongNameTrust;
        fails(hostname, List.of(), "certificate");
        selectIdentity("trusted");
        fails(
                new Settings("redirect", ORG),
                List.of(page(ROOT, 3).put("status", 302).put("mode", "redirect")),
                "HTTP 302");
    }

    @Test
    void acceptsGzipAndBoundsBothCompressedAndExpandedBodySizes() throws Exception {
        Settings good = new Settings("gzip-good", ORG);
        String row = nativeRow(O1, "");
        program(good.name, List.of(page(ROOT, 3, row).put("mode", "gzip")));
        success(run(good));
        consumed(good.name, 1);
        assertRows(good.name, List.of(expected(ORG, O1, O1, null, row)));
        Settings wire = new Settings("wire-large", ORG);
        wire.bytes = 256;
        fails(
                wire,
                List.of(page(ROOT, 3, nativeRow(O1, ",\"large\":\"" + "a".repeat(1000) + "\""))),
                "max_response_bytes");
        Settings decoded = new Settings("decoded-large", ORG);
        decoded.bytes = 256;
        fails(
                decoded,
                List.of(
                        page(ROOT, 3, nativeRow(O1, ",\"large\":\"" + "a".repeat(1000) + "\""))
                                .put("mode", "gzip")),
                "max_response_bytes");
        fails(
                new Settings("unsupported-encoding", ORG),
                List.of(page(ROOT, 3).put("encoding", "br")),
                "encoding");
    }

    @Test
    void rejectsMalformedUtf8DuplicateKeysTrailingContentAndNonrecordArrays() throws Exception {
        int i = 0;
        for (String body :
                List.of(
                        "{\"notAnArray\":[]}",
                        "[true]",
                        "[",
                        "[{\"id\":\"one\",\"id\":\"two\"}]",
                        "[] []")) {
            fails(
                    new Settings("invalid-body-" + i++, ORG),
                    List.of(page(ROOT, 3).put("response", body)),
                    "Cisco Meraki");
        }
        byte[] illegal =
                new byte[] {
                    '[', '{', '"', 'i', 'd', '"', ':', '"', (byte) 0xc3, '(', '"', '}', ']'
                };
        fails(
                new Settings("invalid-utf8", ORG),
                List.of(page(ROOT, 3).put("bytes", Base64.getEncoder().encodeToString(illegal))),
                "Cisco Meraki");
    }

    @Test
    void honorsRetryAfterAndCapsRetryDelayAndAttemptBudget() throws Exception {
        Settings s = new Settings("quota-floor", ORG);
        s.retries = 1;
        ObjectNode throttled = page(ROOT, 3).put("status", 429).put("Retry-After", "2");
        program(s.name, List.of(throttled, page(ROOT, 3)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 1800);
        Settings wait = new Settings("retry-delay-limit", ORG);
        wait.retries = 3;
        wait.retryDelay = 250;
        fails(wait, List.of(throttled), "max_retry_delay_ms");
        Settings attempts = new Settings("retry-attempts", ORG);
        attempts.retries = 1;
        ObjectNode unavailable = page(ROOT, 3).put("status", 503);
        fails(attempts, List.of(unavailable, unavailable), "2 attempts");
    }

    @Test
    void pacesCrossResourceRequestsFromTheSameReader() throws Exception {
        Settings s = new Settings("pacing", SAML);
        s.interval = 600;
        program(
                s.name,
                List.of(
                        page(ROOT, 3, nativeRow(O1, "")),
                        get(organizationPath(O1, "saml"), "{\"enabled\":false}")));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 500);
    }

    @Test
    void rejectsInvalidKeyAndDuplicateResourceSelectionBeforeSendingTraffic() throws Exception {
        Settings credentials = new Settings("invalid-key", ORG);
        credentials.token = "synthetic-invalid header";
        fails(credentials, List.of(), "token");
        fails(new Settings("unknown-resource", "devices"), List.of(), "resources");
        fails(new Settings("duplicate-resource", ORG, ORG), List.of(), "resources");
    }

    @Test
    void interruptsAContinuousBodyAtItsRequestDeadline() throws Exception {
        Settings s = new Settings("request-deadline", ORG);
        s.request = 1500;
        s.operation = 4500;
        fails(s, List.of(page(ROOT, 3, nativeRow(O1, "")).put("mode", "trickle")), "HTTPS");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(s.name + ".closed"));
        assertRows(s.name, List.of());
    }

    @Test
    void cancelsARealEngineJobWithAnUnfinishedTlsResponse() throws Exception {
        Settings s = new Settings("cancel-job", ORG);
        program(s.name, List.of(page(ROOT, 3, nativeRow(O1, "")).put("mode", "trickle")));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(s.config(), id, s.variables()));
        try {
            Awaitility.await()
                    .atMost(25, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".started"));
            success(engine.cancelJob(id));
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> engine.exists(s.name + ".closed"));
            running.get(30, TimeUnit.SECONDS);
            consumed(s.name, 1);
            assertRows(s.name, List.of());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(id);
                running.cancel(true);
            }
        }
    }

    @Test
    void resumesAPinnedOrganizationContinuationWithoutReissuingTheCommittedPage() throws Exception {
        Settings s = new Settings("root-page-save", ORG);
        String first = nativeRow(O1, ""), last = nativeRow(O2, "");
        ObjectNode page1 = page(ROOT, 3, first).put("Link", "<?startingAfter=root-cont>; rel=next");
        ObjectNode page2 = page(ROOT, 3, last).put("query", "perPage=3&startingAfter=root-cont");
        String id = saveBlocked(s, List.of(page1, page2.deepCopy().put("mode", "partial")));
        program("root-page-resumed", List.of(page2));
        success(engine.restoreJob(s.config(), id, s.variables()));
        consumed("root-page-resumed", 1);
        assertRows(
                s.name,
                List.of(expected(ORG, O1, O1, null, first), expected(ORG, O2, O2, null, last)));
    }

    @Test
    void revalidatesBothParentPagesBeforeResumingTheOutstandingNetworkSettings() throws Exception {
        Settings s = new Settings("nested-save", ORG, NET, SETTINGS);
        String organization = nativeRow(O1, ""), network = network(O1, N1);
        String settings = "{\"securePort\":{\"enabled\":true}}";
        ObjectNode parent = page(ROOT, 3, organization);
        ObjectNode middle = page(organizationPath(O1, "networks"), 3, network);
        ObjectNode leaf = get(networkSettings(N1), settings);
        String id = saveBlocked(s, List.of(parent, middle, leaf.deepCopy().put("mode", "partial")));
        program("nested-resumed", List.of(parent, middle, leaf));
        success(engine.restoreJob(s.config(), id, s.variables()));
        consumed("nested-resumed", 3);
        assertRows(
                s.name,
                List.of(
                        expected(ORG, O1, O1, null, organization),
                        expected(NET, N1, O1, N1, network),
                        expected(SETTINGS, null, O1, N1, settings)));
    }

    @Test
    void rejectsChangedNestedParentBeforeFetchingTheOutstandingSettings() throws Exception {
        Settings s = new Settings("network-change-save", SETTINGS);
        ObjectNode parent = page(ROOT, 3, nativeRow(O1, ""));
        String route = organizationPath(O1, "networks");
        ObjectNode middle = page(route, 3, network(O1, N1));
        ObjectNode leaf =
                get(networkSettings(N1), "{\"localStatusPageEnabled\":true}")
                        .put("mode", "partial");
        String id = saveBlocked(s, List.of(parent, middle, leaf));
        program("changed-network-resumed", List.of(parent, page(route, 3, network(O1, N2))));
        Container.ExecResult result = engine.restoreJob(s.config(), id, s.variables());
        assertNotEquals(0, result.getExitCode());
        assertTrue((result.getStdout() + result.getStderr()).contains("checkpoint page changed"));
        consumed("changed-network-resumed", 2);
    }

    @Test
    void resumesThePendingAuditPageWithItsFrozenWindowAndNoDuplicateRows() throws Exception {
        Instant lower = recent();
        Settings s = new Settings("audit-page-save", AUDIT).window(lower, lower.plusSeconds(50));
        String one = audit(lower, "\"before\""), two = audit(lower.plusSeconds(2), "\"after\"");
        ObjectNode parent = page(ROOT, 3, nativeRow(O1, ""));
        ObjectNode first =
                auditPage(O1, s, one).put("Link", "<?startingAfter=audit-resume>; rel=next");
        ObjectNode second =
                auditPage(O1, s, two).put("query", auditQuery(s) + "&startingAfter=audit-resume");
        String id =
                saveBlocked(s, List.of(parent, first, second.deepCopy().put("mode", "partial")));
        program("audit-page-restored", List.of(parent, second));
        success(engine.restoreJob(s.config(), id, s.variables()));
        consumed("audit-page-restored", 2);
        assertRows(
                s.name,
                List.of(
                        expected(AUDIT, null, O1, null, one),
                        expected(AUDIT, null, O1, null, two)));
    }

    @Test
    void restoresTheTerminalSourceWithoutProviderTrafficOrDuplicateFiles() throws Exception {
        Settings s = new Settings("terminal-save", ORG);
        String row = nativeRow(O1, "");
        List<JsonNode> wanted = List.of(expected(ORG, O1, O1, null, row));
        program(s.name, List.of(page(ROOT, 3, row)));
        String id = jobId();
        String config = "/cisco-meraki-terminal.conf";
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(config, id, s.variables()));
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
                tasks.submit(() -> engine.restoreJob(config, id, s.variables()));
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
    void serializesExactly32Of33RecordsAndRejectsAnAlteredPageBeforeItsSuffix() throws Exception {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < 33; i++) {
            rows.add(nativeRow("B" + i, ",\"description\":\"synthetic-response-record\""));
        }
        for (String mode : List.of("resume", "changed")) {
            List<String> replay = new ArrayList<>(rows);
            if (mode.equals("changed")) {
                replay.set(0, replay.get(0).replace("B0", "CHANGED"));
            }
            String name = "reader-" + mode;
            program(
                    name,
                    List.of(
                            page(ROOT, 50, rows.toArray(new String[0])),
                            page(ROOT, 50, replay.toArray(new String[0]))));
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
                tasks.submit(() -> engine.executeJob(s.config(), id, s.variables()));
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

    private static String quote(String value) {
        return value == null ? "null" : JSON.getNodeFactory().textNode(value).toString();
    }

    private static String organizationPath(String organization, String tail) {
        return ROOT + "/" + organization + "/" + tail;
    }

    private static String dataCenter(String organization) {
        return "/api/secureConnect/v1/organizations/" + organization + "/datacenters";
    }

    private static String networkSettings(String network) {
        return "/api/v1/networks/" + network + "/settings";
    }

    private static String nativeRow(String id, String fields) {
        return "{\"id\":" + quote(id) + fields + EXTRA + "}";
    }

    private static String network(String organization, String net) {
        return nativeRow(net, ",\"organizationId\":" + quote(organization));
    }

    private static Instant recent() {
        return Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(2, ChronoUnit.DAYS);
    }

    private static String audit(Instant time, String client) {
        return "{\"ts\":"
                + quote(time.toString())
                + ",\"client\":"
                + client
                + ",\"responseCode\":200"
                + EXTRA
                + "}";
    }

    private static String query(String value) {
        return value.replace(":", "%3A");
    }

    private static String auditQuery(Settings s) {
        return "perPage=" + s.pageSize + "&t0=" + query(s.start) + "&t1=" + query(s.end);
    }

    private static ObjectNode auditPage(String organization, Settings s, String... rows) {
        return get(organizationPath(organization, "apiRequests"), array(rows))
                .put("query", auditQuery(s));
    }

    private static ObjectNode page(String path, int limit, String... rows) {
        return get(path, array(rows)).put("query", "perPage=" + limit);
    }

    private static String array(String... records) {
        return "[" + String.join(",", records) + "]";
    }

    private static ObjectNode get(String path, String response) {
        return JSON.createObjectNode().put("path", path).put("query", "").put("response", response);
    }

    private static JsonNode expected(
            String resource, String id, String organization, String network, String payload)
            throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put("id", id)
                        .put("organization_id", organization)
                        .put("network_id", network);
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
        return engine.executeJob(settings.config(), jobId(), settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(CiscoMerakiPeer.TOKEN), "data API key leaked");
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
                                CiscoMerakiPeer.PASSWORD,
                                "-keypass",
                                CiscoMerakiPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, CiscoMerakiPeer.PASSWORD.toCharArray());
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
        String trust = CiscoMerakiIT.trust;
        String token = CiscoMerakiPeer.TOKEN;
        int request = 60000;
        int operation = 70000;
        int retries;
        int interval;
        int retryDelay = 60000;
        int bytes = 8388608;
        int pages = 10000;
        int scanPages = 100000;
        int items = 10000;
        int pageSize = 3;
        String start;
        String end;

        String config() {
            return start == null ? "/cisco-meraki.conf" : "/cisco-meraki-window.conf";
        }

        Settings window(Instant lower, Instant upper) {
            start = lower.toString();
            end = upper.toString();
            return this;
        }

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
            if (start != null) {
                values.add("api.start=" + start);
                values.add("api.end=" + end);
            }
            return values.toArray(new String[0]);
        }
    }

    /**
     * Hosts normal packaged plugins and independent Java TLS fixtures in one owned worker.
     *
     * <p>DNS and certificate trust overrides apply only to this integration-test container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER = "/tmp/cisco-meraki-peer";
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
            container.withExtraHost(CiscoMerakiPeer.HOST, "127.0.0.1");
            container.withExtraHost("meraki-trap.invalid", "127.0.0.1");
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
                stage(container, CiscoMerakiPeer.class);
                stage(container, CiscoMerakiRecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java CiscoMeraki fixture", failure);
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
                    CiscoMerakiPeer.class.getName(),
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
                    CiscoMerakiRecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/cisco-meraki-" + name;
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

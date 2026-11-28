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
package org.apache.seatunnel.e2e.connector.cin7;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.cin7.Cin7RecoveryProbe;
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
import java.util.Locale;
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
 * Verifies native Cin7 Core reads through the real Java 11 Zeta engine and HTTPS.
 *
 * <p>Independent literal provider fixtures assert both secret headers, per-resource pagination,
 * parent/detail sequencing and stock dimensions before comparing committed complete rows.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class Cin7IT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String BASE = "/ExternalApi/v2";
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
        trust = identity("trusted", Cin7Peer.HOST);
        wrongTrust = identity("untrusted", Cin7Peer.HOST);
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

    private static final String ONE = "10000000-0000-4000-8000-000000000001";
    private static final String TWO = "10000000-0000-4000-8000-000000000002";
    private static final String THREE = "10000000-0000-4000-8000-000000000003";
    // Independent current vendor routes, response arrays and native identity fields.
    private static final String[][] LISTS = {
        {"bank_accounts", "/ref/accountBank", "BankAccountsList", "AccountID"},
        {"attribute_sets", "/ref/attributeset", "AttributeSetList", "ID"},
        {"accounts", "/ref/account", "AccountsList", "Code"},
        {"brands", "/ref/brand", "BrandList", "ID"},
        {"carriers", "/ref/carrier", "CarrierList", "CarrierID"},
        {"customers", "/customer", "CustomerList", "ID"},
        {"deals", "/reference/deals", "Deals", "ID"},
        {"locations", "/ref/location", "LocationList", "ID"},
        {"products", "/product", "Products", "ID"},
        {"purchase_list", "/purchaseList", "PurchaseList", "ID"},
        {"suppliers", "/supplier", "SupplierList", "ID"},
        {"opportunities", "/crm/opportunity", "opportunityList", "ID"},
        {"sale_list", "/saleList", "SaleList", "SaleID"},
        {"taxes", "/ref/tax", "TaxRuleList", "ID"},
        {"product_categories", "/ref/category", "CategoryList", "ID"},
        {"product_family", "/productFamily", "ProductFamilies", "ID"},
        {"leads", "/crm/lead", "LeadList", "ID"},
        {"product_availability", "/ref/productavailability", "ProductAvailabilityList", "ID"}
    };

    @Test
    void exportsAllTwentyResourcesWithNativeIdsAndSharedParentDetailTraversal() throws Exception {
        Settings s = new Settings("all-twenty");
        List<ObjectNode> steps = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (String[] meta : LISTS) {
            String resource = meta[0], id = resource.equals("accounts") ? "00700" : ONE;
            String row =
                    record(
                            resource,
                            id,
                            ",\"Status\":\"DEPRECATED\",\"Amount\":123456789012345678.12345678900");
            steps.add(page(resource, 2, 1, 1, row));
            wanted.add(
                    expected(
                            resource,
                            resource.equals("product_availability")
                                    ? stockId(id, "Main", null, null, null)
                                    : id,
                            null,
                            row));
            if (resource.equals("deals")) {
                steps.add(page(resource, 2, 2, 0));
            }
            if (resource.equals("purchase_list") || resource.equals("sale_list")) {
                String child = resource.equals("purchase_list") ? "purchases" : "sales",
                        value =
                                detailRow(
                                        id,
                                        ",\"Type\":\"Advanced\",\"Invoice\":[{\"Payments\":[{\"Amount\":-1.23456789012345678900}]}]");
                steps.add(detail(child, id, value));
                wanted.add(expected(child, id, id, value));
            }
        }
        program(s.name, steps);
        success(run(s));
        consumed(s.name, 21);
        assertRows(s.name, wanted);
    }

    @Test
    void usesTotalAndExactEchoedPageToRetainAllRowsAcrossAShortFinalPage() throws Exception {
        Settings s = new Settings("total", "products");
        String a = record("products", ONE, ""),
                b = record("products", TWO, ""),
                c = record("products", THREE, "");
        program(s.name, List.of(page("products", 2, 1, 3, a, b), page("products", 2, 2, 3, c)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(
                s.name,
                List.of(
                        expected("products", ONE, null, a),
                        expected("products", TWO, null, b),
                        expected("products", THREE, null, c)));
    }

    @Test
    void completesAllEmptyListsWithoutSpeculativeAdditionalPagesOrDetails() throws Exception {
        Settings s = new Settings("empty");
        List<ObjectNode> steps = new ArrayList<>();
        for (String[] m : LISTS) {
            steps.add(page(m[0], 2, 1, 0));
        }
        program(s.name, steps);
        success(run(s));
        consumed(s.name, 18);
        assertRows(s.name, List.of());
    }

    @Test
    void boundsDealPagesToTwentyFiveAndContinuesPositiveShortPagesUntilEmpty() throws Exception {
        Settings s = new Settings("deals", "deals");
        s.pageSize = 1000;
        String a = record("deals", ONE, ""), b = record("deals", TWO, "");
        program(
                s.name,
                List.of(
                        page("deals", 25, 1, 0, a),
                        page("deals", 25, 2, 0, b),
                        page("deals", 25, 3, 0)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name, List.of(expected("deals", ONE, null, a), expected("deals", TWO, null, b)));
    }

    @Test
    void discoversHiddenParentsAndKeepsSimpleAdvancedAndServicePurchasePayloads() throws Exception {
        Settings s = new Settings("purchases-only", "purchases");
        s.pageSize = 3;
        List<ObjectNode> steps = new ArrayList<>();
        steps.add(
                page(
                        "purchase_list",
                        3,
                        1,
                        3,
                        record("purchase_list", ONE, ""),
                        record("purchase_list", TWO, ""),
                        record("purchase_list", THREE, "")));
        List<JsonNode> wanted = new ArrayList<>();
        String[] ids = {ONE, TWO, THREE}, types = {"Simple", "Advanced", "Service"};
        for (int i = 0; i < 3; i++) {
            String row =
                    detailRow(
                            ids[i],
                            ",\"Type\":\""
                                    + types[i]
                                    + "\",\"IsServiceOnly\":"
                                    + (i == 2)
                                    + ",\"Order\":{\"Lines\":[{\"Tax\":1.2345,\"Quantity\":0.0001}]}");
            steps.add(detail("purchases", ids[i], row));
            wanted.add(expected("purchases", ids[i], ids[i], row));
        }
        program(s.name, steps);
        success(run(s));
        consumed(s.name, 4);
        assertRows(s.name, wanted);
    }

    @Test
    void discoversSalesFromSaleIdAndPreservesTransactionsAndAttachmentsWithoutFetchingUrls()
            throws Exception {
        Settings s = new Settings("sales-only", "sales");
        String summary = record("sale_list", ONE, ",\"ID\":\"wrong-summary-field\""),
                row =
                        detailRow(
                                ONE,
                                ",\"Transactions\":[{\"TransactionID\":\"native\",\"Amount\":9223372036854775808}],\"Attachments\":[{\"DownloadUrl\":\"https://cin7-trap.invalid/private\"}]");
        program(s.name, List.of(page("sale_list", 2, 1, 1, summary), detail("sales", ONE, row)));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, List.of(expected("sales", ONE, ONE, row)));
    }

    @Test
    void preservesStockRowsForDifferentBinsBatchesDatesAndNullVersusEmptyDimensions()
            throws Exception {
        Settings s = new Settings("stock", "product_availability");
        s.pageSize = 5;
        String[] bins = {null, "", "A", "A", "A"},
                batches = {null, null, null, "B", "B"},
                dates = {null, null, null, null, "2026-11-28T00:00:00"};
        List<String> rows = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String row =
                    stock(
                            ONE,
                            "Main",
                            bins[i],
                            batches[i],
                            dates[i],
                            ",\"Available\":-0.0001,\"InTransit\":null");
            rows.add(row);
            wanted.add(
                    expected(
                            "product_availability",
                            stockId(ONE, "Main", bins[i], batches[i], dates[i]),
                            null,
                            row));
        }
        program(
                s.name,
                List.of(page("product_availability", 5, 1, 5, rows.toArray(new String[0]))));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, wanted);
    }

    @Test
    void retainsNumericLookingAccountCodesAndNativeReferenceIdsWithoutCoercion() throws Exception {
        Settings s = new Settings("reference-keys", "bank_accounts", "accounts", "carriers");
        String bank = record("bank_accounts", ONE, ",\"ID\":\"wrong-id\""),
                account = record("accounts", "000700", ""),
                carrier = record("carriers", TWO, ",\"ID\":\"wrong-id\"");
        program(
                s.name,
                List.of(
                        page("bank_accounts", 2, 1, 1, bank),
                        page("accounts", 2, 1, 1, account),
                        page("carriers", 2, 1, 1, carrier)));
        success(run(s));
        consumed(s.name, 3);
        assertRows(
                s.name,
                List.of(
                        expected("bank_accounts", ONE, null, bank),
                        expected("accounts", "000700", null, account),
                        expected("carriers", TWO, null, carrier)));
    }

    @Test
    void rejectsMissingNegativeOrInconsistentTotalsAndPageEchoes() throws Exception {
        int i = 0;
        String row = record("brands", ONE, "");
        for (String body :
                List.of(
                        "{\"BrandList\":[]}",
                        "{\"Page\":1,\"Total\":-1,\"BrandList\":[]}",
                        "{\"Page\":2,\"Total\":0,\"BrandList\":[]}",
                        "{\"Page\":1,\"Total\":0,\"BrandList\":[" + row + "]}",
                        "{\"Page\":1,\"Total\":3,\"BrandList\":[" + row + "]}",
                        "{\"Page\":1,\"Total\":2,\"BrandList\":[]}")) {
            ObjectNode step = page("brands", 2, 1, 0).put("response", body);
            fails(new Settings("bad-total-" + i++, "brands"), List.of(step), "Cin7");
        }
        String two = record("brands", TWO, "");
        fails(
                new Settings("changed-total", "brands"),
                List.of(
                        page("brands", 2, 1, 3, row, two),
                        page(
                                "brands",
                                2,
                                2,
                                4,
                                record("brands", THREE, ""),
                                record("brands", "four", ""))),
                "Cin7");
    }

    @Test
    void rejectsDuplicateIdentitiesMismatchedDetailsAndFiniteReadBudgets() throws Exception {
        String row = record("brands", ONE, "");
        fails(
                new Settings("duplicate-id", "brands"),
                List.of(page("brands", 2, 1, 2, row, row)),
                "duplicate");
        fails(
                new Settings("wrong-detail", "sales"),
                List.of(
                        page("sale_list", 2, 1, 1, record("sale_list", ONE, "")),
                        detail("sales", ONE, detailRow(TWO, ""))),
                "Cin7");
        Settings pages = new Settings("page-budget", "brands");
        pages.pages = 1;
        fails(
                pages,
                List.of(page("brands", 2, 1, 3, row, record("brands", TWO, ""))),
                "max_pages_per_collection");
        Settings scan = new Settings("scan-budget", "brands", "carriers");
        scan.scanPages = 1;
        fails(scan, List.of(page("brands", 2, 1, 0)), "max_pages_per_scan");
    }

    @Test
    void acceptsTheDocumentedMaximumAndRejectsInvalidIdsAndSizesBeforePublication()
            throws Exception {
        Settings good = new Settings("max-size", "brands");
        good.pageSize = 1000;
        program(good.name, List.of(page("brands", 1000, 1, 0)));
        success(run(good));
        consumed(good.name, 1);
        Settings bad = new Settings("bad-size", "brands");
        bad.pageSize = 1001;
        fails(bad, List.of(), "page_size");
        int i = 0;
        for (String row : List.of("{}", "{\"ID\":12}", "{\"ID\":\"\"}")) {
            fails(
                    new Settings("bad-id-" + i++, "brands"),
                    List.of(page("brands", 2, 1, 1, row)),
                    "Cin7");
        }
    }

    @Test
    void retainsOneThousandAndOneRowsAcrossTheMaximumPageBoundary() throws Exception {
        Settings s = new Settings("thousand", "brands");
        s.pageSize = 1000;
        List<String> rows = new ArrayList<>();
        List<JsonNode> wanted = new ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            String id = String.format(Locale.ROOT, "10000000-0000-4000-8000-%012d", i),
                    row = record("brands", id, "");
            rows.add(row);
            wanted.add(expected("brands", id, null, row));
        }
        program(
                s.name,
                List.of(
                        page("brands", 1000, 1, 1001, rows.subList(0, 1000).toArray(new String[0])),
                        page("brands", 1000, 2, 1001, rows.get(1000))));
        success(run(s));
        consumed(s.name, 2);
        assertRows(s.name, wanted);
    }

    @Test
    void rejectsRepeatedDealPagesRatherThanReportingATruncatedSuccess() throws Exception {
        String row = record("deals", ONE, "");
        fails(
                new Settings("deal-repeat", "deals"),
                List.of(page("deals", 2, 1, 0, row), page("deals", 2, 2, 0, row)),
                "Cin7");
    }

    @Test
    void reportsPermissionAndProviderErrorsWithoutLeakingEitherSecretHeader() throws Exception {
        for (int status : List.of(401, 403, 404, 500)) {
            fails(
                    new Settings("http-" + status, "brands"),
                    List.of(
                            page("brands", 2, 1, 0)
                                    .put("status", status)
                                    .put("response", "{\"Error\":\"private-provider-message\"}")),
                    "HTTP " + status);
        }
        fails(
                new Settings("provider-error", "brands"),
                List.of(
                        page("brands", 2, 1, 0)
                                .put("response", "{\"Error\":\"private-provider-message\"}")),
                "Cin7");
    }

    @Test
    void verifiesTrustAndHostnameAndRefusesRedirectCredentialForwarding() throws Exception {
        Settings ca = new Settings("wrong-ca", "brands");
        ca.trust = wrongTrust;
        fails(ca, List.of(), "certificate");
        selectIdentity("wrong-name");
        try {
            Settings san = new Settings("wrong-san", "brands");
            san.trust = wrongNameTrust;
            fails(san, List.of(), "certificate");
        } finally {
            selectIdentity("trusted");
        }
        fails(
                new Settings("redirect", "brands"),
                List.of(page("brands", 2, 1, 0).put("status", 302).put("mode", "redirect")),
                "HTTP 302");
    }

    @Test
    void acceptsGzipAndBoundsCompressedAndExpandedBodySizes() throws Exception {
        Settings s = new Settings("gzip", "brands");
        String row = record("brands", ONE, "");
        program(s.name, List.of(page("brands", 2, 1, 1, row).put("mode", "gzip")));
        success(run(s));
        consumed(s.name, 1);
        assertRows(s.name, List.of(expected("brands", ONE, null, row)));
        for (String mode : List.of("json", "gzip")) {
            Settings small = new Settings("body-" + mode, "brands");
            small.bytes = 256;
            fails(
                    small,
                    List.of(
                            page(
                                            "brands",
                                            2,
                                            1,
                                            1,
                                            record(
                                                    "brands",
                                                    ONE,
                                                    ",\"padding\":\"" + "x".repeat(5000) + "\""))
                                    .put("mode", mode)),
                    "max_response_bytes");
        }
        fails(
                new Settings("encoding", "brands"),
                List.of(page("brands", 2, 1, 0).put("encoding", "br")),
                "encoding");
    }

    @Test
    void rejectsMalformedJsonUtf8EnvelopesAndNonrecordElementsBeforePublication() throws Exception {
        int i = 0;
        for (String body :
                List.of(
                        "null",
                        "{}",
                        "{\"Total\":1,\"Page\":1,\"BrandList\":[null]}",
                        "{\"Total\":0,\"Page\":1,\"BrandList\":[]}{}",
                        "{\"Total\":0,\"Page\":1,\"BrandList\":[],\"BrandList\":[]}")) {
            Settings s = new Settings("json-" + i++, "brands");
            fails(s, List.of(page("brands", 2, 1, 0).put("response", body)), "Cin7");
            assertRows(s.name, List.of());
        }
        fails(
                new Settings("utf8", "brands"),
                List.of(
                        page("brands", 2, 1, 0)
                                .put(
                                        "bytes",
                                        Base64.getEncoder()
                                                .encodeToString(new byte[] {(byte) 0xc3, 0x28}))),
                "Cin7");
    }

    @Test
    void honorsRetryAfterAndCapsAttemptsAndWaitsForQuotaRecovery() throws Exception {
        Settings s = new Settings("retry", "brands");
        s.retries = 1;
        program(
                s.name,
                List.of(
                        page("brands", 2, 1, 0).put("status", 429).put("Retry-After", "2"),
                        page("brands", 2, 1, 0)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 2000);
        Settings exhausted = new Settings("retry-exhausted", "brands");
        exhausted.retries = 1;
        ObjectNode unavailable = page("brands", 2, 1, 0).put("status", 503);
        fails(exhausted, List.of(unavailable, unavailable), "2 attempts");
        Settings cap = new Settings("retry-cap", "brands");
        cap.retries = 1;
        cap.retryDelay = 250;
        fails(
                cap,
                List.of(page("brands", 2, 1, 0).put("status", 429).put("Retry-After", "3")),
                "max_retry_delay_ms");
    }

    @Test
    void pacesSeparateResourcesBeforeSendingTheSharedApplicationCredential() throws Exception {
        Settings s = new Settings("pace", "brands", "carriers");
        s.interval = 600;
        program(s.name, List.of(page("brands", 2, 1, 0), page("carriers", 2, 1, 0)));
        success(run(s));
        consumed(s.name, 2);
        delayAtLeast(s.name, 600);
    }

    @Test
    void rejectsInvalidSelectionsAndSecretValuesBeforeAuthenticatedRequests() throws Exception {
        fails(new Settings("unknown", "unknown"), List.of(), "resources");
        fails(new Settings("duplicate", "brands", "brands"), List.of(), "resources");
        Settings token = new Settings("bad-token", "brands");
        token.token = " ";
        fails(token, List.of(), "token");
        Settings account = new Settings("bad-account", "brands");
        account.app = " ";
        fails(account, List.of(), "access_key");
    }

    @Test
    void boundsContinuousBodyTrafficAndClosesTheSocketAtTheDeadline() throws Exception {
        Settings s = new Settings("deadline", "brands");
        s.request = 2000;
        s.operation = 3000;
        fails(s, List.of(page("brands", 2, 1, 0).put("mode", "trickle")), "Cin7");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(s.name + ".disconnect"));
        long start = Long.parseLong(engine.lines(s.name + ".started").get(0)),
                end = Long.parseLong(engine.lines(s.name + ".disconnect").get(0));
        assertTrue(end - start < 6000);
        assertRows(s.name, List.of());
    }

    @Test
    void cancelsTheActualEngineJobWithoutPublishingAPartialResponse() throws Exception {
        Settings s = new Settings("cancel", "brands");
        program(s.name, List.of(page("brands", 2, 1, 0).put("mode", "trickle")));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/cin7.conf", id, s.variables()));
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
    void restoresTheUnfinishedPageWithoutReissuingItsCommittedPredecessor() throws Exception {
        Settings s = new Settings("page-savepoint", "brands");
        String first = record("brands", ONE, ""),
                second = record("brands", TWO, ""),
                third = record("brands", THREE, "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                page("brands", 2, 1, 3, first, second),
                                page("brands", 2, 2, 3, third).put("mode", "partial")));
        program("page-restored", List.of(page("brands", 2, 2, 3, third)));
        success(engine.restoreJob("/cin7.conf", id, s.variables()));
        consumed("page-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expected("brands", ONE, null, first),
                        expected("brands", TWO, null, second),
                        expected("brands", THREE, null, third)));
    }

    @Test
    void restoresBetweenCollectionsWithoutReissuingAnAlreadyCompletedList() throws Exception {
        Settings s = new Settings("between-savepoint", "brands", "carriers");
        String first = record("brands", ONE, ""), second = record("carriers", TWO, "");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                page("brands", 2, 1, 1, first),
                                page("carriers", 2, 1, 1, second).put("mode", "partial")));
        program("between-restored", List.of(page("carriers", 2, 1, 1, second)));
        success(engine.restoreJob("/cin7.conf", id, s.variables()));
        consumed("between-restored", 1);
        assertRows(
                s.name,
                List.of(
                        expected("brands", ONE, null, first),
                        expected("carriers", TWO, null, second)));
    }

    @Test
    void restoresAnOutstandingDetailWithoutDuplicatingTheCommittedParentSummary() throws Exception {
        Settings s = new Settings("detail-savepoint", "sale_list", "sales");
        String summary = record("sale_list", ONE, ""),
                child = detailRow(ONE, ",\"Transactions\":[{\"Amount\":-1.2345}]");
        String id =
                saveBlocked(
                        s,
                        List.of(
                                page("sale_list", 2, 1, 1, summary),
                                detail("sales", ONE, child).put("mode", "partial")));
        program(
                "detail-restored",
                List.of(page("sale_list", 2, 1, 1, summary), detail("sales", ONE, child)));
        success(engine.restoreJob("/cin7.conf", id, s.variables()));
        consumed("detail-restored", 2);
        assertRows(
                s.name,
                List.of(
                        expected("sale_list", ONE, null, summary),
                        expected("sales", ONE, ONE, child)));
    }

    @Test
    void restoresATerminalSourceWithZeroHttpAndNoDuplicateCommittedRows() throws Exception {
        Settings s = new Settings("terminal-savepoint", "brands");
        String row = record("brands", ONE, "");
        List<JsonNode> wanted = List.of(expected("brands", ONE, null, row));
        program(s.name, List.of(page("brands", 2, 1, 1, row)));
        String id = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob("/cin7-terminal.conf", id, s.variables()));
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
                tasks.submit(() -> engine.restoreJob("/cin7-terminal.conf", id, s.variables()));
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
    void serializes32Of33RowsAndRejectsAPrefixChangeBeforePublishingTheSuffix() throws Exception {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < 33; i++) {
            rows.add(record("brands", "B" + i, ",\"description\":\"synthetic-response-record\""));
        }
        for (String mode : List.of("resume", "changed")) {
            List<String> replay = new ArrayList<>(rows);
            if (mode.equals("changed")) {
                replay.set(0, replay.get(0).replace("B0", "CHANGED"));
            }
            List<ObjectNode> steps =
                    List.of(
                            page("brands", 50, 1, 33, rows.toArray(new String[0])),
                            page("brands", 50, 1, 33, replay.toArray(new String[0])));
            String name = "reader-" + mode;
            program(name, steps);
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
                tasks.submit(() -> engine.executeJob("/cin7.conf", id, s.variables()));
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

    private static String[] meta(String resource) {
        for (String[] value : LISTS) {
            if (value[0].equals(resource)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown independent fixture resource " + resource);
    }

    private static String record(String resource, String id, String extra) {
        return resource.equals("product_availability")
                ? stock(id, "Main", null, null, null, extra)
                : "{\"" + meta(resource)[3] + "\":" + quote(id) + extra + EXTRA + "}";
    }

    private static String detailRow(String id, String extra) {
        return "{\"ID\":" + quote(id) + extra + EXTRA + "}";
    }

    private static String quote(String value) {
        return value == null ? "null" : JSON.getNodeFactory().textNode(value).toString();
    }

    private static String stock(
            String id, String location, String bin, String batch, String expiry, String extra) {
        return "{\"ID\":"
                + quote(id)
                + ",\"Location\":"
                + quote(location)
                + ",\"Bin\":"
                + quote(bin)
                + ",\"Batch\":"
                + quote(batch)
                + ",\"ExpiryDate\":"
                + quote(expiry)
                + extra
                + EXTRA
                + "}";
    }

    private static String stockId(
            String id, String location, String bin, String batch, String expiry) {
        ArrayNode value = JSON.createArrayNode();
        for (String element : new String[] {id, location, bin, batch, expiry}) {
            value.add(element);
        }
        return value.toString();
    }

    private static ObjectNode page(
            String resource, int limit, int page, long total, String... rows) {
        String[] m = meta(resource);
        String flags = "";
        switch (resource) {
            case "customers":
                flags = "&IncludeDeprecated=true&IncludeProductPrices=true";
                break;
            case "products":
                flags =
                        "&IncludeBOM=true&IncludeMovements=true&IncludeSuppliers=true&IncludeDeprecated=true&IncludeAttachments=true&IncludeCustomPrices=true&IncludeReorderLevels=true";
                break;
            case "suppliers":
                flags = "&IncludeDeprecated=true";
                break;
            default:
                break;
        }
        boolean deals = resource.equals("deals");
        String query =
                (deals ? "page=" : "Page=")
                        + page
                        + (deals ? "&limit=" : "&Limit=")
                        + limit
                        + flags;
        String body =
                "{"
                        + (deals ? "" : "\"Total\":" + total + ",\"Page\":" + page + ",")
                        + "\""
                        + m[2]
                        + "\":"
                        + array(rows)
                        + "}";
        return get(BASE + m[1], body).put("query", query);
    }

    private static ObjectNode detail(String resource, String id, String row) {
        boolean purchase = resource.equals("purchases");
        return get(BASE + (purchase ? "/advanced-purchase" : "/sale"), row)
                .put(
                        "query",
                        "ID="
                                + id
                                + (purchase ? "" : "&IncludeTransactions=true")
                                + "&CombineAdditionalCharges=true");
    }

    private static String array(String... records) {
        return "[" + String.join(",", records) + "]";
    }

    private static ObjectNode get(String path, String response) {
        return JSON.createObjectNode().put("path", path).put("query", "").put("response", response);
    }

    private static JsonNode expected(String resource, String id, String parentId, String payload)
            throws IOException {
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put("id", id)
                        .put("parent_id", parentId);
        row.set("record", JSON.readTree(payload));
        return row;
    }

    private void assertRows(String name, List<JsonNode> expected) throws Exception {
        Map<JsonNode, Integer> actual = new HashMap<>(), wanted = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode parsed = JSON.readTree(line);
            assertTrue(parsed.isObject() && parsed.size() == 4, line);
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
        return engine.executeJob("/cin7.conf", jobId(), settings.variables());
    }

    private void fails(Settings settings, List<ObjectNode> steps, String reason) throws Exception {
        program(settings.name, steps);
        Container.ExecResult result = run(settings);
        String output = result.getStdout() + result.getStderr();
        assertNotEquals(0, result.getExitCode(), output);
        assertTrue(output.contains(reason), output);
        assertFalse(output.contains(Cin7Peer.TOKEN), "data API key leaked");
        assertFalse(output.contains(Cin7Peer.APP), "app key leaked");
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
                                Cin7Peer.PASSWORD,
                                "-keypass",
                                Cin7Peer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, keytool.waitFor(), output);
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, Cin7Peer.PASSWORD.toCharArray());
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
        String trust = Cin7IT.trust;
        String token = Cin7Peer.TOKEN;
        int request = 60000;
        int operation = 70000;
        int retries;
        int interval;
        int retryDelay = 60000;
        int bytes = 8388608;
        int pages = 10000;
        int scanPages = 100000;
        String app = Cin7Peer.APP;
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
                                    "api.app=" + app,
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
        private static final String PEER = "/tmp/cin7-peer";
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
            container.withExtraHost(Cin7Peer.HOST, "127.0.0.1");
            container.withExtraHost("cin7-trap.invalid", "127.0.0.1");
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
                stage(container, Cin7Peer.class);
                stage(container, Cin7RecoveryProbe.class);
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
                throw new IOException("Cannot stage the Java Cin7 fixture", failure);
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
                    Cin7Peer.class.getName(),
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
                    Cin7RecoveryProbe.class.getName(),
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
            String path = "/tmp/seatunnel_mnt/cin7-" + name;
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

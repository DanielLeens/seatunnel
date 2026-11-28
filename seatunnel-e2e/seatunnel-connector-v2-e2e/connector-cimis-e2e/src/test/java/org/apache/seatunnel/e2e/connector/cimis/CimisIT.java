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
package org.apache.seatunnel.e2e.connector.cimis;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.cimis.CimisRecoveryProbe;
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
import java.time.LocalDate;
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
 * Runs Cimis through a real SeaTunnel Zeta engine and an isolated Java TLS account.
 *
 * <p>Tests compare page numbers and bounds in captured GET requests and inspect LocalFile's
 * committed JSON rows. The provider peer never calls the real billable API.
 */
class CimisIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final String KEY = "synthetic-cimis-key+/=?";
    private static final String HIDDEN_MESSAGE = "private-provider-response-token";
    private static final String HOST = "cimis-peer";
    private static final String PEM = "/tmp/cimis.pem";
    private static final String OTHER_PEM = "/tmp/unrelated-cimis.pem";
    private static final String ENDPOINT = "https://" + HOST + ":18443";
    private static final String STATIONS = "/StationWeb/GetAllStations";
    private static final String SPATIAL_ZIPS = "/SpatialWeb/GetAllSpatialZipCodes";
    private static final String STATION_ZIPS = "/StationWeb/GetAllStationsZipCodes";
    private static final String BY_STATION = "/StationWeb/GetDataByStationNumber";
    private static final String BY_ZIP = "/GeoStationWeb/GetDataByGeoStationZipCodes";
    private static final String BY_COORDINATE = "/SpatialWeb/GetDataBySpatialCoordinates";
    private static final String BY_ADDRESS = "/SpatialWeb/GetDataByAddresses";
    private static final String START = "2024-02-28";
    private static final String LEAP = "2024-02-29";
    private static final String END = "2024-03-01";
    private static final String VOLUME_LIMIT = "[ERR2112-DATA VOLUME VIOLATION]";
    private static final List<String> ALL =
            List.of("stations", "spatial_zipcodes", "station_zipcodes", "data");
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
                "keytool -genkeypair -alias provider -storetype PKCS12 -storepass cimis-fixture "
                        + "-keyalg RSA -keysize 2048 -validity 30 -dname CN="
                        + HOST
                        + " -ext SAN=dns:"
                        + HOST
                        + ",dns:cimis-redirect-host,"
                        + runnerType
                        + runner
                        + " -keystore ";
        String export =
                "keytool -exportcert -rfc -alias provider -storepass cimis-fixture -keystore ";
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
                        + CimisPeer.class.getName()
                        + " /tmp/provider.p12";
        String providerClass = "/" + CimisPeer.class.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream bits = CimisPeer.class.getResourceAsStream(providerClass)) {
            if (bits == null) {
                throw new IllegalStateException("Missing packaged HTTPS fixture");
            }
            bytecode = bits.readAllBytes();
        }
        peer =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "cimis-other-host", "cimis-redirect-host")
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
    void persistsCurrentFourResourcesWithRawReferenceAndWeatherValues() throws Exception {
        ObjectNode station =
                stationMarker("7")
                        .put("Name", "观测站 🧭")
                        .put("IsActive", "False")
                        .putNull("ZipCodes");
        ObjectNode spatial = zip("01234").put("IsActive", "True").put("ConnectDate", "2/29/2024");
        ObjectNode stationZip = zip("01234").put("StationNbr", 7);
        ObjectNode otherStation = zip("01234").put("StationNbr", 18);
        ObjectNode anotherZip = zip("09876").put("StationNbr", 7);
        ObjectNode record = observation(LEAP, "daily", "7", null).put("Standard", "english");
        record.putObject("DayAsceEto").putNull("Value").put("Qc", "M").put("Unit", "(in)");
        record.putObject("DayAirTmpAvg").put("Value", "50.000").put("Qc", " ").put("Unit", "(°F)");
        record.put("newBigInt", new BigInteger("9223372036854775808"));
        record.put("newDecimal", new BigDecimal("123456789012345678.123456789000"));
        reset(
                ok(STATIONS, envelope("Stations", station)),
                ok(SPATIAL_ZIPS, envelope("ZipCodes", spatial)),
                ok(STATION_ZIPS, envelope("ZipCodes", stationZip, otherStation, anotherZip)),
                ok(BY_STATION, weather(provider("station", record))));
        ObjectNode config = settings("station_numbers", LEAP, LEAP, "007");
        config.putArray("daily_data_items").add("day-asce-eto").add("day-air-tmp-avg");
        try (Job job = job(ALL, config, false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            outReference("stations", "7", station),
                            outReference("spatial_zipcodes", "01234", spatial),
                            outReference("station_zipcodes", "7:01234", stationZip),
                            outReference("station_zipcodes", "18:01234", otherStation),
                            outReference("station_zipcodes", "7:09876", anotherZip),
                            outData("station_numbers", "7", "station", record)));
        }
        assertTrace(
                wire(STATIONS),
                wire(SPATIAL_ZIPS),
                wire(STATION_ZIPS),
                stationWire("7", LEAP, LEAP, false, "day-asce-eto,day-air-tmp-avg"));
    }

    @Test
    void separatesDailyAndHourlyRequestsOverLeapDayAndKeepsOriginal2400() throws Exception {
        ObjectNode first = observation(LEAP, "daily", "7", null);
        ObjectNode second = observation(END, "daily", "7", null);
        ObjectNode noon = observation(LEAP, "hourly", "7", "1200");
        ObjectNode midnight = observation(LEAP, "hourly", "7", "2400");
        ObjectNode next = observation(END, "hourly", "7", "0100");
        ObjectNode other = observation(LEAP, "daily", "8", null);
        reset(
                ok(BY_STATION, weather(provider("station", first))),
                ok(BY_STATION, weather(provider("station", second))),
                ok(BY_STATION, weather(provider("station", noon, midnight))),
                ok(BY_STATION, weather(provider("station", next))),
                ok(BY_STATION, weather(provider("station", other))),
                ok(BY_STATION, weather()),
                ok(BY_STATION, weather()),
                ok(BY_STATION, weather()));
        ObjectNode config = settings("station_numbers", START, END, "7", "8").put("slice_days", 2);
        config.putArray("daily_data_items").add("day-asce-eto");
        config.putArray("hourly_data_items").add("hly-air-tmp");
        try (Job job = job(List.of("data"), config, false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            outData("station_numbers", "7", "station", first),
                            outData("station_numbers", "7", "station", second),
                            outData("station_numbers", "7", "station", noon),
                            outData("station_numbers", "7", "station", midnight),
                            outData("station_numbers", "7", "station", next),
                            outData("station_numbers", "8", "station", other)));
        }
        assertTrace(
                stationWire("7", START, LEAP, false, "day-asce-eto"),
                stationWire("7", END, END, false, "day-asce-eto"),
                stationWire("7", START, LEAP, true, "hly-air-tmp"),
                stationWire("7", END, END, true, "hly-air-tmp"),
                stationWire("8", START, LEAP, false, "day-asce-eto"),
                stationWire("8", END, END, false, "day-asce-eto"),
                stationWire("8", START, LEAP, true, "hly-air-tmp"),
                stationWire("8", END, END, true, "hly-air-tmp"));
    }

    @Test
    void preservesCalendarYearBoundariesWithoutJulianOrYearCollisions() throws Exception {
        String last = "2023-12-31";
        String newYear = "2024-01-01";
        ObjectNode prior = observation(last, "daily", "12", null);
        ObjectNode later = observation(newYear, "daily", "12", null);
        reset(
                ok(BY_STATION, weather(provider("station", prior))),
                ok(BY_STATION, weather(provider("station", later))));
        try (Job job =
                job(
                        List.of("data"),
                        settings("station_numbers", last, newYear, "12").put("slice_days", 1),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            outData("station_numbers", "12", "station", prior),
                            outData("station_numbers", "12", "station", later)));
        }
        assertTrace(
                stationWire("12", last, last, false, null),
                stationWire("12", newYear, newYear, false, null));
    }

    @Test
    void usesCurrentGeoStationZipPathAndRetainsBothStationAndSpatialProvenance() throws Exception {
        ObjectNode station = observation(LEAP, "daily", "7", null).put("ZipCodes", "90001, 01234");
        ObjectNode spatial = observation(LEAP, "daily", null, null).put("ZipCodes", "01234");
        ObjectNode hour = observation(LEAP, "hourly", "7", "2400").put("ZipCodes", "01234");
        reset(
                ok(BY_ZIP, weather(provider("station", station), provider("spatial", spatial))),
                ok(BY_ZIP, weather(provider("station", hour))));
        ObjectNode config = settings("zip_codes", LEAP, LEAP, "01234").put("unit_of_measure", "M");
        config.putArray("daily_data_items").add("day-sol-rad-avg");
        config.putArray("hourly_data_items").add("hly-sol-rad");
        try (Job job = job(List.of("data"), config, false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            outData("zip_codes", "01234", "station", station),
                            outData("zip_codes", "01234", "spatial", spatial),
                            outData("zip_codes", "01234", "station", hour)));
        }
        assertTrace(
                dataWire(
                        BY_ZIP,
                        "zipCodes",
                        "01234",
                        LEAP,
                        LEAP,
                        "M",
                        false,
                        "SCS",
                        "day-sol-rad-avg"),
                dataWire(BY_ZIP, "zipCodes", "01234", LEAP, LEAP, "M", true, "WSN", "hly-sol-rad"));
    }

    @Test
    void usesSeparateSpatialCoordinatesAndAddressRoutesWithoutInventingAnHour() throws Exception {
        String coordinate = "lat=34.99,lng=-118.34";
        String address = "addr-name=L'Eau & Jardin,addr=100 Test Avenue, Fictional, CA 01234";
        ObjectNode byCoordinate =
                observation(LEAP, "daily", null, null)
                        .put("Coordinate", coordinate)
                        .put("Address", "");
        ObjectNode byAddress =
                observation(LEAP, "daily", null, null)
                        .put("Coordinate", "lat=38.99,lng=-121.11")
                        .put("Address", "100 Test Avenue");
        reset(
                ok(BY_COORDINATE, weather(provider("spatial", byCoordinate))),
                ok(BY_ADDRESS, weather(provider("spatial", byAddress))));
        for (boolean coord : List.of(true, false)) {
            String mode = coord ? "coordinates" : "addresses";
            String selected = coord ? "lat=34.9900,lng=-118.340000" : address;
            ObjectNode config = settings(mode, LEAP, LEAP, selected).put("unit_of_measure", "M");
            config.putArray("daily_data_items").add("day-sol-rad-avg");
            try (Job job = job(List.of("data"), config, false, false)) {
                good(engine.executeJob(job.configName));
                assertOutput(
                        job,
                        List.of(
                                outData(
                                        mode,
                                        coord ? coordinate : address,
                                        "spatial",
                                        coord ? byCoordinate : byAddress)));
            }
        }
        assertTrace(
                dataWire(
                        BY_COORDINATE,
                        "coordinates",
                        coordinate,
                        LEAP,
                        LEAP,
                        "M",
                        null,
                        null,
                        "day-sol-rad-avg"),
                dataWire(
                        BY_ADDRESS,
                        "addresses",
                        address,
                        LEAP,
                        LEAP,
                        "M",
                        null,
                        null,
                        "day-sol-rad-avg"));
    }

    @Test
    void halvesOnlyAnExplicitVolumeRejectedSliceAndContinuesWithoutOverlappingDates()
            throws Exception {
        ObjectNode a = observation(START, "daily", "17", null);
        ObjectNode b = observation(LEAP, "daily", "17", null);
        ObjectNode c = observation(END, "daily", "17", null);
        reset(
                volume(BY_STATION),
                ok(BY_STATION, weather(provider("station", a))),
                ok(BY_STATION, weather(provider("station", b))),
                ok(BY_STATION, weather(provider("station", c))));
        ObjectNode config = settings("station_numbers", START, END, "17").put("max_requests", 4);
        try (Job job = job(List.of("data"), config, false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(
                    job,
                    List.of(
                            outData("station_numbers", "17", "station", a),
                            outData("station_numbers", "17", "station", b),
                            outData("station_numbers", "17", "station", c)));
        }
        assertTrace(
                stationWire("17", START, END, false, null),
                stationWire("17", START, START, false, null),
                stationWire("17", LEAP, LEAP, false, null),
                stationWire("17", END, END, false, null));
        rejectOne(
                volume(BY_STATION),
                List.of("data"),
                settings("station_numbers", LEAP, LEAP, "17"),
                stationWire("17", LEAP, LEAP, false, null),
                "single-day request exceeds CIMIS volume limit");
        rejectOne(
                frame(
                        BY_STATION,
                        400,
                        "plain",
                        "",
                        ("bad request " + VOLUME_LIMIT).getBytes(StandardCharsets.UTF_8)),
                List.of("data"),
                settings("station_numbers", START, END, "17"),
                stationWire("17", START, END, false, null),
                "HTTP 400");
    }

    @Test
    void readsReferenceOnlyWithoutDateSettingsAndRecognizesExplicitEmptyCollections()
            throws Exception {
        reset(
                ok(STATIONS, envelope("Stations")),
                ok(SPATIAL_ZIPS, envelope("ZipCodes")),
                ok(STATION_ZIPS, envelope("ZipCodes")));
        try (Job job =
                job(
                        List.of("stations", "spatial_zipcodes", "station_zipcodes"),
                        options(),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(STATIONS), wire(SPATIAL_ZIPS), wire(STATION_ZIPS));
        reset(ok(BY_STATION, weather()), ok(BY_STATION, weather(provider("station"))));
        try (Job job =
                job(
                        List.of("data"),
                        settings("station_numbers", LEAP, LEAP, "17", "18"),
                        false,
                        false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(
                stationWire("17", LEAP, LEAP, false, null),
                stationWire("18", LEAP, LEAP, false, null));
    }

    private static String volume(String route) {
        return frame(
                route,
                400,
                "plain",
                "",
                (VOLUME_LIMIT + " The request exceeds the maximum data limit.")
                        .getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void rejectsInvalidCalendarTargetsAndRetiredOrUnsupportedMetricNamesBeforeNetwork()
            throws Exception {
        reset();
        failJob(
                List.of("data"),
                settings("station_numbers", "2023-02-29", LEAP, "17"),
                "valid yyyy-MM-dd date");
        failJob(
                List.of("data"),
                settings("station_numbers", END, LEAP, "17"),
                "must be on or after start_date");
        failJob(
                List.of("data"),
                settings("station_numbers", LEAP, END, "17,18"),
                "invalid station_numbers target");
        failJob(
                List.of("data"),
                settings("station_numbers", LEAP, END, "017", "17"),
                "duplicate normalized target");
        failJob(
                List.of("data"),
                settings("coordinates", LEAP, END, "lat=91,lng=-120"),
                "invalid coordinates target");
        failJob(
                List.of("data"),
                settings("coordinates", LEAP, END, "lat=34;lng=-120"),
                "invalid coordinates target");
        failJob(
                List.of("data"),
                settings("addresses", LEAP, END, "addr-name=A,addr=X;addr-name=B,addr=Y"),
                "invalid addresses target");
        ObjectNode unsupported = settings("coordinates", LEAP, END, "lat=34,lng=-120");
        unsupported.putArray("hourly_data_items").add("hly-air-tmp");
        failJob(List.of("data"), unsupported, "spatial coordinates and addresses are daily only");
        ObjectNode wrongSpatialMetric = settings("coordinates", LEAP, END, "lat=34,lng=-120");
        wrongSpatialMetric.putArray("daily_data_items").add("day-air-tmp-avg");
        failJob(List.of("data"), wrongSpatialMetric, "supported CIMIS 3.0 metrics");
        ObjectNode deprecated = settings("station_numbers", LEAP, END, "17");
        deprecated.putArray("daily_data_items").add("day-eto");
        failJob(List.of("data"), deprecated, "supported CIMIS 3.0 metrics");
        failJob(
                List.of("stations"),
                options().put("endpoint", "http://cimis-peer:18443"),
                "expected a bare HTTPS origin");
        failJob(
                List.of("stations"),
                options().put("access_key", "spaces invalid"),
                "printable subscription key");
        failJob(List.of("stations"), options().put("slice_days", 32), "slice_days");
        assertTrace();
    }

    @Test
    void rejectsUnprovenHeadersAndCorrectlyDistinguishesEmptyDataFromErrorObjects()
            throws Exception {
        rejectOne(
                ok(
                        STATIONS,
                        envelope("Stations", stationMarker("7")),
                        "<https://cimis-redirect-host:18443/next>; rel=\"next\""),
                List.of("stations"),
                options(),
                wire(STATIONS),
                "unexpected Link metadata");
        for (JsonNode value :
                List.of(
                        options().put("Error", HIDDEN_MESSAGE),
                        object("Data", options().put("Error", HIDDEN_MESSAGE)),
                        object("Data", rows()),
                        object("error", options().put("message", HIDDEN_MESSAGE)))) {
            rejectOne(
                    ok(BY_STATION, value),
                    List.of("data"),
                    settings("station_numbers", LEAP, LEAP, "17"),
                    stationWire("17", LEAP, LEAP, false, null),
                    "CIMIS data");
        }
        rejectOne(
                ok(
                        STATION_ZIPS,
                        envelope("ZipCodes", options().put("StationNbr", 7).put("ZipCode", 123))),
                List.of("station_zipcodes"),
                options(),
                wire(STATION_ZIPS),
                "invalid CIMIS identity text");
        rejectOne(
                ok(STATIONS, envelope("Stations", stationMarker("7"), stationMarker("07"))),
                List.of("stations"),
                options(),
                wire(STATIONS),
                "duplicate record identity");
    }

    @Test
    void refusesWeatherFromAnotherDateStationZipOrRequestedInterval() throws Exception {
        ObjectNode config = settings("station_numbers", LEAP, LEAP, "17");
        for (ObjectNode bad :
                List.of(
                        observation(START, "daily", "17", null),
                        observation(LEAP, "daily", "18", null),
                        observation(LEAP, "hourly", "17", "1200"))) {
            rejectOne(
                    ok(BY_STATION, weather(provider("station", bad))),
                    List.of("data"),
                    config,
                    stationWire("17", LEAP, LEAP, false, null),
                    "CIMIS data");
        }
        ObjectNode hourly = settings("station_numbers", LEAP, LEAP, "17");
        hourly.putArray("hourly_data_items").add("hly-air-tmp");
        rejectOne(
                ok(
                        BY_STATION,
                        weather(provider("station", observation(LEAP, "hourly", "17", "2500")))),
                List.of("data"),
                hourly,
                stationWire("17", LEAP, LEAP, true, "hly-air-tmp"),
                "expected an original hour");
        rejectOne(
                ok(
                        BY_ZIP,
                        weather(
                                provider(
                                        "spatial",
                                        observation(LEAP, "daily", null, null)
                                                .put("ZipCodes", "11111")))),
                List.of("data"),
                settings("zip_codes", LEAP, LEAP, "01234"),
                dataWire(BY_ZIP, "zipCodes", "01234", LEAP, LEAP, "E", false, "SCS", null),
                "record ZIP list does not include the requested target");
        rejectOne(
                ok(
                        BY_STATION,
                        weather(provider("spatial", observation(LEAP, "daily", null, null)))),
                List.of("data"),
                config,
                stationWire("17", LEAP, LEAP, false, null),
                "provider type does not match");
    }

    @Test
    void boundsAllRequestsIncludingASafeSubdivisionWithoutSilentlySkippingSlices()
            throws Exception {
        reset(volume(BY_STATION));
        failJob(
                List.of("data"),
                settings("station_numbers", START, END, "17").put("max_requests", 1),
                "max_requests exhausted");
        assertTrace(stationWire("17", START, END, false, null));
    }

    @Test
    void preservesCredentialsAcrossRetriesAndSuppressesHidden503FollowUp() throws Exception {
        reset(reply(STATIONS, 429, "plain", "2", options()), ok(STATIONS, envelope("Stations")));
        try (Job job = job(List.of("stations"), options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of());
        }
        assertTrace(wire(STATIONS), wire(STATIONS));
        List<Seen> trace = seen();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(trace.get(1).at - trace.get(0).at) >= 1980);
        rejectOne(
                reply(STATIONS, 429, "plain", "90", options()),
                List.of("stations"),
                options(),
                wire(STATIONS),
                "retry delay exceeds request deadline");
        rejectOne(
                reply(STATIONS, 503, "plain", "0", options()),
                List.of("stations"),
                options().put("max_attempts", 1),
                wire(STATIONS),
                "HTTP 503 retry budget exhausted");
        for (int status : List.of(401, 403, 404, 204, 206, 422)) {
            rejectOne(
                    frame(
                            STATIONS,
                            status,
                            "binary",
                            "",
                            HIDDEN_MESSAGE.getBytes(StandardCharsets.UTF_8)),
                    List.of("stations"),
                    options(),
                    wire(STATIONS),
                    "HTTP " + status);
        }
    }

    @Test
    void enforcesStrictJsonMimeGzipUtf8AndDecodedByteBounds() throws Exception {
        ObjectNode station = stationMarker("7").put("Name", "天气");
        reset(reply(STATIONS, 200, "gzip", "", envelope("Stations", station)));
        try (Job job = job(List.of("stations"), options(), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(outReference("stations", "7", station)));
        }
        assertTrace(wire(STATIONS));
        rejectOne(
                reply(
                        STATIONS,
                        200,
                        "gzip",
                        "",
                        envelope("Stations", stationMarker("7").put("Name", "x".repeat(8192)))),
                List.of("stations"),
                options().put("max_response_bytes", 1024),
                wire(STATIONS),
                "response exceeds max_response_bytes");
        rejectOne(
                reply(STATIONS, 200, "unsupported", "", envelope("Stations", station)),
                List.of("stations"),
                options(),
                wire(STATIONS),
                "unsupported response compression");
        rejectOne(
                reply(STATIONS, 200, "binary", "", envelope("Stations", station)),
                List.of("stations"),
                options(),
                wire(STATIONS),
                "expected Content-Type application/json");
        for (byte[] invalid :
                List.of(
                        new byte[] {'{', (byte) 0xFF, '}'},
                        "{\"Stations\":[{\"StationNbr\":\"7\",\"StationNbr\":\"8\"}]}"
                                .getBytes(StandardCharsets.UTF_8),
                        "{} {}".getBytes(StandardCharsets.UTF_8))) {
            rejectOne(
                    frame(STATIONS, 200, "plain", "", invalid),
                    List.of("stations"),
                    options(),
                    wire(STATIONS),
                    "expected a successful, valid UTF-8 JSON object");
        }
    }

    @Test
    void retriesAnIncompleteBodyWithoutPublishingUnvalidatedRows() throws Exception {
        reset(
                reply(STATIONS, 200, "truncated", "", envelope("Stations", stationMarker("7"))),
                ok(STATIONS, envelope("Stations", stationMarker("8"))));
        try (Job job = job(List.of("stations"), options().put("max_attempts", 2), false, false)) {
            good(engine.executeJob(job.configName));
            assertOutput(job, List.of(outReference("stations", "8", stationMarker("8"))));
        }
        assertTrace(wire(STATIONS), wire(STATIONS));
    }

    @Test
    void checksTrustAndHostnameBeforeSendingKeysAndNeverFollowsRedirects() throws Exception {
        reset();
        failJob(
                List.of("stations"),
                options().put("trust_certificate", OTHER_PEM),
                "TLS certificate or hostname");
        failJob(
                List.of("stations"),
                options().put("endpoint", "https://cimis-other-host:18443"),
                "TLS certificate or hostname");
        assertTrace();
        rejectOne(
                reply(STATIONS, 302, "redirect", "", options()),
                List.of("stations"),
                options(),
                wire(STATIONS),
                "HTTP 302");
    }

    @Test
    void closesAnActiveReadAtDeadlineAndDoesNotAdvanceOnCancellation() throws Exception {
        rejectOne(
                stall(STATIONS),
                List.of("stations"),
                options().put("request_timeout_ms", 2500),
                wire(STATIONS),
                "request deadline exceeded");
        await(
                () -> trace().contains("disconnect\t" + STATIONS),
                "deadline closed current response body");
        reset(stall(BY_STATION));
        try (Job job =
                job(
                        List.of("data"),
                        settings("station_numbers", LEAP, LEAP, "17", "18")
                                .put("request_timeout_ms", 60000),
                        true,
                        false)) {
            Future<Container.ExecResult> running = start(job, job.id);
            try {
                await(() -> seen().size() == 1, "first target deliberately stalled");
            } finally {
                cancel(job.id, running);
            }
            await(
                    () -> trace().contains("disconnect\t" + BY_STATION),
                    "cancellation closed data request");
            assertOutput(job, List.of());
        }
        assertTrace(stationWire("17", LEAP, LEAP, false, null));
    }

    @Test
    void resumesASubdividedCalendarSliceAtTheSameDateAndTarget() throws Exception {
        ObjectNode first = observation(START, "daily", "17", null);
        ObjectNode second = observation(LEAP, "daily", "17", null);
        ObjectNode third = observation(END, "daily", "17", null);
        List<ObjectNode> committed = List.of(outData("station_numbers", "17", "station", first));
        reset(
                volume(BY_STATION),
                ok(BY_STATION, weather(provider("station", first))),
                stall(BY_STATION));
        try (Job job =
                job(
                        List.of("data"),
                        settings("station_numbers", START, END, "17")
                                .put("request_timeout_ms", 60000),
                        true,
                        false)) {
            saveActive(job, 3, committed, BY_STATION);
            queue(
                    ok(BY_STATION, weather(provider("station", second))),
                    ok(BY_STATION, weather(provider("station", third))));
            good(engine.restoreJob(job.configName, job.id));
            assertOutput(
                    job,
                    List.of(
                            outData("station_numbers", "17", "station", first),
                            outData("station_numbers", "17", "station", second),
                            outData("station_numbers", "17", "station", third)));
        }
        assertTrace(
                stationWire("17", START, END, false, null),
                stationWire("17", START, START, false, null),
                stationWire("17", LEAP, LEAP, false, null),
                stationWire("17", LEAP, LEAP, false, null),
                stationWire("17", END, END, false, null));
    }

    @Test
    void rejectsReadScopeChangesBeforeNetworkAndDoesNotReplayFinishedMetadata() throws Exception {
        reset(stall(BY_STATION));
        try (Job original =
                        job(
                                List.of("data"),
                                settings("station_numbers", START, END, "17")
                                        .put("request_timeout_ms", 60000),
                                true,
                                false);
                Job changed =
                        job(
                                List.of("data"),
                                settings("station_numbers", START, END, "18"),
                                true,
                                false)) {
            saveActive(original, 1, List.of(), BY_STATION);
            bad(engine.restoreJob(changed.configName, original.id), "read scope changed");
        }
        assertTrace(stationWire("17", START, END, false, null));
        ObjectNode station = stationMarker("7");
        ObjectNode expected = outReference("stations", "7", station);
        reset(ok(STATIONS, envelope("Stations", station)));
        try (Job job = job(List.of("stations"), options(), true, true)) {
            saveActive(job, 1, List.of(expected), null);
            Future<Container.ExecResult> restored =
                    submissions.submit(() -> engine.restoreJob(job.configName, job.id));
            try {
                await(
                        () -> "RUNNING".equals(engine.getJobStatus(job.id)),
                        "restored independent pipeline");
                long before = engine.checkpointCount(job.id);
                await(
                        () -> engine.checkpointCount(job.id) > before,
                        "restored pipeline continues checkpoints");
                assertOutput(job, List.of(expected));
                assertTrace(wire(STATIONS));
            } finally {
                cancel(job.id, restored);
            }
        }
    }

    @Test
    void emitsOnlyTheUnpublishedHourlySuffixAndNeverSubdividesAPartialReplay() throws Exception {
        ArrayNode entries = rows();
        List<String> expected = new ArrayList<>();
        for (int hour = 0; hour < 33; hour++) {
            ObjectNode value =
                    observation(
                                    hour < 24 ? START : LEAP,
                                    "hourly",
                                    "17",
                                    String.format(Locale.ROOT, "%04d", (hour % 24 + 1) * 100))
                            .put("label", "private weather sample");
            expected.add(
                    outData("station_numbers", "17", "station", value)
                            .path("record_id")
                            .textValue());
            entries.add(value);
        }
        ObjectNode good = weather(provider("station"));
        ((ObjectNode) good.path("Data").path("Providers").get(0)).set("Records", entries);
        ObjectNode changed = good.deepCopy();
        ((ObjectNode) changed.path("Data").path("Providers").get(0).path("Records").get(0))
                .put("label", "changed earlier record");
        ObjectNode reordered = good.deepCopy();
        ArrayNode other =
                (ArrayNode) reordered.path("Data").path("Providers").get(0).path("Records");
        other.set(31, entries.get(32));
        other.set(32, entries.get(31));
        reset(
                ok(BY_STATION, good),
                ok(BY_STATION, good),
                ok(BY_STATION, changed),
                ok(BY_STATION, reordered),
                volume(BY_STATION));
        CimisRecoveryProbe probe = recovery(List.of("data"));
        List<String> prefix = new ArrayList<>();
        byte[] snapshot = probe.firstChunk(prefix);
        assertEquals(expected.subList(0, 32), prefix);
        String stored = new String(snapshot, StandardCharsets.ISO_8859_1);
        assertFalse(stored.contains(KEY) || stored.contains("private weather sample"));
        List<String> suffix = new ArrayList<>();
        probe.consume(snapshot, suffix);
        assertEquals(expected.subList(32, 33), suffix);
        for (String diagnostic :
                List.of(
                        "provider slice changed since checkpoint",
                        "provider slice changed since checkpoint",
                        "HTTP 400 data volume limit")) {
            List<String> rejected = new ArrayList<>();
            Exception failure =
                    assertThrows(Exception.class, () -> probe.consume(snapshot, rejected));
            assertTrue(failure.getMessage().contains(diagnostic));
            assertTrue(rejected.isEmpty());
        }
        Seen expectedRequest = stationWire("17", START, LEAP, true, "hly-air-tmp");
        assertTrace(
                expectedRequest,
                expectedRequest,
                expectedRequest,
                expectedRequest,
                expectedRequest);
    }

    @Test
    void resumesPendingCoordinatorAndCanCompleteWithoutARequestWhenUnassignedOrCancelled()
            throws Exception {
        reset();
        CimisRecoveryProbe probe = recovery(List.of("stations"));
        byte[] pending = probe.recoverUnassigned();
        probe.finishWithoutAssignment();
        List<String> output = new ArrayList<>();
        probe.stopBeforeAssignment(output);
        assertTrue(output.isEmpty());
        assertTrace();
        queue(ok(STATIONS, envelope("Stations", stationMarker("7"))));
        probe.consume(pending, output);
        assertEquals(List.of("7"), output);
        assertTrace(wire(STATIONS));
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

    private CimisRecoveryProbe recovery(List<String> resources) throws Exception {
        URI url =
                new URI("https", null, peer.getHost(), peer.getMappedPort(18443), null, null, null);
        return new CimisRecoveryProbe(url.toString(), certificate.toString(), KEY, resources);
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
        String name = "cimis-" + UUID.randomUUID();
        String path = "/tmp/seatunnel_mnt/" + name;
        ObjectNode settings =
                options()
                        .put("plugin_output", "cimis_data")
                        .put("access_key", KEY)
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
                        + "source {\n Cimis "
                        + settings
                        + "\n"
                        + anchorInput
                        + "}\n"
                        + "sink {\n LocalFile {\n plugin_input=cimis_data\n path=\""
                        + path
                        + "\"\n"
                        + " file_format_type=json\n is_enable_transaction=true\n custom_filename=true\n"
                        + " file_name_expression=\"cimis_$"
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

    private static ObjectNode settings(String mode, String start, String end, String... targets) {
        ObjectNode settings =
                options().put("target_type", mode).put("start_date", start).put("end_date", end);
        ArrayNode selected = settings.putArray("targets");
        for (String target : targets) {
            selected.add(target);
        }
        return settings;
    }

    private static ObjectNode stationMarker(String id) {
        return options().put("StationNbr", id).put("Name", "Reference Station");
    }

    private static ObjectNode zip(String id) {
        return options().put("ZipCode", id).put("IsActive", "True");
    }

    private static ObjectNode observation(String date, String scope, String id, String hour) {
        ObjectNode result =
                options()
                        .put("Date", date)
                        .put("Julian", String.valueOf(LocalDate.parse(date).getDayOfYear()))
                        .put("Scope", scope);
        if (id != null) {
            result.put("Station", id);
        }
        if (hour != null) {
            result.put("Hour", hour);
        }
        return result;
    }

    private static ObjectNode provider(String type, JsonNode... rows) {
        return envelope("Records", rows)
                .put("Name", "cimis")
                .put("Type", type)
                .put("Owner", "water.ca.gov");
    }

    private static ObjectNode weather(JsonNode... providers) {
        return object("Data", envelope("Providers", providers));
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

    private static ObjectNode outReference(String resource, String id, ObjectNode payload) {
        return object("payload", payload)
                .put("resource", resource)
                .put("record_id", id)
                .putNull("target")
                .putNull("date")
                .putNull("hour")
                .putNull("provider_name")
                .putNull("provider_type")
                .putNull("provider_owner");
    }

    private static ObjectNode outData(String mode, String target, String type, ObjectNode payload)
            throws Exception {
        String location =
                "station".equals(type)
                        ? payload.path("Station").asText()
                        : String.join(
                                "\n",
                                payload.path("Coordinate").asText(""),
                                payload.path("Address").asText(""),
                                payload.path("ZipCodes").asText(""));
        String date = payload.path("Date").textValue();
        String scope = payload.path("Scope").textValue();
        String hour = payload.path("Hour").textValue();
        String joined =
                String.join(
                        "\n",
                        mode,
                        target,
                        "cimis",
                        type,
                        "water.ca.gov",
                        location,
                        date,
                        scope,
                        hour == null ? "" : hour);
        String id =
                Base64.getUrlEncoder()
                        .withoutPadding()
                        .encodeToString(
                                MessageDigest.getInstance("SHA-256")
                                        .digest(joined.getBytes(StandardCharsets.UTF_8)));
        return object("payload", payload)
                .put("resource", "data")
                .put("record_id", id)
                .put("target", target)
                .put("date", date)
                .put("hour", hour)
                .put("provider_name", "cimis")
                .put("provider_type", type)
                .put("provider_owner", "water.ca.gov");
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
            assertEquals(
                    "true",
                    parts[4],
                    "expected exact APIM subscription header and no Authorization");
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

    private static Seen wire(String path) {
        return new Seen(path, Map.of(), 0L);
    }

    private static Seen dataWire(
            String path,
            String key,
            String target,
            String start,
            String end,
            String unit,
            Boolean hourly,
            String prefer,
            String items) {
        Map<String, String> query = new TreeMap<>();
        query.put(key, target);
        query.put("startDate", start);
        query.put("endDate", end);
        query.put("unitOfMeasure", unit);
        if (hourly != null) {
            query.put("isHourly", hourly.toString());
        }
        if (prefer != null) {
            query.put("prefer", prefer);
        }
        if (items != null) {
            query.put("dataItems", items);
        }
        return new Seen(path, query, 0L);
    }

    private static Seen stationWire(
            String target, String from, String to, boolean hourly, String items) {
        return dataWire(BY_STATION, "stationNbrs", target, from, to, "E", hourly, null, items);
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
        assertFalse(visible.contains(KEY), "subscription key leaked");
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
        private final String id = CimisIT.id();

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
                                    + "' -maxdepth 1 -type f -name 'cimis_*' -exec cat {} +; fi");
            assertEquals(0, files.getExitCode(), files.getStderr());
            List<ObjectNode> output = new ArrayList<>();
            for (String line : files.getStdout().split("\\R")) {
                if (!line.isBlank()) {
                    ObjectNode row = (ObjectNode) JSON.readTree(line);
                    assertTrue(row.path("payload").isTextual());
                    row.set("payload", JSON.readTree(row.path("payload").textValue()));
                    for (String nullable :
                            List.of(
                                    "target",
                                    "date",
                                    "hour",
                                    "provider_name",
                                    "provider_type",
                                    "provider_owner")) {
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
            // The Cimis pipeline can already have ended. Observe all active pipeline counters.
            long completed = 0;
            for (JsonNode pipeline : JSON.readTree(response.body()).path("pipelines")) {
                completed += pipeline.path("counts").path("completed").asLong();
            }
            return completed;
        }
    }
}

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
package org.apache.seatunnel.connectors.seatunnel.cimis;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Validates CIMIS resource, target and date selection.
 *
 * <p>Only a scope fingerprint is needed in checkpoint state.
 */
final class CimisConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CIMIS API Management subscription key.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Selected CIMIS station, ZIP or weather data resources.");
    static final Option<String> TARGET_TYPE =
            Options.key("target_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Weather selector: station_numbers, zip_codes, coordinates or addresses.");
    static final Option<List<String>> TARGETS =
            Options.key("targets")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "One or more site, ZIP, coordinate or address values of the selected type.");
    static final Option<String> START =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive first CIMIS calendar date, yyyy-MM-dd.");
    static final Option<String> END =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive last CIMIS calendar date, yyyy-MM-dd.");
    static final Option<String> UNIT =
            Options.key("unit_of_measure")
                    .stringType()
                    .defaultValue("E")
                    .withDescription("E for English units or M for metric units.");
    static final Option<String> ZIP_PREFERENCE =
            Options.key("zip_preference")
                    .stringType()
                    .defaultValue("SCS")
                    .withDescription("Preferred provider for daily ZIP targets, SCS or WSN.");
    static final Option<List<String>> DAILY =
            Options.key("daily_data_items")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Requested current daily CIMIS data items.");
    static final Option<List<String>> HOURLY =
            Options.key("hourly_data_items")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Requested current hourly station data items.");
    static final Option<Integer> SLICE_DAYS =
            Options.key("slice_days")
                    .intType()
                    .defaultValue(31)
                    .withDescription("Maximum number of calendar days per target request.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(100000)
                    .withDescription(
                            "Maximum completed reference and weather responses or safe volume subdivisions processed in a scan.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Maximum attempts per page including the initial HTTPS request.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total page acquisition budget in milliseconds, including retries.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded bytes in one successful page response.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom HTTPS origin that requires a worker-local trust certificate.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Worker-local PEM certificate for the configured custom origin.");

    final String accessKey;
    final List<CimisResource> plan;
    final CimisTarget targetType;
    final List<String> targets;
    final LocalDate start;
    final LocalDate end;
    final String unit;
    final String prefer;
    final List<String> daily;
    final List<String> hourly;
    /**
     * Selected observation intervals in order.
     *
     * <p>Independent requests preserve the service's daily and hourly boundaries.
     */
    final List<Boolean> hourlyModes;

    final int sliceDays;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;

    CimisConfig(ReadonlyConfig options) {
        accessKey = options.get(ACCESS_KEY);
        if (accessKey == null
                || accessKey.isEmpty()
                || accessKey.length() > 4096
                || accessKey.chars().anyMatch(character -> character < 33 || character > 126)) {
            throw invalid("access_key", "expected a printable subscription key without whitespace");
        }
        List<String> selected = options.get(RESOURCES);
        if (selected == null
                || selected.isEmpty()
                || selected.size() > CimisResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        EnumSet<CimisResource> unique = EnumSet.noneOf(CimisResource.class);
        for (String name : selected) {
            if (!unique.add(CimisResource.from(name))) {
                throw invalid("resources", "choose each resource only once");
            }
        }
        plan = List.copyOf(unique);
        boolean data = unique.contains(CimisResource.DATA);
        targetType = data ? CimisTarget.from(options.get(TARGET_TYPE)) : null;
        targets = data ? targets(options.get(TARGETS), targetType) : List.of();
        start = data ? date(options.get(START), "start_date") : null;
        end = data ? date(options.get(END), "end_date") : null;
        if (data && end.isBefore(start)) {
            throw invalid("end_date", "must be on or after start_date");
        }
        unit = options.get(UNIT);
        if (!"E".equals(unit) && !"M".equals(unit)) {
            throw invalid("unit_of_measure", "expected E or M");
        }
        prefer = options.get(ZIP_PREFERENCE);
        if (!"SCS".equals(prefer) && !"WSN".equals(prefer)) {
            throw invalid("zip_preference", "expected SCS or WSN");
        }
        boolean spatial = data && targetType.spatialOnly;
        daily = data ? CimisDataItems.checked(options.get(DAILY), false, spatial) : List.of();
        hourly = data ? CimisDataItems.checked(options.get(HOURLY), true, false) : List.of();
        if (spatial && !hourly.isEmpty()) {
            throw invalid("hourly_data_items", "spatial coordinates and addresses are daily only");
        }
        List<Boolean> modes = new ArrayList<>(2);
        if (data && (!daily.isEmpty() || hourly.isEmpty())) {
            modes.add(false);
        }
        if (!hourly.isEmpty()) {
            modes.add(true);
        }
        hourlyModes = List.copyOf(modes);
        sliceDays = range(options, SLICE_DAYS, 1, 31);
        maxRequests = range(options, MAX_REQUESTS, 1, 1000000);
        attempts = range(options, MAX_ATTEMPTS, 1, 10);
        timeout = range(options, TIMEOUT, 100, 300000);
        maxBytes = range(options, MAX_BYTES, 1024, 64 * 1024 * 1024);
        String endpoint = options.get(ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = endpoint == null ? "https://et.water.ca.gov" : httpsOrigin(endpoint);
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(accessKey),
                                plan.toString(),
                                origin,
                                Objects.toString(targetType, ""),
                                String.join("\n", targets),
                                Objects.toString(start, ""),
                                Objects.toString(end, ""),
                                unit,
                                prefer,
                                daily.toString(),
                                hourly.toString(),
                                Integer.toString(sliceDays),
                                Integer.toString(maxRequests),
                                Integer.toString(maxBytes),
                                Objects.toString(trustCertificate, "")));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(ACCESS_KEY, RESOURCES)
                .optional(
                        TARGET_TYPE,
                        TARGETS,
                        START,
                        END,
                        UNIT,
                        ZIP_PREFERENCE,
                        DAILY,
                        HOURLY,
                        SLICE_DAYS,
                        MAX_REQUESTS,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        ENDPOINT,
                        TRUST_CERTIFICATE)
                .build();
    }

    private static LocalDate date(String value, String key) {
        if (value != null && value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
            try {
                LocalDate date = LocalDate.parse(value);
                if (date.getYear() >= 1) {
                    return date;
                }
            } catch (DateTimeParseException ignored) {
                // Avoid returning supplied values in diagnostic text.
            }
        }
        throw invalid(key, "data requires a valid yyyy-MM-dd date");
    }

    private static List<String> targets(List<String> values, CimisTarget family) {
        if (values == null || values.isEmpty() || values.size() > 100) {
            throw invalid("targets", "data requires 1 to 100 targets");
        }
        Set<String> unique = new LinkedHashSet<>();
        for (String target : values) {
            if (!unique.add(family.validated(target))) {
                throw invalid("targets", "duplicate normalized target");
            }
        }
        return List.copyOf(unique);
    }

    private static String httpsOrigin(String value) {
        try {
            URI uri = URI.create(value);
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (uri.getPort() == -1 || (uri.getPort() > 0 && uri.getPort() <= 65535))
                    && ("".equals(uri.getRawPath()) || "/".equals(uri.getRawPath()))) {
                return "https://" + uri.getRawAuthority();
            }
        } catch (IllegalArgumentException ignored) {
            // The invalid option value may contain secrets and must not enter diagnostics.
        }
        throw invalid("endpoint", "expected a bare HTTPS origin");
    }

    private static int range(ReadonlyConfig settings, Option<Integer> option, int min, int max) {
        int value = settings.get(option);
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a value from " + min + " to " + max);
        }
        return value;
    }

    static String digest(String text) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(sha256().digest(text.getBytes(StandardCharsets.UTF_8)));
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String key, String message) {
        return new IllegalArgumentException("Cimis option " + key + ": " + message);
    }
}

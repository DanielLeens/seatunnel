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
package org.apache.seatunnel.connectors.seatunnel.chift;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Validates Chift account authentication and requested resources.
 *
 * <p>Only a scope fingerprint is needed in checkpoint state.
 */
final class ChiftConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern UUID_PATTERN =
            Pattern.compile("[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}");

    static final Option<String> CLIENT_ID =
            Options.key("auth_client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Chift API client ID.");
    static final Option<String> CLIENT_SECRET =
            Options.key("auth_client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Chift API client secret.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription("One or more Chift resources.");
    static final Option<String> ACCOUNT_ID =
            Options.key("auth_account_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Chift account UUID associated with the credentials.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive execution start timestamp with a UTC offset.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive execution start timestamp with a UTC offset.");
    static final Option<String> TIME_ZONE =
            Options.key("legacy_timezone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Zone for offset-free execution timestamps, if the deployment returns them.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription("Requested page width for sync executions.");
    static final Option<Integer> MAX_PARENTS =
            Options.key("max_parents")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Maximum consumer or sync identifiers retained for child traversal.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("Maximum successful page responses processed in a scan.");
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

    final String clientId;
    final String clientSecret;
    final String accountId;
    final Instant start;
    final Instant end;
    final ZoneId timezone;
    final List<ChiftResource> resources;
    /**
     * Ordered reads including the parents of any requested child collection.
     *
     * <p>Automatically added parents are used for scope resolution only.
     */
    final List<ChiftResource> plan;

    final int pageSize;
    final int maxParents;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;

    ChiftConfig(ReadonlyConfig options) {
        clientId = credential(options, CLIENT_ID);
        clientSecret = credential(options, CLIENT_SECRET);
        accountId = options.get(ACCOUNT_ID);
        if (!isUuid(accountId)) {
            throw invalid("auth_account_id", "expected a UUID");
        }
        List<String> selected = options.get(RESOURCES);
        if (selected == null
                || selected.isEmpty()
                || selected.size() > ChiftResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        EnumSet<ChiftResource> unique = EnumSet.noneOf(ChiftResource.class);
        for (String name : selected) {
            if (!unique.add(ChiftResource.from(name))) {
                throw invalid("resources", "choose each resource only once");
            }
        }
        resources = List.copyOf(unique);
        if (unique.contains(ChiftResource.CONNECTIONS)) {
            unique.add(ChiftResource.CONSUMERS);
        }
        if (unique.contains(ChiftResource.EXECUTIONS)) {
            unique.add(ChiftResource.SYNCS);
        }
        plan = List.copyOf(unique);
        start = date(options.get(START_DATE), "start_date");
        end = date(options.get(END_DATE), "end_date");
        if ((start != null || end != null) && !unique.contains(ChiftResource.EXECUTIONS)) {
            throw invalid("start_date / end_date", "date filters apply only to executions");
        }
        if (start != null && end != null && !end.isAfter(start)) {
            throw invalid("end_date", "must be later than start_date");
        }
        String zone = options.get(TIME_ZONE);
        try {
            timezone = zone == null ? null : ZoneId.of(zone);
        } catch (DateTimeException invalid) {
            throw invalid("legacy_timezone", "expected an IANA zone or UTC offset");
        }
        pageSize = range(options, PAGE_SIZE, 1, 500);
        maxParents = range(options, MAX_PARENTS, 1, 50000);
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
        origin = endpoint == null ? "https://api.chift.eu" : httpsOrigin(endpoint);
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(clientId),
                                digest(clientSecret),
                                digest(accountId),
                                resources.toString(),
                                origin,
                                Objects.toString(timezone, ""),
                                Objects.toString(start, ""),
                                Objects.toString(end, ""),
                                Integer.toString(pageSize),
                                Integer.toString(maxParents),
                                Integer.toString(maxBytes),
                                Integer.toString(maxRequests),
                                Objects.toString(trustCertificate, "")));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(CLIENT_ID, CLIENT_SECRET, ACCOUNT_ID, RESOURCES)
                .optional(
                        START_DATE,
                        END_DATE,
                        TIME_ZONE,
                        PAGE_SIZE,
                        MAX_PARENTS,
                        MAX_REQUESTS,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        ENDPOINT,
                        TRUST_CERTIFICATE)
                .build();
    }

    static boolean isUuid(String text) {
        return text != null && UUID_PATTERN.matcher(text).matches();
    }

    Instant cutoff() {
        if (end == null && start != null) {
            Instant now = Instant.now();
            if (!now.isAfter(start)) {
                throw invalid("start_date", "must precede the captured cutoff");
            }
            return now;
        }
        return end;
    }

    private static String credential(ReadonlyConfig options, Option<String> setting) {
        String value = options.get(setting);
        if (value == null
                || value.isBlank()
                || value.length() > 4096
                || value.codePoints()
                        .anyMatch(
                                character ->
                                        Character.isISOControl(character)
                                                || character >= Character.MIN_SURROGATE
                                                        && character <= Character.MAX_SURROGATE)) {
            throw invalid(
                    setting.key(), "expected a nonempty credential without control characters");
        }
        return value;
    }

    private static Instant date(String value, String key) {
        if (value == null) {
            return null;
        }
        try {
            Instant parsed = OffsetDateTime.parse(value).toInstant();
            if (parsed.isAfter(Instant.parse("0001-01-03T00:00:00Z"))
                    && parsed.isBefore(Instant.parse("9999-12-29T00:00:00Z"))) {
                return parsed;
            }
        } catch (DateTimeParseException ignored) {
            // Invalid option values must not enter diagnostic text.
        }
        throw invalid(key, "expected an ISO-8601 timestamp with a UTC offset");
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
        return new IllegalArgumentException("Chift option " + key + ": " + message);
    }
}

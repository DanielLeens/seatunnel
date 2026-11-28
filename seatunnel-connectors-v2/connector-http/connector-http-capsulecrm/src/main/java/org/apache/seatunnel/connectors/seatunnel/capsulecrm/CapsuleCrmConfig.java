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
package org.apache.seatunnel.connectors.seatunnel.capsulecrm;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Validates Capsule CRM resource selection and transport bounds.
 *
 * <p>Only a scope fingerprint is needed in checkpoint state.
 */
final class CapsuleCrmConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Capsule personal access token or pre-issued OAuth bearer token.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription("One or more Capsule CRM API resources.");
    static final Option<String> ENTITY =
            Options.key("entity")
                    .stringType()
                    .defaultValue("parties")
                    .withDescription(
                            "Tag and custom-field entity: parties, opportunities or kases.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive opportunity updatedAt lower bound as an ISO-8601 timestamp.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive opportunity updatedAt upper bound as an ISO-8601 timestamp.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Requested size for supported Capsule API pages.");
    static final Option<Integer> MAX_PARENTS =
            Options.key("max_parents")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum organisation IDs retained for employee reads.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("Maximum successful list responses processed in a scan.");
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

    final String token;
    final String entity;
    final Instant start;
    final Instant end;
    final int pageSize;
    final List<CapsuleCrmResource> resources;
    /**
     * Ordered reads with implicit organisation discovery.
     *
     * <p>Output selection remains independent of the party query required to address employee
     * routes.
     */
    final List<CapsuleCrmResource> plan;

    final int maxParents;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;
    final boolean discoverParents;

    CapsuleCrmConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || token.chars().anyMatch(value -> value < 33 || value > 126)) {
            throw invalid(
                    "token", "expected a token without a prefix, spaces or control characters");
        }
        List<String> selected = options.get(RESOURCES);
        if (selected == null
                || selected.isEmpty()
                || selected.size() > CapsuleCrmResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        entity = options.get(ENTITY);
        if (entity == null || !List.of("parties", "opportunities", "kases").contains(entity)) {
            throw invalid("entity", "expected parties, opportunities or kases");
        }
        start = date(options.get(START_DATE), "start_date");
        end = date(options.get(END_DATE), "end_date");
        if (start != null && end != null && !end.isAfter(start)) {
            throw invalid("end_date", "must be later than start_date");
        }
        EnumSet<CapsuleCrmResource> unique = EnumSet.noneOf(CapsuleCrmResource.class);
        for (String name : selected) {
            if (!unique.add(CapsuleCrmResource.from(name))) {
                throw invalid("resources", "choose each resource only once");
            }
        }
        resources = List.copyOf(unique);
        discoverParents = resources.contains(CapsuleCrmResource.EMPLOYEES);
        if (discoverParents) {
            unique.add(CapsuleCrmResource.PARTIES);
        }
        if ((start != null || end != null)
                && !resources.contains(CapsuleCrmResource.OPPORTUNITIES)) {
            throw invalid("start_date / end_date", "date filters apply only to opportunities");
        }
        plan = List.copyOf(unique);
        pageSize = range(options, PAGE_SIZE, 1, 100);
        maxParents = range(options, MAX_PARENTS, 1, 50000);
        maxRequests = range(options, MAX_REQUESTS, 1, 1000000);
        attempts = range(options, MAX_ATTEMPTS, 1, 10);
        timeout = range(options, TIMEOUT, 100, 3700000);
        maxBytes = range(options, MAX_BYTES, 1024, 64 * 1024 * 1024);
        String endpoint = options.get(ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = endpoint == null ? "https://api.capsulecrm.com" : httpsOrigin(endpoint);
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(token),
                                entity,
                                Objects.toString(start, ""),
                                Objects.toString(end, ""),
                                Integer.toString(pageSize),
                                Integer.toString(maxParents),
                                Integer.toString(maxRequests),
                                Integer.toString(maxBytes),
                                resources.toString(),
                                origin,
                                Objects.toString(trustCertificate, "")));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(TOKEN, RESOURCES)
                .optional(
                        ENTITY,
                        START_DATE,
                        END_DATE,
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

    Instant cutoff() {
        return end == null && start != null ? Instant.now() : end;
    }

    String since() {
        // The API filters strictly after since. Overlap one whole second, then filter precisely.
        return start == null
                ? null
                : start.truncatedTo(ChronoUnit.SECONDS).minusSeconds(1).toString();
    }

    private static Instant date(String value, String key) {
        if (value == null) {
            return null;
        }
        try {
            OffsetDateTime parsed = OffsetDateTime.parse(value);
            Instant instant = parsed.toInstant();
            if (instant.isAfter(Instant.parse("0001-01-01T00:00:00Z"))
                    && instant.isBefore(Instant.parse("9999-12-31T00:00:00Z"))) {
                return instant;
            }
        } catch (DateTimeParseException ignored) {
            // Do not copy a submitted configuration value into errors.
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
        return new IllegalArgumentException("Capsule CRM option " + key + ": " + message);
    }
}

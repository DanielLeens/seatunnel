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
package org.apache.seatunnel.connectors.seatunnel.clazar;

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
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Validates Clazar resource selection, authentication, and the bounded scan window.
 *
 * <p>Checkpoints keep a scope digest instead of configuration credentials.
 */
final class ClazarConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern UUID =
            Pattern.compile("[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}");

    static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clazar client ID for the token exchange.");
    static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clazar client secret for the token exchange.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Clazar entity collections or marketplace analytics datasets.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive modified timestamp with an offset for entities.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive modified timestamp with an offset for entities.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Requested page size for entity and analytics collections.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum committed pages per scan.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Maximum attempts per page GET including authentication renewal.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Acquisition deadline including token exchange, retries, and decoding.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded bytes per page response.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional custom bare HTTPS origin requiring a trust file.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Worker-local X.509 PEM certificate file for a custom origin.");

    final String clientId;
    final String clientSecret;
    final List<ClazarResource> resources;
    final Instant start;
    final Instant end;
    final int pageSize;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;

    ClazarConfig(ReadonlyConfig options) {
        clientId = credential(options, CLIENT_ID);
        clientSecret = credential(options, CLIENT_SECRET);
        resources = selections(options.get(RESOURCES));
        start = timestamp(options.get(START_DATE), "start_date");
        end = timestamp(options.get(END_DATE), "end_date");
        if ((start != null || end != null) && resources.stream().noneMatch(r -> r.entity)) {
            throw invalid("start_date / end_date", "choose an entity collection for date bounds");
        }
        if (start != null && end != null && !end.isAfter(start)) {
            throw invalid("end_date", "must be later than start_date");
        }
        pageSize = range(options, PAGE_SIZE, 1, 5000);
        maxRequests = range(options, MAX_REQUESTS, 1, 20000);
        attempts = range(options, MAX_ATTEMPTS, 1, 6);
        timeout = range(options, TIMEOUT, 100, 300000);
        maxBytes = range(options, MAX_BYTES, 1024, 32 * 1024 * 1024);
        String endpoint = options.get(ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = endpoint == null ? "https://api.clazar.io" : httpsOrigin(endpoint);
        scope =
                digest(
                        String.join(
                                "\n",
                                "clazar-v1",
                                digest(clientId),
                                digest(clientSecret),
                                resources.toString(),
                                Objects.toString(start, ""),
                                Objects.toString(end, ""),
                                Integer.toString(pageSize),
                                Integer.toString(maxRequests),
                                Integer.toString(maxBytes),
                                origin,
                                Objects.toString(trustCertificate, "")));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(CLIENT_ID, CLIENT_SECRET, RESOURCES)
                .optional(
                        START_DATE,
                        END_DATE,
                        PAGE_SIZE,
                        MAX_REQUESTS,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        ENDPOINT,
                        TRUST_CERTIFICATE)
                .build();
    }

    private static List<ClazarResource> selections(List<String> names) {
        if (names == null || names.isEmpty() || names.size() > ClazarResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        EnumSet<ClazarResource> selected = EnumSet.noneOf(ClazarResource.class);
        for (String name : names) {
            if (!selected.add(ClazarResource.from(name))) {
                throw invalid("resources", "choose each resource only once");
            }
        }
        return List.copyOf(selected);
    }

    private static String credential(ReadonlyConfig options, Option<String> key) {
        String value = options.get(key);
        if (value == null
                || value.isBlank()
                || value.length() > 4096
                || value.codePoints().anyMatch(ClazarConfig::unsafeCharacter)) {
            throw invalid(key.key(), "expected a nonempty credential");
        }
        return value;
    }

    static boolean unsafeCharacter(int codePoint) {
        return Character.isISOControl(codePoint)
                || codePoint >= Character.MIN_SURROGATE && codePoint <= Character.MAX_SURROGATE;
    }

    static boolean isUuid(String value) {
        return value != null && UUID.matcher(value).matches();
    }

    Instant cutoff() {
        if (end != null || start == null) {
            return end;
        }
        Instant now = Instant.now();
        if (!now.isAfter(start)) {
            throw invalid("start_date", "must precede the captured cutoff");
        }
        return now;
    }

    private static Instant timestamp(String value, String key) {
        if (value == null) {
            return null;
        }
        try {
            if (value.length() <= 64) {
                return OffsetDateTime.parse(value).toInstant();
            }
        } catch (DateTimeParseException ignored) {
            // Do not echo an invalid option value.
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
            // Credentials embedded in a bad URI must stay out of diagnostics.
        }
        throw invalid("endpoint", "expected a bare HTTPS origin");
    }

    private static int range(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a value from " + min + " to " + max);
        }
        return value;
    }

    static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    static String digest(byte[] value) {
        try {
            return Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String key, String message) {
        return new IllegalArgumentException("Clazar option " + key + ": " + message);
    }
}

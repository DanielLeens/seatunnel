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
package org.apache.seatunnel.connectors.seatunnel.chargify;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Validates Chargify resource selection and transport bounds.
 *
 * <p>Only a scope fingerprint is needed in checkpoint state.
 */
final class ChargifyConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Maxio Advanced Billing API key.");
    static final Option<String> SITE =
            Options.key("site")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Single site DNS label, without a protocol or domain suffix.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "One or more resources: customers, subscriptions, invoices, coupons, transactions.");
    static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .defaultValue("us")
                    .withDescription("Advanced Billing environment, us or eu.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(200)
                    .withDescription("Maximum requested rows per collection page.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("Maximum successfully processed pages processed in a scan.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Maximum attempts per page including the initial HTTPS request.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(240000)
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
    final String site;
    final String region;
    final List<ChargifyResource> plan;
    final int pageSize;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;

    ChargifyConfig(ReadonlyConfig options) {
        accessKey = options.get(ACCESS_KEY);
        if (accessKey == null
                || accessKey.isEmpty()
                || accessKey.length() > 4096
                || accessKey
                        .chars()
                        .anyMatch(
                                character ->
                                        character < 33 || character > 126 || character == ':')) {
            throw invalid("access_key", "expected an API key without whitespace or a colon");
        }
        String label = options.get(SITE);
        if (label == null || !label.matches("[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?")) {
            throw invalid("site", "expected one DNS site label");
        }
        site = label.toLowerCase(Locale.ROOT);
        region = options.get(REGION);
        if (!"us".equals(region) && !"eu".equals(region)) {
            throw invalid("region", "expected us or eu");
        }
        List<String> selected = options.get(RESOURCES);
        if (selected == null
                || selected.isEmpty()
                || selected.size() > ChargifyResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        EnumSet<ChargifyResource> unique = EnumSet.noneOf(ChargifyResource.class);
        for (String name : selected) {
            if (!unique.add(ChargifyResource.from(name))) {
                throw invalid("resources", "choose each resource only once");
            }
        }
        plan = List.copyOf(unique);
        pageSize = range(options, PAGE_SIZE, 1, 200);
        maxRequests = range(options, MAX_REQUESTS, 1, 1000000);
        attempts = range(options, MAX_ATTEMPTS, 1, 10);
        timeout = range(options, TIMEOUT, 100, 600000);
        maxBytes = range(options, MAX_BYTES, 1024, 64 * 1024 * 1024);
        String endpoint = options.get(ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin =
                endpoint == null
                        ? "https://"
                                + site
                                + ("eu".equals(region) ? ".ebilling.maxio.com" : ".chargify.com")
                        : httpsOrigin(endpoint);
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(accessKey),
                                site,
                                region,
                                plan.toString(),
                                Integer.toString(pageSize),
                                Integer.toString(maxRequests),
                                Integer.toString(maxBytes),
                                origin,
                                Objects.toString(trustCertificate, "")));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(ACCESS_KEY, SITE, RESOURCES)
                .optional(
                        REGION,
                        PAGE_SIZE,
                        MAX_REQUESTS,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        ENDPOINT,
                        TRUST_CERTIFICATE)
                .build();
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
        return new IllegalArgumentException("Chargify option " + key + ": " + message);
    }
}

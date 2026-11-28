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

package org.apache.seatunnel.connectors.seatunnel.circa;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.API_KEY;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.INCREMENTAL_START;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.LOOKBACK_SECONDS;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.circa.CircaSourceOptions.TLS_TRUST_CERTIFICATE;

/**
 * Holds validated settings, fixed API authority and logical read bounds.
 *
 * <p>Checkpoint identity is bound to the same organization key and resource without saving the key
 * in a split.
 */
final class CircaConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern BEARER = Pattern.compile("[A-Za-z0-9._~+/=-]{1,4096}");
    final String host = "app.circa.co";
    final String token;
    final CircaResource resource;
    final Instant lower;
    final Instant end;
    final String scope;
    final int maxPages;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final String trustCertificate;

    CircaConfig(ReadonlyConfig options) {
        token = options.get(API_KEY);
        if (token == null || !BEARER.matcher(token).matches()) {
            throw invalid("api_key must be a valid Bearer credential");
        }
        resource = CircaResource.of(options.get(RESOURCE));
        Instant start = readTime(options, START_DATE);
        Instant prior = readTime(options, INCREMENTAL_START);
        end = readTime(options, END_DATE);
        int lookback = number(options, LOOKBACK_SECONDS, 0, 86400);
        if ((!resource.incremental && (start != null || prior != null || end != null))
                || start == null && (prior != null || end != null)) {
            throw invalid("date bounds require start_date on events, contacts or companies");
        }
        Instant candidate = prior == null ? null : prior.minusSeconds(lookback);
        lower = start != null && candidate != null && candidate.isAfter(start) ? candidate : start;
        if (end != null && end.isBefore(lower)) {
            throw invalid("end_date precedes the effective lower bound");
        }
        maxPages = number(options, MAX_PAGES, 1, 100000);
        connectTimeout = number(options, CONNECT_TIMEOUT_MS, 1, 120000);
        readTimeout = number(options, READ_TIMEOUT_MS, 1, 300000);
        deadlineMs = number(options, REQUEST_DEADLINE_MS, 1, 300000);
        maxAttempts = number(options, MAX_ATTEMPTS, 1, 10);
        maxBytes = number(options, MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        trustCertificate =
                options.getOptional(TLS_TRUST_CERTIFICATE)
                        .map(CircaConfig::certificate)
                        .orElse(null);
        scope =
                CircaJson.fingerprint(
                        List.of(
                                host,
                                resource.id,
                                token,
                                lower == null ? "" : lower.toString(),
                                end == null ? "" : end.toString(),
                                Integer.toString(maxPages)));
    }

    static Instant timestamp(String value) {
        return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
    }

    private static Instant readTime(ReadonlyConfig input, Option<String> key) {
        String value = input.getOptional(key).orElse(null);
        if (value == null) {
            return null;
        }
        try {
            Instant result = timestamp(value);
            if (result.getNano() == 0
                    && result.isAfter(Instant.parse("0001-01-01T00:00:00Z"))
                    && result.isBefore(Instant.parse("9999-12-31T23:59:59Z"))) {
                return result;
            }
        } catch (DateTimeException ignored) {
            // Report only the key because public options may come from substituted secrets.
        }
        throw invalid(key.key() + " must be a whole-second ISO 8601 timestamp with an offset");
    }

    private static String certificate(String value) {
        try {
            if (Path.of(value).isAbsolute()) {
                return value;
            }
        } catch (InvalidPathException ignored) {
            // Keep local paths out of factory errors.
        }
        throw invalid("tls_trust_certificate must be an absolute path");
    }

    private static int number(ReadonlyConfig input, Option<Integer> key, int min, int max) {
        int value = input.get(key);
        if (value < min || value > max) {
            throw invalid(key.key() + " must be between " + min + " and " + max);
        }
        return value;
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("Circa source: " + detail);
    }
}

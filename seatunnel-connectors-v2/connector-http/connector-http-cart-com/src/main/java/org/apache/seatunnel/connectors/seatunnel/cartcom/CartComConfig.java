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

package org.apache.seatunnel.connectors.seatunnel.cartcom;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Validates the provider origin, exclusive authentication form and immutable read bounds.
 *
 * <p>The configuration digest binds checkpoints to their original merchant, credentials, resource
 * and query without putting credentials themselves in split state.
 */
final class CartComConfig implements Serializable {
    static final String CENTRAL_HOST = "public.americommerce.com";
    private static final long serialVersionUID = 1L;
    private static final Pattern DNS_HOST =
            Pattern.compile(
                    "(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\\.)+[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?");
    private static final Pattern HEADER_TOKEN = Pattern.compile("[\\x21-\\x7e]{1,8192}");

    final String host;
    final String token;
    final String siteId;
    final String username;
    final String secret;
    final String trustCertificate;
    final CartComResource resource;
    final Instant start;
    final Instant end;
    final Instant lower;
    final int pageSize;
    final int maxPages;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final String scope;

    CartComConfig(ReadonlyConfig options) {
        resource = CartComResource.parse(options.get(CartComSourceOptions.RESOURCE));
        token = options.getOptional(CartComSourceOptions.TOKEN).orElse(null);
        siteId = options.getOptional(CartComSourceOptions.SITE_ID).orElse(null);
        username = options.getOptional(CartComSourceOptions.USERNAME).orElse(null);
        secret = options.getOptional(CartComSourceOptions.SECRET_KEY).orElse(null);
        String merchant = options.getOptional(CartComSourceOptions.STORE_HOST).orElse(null);
        if (merchant != null || token != null) {
            if (merchant == null
                    || !header(token)
                    || siteId != null
                    || username != null
                    || secret != null) {
                throw invalid("single-store mode requires store_host and token only");
            }
            host = merchant.toLowerCase(Locale.ROOT);
            if (!DNS_HOST.matcher(host).matches() || CENTRAL_HOST.equals(host)) {
                throw invalid("store_host must be a merchant DNS hostname without a URL or port");
            }
        } else {
            if (!header(siteId)
                    || !header(username)
                    || siteId.length() > 256
                    || username.length() > 256
                    || secret == null
                    || secret.isBlank()
                    || secret.length() > 8192
                    || !StandardCharsets.UTF_8.newEncoder().canEncode(secret)) {
                throw invalid("central router mode requires site_id, username and secret_key");
            }
            host = CENTRAL_HOST;
        }
        trustCertificate =
                options.getOptional(CartComSourceOptions.TLS_TRUST_CERTIFICATE)
                        .map(CartComConfig::certificatePath)
                        .orElse(null);
        start = dateOption(options.get(CartComSourceOptions.START_DATE), "start_date");
        end =
                options.getOptional(CartComSourceOptions.END_DATE)
                        .map(value -> dateOption(value, "end_date"))
                        .orElse(null);
        Instant resume =
                options.getOptional(CartComSourceOptions.INCREMENTAL_START)
                        .map(value -> dateOption(value, "incremental_start"))
                        .orElse(null);
        int lookback = range(options, CartComSourceOptions.LOOKBACK_SECONDS, 0, 31_536_000);
        if (!resource.incremental && (resume != null || lookback != 0)) {
            throw invalid("incremental_start and lookback_seconds require an updated_at resource");
        }
        if (end != null && (end.isBefore(start) || resume != null && end.isBefore(resume))) {
            throw invalid("end_date must not precede start_date or incremental_start");
        }
        Instant replay = resume == null ? start : resume.minusSeconds(lookback);
        lower = replay.isBefore(start) ? start : replay;
        pageSize = range(options, CartComSourceOptions.PAGE_SIZE, 1, 1000);
        maxPages = range(options, CartComSourceOptions.MAX_PAGES, 1, 20000);
        connectTimeout = range(options, CartComSourceOptions.CONNECT_TIMEOUT_MS, 1, 120000);
        readTimeout = range(options, CartComSourceOptions.READ_TIMEOUT_MS, 1, 120000);
        deadlineMs = range(options, CartComSourceOptions.REQUEST_DEADLINE_MS, 1, 300000);
        maxAttempts = range(options, CartComSourceOptions.MAX_ATTEMPTS, 1, 10);
        maxBytes = range(options, CartComSourceOptions.MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        String auth =
                token != null
                        ? CartComJson.fingerprint(List.of("single-store", token))
                        : CartComJson.fingerprint(List.of("central", siteId, username, secret));
        scope =
                CartComJson.fingerprint(
                        List.of(
                                "cart-com-v1",
                                host,
                                auth,
                                resource.id,
                                lower.toString(),
                                String.valueOf(end),
                                Integer.toString(pageSize)));
    }

    /**
     * Converts a UTC, offset-bearing or offset-free API timestamp to an instant.
     *
     * <p>Offset-free timestamps use UTC. The normalization is shared by configuration and rows so
     * local window checks use the same time basis as the encoded request.
     */
    static Instant timestamp(String value) {
        if (value == null || value.isEmpty() || value.length() > 40) {
            throw new DateTimeException("invalid timestamp");
        }
        if (value.length() == 10) {
            return LocalDate.parse(value).atStartOfDay().toInstant(ZoneOffset.UTC);
        }
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeException ignored) {
            return LocalDateTime.parse(value).toInstant(ZoneOffset.UTC);
        }
    }

    private static Instant dateOption(String value, String key) {
        try {
            return timestamp(value);
        } catch (DateTimeException ignored) {
            throw invalid(key + " must be an ISO 8601 date or timestamp");
        }
    }

    private static boolean header(String text) {
        return text != null && HEADER_TOKEN.matcher(text).matches();
    }

    private static String certificatePath(String value) {
        try {
            if (!value.isBlank() && value.length() <= 4096) {
                Path path = Path.of(value);
                if (path.isAbsolute() && path.normalize().getFileName() != null) {
                    return path.normalize().toString();
                }
            }
        } catch (InvalidPathException ignored) {
            // Do not echo the configured path.
        }
        throw invalid("tls_trust_certificate must be an absolute engine-local file path");
    }

    private static int range(ReadonlyConfig options, Option<Integer> option, int low, int high) {
        Integer value = options.get(option);
        if (value == null || value < low || value > high) {
            throw invalid(option.key() + " must be within [" + low + ", " + high + "]");
        }
        return value;
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("CartCom configuration: " + reason);
    }
}

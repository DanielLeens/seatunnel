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

package org.apache.seatunnel.connectors.seatunnel.chargebee;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Validates one selected site and its Product Catalog query scope before a network request.
 *
 * <p>Checkpoints bind to a site and API key digest, the resource, its date window and page size.
 */
final class ChargebeeConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern DNS_LABEL =
            Pattern.compile("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?");
    private static final Pattern KEY = Pattern.compile("[\\x21-\\x39\\x3b-\\x7e]{1,8192}");

    final String host;
    final String token;
    final String catalog;
    final String trustCertificate;
    final ChargebeeResource resource;
    final long lower;
    final Long end;
    final int pageSize;
    final int maxPages;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final String scope;

    ChargebeeConfig(ReadonlyConfig options) {
        String site = options.get(ChargebeeSourceOptions.SITE);
        String label = site == null ? "" : site.toLowerCase(Locale.ROOT);
        if (!DNS_LABEL.matcher(label).matches()) {
            throw invalid("site must be one Chargebee DNS label");
        }
        host = label + ".chargebee.com";
        token = options.get(ChargebeeSourceOptions.TOKEN);
        if (token == null || !KEY.matcher(token).matches()) {
            throw invalid("token must be a Chargebee API key without spaces or colons");
        }
        catalog = options.get(ChargebeeSourceOptions.PRODUCT_CATALOG);
        if (!"1.0".equals(catalog) && !"2.0".equals(catalog)) {
            throw invalid("product_catalog must be 1.0 or 2.0");
        }
        resource = ChargebeeResource.parse(options.get(ChargebeeSourceOptions.RESOURCE), catalog);
        long start = seconds(options.get(ChargebeeSourceOptions.START_DATE), "start_date");
        end =
                options.getOptional(ChargebeeSourceOptions.END_DATE)
                        .map(value -> seconds(value, "end_date"))
                        .orElse(null);
        Long prior =
                options.getOptional(ChargebeeSourceOptions.INCREMENTAL_START)
                        .map(value -> seconds(value, "incremental_start"))
                        .orElse(null);
        if (prior != null && resource.cursor == null) {
            throw invalid("incremental_start requires a resource with a date cursor");
        }
        if (end != null && (end < start || prior != null && end < prior)) {
            throw invalid("end_date must not precede start_date or incremental_start");
        }
        int lookback = range(options, ChargebeeSourceOptions.LOOKBACK_SECONDS, 0, 31_536_000);
        lower = prior == null ? start : Math.max(start, prior - lookback);
        trustCertificate =
                options.getOptional(ChargebeeSourceOptions.TLS_TRUST_CERTIFICATE)
                        .map(ChargebeeConfig::certificate)
                        .orElse(null);
        pageSize = range(options, ChargebeeSourceOptions.PAGE_SIZE, 1, 100);
        maxPages = range(options, ChargebeeSourceOptions.MAX_PAGES, 1, 20000);
        connectTimeout = range(options, ChargebeeSourceOptions.CONNECT_TIMEOUT_MS, 1, 120000);
        readTimeout = range(options, ChargebeeSourceOptions.READ_TIMEOUT_MS, 1, 120000);
        deadlineMs = range(options, ChargebeeSourceOptions.REQUEST_DEADLINE_MS, 1, 300000);
        maxAttempts = range(options, ChargebeeSourceOptions.MAX_ATTEMPTS, 1, 11);
        maxBytes =
                range(options, ChargebeeSourceOptions.MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        scope =
                ChargebeeJson.fingerprint(
                        List.of(
                                "chargebee-v1",
                                host,
                                token,
                                catalog,
                                resource.id,
                                Long.toString(lower),
                                String.valueOf(end),
                                Integer.toString(pageSize)));
    }

    private static long seconds(String value, String key) {
        try {
            if (value != null && value.length() <= 40) {
                OffsetDateTime date = OffsetDateTime.parse(value);
                long epoch = date.toEpochSecond();
                if (date.getNano() == 0 && epoch >= 0 && date.getYear() <= 9999) {
                    return epoch;
                }
            }
        } catch (DateTimeException ignored) {
            // Report configuration keys without echoing values.
        }
        throw invalid(key + " must be an ISO 8601 timestamp at whole-second precision");
    }

    /**
     * Checks the provider's opaque cursor before accepting or persisting it.
     *
     * <p>The value is transmitted as received after normal form query encoding.
     */
    static boolean validOffset(String value) {
        return value != null
                && !value.isEmpty()
                && value.length() <= 1000
                && StandardCharsets.UTF_8.newEncoder().canEncode(value);
    }

    private static String certificate(String path) {
        try {
            if (!path.isBlank() && path.length() <= 4096) {
                Path file = Path.of(path);
                if (file.isAbsolute() && file.normalize().getFileName() != null) {
                    return file.normalize().toString();
                }
            }
        } catch (InvalidPathException ignored) {
            // Do not echo a local file path.
        }
        throw invalid("tls_trust_certificate must be an absolute engine-local file path");
    }

    private static int range(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        Integer value = options.get(option);
        if (value == null || value < min || value > max) {
            throw invalid(option.key() + " must be within [" + min + ", " + max + "]");
        }
        return value;
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Chargebee configuration: " + reason);
    }
}

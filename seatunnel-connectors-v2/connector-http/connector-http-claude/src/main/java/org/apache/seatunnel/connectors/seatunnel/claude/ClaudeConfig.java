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
package org.apache.seatunnel.connectors.seatunnel.claude;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.MAX_ITEMS;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.MAX_SCAN_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.claude.ClaudeSourceOptions.TOKEN;

/**
 * Validates Claude Enterprise Analytics credentials, scan dates, and request limits.
 *
 * <p>The Analytics API key uses an option already masked by SeaTunnel.
 */
final class ClaudeConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "api.anthropic.com";
    static final String API_VERSION = "2026-10-21";
    static final LocalDate EARLIEST = LocalDate.of(2026, 1, 1);

    final String token;
    final String certificate;

    final List<ClaudeResource> resources;
    final LocalDate start;
    final LocalDate end;
    final int maxItems;

    final int pageSize;
    final int maxPages;
    final int scanPages;
    final int connectTimeout;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int maxRetryDelay;
    final int bodyLimit;
    final String scope;

    ClaudeConfig(ReadonlyConfig options) {
        token = header(options, TOKEN);
        resources = List.copyOf(selections(options.get(RESOURCES)));
        start = date(options, START_DATE);
        end = date(options, END_DATE);
        if (start != null && end != null && start.isAfter(end)) {
            throw invalid(END_DATE);
        }
        maxItems = number(options, MAX_ITEMS, 1, 100000);

        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        pageSize = number(options, PAGE_SIZE, 1, 1000);
        maxPages = number(options, MAX_PAGES, 1, 100000);
        scanPages = number(options, MAX_SCAN_PAGES, 1, 1000000);
        connectTimeout = number(options, CONNECT_TIMEOUT, 100, 300000);
        requestTimeout = number(options, REQUEST_TIMEOUT, 100, 300000);
        operationTimeout = number(options, OPERATION_TIMEOUT, requestTimeout, 600000);
        interval = number(options, INTERVAL, 0, 60000);
        retries = number(options, RETRIES, 0, 8);
        maxRetryDelay = number(options, RETRY_DELAY, 250, 600000);
        bodyLimit = number(options, BODY_LIMIT, 256, 16777216);
        scope =
                hash(
                        String.join(
                                "\n",
                                API_HOST,
                                API_VERSION,
                                hash(token),
                                resources.toString(),
                                Integer.toString(pageSize),
                                String.valueOf(start),
                                String.valueOf(end)));
    }

    /**
     * Resolves each report family's default window from the coordinator's UTC clock.
     *
     * <p>The resolved bounds stay fixed across checkpoints and do not advance during restore.
     */
    ClaudeSplit newSplit(Instant now) {
        LocalDate today = now.atOffset(ZoneOffset.UTC).toLocalDate();
        LocalDate first = start == null ? later(EARLIEST, today.minusDays(7)) : start;
        if (first.isAfter(today.minusDays(1))) {
            throw invalid(START_DATE);
        }
        if (end != null && !end.isBefore(today)) {
            throw invalid(END_DATE);
        }
        LocalDate adoptionEnd = end == null ? today.minusDays(2) : end.plusDays(1);
        LocalDate reportEnd = end == null ? today : end.plusDays(1);
        if (resources.stream().anyMatch(ClaudeResource::isReport)
                && first.atStartOfDay()
                        .toInstant(ZoneOffset.UTC)
                        .isBefore(now.minus(Duration.ofDays(365)))) {
            throw new ClaudeResource.Failure(
                    "date window", "reports are limited to the previous 365 days");
        }
        return new ClaudeSplit(this, first, adoptionEnd, reportEnd);
    }

    private static LocalDate later(LocalDate left, LocalDate right) {
        return left.isAfter(right) ? left : right;
    }

    private static LocalDate date(ReadonlyConfig options, Option<String> option) {
        String value = options.getOptional(option).orElse(null);
        if (value == null) {
            return null;
        }
        if (!value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
            throw invalid(option);
        }
        try {
            LocalDate date = LocalDate.parse(value);
            if (date.isBefore(EARLIEST)) {
                throw invalid(option);
            }
            return date;
        } catch (DateTimeParseException ex) {
            throw invalid(option);
        }
    }

    private static String header(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null
                || value.isBlank()
                || value.length() > 8192
                || value.chars().anyMatch(c -> c < 0x21 || c > 0x7e)) {
            throw invalid(option);
        }
        return value;
    }

    private static EnumSet<ClaudeResource> selections(List<String> names) {
        if (names.isEmpty()) {
            return EnumSet.allOf(ClaudeResource.class);
        }
        EnumSet<ClaudeResource> selected = EnumSet.noneOf(ClaudeResource.class);
        List<ClaudeResource> available = List.of(ClaudeResource.values());
        for (String name : names) {
            ClaudeResource resource =
                    available.stream()
                            .filter(candidate -> candidate.name.equals(name))
                            .findFirst()
                            .orElseThrow(() -> invalid(RESOURCES));
            if (!selected.add(resource)) {
                throw invalid(RESOURCES);
            }
        }
        return selected;
    }

    private static int number(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option);
        }
        return value;
    }

    /**
     * Creates a connector-private trust store only if an explicit certificate is supplied.
     *
     * <p>The transport always enables HTTPS endpoint identification on each socket.
     */
    SSLSocketFactory socketFactory() {
        if (certificate == null) {
            return (SSLSocketFactory) SSLSocketFactory.getDefault();
        }
        try {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(null, null);
            store.setCertificateEntry("claude", parseCertificate(certificate));
            TrustManagerFactory manager =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            manager.init(store);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, manager.getTrustManagers(), null);
            return context.getSocketFactory();
        } catch (GeneralSecurityException | IOException ex) {
            throw invalid(CERTIFICATE);
        }
    }

    private static Certificate parseCertificate(String supplied) {
        if (supplied.length() > 65536) {
            throw invalid(CERTIFICATE);
        }
        try {
            String pem = supplied.trim();
            String prefix = "-----BEGIN CERTIFICATE-----";
            String suffix = "-----END CERTIFICATE-----";
            if (pem.startsWith(prefix) && pem.endsWith(suffix)) {
                pem = pem.substring(prefix.length(), pem.length() - suffix.length());
            }
            byte[] decoded = Base64.getDecoder().decode(pem.replaceAll("\\s+", ""));
            ByteArrayInputStream input = new ByteArrayInputStream(decoded);
            Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(input);
            if (input.available() != 0) {
                throw invalid(CERTIFICATE);
            }
            return cert;
        } catch (GeneralSecurityException | IllegalArgumentException ex) {
            throw invalid(CERTIFICATE);
        }
    }

    static String hash(String text) {
        try {
            byte[] bytes =
                    MessageDigest.getInstance("SHA-256")
                            .digest(text.getBytes(StandardCharsets.UTF_8));
            char[] hex = "0123456789abcdef".toCharArray();
            char[] result = new char[bytes.length * 2];
            for (int i = 0; i < bytes.length; i++) {
                result[2 * i] = hex[(bytes[i] & 255) >>> 4];
                result[2 * i + 1] = hex[bytes[i] & 15];
            }
            return new String(result);
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("Claude SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Claude option " + option.key());
    }
}

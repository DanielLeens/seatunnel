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
package org.apache.seatunnel.connectors.seatunnel.ciscomeraki;

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
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.MAX_ITEMS;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.MAX_SCAN_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSourceOptions.TOKEN;

/**
 * Validates Cisco Meraki authentication, query scope, and bounds.
 *
 * <p>The API key uses an option already masked by SeaTunnel.
 */
final class CiscoMerakiConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "api.meraki.com";

    final String token;
    final String certificate;

    final EnumSet<CiscoMerakiResource> resources;
    final List<CiscoMerakiResource> children;
    final Instant start;
    final Instant end;
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

    CiscoMerakiConfig(ReadonlyConfig options) {
        token = header(options, TOKEN);
        resources = selections(options.get(RESOURCES));
        children =
                List.of(
                                CiscoMerakiResource.DATACENTERS,
                                CiscoMerakiResource.NETWORKS,
                                CiscoMerakiResource.DEVICES,
                                CiscoMerakiResource.API_REQUESTS,
                                CiscoMerakiResource.ADMINS,
                                CiscoMerakiResource.SAML)
                        .stream()
                        .filter(
                                resource ->
                                        resources.contains(resource)
                                                || resource == CiscoMerakiResource.NETWORKS
                                                        && resources.contains(
                                                                CiscoMerakiResource.SETTINGS))
                        .collect(Collectors.toUnmodifiableList());

        start = instant(options, START_DATE);
        end = instant(options, END_DATE);
        if (start != null && end != null && start.isAfter(end)) {
            throw invalid(END_DATE);
        }
        maxItems = number(options, MAX_ITEMS, 1, 100000);

        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        pageSize = number(options, PAGE_SIZE, 3, 1000);
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
                                hash(token),
                                resources.toString(),
                                Integer.toString(pageSize),
                                String.valueOf(start),
                                String.valueOf(end)));
    }

    /**
     * Freezes the log window when the coordinator creates the scan.
     *
     * <p>The API limits request logs to 31 days. An omitted lower bound leaves margin for queued
     * jobs and request processing.
     */
    CiscoMerakiSplit newSplit(Instant now) {
        Instant until = end == null ? now : end;
        Instant since = start == null ? until.minus(Duration.ofDays(30)) : start;
        if (since.isAfter(until)) {
            throw invalid(START_DATE);
        }
        if (resources.contains(CiscoMerakiResource.API_REQUESTS)
                && (until.isAfter(now) || since.isBefore(now.minus(Duration.ofDays(31))))) {
            throw CiscoMerakiResource.API_REQUESTS.failure(
                    "the request log window must be within the previous 31 days");
        }
        return new CiscoMerakiSplit(this, since, until);
    }

    private static Instant instant(ReadonlyConfig options, Option<String> option) {
        String configured = options.getOptional(option).orElse(null);
        if (configured == null) {
            return null;
        }
        try {
            OffsetDateTime value = OffsetDateTime.parse(configured);
            if (value.getYear() < 1 || value.getYear() > 9999) {
                throw invalid(option);
            }
            return value.toInstant();
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

    private static EnumSet<CiscoMerakiResource> selections(List<String> names) {
        if (names.isEmpty()) {
            return EnumSet.allOf(CiscoMerakiResource.class);
        }
        EnumSet<CiscoMerakiResource> selected = EnumSet.noneOf(CiscoMerakiResource.class);
        List<CiscoMerakiResource> available = List.of(CiscoMerakiResource.values());
        for (String name : names) {
            CiscoMerakiResource resource =
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
            store.setCertificateEntry("ciscomeraki", parseCertificate(certificate));
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
            throw new IllegalStateException("CiscoMeraki SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid CiscoMeraki option " + option.key());
    }
}

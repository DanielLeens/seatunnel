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
package org.apache.seatunnel.connectors.seatunnel.chameleon;

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
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.MAX_SCAN_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.chameleon.ChameleonSourceOptions.TOKEN;

/**
 * Validates Chameleon credentials, output time bounds and resource selection.
 *
 * <p>Adds Microsurvey discovery when response records are selected.
 */
final class ChameleonConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "api.chameleon.io";

    final String token;
    final String certificate;
    private final Set<ChameleonResource> selected;
    final List<ChameleonResource> roots;
    final List<ChameleonResource> children;
    final Instant start;
    final Instant end;

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

    ChameleonConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isBlank()
                || token.length() > 8192
                || token.chars().anyMatch(c -> c < 0x21 || c > 0x7e)) {
            throw invalid(TOKEN);
        }
        selected = selections(options.get(RESOURCES));
        children =
                selected.contains(ChameleonResource.SURVEY_RESPONSES)
                        ? List.of(ChameleonResource.SURVEY_RESPONSES)
                        : List.of();
        List<ChameleonResource> rootCollections = new ArrayList<>();
        for (ChameleonResource resource : ChameleonResource.values()) {
            if (!resource.scoped
                    && (selected.contains(resource)
                            || resource == ChameleonResource.SURVEYS && !children.isEmpty())) {
                rootCollections.add(resource);
            }
        }
        roots = List.copyOf(rootCollections);
        start = date(options, START_DATE);
        end = date(options, END_DATE);
        if (start != null && end != null && start.isAfter(end)) {
            throw invalid(END_DATE);
        }

        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        pageSize = number(options, PAGE_SIZE, 1, 500);
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
                                selected.toString(),
                                Integer.toString(pageSize),
                                String.valueOf(start),
                                String.valueOf(end)));
    }

    ChameleonSplit newSplit(Instant now) {
        return new ChameleonSplit(this, end == null && start != null ? now : end);
    }

    boolean inWindow(Instant time, Instant upper) {
        return (start == null || !time.isBefore(start)) && (upper == null || time.isBefore(upper));
    }

    private static Instant date(ReadonlyConfig options, Option<String> option) {
        String value = options.getOptional(option).orElse(null);
        if (value == null) {
            return null;
        }
        try {
            return parseTime(value);
        } catch (DateTimeException ex) {
            throw invalid(option);
        }
    }

    static Instant parseTime(String value) {
        if (value.length() > 64) {
            throw new DateTimeException("Timestamp exceeds 64 characters");
        }
        return OffsetDateTime.parse(value).toInstant();
    }

    boolean emits(ChameleonResource resource) {
        return selected.contains(resource);
    }

    private static Set<ChameleonResource> selections(List<String> names) {
        if (names.isEmpty()) {
            return EnumSet.allOf(ChameleonResource.class);
        }
        Set<ChameleonResource> selected = EnumSet.noneOf(ChameleonResource.class);
        List<ChameleonResource> available = List.of(ChameleonResource.values());
        for (String name : names) {
            ChameleonResource resource =
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
            store.setCertificateEntry("chameleon", parseCertificate(certificate));
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
            throw new IllegalStateException("Chameleon SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Chameleon option " + option.key());
    }
}

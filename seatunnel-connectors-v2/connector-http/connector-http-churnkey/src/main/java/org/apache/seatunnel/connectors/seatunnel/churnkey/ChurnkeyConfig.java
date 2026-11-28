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
package org.apache.seatunnel.connectors.seatunnel.churnkey;

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
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.BOUNCED;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.BREAKDOWN;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.MAX_ITEMS;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.MAX_SCAN_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.churnkey.ChurnkeySourceOptions.TOKEN;

/**
 * Validates Churnkey authentication, resource selection and aggregation scope.
 *
 * <p>Both authentication fields use option names already masked by SeaTunnel.
 */
final class ChurnkeyConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "api.churnkey.co";

    private static final Set<String> DIMENSIONS =
            Set.of(
                    "month",
                    "trial",
                    "billingInterval",
                    "planId",
                    "aborted",
                    "canceled",
                    "offerType",
                    "saveType",
                    "couponId",
                    "pauseDuration",
                    "blueprintId",
                    "abtest",
                    "surveyResponse");

    final String token;
    final String appId;
    final String certificate;

    final List<ChurnkeyResource> resources;
    final boolean bounced;
    final String breakdown;
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

    ChurnkeyConfig(ReadonlyConfig options) {
        token = header(options, TOKEN);
        appId = header(options, ACCESS_KEY);
        resources = List.copyOf(selections(options.get(RESOURCES)));
        bounced = options.get(BOUNCED);
        List<String> dimensions = options.get(BREAKDOWN);
        if (new HashSet<>(dimensions).size() != dimensions.size()
                || !DIMENSIONS.containsAll(dimensions)) {
            throw invalid(BREAKDOWN);
        }
        breakdown = String.join("-", dimensions);

        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        pageSize = number(options, PAGE_SIZE, 1, 10000);
        maxItems = number(options, MAX_ITEMS, 1, 100000);
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
                                hash(appId),
                                resources.toString(),
                                Integer.toString(pageSize),
                                Boolean.toString(bounced),
                                breakdown));
    }

    ChurnkeySplit newSplit() {
        return new ChurnkeySplit(this);
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

    private static EnumSet<ChurnkeyResource> selections(List<String> names) {
        if (names.isEmpty()) {
            return EnumSet.allOf(ChurnkeyResource.class);
        }
        EnumSet<ChurnkeyResource> selected = EnumSet.noneOf(ChurnkeyResource.class);
        List<ChurnkeyResource> available = List.of(ChurnkeyResource.values());
        for (String name : names) {
            ChurnkeyResource resource =
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
            store.setCertificateEntry("churnkey", parseCertificate(certificate));
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
            throw new IllegalStateException("Churnkey SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Churnkey option " + option.key());
    }
}

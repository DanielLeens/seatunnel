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
package org.apache.seatunnel.connectors.seatunnel.cin7;

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
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.MAX_SCAN_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.cin7.Cin7SourceOptions.TOKEN;

/**
 * Validates Cin7 Core credentials, resource dependencies, and scan limits.
 *
 * <p>Both authentication fields use option names already masked by SeaTunnel.
 */
final class Cin7Config implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "inventory.dearsystems.com";

    final String token;
    final String accountId;
    final String certificate;

    final EnumSet<Cin7Resource> resources;
    final List<Cin7Resource> collections;

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

    Cin7Config(ReadonlyConfig options) {
        token = header(options, TOKEN);
        accountId = header(options, ACCESS_KEY);
        resources = selections(options.get(RESOURCES));
        collections =
                List.of(Cin7Resource.values()).stream()
                        .filter(resource -> resource.array != null)
                        .filter(
                                resource ->
                                        resources.contains(resource)
                                                || resource.detail() != null
                                                        && resources.contains(resource.detail()))
                        .collect(Collectors.toUnmodifiableList());

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
                                hash(token),
                                hash(accountId),
                                resources.toString(),
                                Integer.toString(pageSize)));
    }

    Cin7Split newSplit() {
        return new Cin7Split(this);
    }

    Cin7Resource selectedDetail(Cin7Resource collection) {
        Cin7Resource detail = collection.detail();
        return detail != null && resources.contains(detail) ? detail : null;
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

    private static EnumSet<Cin7Resource> selections(List<String> names) {
        if (names.isEmpty()) {
            return EnumSet.allOf(Cin7Resource.class);
        }
        EnumSet<Cin7Resource> selected = EnumSet.noneOf(Cin7Resource.class);
        List<Cin7Resource> available = List.of(Cin7Resource.values());
        for (String name : names) {
            Cin7Resource resource =
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
            store.setCertificateEntry("cin7", parseCertificate(certificate));
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
            throw new IllegalStateException("Cin7 SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Cin7 option " + option.key());
    }
}

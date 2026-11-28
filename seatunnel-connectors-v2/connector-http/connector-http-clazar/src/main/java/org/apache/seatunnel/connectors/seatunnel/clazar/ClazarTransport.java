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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Restricted Clazar HTTPS client for one worker.
 *
 * <p>Every request is a GET to the fixed origin. The page deadline includes retry waits, transfers
 * and parsing. A private trust store is used only for an explicitly configured custom origin.
 */
final class ClazarTransport implements AutoCloseable {
    private static final int MAX_PEM_BYTES = 65536;
    private static final int MAX_AUTH_RESPONSE_BYTES = 65536;
    private static final MediaType JSON = MediaType.get("application/json");
    private static final String AUTHENTICATION = "authentication";
    private final ClazarConfig options;
    private final HttpUrl origin;
    private final OkHttpClient client;

    // The same monitor binds close to the active call.
    private Call running;
    private boolean closed;
    // The client owns the credential for this reader only and never serializes it.
    private volatile AccessCredential credential;

    ClazarTransport(ClazarConfig options) throws ClazarReadException {
        this.options = options;
        origin = HttpUrl.get(options.origin);
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .proxy(Proxy.NO_PROXY)
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .retryOnConnectionFailure(false)
                        .connectionPool(new ConnectionPool(1, 20L, TimeUnit.SECONDS))
                        .connectTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .readTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .callTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .addNetworkInterceptor(
                                chain -> {
                                    Response response = chain.proceed(chain.request());
                                    String retry = response.header("Retry-After");
                                    // The library reissues 503/zero even with connection retries
                                    // disabled.
                                    if (response.code() == 503
                                            && retry != null
                                            && retry.matches("0+")) {
                                        List<String> values = response.headers("Retry-After");
                                        Response.Builder bounded =
                                                response.newBuilder().removeHeader("Retry-After");
                                        if (values.size() > 1) {
                                            // Preserve malformed timing while preventing the
                                            // implicit follow-up.
                                            bounded.header(
                                                    "Retry-After", String.join(", ", values));
                                        }
                                        return bounded.build();
                                    }
                                    return response;
                                });
        if (options.trustCertificate != null) {
            loadTrust(builder);
        }
        client = builder.build();
    }

    /**
     * Reads one provider page using a cached, expiring bearer credential.
     *
     * <p>A rejected GET may acquire one new credential within that page's deadline and attempt
     * budget. A credential exchange is never replayed after an ambiguous failure.
     */
    byte[] get(ClazarResource resource, HttpUrl target, long deadline)
            throws ClazarReadException, InterruptedException {
        String exhausted = "HTTPS retry budget exhausted";
        boolean renewed = false;
        for (int attempt = 0; attempt < options.attempts; attempt++) {
            checkDeadline(resource.id, deadline);
            String accessToken = token(deadline);
            Request request =
                    new Request.Builder()
                            .url(target)
                            .get()
                            .header("Authorization", "Bearer " + accessToken)
                            .header("Accept", "application/json")
                            .header("User-Agent", "Apache-SeaTunnel-Clazar/1.0")
                            .build();
            Call call = begin(request, resource.id, deadline);
            long delay = 200L << attempt;
            try (Response response = call.execute()) {
                int status = response.code();
                if (status == 200) {
                    return readBody(response, resource.id, options.maxBytes);
                }
                if (status == 401 && !renewed && attempt + 1 < options.attempts) {
                    renewed = true;
                    credential = null;
                    continue;
                }
                if (status == 401 || status == 403) {
                    throw resource.failure(
                            "access denied (HTTP "
                                    + status
                                    + "); verify Clazar client credentials and resource permissions");
                }
                if (!retryable(status)) {
                    throw resource.failure("provider rejected the request (HTTP " + status + ")");
                }
                if (status == 429) {
                    delay = 1000L << attempt;
                }
                delay = Math.max(delay, serverDelay(response, resource));
                exhausted = "HTTP " + status + " retry budget exhausted";
            } catch (ClazarReadException failure) {
                throw failure;
            } catch (SSLException failure) {
                throw resource.failure("TLS certificate or hostname validation failed");
            } catch (IOException failure) {
                checkDeadline(resource.id, deadline);
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    throw resource.failure("HTTPS request was cancelled");
                }
                exhausted = "HTTPS retry budget exhausted";
            } finally {
                released(call);
            }
            if (attempt + 1 < options.attempts) {
                if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                    throw resource.failure("retry delay exceeds request deadline");
                }
                Thread.sleep(delay);
            }
        }
        throw resource.failure(exhausted);
    }

    private String token(long deadline) throws ClazarReadException {
        AccessCredential current = credential;
        if (current != null && System.nanoTime() - current.renewAt < 0L) {
            return current.value;
        }
        AccessCredential fetched = exchange(deadline);
        synchronized (this) {
            if (closed) {
                throw new ClazarReadException(AUTHENTICATION, "token exchange was cancelled");
            }
            credential = fetched;
        }
        return fetched.value;
    }

    /**
     * Uses the documented two-field JSON authentication request exactly once.
     *
     * <p>The observed access-token lifetime is measured from before sending the exchange.
     */
    private AccessCredential exchange(long deadline) throws ClazarReadException {
        checkDeadline(AUTHENTICATION, deadline);
        Request request =
                new Request.Builder()
                        .url(origin.newBuilder().encodedPath("/authenticate/").build())
                        .post(
                                RequestBody.create(
                                        ClazarJson.credentials(
                                                options.clientId, options.clientSecret),
                                        JSON))
                        .header("Accept", "application/json")
                        .header("User-Agent", "Apache-SeaTunnel-Clazar/1.0")
                        .build();
        long started = System.nanoTime();
        Call call = begin(request, AUTHENTICATION, deadline);
        try (Response response = call.execute()) {
            if (response.code() != 200) {
                throw new ClazarReadException(
                        AUTHENTICATION,
                        "provider rejected the token exchange (HTTP " + response.code() + ")");
            }
            JsonNode document =
                    ClazarJson.object(
                            readBody(
                                    response,
                                    AUTHENTICATION,
                                    Math.min(MAX_AUTH_RESPONSE_BYTES, options.maxBytes)),
                            AUTHENTICATION);
            checkDeadline(AUTHENTICATION, deadline);
            String token = document.path("access_token").textValue();
            String type = document.path("token_type").textValue();
            JsonNode ttl = document.path("expires_in");
            if (token == null
                    || token.isEmpty()
                    || token.length() > 4096
                    || token.chars().anyMatch(character -> character < 33 || character > 126)
                    || !"Bearer".equalsIgnoreCase(type)
                    || !ttl.isIntegralNumber()
                    || !ttl.canConvertToInt()
                    || ttl.intValue() <= 0) {
                throw new ClazarReadException(AUTHENTICATION, "invalid bearer token response");
            }
            long lifetime = TimeUnit.SECONDS.toNanos(ttl.intValue());
            long renewAt =
                    started + lifetime - Math.min(TimeUnit.SECONDS.toNanos(30), lifetime / 10);
            if (System.nanoTime() - renewAt >= 0L) {
                throw new ClazarReadException(AUTHENTICATION, "token expired during exchange");
            }
            return new AccessCredential(token, renewAt);
        } catch (ClazarReadException failure) {
            throw failure;
        } catch (SSLException failure) {
            throw new ClazarReadException(
                    AUTHENTICATION, "TLS certificate or hostname validation failed");
        } catch (IOException unknown) {
            checkDeadline(AUTHENTICATION, deadline);
            throw new ClazarReadException(
                    AUTHENTICATION, "token exchange did not complete; exchange was not retried");
        } finally {
            released(call);
        }
    }

    private static boolean retryable(int code) {
        return code == 408
                || code == 429
                || code == 500
                || code == 502
                || code == 503
                || code == 504;
    }

    private static long serverDelay(Response response, ClazarResource resource)
            throws ClazarReadException {
        List<String> timing = response.headers("Retry-After");
        if (timing.isEmpty()) {
            return 0L;
        }
        if (timing.size() != 1 || timing.get(0).isEmpty() || timing.get(0).length() > 96) {
            throw resource.failure("provider sent invalid retry timing");
        }
        String value = timing.get(0);
        try {
            if (value.matches("[0-9]+")) {
                return Math.multiplyExact(Long.parseLong(value), 1000L);
            }
            return until(
                    Instant.now(),
                    ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant());
        } catch (ArithmeticException | NumberFormatException overflow) {
            return Long.MAX_VALUE;
        } catch (DateTimeException invalid) {
            throw resource.failure("provider sent invalid retry timing");
        }
    }

    private static long until(Instant now, Instant target) {
        Duration remaining = Duration.between(now, target);
        if (remaining.isZero() || remaining.isNegative()) {
            return 0L;
        }
        // Round up: never retry just before the advertised boundary.
        return Math.addExact(remaining.toMillis(), remaining.getNano() % 1000000 == 0 ? 0L : 1L);
    }

    private byte[] readBody(Response response, String operation, int maxBytes) throws IOException {
        // OkHttp unwraps negotiated gzip and exposes decoded bytes to the limit below.
        String encoding = response.header("Content-Encoding");
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw new ClazarReadException(operation, "unsupported response compression");
        }
        ResponseBody body = response.body();
        MediaType type = body == null ? null : body.contentType();
        if (type == null
                || !"application".equals(type.type())
                || !"json".equals(type.subtype())
                || (type.parameter("charset") != null
                        && !StandardCharsets.UTF_8.equals(type.charset()))) {
            throw new ClazarReadException(
                    operation, "expected Content-Type application/json with UTF-8");
        }
        if (body.contentLength() > maxBytes) {
            throw new ClazarReadException(operation, "response exceeds max_response_bytes");
        }
        try (InputStream source = body.byteStream();
                ByteArrayOutputStream captured = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            for (int count; (count = source.read(buffer)) >= 0; ) {
                if (count > maxBytes - captured.size()) {
                    throw new ClazarReadException(operation, "response exceeds max_response_bytes");
                }
                captured.write(buffer, 0, count);
            }
            return captured.toByteArray();
        }
    }

    private synchronized Call begin(Request request, String operation, long deadline)
            throws ClazarReadException {
        if (closed) {
            throw new ClazarReadException(operation, "HTTPS request was cancelled");
        }
        running = client.newCall(request);
        running.timeout().deadlineNanoTime(deadline);
        return running;
    }

    private synchronized void released(Call call) {
        if (running == call) {
            running = null;
        }
    }

    static void checkDeadline(String operation, long deadline) throws ClazarReadException {
        if (System.nanoTime() - deadline >= 0L) {
            throw new ClazarReadException(operation, "request deadline exceeded");
        }
    }

    /**
     * Cancels in-flight I/O and releases this connector's private connection pool.
     *
     * <p>The JVM trust configuration and other clients are untouched.
     */
    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            credential = null;
            if (running != null) {
                running.cancel();
            }
            client.connectionPool().evictAll();
            client.dispatcher().executorService().shutdownNow();
        }
    }

    private void loadTrust(OkHttpClient.Builder builder) throws ClazarReadException {
        try {
            Path path = Path.of(options.trustCertificate);
            if (!Files.isRegularFile(path)) {
                throw new ClazarReadException(
                        "transport", "trust_certificate is not a regular file");
            }
            byte[] pem;
            try (InputStream source = Files.newInputStream(path)) {
                pem = source.readNBytes(MAX_PEM_BYTES + 1);
            }
            if (pem.length == 0 || pem.length > MAX_PEM_BYTES) {
                throw new ClazarReadException("transport", "trust_certificate has an invalid size");
            }
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(pem));
            if (certificates.isEmpty() || certificates.size() > 16) {
                throw new ClazarReadException(
                        "transport", "trust_certificate must contain a bounded X.509 chain");
            }
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null);
            int index = 0;
            for (Certificate certificate : certificates) {
                store.setCertificateEntry("ca-" + index++, certificate);
            }
            TrustManagerFactory factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(store);
            for (TrustManager trust : factory.getTrustManagers()) {
                if (trust instanceof X509TrustManager) {
                    X509TrustManager manager = (X509TrustManager) trust;
                    SSLContext tls = SSLContext.getInstance("TLS");
                    tls.init(null, new TrustManager[] {manager}, null);
                    builder.sslSocketFactory(tls.getSocketFactory(), manager);
                    return;
                }
            }
        } catch (ClazarReadException failure) {
            throw failure;
        } catch (IOException | GeneralSecurityException | RuntimeException failure) {
            throw new ClazarReadException("transport", "could not load trust_certificate");
        }
        throw new ClazarReadException("transport", "could not establish X.509 trust");
    }
    /**
     * Bearer credential with its local refresh deadline.
     *
     * <p>The token stays private to one worker's client and is never stored in split state.
     */
    private static final class AccessCredential {
        private final String value;
        private final long renewAt;

        private AccessCredential(String value, long renewAt) {
            this.value = value;
            this.renewAt = renewAt;
        }
    }
}

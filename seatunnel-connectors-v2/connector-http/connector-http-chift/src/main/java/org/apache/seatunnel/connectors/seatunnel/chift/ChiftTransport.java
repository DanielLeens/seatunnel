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
package org.apache.seatunnel.connectors.seatunnel.chift;

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
 * Restricted regional Chift client for one reader.
 *
 * <p>Metadata GETs and JSON token exchanges use the same fixed Chift origin. The page deadline
 * includes retry waits, transfers and parsing. A private trust store is used only for an explicitly
 * configured custom origin.
 */
final class ChiftTransport implements AutoCloseable {
    private static final int MAX_PEM_BYTES = 65536;
    private static final int MAX_TOKEN_BYTES = 65536;
    private static final MediaType JSON_TYPE = MediaType.get("application/json");
    private static final String USER_AGENT = "Apache-SeaTunnel-Chift/1.0";
    private final ChiftConfig options;
    private final HttpUrl origin;
    private final OkHttpClient client;
    // These values are used by the single acquisition worker and never enter its checkpoint.
    private String accessToken;
    private long expiresAt;

    // The same monitor binds close to the active call.
    private Call running;
    private boolean closed;

    ChiftTransport(ChiftConfig options) throws ChiftReadException {
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
     * Reads a single fixed-scope target with one shared deadline for auth and retries.
     *
     * <p>Only a valid first unauthorized response can trigger token renewal for this read.
     */
    byte[] get(ChiftResource resource, ChiftSplit state, long deadline)
            throws ChiftReadException, InterruptedException {
        HttpUrl.Builder url =
                origin.newBuilder().encodedPath(resource.path(state.parent(resource)));
        if (resource == ChiftResource.EXECUTIONS) {
            url.addQueryParameter("page", Integer.toString(state.page))
                    .addQueryParameter("size", Integer.toString(options.pageSize));
        }
        HttpUrl target = url.build();
        boolean reauthenticated = false;
        String exhausted = "HTTPS retry budget exhausted";
        for (int attempt = 0; attempt < options.attempts; attempt++) {
            Request request =
                    new Request.Builder()
                            .url(target)
                            .get()
                            .header("Authorization", "Bearer " + bearer(resource, deadline))
                            .header("Accept", "application/json")
                            .header("User-Agent", USER_AGENT)
                            .build();
            checkDeadline(resource, deadline);
            Call call = begin(request, resource, deadline);
            long delay = 200L << attempt;
            try (Response response = call.execute()) {
                int status = response.code();
                if (status == 200) {
                    if (resource != ChiftResource.EXECUTIONS
                            && !response.headers("Link").isEmpty()) {
                        throw resource.failure(
                                "unexpected Link metadata for a complete collection");
                    }
                    return readBody(response, resource, options.maxBytes);
                }
                if (status == 401 && !reauthenticated && attempt + 1 < options.attempts) {
                    accessToken = null;
                    reauthenticated = true;
                    continue;
                }
                if (!retryable(status)) {
                    if (status == 401 || status == 403) {
                        throw resource.failure(
                                "access denied (HTTP " + status + "); verify Chift permissions");
                    }
                    throw resource.failure("provider rejected the request (HTTP " + status + ")");
                }
                delay =
                        status == 429
                                ? rateDelay(response, resource)
                                : Math.max(delay, serverDelay(response, resource));
                exhausted = "HTTP " + status + " retry budget exhausted";
            } catch (ChiftReadException failure) {
                throw failure;
            } catch (SSLException failure) {
                throw resource.failure("TLS certificate or hostname validation failed");
            } catch (IOException failure) {
                checkDeadline(resource, deadline);
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    throw resource.failure("HTTPS request was cancelled");
                }
                exhausted = "HTTPS retry budget exhausted";
            } finally {
                released(call);
            }
            if (attempt + 1 < options.attempts) {
                pause(delay, resource, deadline);
            }
        }
        throw resource.failure(exhausted);
    }

    private String bearer(ChiftResource resource, long deadline)
            throws ChiftReadException, InterruptedException {
        if (accessToken != null && expiresAt - System.nanoTime() > 0L) {
            return accessToken;
        }
        RequestBody body = RequestBody.create(ChiftPage.credentials(options, resource), JSON_TYPE);
        Request request =
                new Request.Builder()
                        .url(origin.newBuilder().encodedPath("/token").build())
                        .post(body)
                        .header("Accept", "application/json")
                        .header("User-Agent", USER_AGENT)
                        .build();
        for (int attempt = 0; attempt < options.attempts; attempt++) {
            checkDeadline(resource, deadline);
            Call call = begin(request, resource, deadline);
            long delay;
            try (Response response = call.execute()) {
                int status = response.code();
                if (status == 200) {
                    cacheToken(readBody(response, resource, MAX_TOKEN_BYTES), resource);
                    return accessToken;
                }
                if (status != 429 || attempt + 1 == options.attempts) {
                    throw resource.failure("token request failed (HTTP " + status + ")");
                }
                delay = rateDelay(response, resource);
            } catch (ChiftReadException failure) {
                throw failure;
            } catch (SSLException failure) {
                throw resource.failure("TLS certificate or hostname validation failed");
            } catch (IOException failure) {
                checkDeadline(resource, deadline);
                // No automatic repeat if the token POST may already have been accepted.
                throw resource.failure("token request outcome unknown");
            } finally {
                released(call);
            }
            pause(delay, resource, deadline);
        }
        throw resource.failure("token retry budget exhausted");
    }

    private void cacheToken(byte[] reply, ChiftResource resource) throws ChiftReadException {
        JsonNode root = ChiftPage.document(reply, resource);
        JsonNode token = root.path("access_token");
        JsonNode type = root.path("token_type");
        JsonNode expires = root.path("expires_in");
        JsonNode on = root.path("expires_on");
        if (!token.isTextual()
                || token.textValue().isEmpty()
                || token.textValue().length() > 8192
                || token.textValue()
                        .chars()
                        .anyMatch(character -> character < 33 || character > 126)
                || !type.isTextual()
                || !"Bearer".equalsIgnoreCase(type.textValue())
                || !expires.isIntegralNumber()
                || !expires.canConvertToLong()
                || expires.longValue() <= 0L
                || !on.isIntegralNumber()
                || !on.canConvertToLong()) {
            throw resource.failure("invalid token response");
        }
        try {
            Duration absolute =
                    Duration.between(Instant.now(), Instant.ofEpochSecond(on.longValue()));
            if (absolute.isZero() || absolute.isNegative()) {
                throw resource.failure("provider returned an expired token");
            }
            Duration relative = Duration.ofSeconds(Math.min(1800L, expires.longValue()));
            Duration usable = absolute.compareTo(relative) < 0 ? absolute : relative;
            long nanos = usable.toNanos();
            long safety = Math.min(TimeUnit.SECONDS.toNanos(60L), nanos / 10L);
            expiresAt = System.nanoTime() + nanos - safety;
            accessToken = token.textValue();
        } catch (DateTimeException | ArithmeticException invalid) {
            throw resource.failure("invalid token expiry");
        }
    }

    private static void pause(long millis, ChiftResource resource, long deadline)
            throws ChiftReadException, InterruptedException {
        if (millis >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
            throw resource.failure("retry delay exceeds request deadline");
        }
        Thread.sleep(millis);
    }

    private static boolean retryable(int code) {
        return code == 408
                || code == 429
                || code == 500
                || code == 502
                || code == 503
                || code == 504;
    }

    private static long rateDelay(Response response, ChiftResource resource)
            throws ChiftReadException {
        return Math.max(1000L, serverDelay(response, resource));
    }

    private static long serverDelay(Response response, ChiftResource resource)
            throws ChiftReadException {
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

    private byte[] readBody(Response response, ChiftResource resource, int byteLimit)
            throws IOException {
        // OkHttp unwraps negotiated gzip and exposes decoded bytes to the limit below.
        String encoding = response.header("Content-Encoding");
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw resource.failure("unsupported response compression");
        }
        ResponseBody body = response.body();
        MediaType type = body == null ? null : body.contentType();
        if (type == null
                || !"application".equals(type.type())
                || (!"json".equals(type.subtype())
                        && !(allowHal && "hal+json".equals(type.subtype())))
                || (type.parameter("charset") != null
                        && !StandardCharsets.UTF_8.equals(type.charset()))) {
            throw resource.failure("expected Content-Type application/json with UTF-8");
        }
        if (body.contentLength() > byteLimit) {
            throw resource.failure("response exceeds max_response_bytes");
        }
        try (InputStream source = body.byteStream();
                ByteArrayOutputStream captured = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            for (int count; (count = source.read(buffer)) >= 0; ) {
                if (count > byteLimit - captured.size()) {
                    throw resource.failure("response exceeds max_response_bytes");
                }
                captured.write(buffer, 0, count);
            }
            return captured.toByteArray();
        }
    }

    private synchronized Call begin(Request request, ChiftResource resource, long deadline)
            throws ChiftReadException {
        if (closed) {
            throw resource.failure("HTTPS request was cancelled");
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

    static void checkDeadline(ChiftResource resource, long deadline) throws ChiftReadException {
        if (System.nanoTime() - deadline >= 0L) {
            throw resource.failure("request deadline exceeded");
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
            if (running != null) {
                running.cancel();
            }
            client.connectionPool().evictAll();
            client.dispatcher().executorService().shutdownNow();
        }
    }

    private void loadTrust(OkHttpClient.Builder builder) throws ChiftReadException {
        try {
            Path path = Path.of(options.trustCertificate);
            if (!Files.isRegularFile(path)) {
                throw new ChiftReadException(
                        "transport", "trust_certificate is not a regular file");
            }
            byte[] pem;
            try (InputStream source = Files.newInputStream(path)) {
                pem = source.readNBytes(MAX_PEM_BYTES + 1);
            }
            if (pem.length == 0 || pem.length > MAX_PEM_BYTES) {
                throw new ChiftReadException("transport", "trust_certificate has an invalid size");
            }
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(pem));
            if (certificates.isEmpty() || certificates.size() > 16) {
                throw new ChiftReadException(
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
        } catch (ChiftReadException failure) {
            throw failure;
        } catch (IOException | GeneralSecurityException | RuntimeException failure) {
            throw new ChiftReadException("transport", "could not load trust_certificate");
        }
        throw new ChiftReadException("transport", "could not establish X.509 trust");
    }
}

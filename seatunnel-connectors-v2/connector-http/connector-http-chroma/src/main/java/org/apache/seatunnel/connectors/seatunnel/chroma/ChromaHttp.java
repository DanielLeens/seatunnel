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

package org.apache.seatunnel.connectors.seatunnel.chroma;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs bounded HTTP commands using JVM trust checks and explicit per-request credentials. It never
 * follows redirects or logs wire data, and closes the active socket on interruption.
 */
final class ChromaHttp implements AutoCloseable {
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(8192)
                                            .build())
                            .build());
    private final ChromaConfig config;
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-chroma-https");
                        thread.setDaemon(true);
                        return thread;
                    });
    private final AtomicReference<Socket> socket = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Future<Response>> future = new AtomicReference<>();
    private final Object retrySignal = new Object();
    private SSLSocketFactory trustedFactory;
    private long flushDeadline = Long.MAX_VALUE;

    ChromaHttp(ChromaConfig config) {
        this.config = config;
    }

    void beginFlush() {
        flushDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.flushTimeout);
    }

    void endFlush() {
        flushDeadline = Long.MAX_VALUE;
    }

    /**
     * Executes one logical JSON command. Mutating commands retry only explicit rate rejections.
     * Safe read commands may also retry acknowledged service unavailability.
     */
    ObjectNode post(
            URI url,
            String header,
            String credential,
            ObjectNode data,
            String operation,
            boolean safeRead)
            throws IOException {
        return ChromaJson.readObject(
                command(url, header, credential, ChromaJson.bytes(data), operation, safeRead));
    }

    JsonNode mutation(URI url, byte[] body, String operation) throws IOException {
        byte[] bytes =
                command(url, config.authHeader, config.authorization, body, operation, false);
        return bytes.length == 0 ? null : ChromaJson.readValue(bytes);
    }

    private byte[] command(
            URI url,
            String header,
            String credential,
            byte[] bytes,
            String operation,
            boolean safeRead)
            throws IOException {
        if (bytes.length > config.maxBody) {
            throw failure(operation, "request body exceeds max_body_bytes");
        }
        return execute(url, header, credential, bytes, config.maxBody, operation, safeRead);
    }

    byte[] getRanks(URI url, int maxBytes) throws IOException {
        return execute(url, null, "", null, maxBytes, "tokenizer download", true);
    }

    private byte[] execute(
            URI url,
            String header,
            String credential,
            byte[] body,
            int maxBytes,
            String operation,
            boolean safeRead)
            throws IOException {
        long deadline =
                Math.min(
                        flushDeadline,
                        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestTimeout));
        for (int attempt = 1; ; attempt++) {
            if (deadline - System.nanoTime() <= 0) {
                throw failure(operation, "request deadline exceeded");
            }
            Response result;
            try {
                result = request(url, header, credential, body, maxBytes, deadline, operation);
            } catch (IOException error) {
                if (safeRead || closed.get()) {
                    throw error;
                }
                throw new IOException(error.getMessage() + ", mutation outcome may be unknown");
            }
            if (successful(result.status)) {
                return result.body;
            }
            boolean rate = result.status == 429;
            boolean unavailable = safeRead && result.status >= 500 && result.status <= 599;
            if (!(rate || unavailable) || attempt == config.maxAttempts) {
                String cause = "HTTP " + result.status;
                if (!safeRead && result.status >= 500 && result.status <= 599) {
                    cause += ", mutation outcome may be unknown";
                }
                throw failure(operation, cause);
            }
            long backoff = 250L << Math.min(attempt - 1, 6);
            long delay =
                    Math.max(
                            result.retryAfter,
                            ThreadLocalRandom.current().nextLong(backoff, 2 * backoff));
            long nanos = TimeUnit.MILLISECONDS.toNanos(delay);
            if (nanos >= deadline - System.nanoTime()) {
                throw failure(operation, "retry delay exceeds request deadline");
            }
            try {
                long retryDeadline = System.nanoTime() + nanos;
                synchronized (retrySignal) {
                    long remaining;
                    while (!closed.get() && (remaining = retryDeadline - System.nanoTime()) > 0) {
                        TimeUnit.NANOSECONDS.timedWait(retrySignal, remaining);
                    }
                    if (closed.get()) {
                        throw failure(operation, "request canceled");
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw failure(operation, "request interrupted");
            }
        }
    }

    private Response request(
            URI url,
            String header,
            String credential,
            byte[] body,
            int maxBytes,
            long deadline,
            String operation)
            throws IOException {
        if (closed.get()) {
            throw failure(operation, "request canceled");
        }
        Future<Response> active;
        try {
            active =
                    worker.submit(
                            () -> exchange(url, header, credential, body, maxBytes, deadline));
        } catch (RejectedExecutionException ignored) {
            throw failure(operation, "request canceled");
        }
        future.set(active);
        if (closed.get()) {
            active.cancel(true);
        }
        try {
            return active.get(Math.max(1L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            closeSocket(socket.getAndSet(null));
            active.cancel(true);
            throw failure(operation, "request deadline exceeded");
        } catch (InterruptedException e) {
            closeSocket(socket.getAndSet(null));
            active.cancel(true);
            Thread.currentThread().interrupt();
            throw failure(operation, "request interrupted");
        } catch (CancellationException e) {
            throw failure(operation, "request canceled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SafeFailure) {
                throw failure(operation, e.getCause().getMessage());
            }
            throw failure(operation, closed.get() ? "request canceled" : "HTTP exchange failed");
        } finally {
            future.compareAndSet(active, null);
        }
    }

    private Response exchange(
            URI url, String header, String credential, byte[] body, int maxBytes, long deadline)
            throws IOException, HttpException {
        Socket network = new Socket();
        socket.set(network);
        try {
            if (closed.get() || Thread.currentThread().isInterrupted()) {
                throw new SafeFailure("request canceled");
            }
            String host = url.getHost();
            boolean secure = "https".equalsIgnoreCase(url.getScheme());
            int port = url.getPort() == -1 ? (secure ? 443 : 80) : url.getPort();
            network.connect(
                    new InetSocketAddress(host, port), timeout(deadline, config.connectTimeout));
            network.setSoTimeout(timeout(deadline, config.readTimeout));
            try (Socket transport = secure ? tls(network, url, host, port, deadline) : network) {
                DefaultBHttpClientConnection connection = CONNECTIONS.createConnection(transport);
                String target =
                        url.getRawQuery() == null
                                ? url.getRawPath()
                                : url.getRawPath() + "?" + url.getRawQuery();
                BasicHttpRequest request;
                if (body == null) {
                    request = new BasicHttpRequest("GET", target);
                } else {
                    BasicHttpEntityEnclosingRequest post =
                            new BasicHttpEntityEnclosingRequest("POST", target);
                    post.setEntity(new ByteArrayEntity(body));
                    post.setHeader("Content-Type", "application/json");
                    post.setHeader("Content-Length", Integer.toString(body.length));
                    request = post;
                }
                request.setHeader("Host", url.getRawAuthority());
                if (!credential.isEmpty()) {
                    request.setHeader(header, credential);
                }
                request.setHeader("Connection", "close");
                request.setHeader("Accept-Encoding", "identity");
                request.setHeader(
                        "Accept", body == null ? "application/octet-stream" : "application/json");
                request.setHeader("User-Agent", "Apache-SeaTunnel-Chroma");
                HttpResponse result =
                        new HttpRequestExecutor()
                                .execute(request, connection, new BasicHttpContext());
                int status = result.getStatusLine().getStatusCode();
                if (!successful(status)) {
                    return new Response(status, null, retryAfter(result));
                }
                return new Response(
                        status, status == 204 ? new byte[0] : readBody(result, maxBytes), 0);
            }
        } finally {
            socket.compareAndSet(network, null);
            closeSocket(network);
        }
    }

    private static boolean successful(int status) {
        return status == 200 || status == 201 || status == 204;
    }

    private SSLSocket tls(Socket network, URI url, String host, int port, long deadline)
            throws IOException {
        SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        if (!config.trustCertificate.isEmpty()
                && (sameOrigin(url, config.collectionsUrl)
                        || sameOrigin(url, config.embeddingUrl))) {
            if (trustedFactory == null) {
                trustedFactory = trustFactory();
            }
            factory = trustedFactory;
        }
        SSLSocket tls = (SSLSocket) factory.createSocket(network, host, port, true);
        try {
            SSLParameters params = tls.getSSLParameters();
            params.setEndpointIdentificationAlgorithm("HTTPS");
            tls.setSSLParameters(params);
            tls.setEnabledProtocols(
                    Arrays.stream(tls.getEnabledProtocols())
                            .filter(name -> "TLSv1.2".equals(name) || "TLSv1.3".equals(name))
                            .toArray(String[]::new));
            tls.startHandshake();
            tls.setSoTimeout(timeout(deadline, config.readTimeout));
            return tls;
        } catch (IOException | RuntimeException error) {
            closeSocket(tls);
            throw error;
        }
    }

    private SSLSocketFactory trustFactory() throws IOException {
        try (InputStream file = Files.newInputStream(Path.of(config.trustCertificate))) {
            byte[] input = file.readNBytes(65537);
            if (input.length > 65536) {
                throw new SafeFailure("trust certificate exceeds 65536 bytes");
            }
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(input));
            if (certificates.isEmpty()) {
                throw new SafeFailure("trust certificate file is empty");
            }
            KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
            trust.load(null, null);
            int index = 0;
            for (Certificate certificate : certificates) {
                trust.setCertificateEntry(Integer.toString(index++), certificate);
            }
            TrustManagerFactory managers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            managers.init(trust);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, managers.getTrustManagers(), null);
            return context.getSocketFactory();
        } catch (GeneralSecurityException | IOException ignored) {
            throw new SafeFailure("cannot load the configured TLS trust certificate");
        }
    }

    private static boolean sameOrigin(URI left, URI right) {
        return right != null
                && left.getScheme().equalsIgnoreCase(right.getScheme())
                && left.getHost().equalsIgnoreCase(right.getHost())
                && (left.getPort() == -1 ? 443 : left.getPort())
                        == (right.getPort() == -1 ? 443 : right.getPort());
    }

    private static byte[] readBody(HttpResponse result, int maxBytes) throws IOException {
        HttpEntity entity = result.getEntity();
        Header encoding = result.getFirstHeader("Content-Encoding");
        Header[] transfer = result.getHeaders("Transfer-Encoding");
        Header[] contentLength = result.getHeaders("Content-Length");
        if (entity == null
                || transfer.length > 1
                || contentLength.length > 1
                || result.containsHeader("Content-Range")
                || (transfer.length > 0
                        && (contentLength.length > 0
                                || !"chunked".equalsIgnoreCase(transfer[0].getValue())))
                || (encoding != null && !"identity".equalsIgnoreCase(encoding.getValue()))) {
            throw new SafeFailure("invalid or unsupported response framing");
        }
        if (entity.getContentLength() > maxBytes) {
            throw new SafeFailure("response exceeds the configured byte bound");
        }
        InputStream body = entity.getContent();
        byte[] bytes = body.readNBytes(maxBytes + 1);
        if (bytes.length > maxBytes) {
            throw new SafeFailure("response exceeds the configured byte bound");
        }
        // The socket closes in exchange even when a body remains unread.
        return bytes;
    }

    private static long retryAfter(HttpResponse response) {
        Header value = response.getFirstHeader("Retry-After");
        if (value == null) {
            return 0;
        }
        String raw = value.getValue().trim();
        try {
            return TimeUnit.SECONDS.toMillis(Math.max(0, Long.parseLong(raw)));
        } catch (NumberFormatException ignored) {
            Date date = DateUtils.parseDate(raw);
            return date == null ? 0 : Math.max(0, date.getTime() - System.currentTimeMillis());
        }
    }

    private static int timeout(long deadline, int max) throws IOException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new SafeFailure("request deadline exceeded");
        }
        return (int) Math.min(max, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
    }

    static IOException failure(String operation, String cause) {
        return new IOException("Chroma sink " + operation + ": " + cause);
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The caller already receives the sanitized operation result.
            }
        }
    }

    /**
     * Closes active I/O without waiting for the network worker and seals future requests. DNS may
     * complete later on the daemon worker, which cannot issue more operations.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeSocket(socket.getAndSet(null));
            Future<Response> active = future.getAndSet(null);
            if (active != null) {
                active.cancel(true);
            }
            synchronized (retrySignal) {
                retrySignal.notifyAll();
            }
            worker.shutdownNow();
        }
    }

    /**
     * Carries only the validated response bytes and an optional bounded retry instruction.
     * Non-success payloads are deliberately never returned to the writer.
     */
    private static final class Response {
        final int status;
        final byte[] body;
        final long retryAfter;

        Response(int status, byte[] body, long retryAfter) {
            this.status = status;
            this.body = body;
            this.retryAfter = retryAfter;
        }
    }

    /**
     * Propagates a fixed transport error without leaking network targets or response content. The
     * caller adds the operation name after unwrapping the worker result.
     */
    private static final class SafeFailure extends IOException {
        SafeFailure(String reason) {
            super(reason);
        }
    }
}

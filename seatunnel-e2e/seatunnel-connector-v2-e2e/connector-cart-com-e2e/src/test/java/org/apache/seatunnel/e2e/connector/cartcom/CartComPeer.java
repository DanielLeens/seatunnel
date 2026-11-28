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

package org.apache.seatunnel.e2e.connector.cartcom;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * Serves an independent sequence of read-only CartCom contract responses over real TLS.
 *
 * <p>The production hostname resolves to this loopback peer only in the owned engine container.
 * Literal path/query expectations come from provider contracts, not connector resource classes.
 */
public final class CartComPeer {
    static final String HOST = "store.cart.test";
    static final String CENTRAL = "public.americommerce.com";
    static final String SITE = "9142";
    static final String USER = "fixture-provisioned-user";
    static final String SECRET = "fixture-central-secret+é";
    static final String TOKEN = "fixture-merchant-token+&=";
    static final String PASSWORD = "test-only-cart-keystore-password";
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path directory;
    private final Map<String, AtomicInteger> indices = new ConcurrentHashMap<>();

    private CartComPeer(Path directory) {
        this.directory = directory;
    }

    /**
     * Starts a container-local HTTPS peer and stops when its owner writes a shutdown marker.
     *
     * <p>The test CA is passed explicitly to the source and never changes the machine trust store.
     */
    public static void main(String[] args) throws Exception {
        CartComPeer peer = new CartComPeer(Path.of(args[0]));
        ExecutorService threads = Executors.newCachedThreadPool();
        String selected = "trusted";
        HttpsServer server = peer.start(selected, threads);
        try {
            Files.writeString(peer.directory.resolve("ready"), "ready");
            while (!Files.exists(peer.directory.resolve("stop"))) {
                Path selection = peer.directory.resolve("identity");
                String desired =
                        Files.exists(selection) ? Files.readString(selection).trim() : "trusted";
                if (!selected.equals(desired)) {
                    server.stop(0);
                    server = peer.start(desired, threads);
                    selected = desired;
                    Files.writeString(peer.directory.resolve("identity-ready"), selected);
                }
                Thread.sleep(25L);
            }
        } finally {
            server.stop(0);
            threads.shutdownNow();
        }
    }

    private HttpsServer start(String identity, ExecutorService threads) throws Exception {
        if (!identity.equals("trusted") && !identity.equals("wrong-name")) {
            throw new IllegalArgumentException("Unknown owned peer certificate");
        }
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(directory.resolve(identity + ".p12"))) {
            keys.load(input, PASSWORD.toCharArray());
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(keys, PASSWORD.toCharArray());
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(manager.getKeyManagers(), null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 443), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        server.setExecutor(threads);
        server.createContext("/", this::handle);
        server.start();
        return server;
    }

    private void handle(HttpExchange exchange) throws IOException {
        String name = Files.readString(directory.resolve("active")).trim();
        try {
            JsonNode script = JSON.readTree(Files.readString(directory.resolve(name + ".json")));
            int index =
                    indices.computeIfAbsent(name, ignored -> new AtomicInteger()).getAndIncrement();
            if (index >= script.size()) {
                reject(exchange, name, "unexpected request");
                return;
            }
            JsonNode step = script.get(index);
            String host = step.path("host").asText(HOST);
            String target = exchange.getRequestURI().toASCIIString();
            if (!"GET".equals(exchange.getRequestMethod())
                    || !host.equals(exchange.getRequestHeaders().getFirst("Host"))
                    || !step.path("target").asText().equals(target)
                    || !validAuth(exchange, host, target)
                    || exchange.getRequestBody().read() != -1
                    || exchange.getRequestHeaders().containsKey("Authorization")
                    || exchange.getRequestHeaders().containsKey("Cookie")) {
                reject(exchange, name, "request contract mismatch at step " + index);
                return;
            }
            append(name, ".requests", index + " " + host + " " + target);
            append(name, ".times", Long.toString(System.currentTimeMillis()));
            if (step.has("delay_seconds")) {
                String date =
                        ZonedDateTime.now(ZoneOffset.UTC)
                                .plusSeconds(step.path("delay_seconds").intValue())
                                .format(DateTimeFormatter.RFC_1123_DATE_TIME);
                exchange.getResponseHeaders().set("Retry-After", date);
            } else if (step.has("Retry-After")) {
                exchange.getResponseHeaders().set("Retry-After", step.path("Retry-After").asText());
            }
            int status = step.path("status").asInt(200);
            String mode = step.path("mode").asText("json");
            if (mode.equals("redirect")) {
                exchange.getResponseHeaders()
                        .set("Location", "https://cartcom-trap.invalid/private");
                exchange.sendResponseHeaders(status, -1);
                return;
            }
            byte[] body =
                    step.has("bytes")
                            ? Base64.getDecoder().decode(step.path("bytes").asText())
                            : step.path("response").asText().getBytes(StandardCharsets.UTF_8);
            if (mode.equals("partial") || mode.equals("trickle")) {
                stream(exchange, name, body, mode.equals("partial"));
                return;
            }
            if (mode.equals("gzip")) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(buffer)) {
                    gzip.write(body);
                }
                body = buffer.toByteArray();
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            }
            if (step.has("encoding")) {
                exchange.getResponseHeaders()
                        .set("Content-Encoding", step.path("encoding").asText());
            }
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length != 0) {
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
            }
        } catch (IllegalArgumentException | GeneralSecurityException invalid) {
            reject(exchange, name, "invalid request encoding");
        } finally {
            exchange.close();
        }
    }

    private void stream(HttpExchange exchange, String name, byte[] body, boolean partial)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body.length == 0 ? '{' : body[0]);
            output.flush();
            append(name, ".started", Long.toString(System.currentTimeMillis()));
            if (partial) {
                if (release(name)) {
                    output.write(body, 1, body.length - 1);
                }
            } else {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
                while (System.nanoTime() - deadline < 0) {
                    output.write(' ');
                    output.flush();
                    Thread.sleep(50L);
                }
            }
        } catch (IOException closed) {
            append(name, ".disconnect", Long.toString(System.currentTimeMillis()));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean release(String name) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        try {
            while (!Files.exists(directory.resolve(name + ".release"))) {
                if (Files.exists(directory.resolve("stop")) || System.nanoTime() - deadline >= 0) {
                    return false;
                }
                Thread.sleep(25L);
            }
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Computes the test oracle against the received request line, independently of the source.
     *
     * <p>The peer checks that the alternate scheme's credentials never appear on this origin.
     */
    private static boolean validAuth(HttpExchange exchange, String host, String target)
            throws GeneralSecurityException {
        if (HOST.equals(host)) {
            return TOKEN.equals(exchange.getRequestHeaders().getFirst("X-AC-Auth-Token"))
                    && !exchange.getRequestHeaders().containsKey("X-AC-PUB-Site-ID")
                    && !exchange.getRequestHeaders().containsKey("X-AC-PUB-User")
                    && !exchange.getRequestHeaders().containsKey("X-AC-PUB-Auth-Signature");
        }
        Mac oracle = Mac.getInstance("HmacSHA256");
        oracle.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] message = String.join("&", "GET", target, USER).getBytes(StandardCharsets.UTF_8);
        String signature = Base64.getEncoder().encodeToString(oracle.doFinal(message));
        return CENTRAL.equals(host)
                && !exchange.getRequestHeaders().containsKey("X-AC-Auth-Token")
                && SITE.equals(exchange.getRequestHeaders().getFirst("X-AC-PUB-Site-ID"))
                && USER.equals(exchange.getRequestHeaders().getFirst("X-AC-PUB-User"))
                && signature.equals(
                        exchange.getRequestHeaders().getFirst("X-AC-PUB-Auth-Signature"));
    }

    private void reject(HttpExchange exchange, String name, String reason) throws IOException {
        append(name, ".invalid", reason);
        exchange.sendResponseHeaders(500, -1);
    }

    private void append(String name, String suffix, String value) throws IOException {
        Files.writeString(
                directory.resolve(name + suffix),
                value + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }
}

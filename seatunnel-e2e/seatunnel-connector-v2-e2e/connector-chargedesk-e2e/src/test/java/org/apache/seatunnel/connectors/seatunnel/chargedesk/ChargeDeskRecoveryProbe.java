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
package org.apache.seatunnel.connectors.seatunnel.chargedesk;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Verifies serialized offset replay and provider ceilings using the packaged Java connector.
 *
 * <p>These supplemental container modes share the independent peer with actual Zeta savepoints.
 */
public final class ChargeDeskRecoveryProbe {
    private ChargeDeskRecoveryProbe() {}

    /**
     * Restores a published prefix, changed raw pages and lifecycle or ceiling positions.
     *
     * <p>No checkpoint contains the Basic credential or the original provider payload.
     */
    public static void main(String[] args) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("token", "synthetic-chargedesk-secret+/=");
        options.put("resources", List.of("subscriptions"));
        options.put("tls_trust_certificate", Files.readString(Path.of(args[0])));
        options.put("max_retries", 0);
        options.put("request_interval_ms", 0);
        options.put("page_size", 50);
        options.put("start_date", 1704067200L);
        options.put("end_date", 1704153600L);
        ChargeDeskConfig config = new ChargeDeskConfig(ReadonlyConfig.fromMap(options));
        if (args[1].equals("zero")) {
            zero(config, options);
            return;
        }
        if (args[1].startsWith("cap")) {
            cap(options, args[1]);
            return;
        }
        Rows before = new Rows();
        ChargeDeskSplit saved;
        try (ChargeDeskReader reader = opened(config, new Context())) {
            require(reader.snapshotState(0L).isEmpty(), "unassigned state must remain empty");
            reader.addSplits(List.of(config.newSplit(Instant.ofEpochSecond(1704153600L))));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.values.size() < 32 && System.nanoTime() < deadline) {
                reader.pollNext(before);
                Thread.sleep(5);
            }
            require(before.values.size() == 32, "exactly32 selected rows must precede checkpoint");
            saved = roundTrip(reader.snapshotState(1L).get(0));
            require(
                    saved.row == 32 && saved.offset == 0 && saved.page == 1 && saved.digest != null,
                    "flat row advances atomically with collection");
        }
        String encoded = new String(serialize(saved), StandardCharsets.ISO_8859_1);
        require(
                !encoded.contains("synthetic-chargedesk-secret")
                        && !encoded.contains("synthetic-response-record"),
                "state must omit credentials and provider bodies");
        Rows after = new Rows();
        Context done = new Context();
        boolean rejected = false;
        try (ChargeDeskReader reader = opened(config, done)) {
            reader.addSplits(List.of(saved));
            await(reader, after, done);
            require(
                    args[1].equals("resume") && after.values.size() == 1,
                    "only one selected suffix may remain");
            require(
                    "S32".equals(after.values.get(0).getField(1))
                            && "2024-01-01T00:00:00Z".equals(after.values.get(0).getField(2)),
                    "exact suffix identity and time must survive");
            terminal(config, reader.snapshotState(2L).get(0));
        } catch (ChargeDeskResource.Failure ex) {
            require(
                    args[1].startsWith("changed")
                            && ex.getMessage().contains("changed before replay"),
                    "changed raw page must fail for its saved digest");
            require(after.values.isEmpty(), "invalid replay must publish no suffix");
            rejected = true;
        }
        require(
                rejected == args[1].startsWith("changed"),
                "expected replay disposition must occur");
    }

    private static void cap(Map<String, Object> supplied, String mode) throws Exception {
        Map<String, Object> options = new HashMap<>(supplied);
        options.put("resources", List.of(mode.contains("charge") ? "charges" : "products"));
        options.put("page_size", 500);
        options.remove("start_date");
        options.remove("end_date");
        ChargeDeskConfig config = new ChargeDeskConfig(ReadonlyConfig.fromMap(options));
        ChargeDeskSplit split = config.newSplit(Instant.ofEpochSecond(1704153600L));
        int maximum = mode.contains("charge") ? 50000 : 10000;
        split.offset = mode.equals("cap-clamp") ? 9999 : maximum;
        split.page = maximum / 500 + 1;
        split.acceptedPages = split.page - 1;
        Rows rows = new Rows();
        Context context = new Context();
        boolean failed = false;
        try (ChargeDeskReader reader = opened(config, context)) {
            reader.addSplits(List.of(roundTrip(split)));
            await(reader, rows, context);
            require(!mode.contains("nonempty"), "nonempty native ceiling must not report success");
            require(
                    rows.values.size() == (mode.equals("cap-clamp") ? 1 : 0),
                    "clamped final page must preserve its sole row");
        } catch (ChargeDeskResource.Failure ex) {
            require(
                    mode.contains("nonempty") && ex.getMessage().contains("native offset limit"),
                    "native cap must fail explicitly");
            require(rows.values.isEmpty(), "unprovable cap must fail before publishing rows");
            failed = true;
        }
        require(
                failed == mode.contains("nonempty"),
                "cap disposition must match the actual response");
    }

    private static void zero(ChargeDeskConfig config, Map<String, Object> options)
            throws Exception {
        ChargeDeskSplit finished = config.newSplit(Instant.ofEpochSecond(1704153600L));
        finished.resourceIndex = config.resources.size();
        terminal(config, finished);
        Context idle = new Context();
        try (ChargeDeskReader reader = opened(config, idle)) {
            reader.handleNoMoreSplits();
            reader.pollNext(new Rows());
            require(
                    idle.done && reader.snapshotState(0L).isEmpty(),
                    "unassigned EOF must stay quiet");
        }
        try (ChargeDeskReader reader = new ChargeDeskReader(config, new Context())) {
            reader.cancel();
            reader.open();
            reader.addSplits(List.of(config.newSplit(Instant.ofEpochSecond(1704153600L))));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(rows.values.isEmpty(), "cancel before open remains sticky");
        }
        for (String field : List.of("token", "resources", "page_size", "start_date")) {
            Map<String, Object> changed = new HashMap<>(options);
            changed.put(
                    field,
                    field.equals("token")
                            ? "different-secret"
                            : field.equals("resources")
                                    ? List.of("products")
                                    : field.equals("page_size") ? 1 : 1704067201L);
            rejected(
                    new ChargeDeskConfig(ReadonlyConfig.fromMap(changed)),
                    config.newSplit(Instant.ofEpochSecond(1704153600L)));
        }
        ChargeDeskSplit invalid = config.newSplit(Instant.ofEpochSecond(1704153600L));
        invalid.row = -1;
        rejected(config, invalid);
        Map<String, Object> openEnd = new HashMap<>(options);
        openEnd.remove("end_date");
        ChargeDeskConfig moving = new ChargeDeskConfig(ReadonlyConfig.fromMap(openEnd));
        ChargeDeskSplit frozen = roundTrip(moving.newSplit(Instant.ofEpochSecond(1704153600L)));
        frozen.validate(moving);
        require(
                Long.valueOf(1704153600L).equals(frozen.windowEnd),
                "start-only scan must retain a frozen epoch upper bound");
        ChargeDeskSource source = new ChargeDeskSource(ReadonlyConfig.fromMap(options));
        List<ChargeDeskSplit> assignments = new ArrayList<>();
        Set<Integer> registered = new HashSet<>();
        SourceSplitEnumerator.Context<ChargeDeskSplit> coordinator =
                enumeratorContext(registered, assignments);
        ChargeDeskSplit pending;
        try (SourceSplitEnumerator<ChargeDeskSplit, ChargeDeskSplit> first =
                source.createEnumerator(coordinator)) {
            first.open();
            first.run();
            require(
                    first.currentUnassignedSplitSize() == 1 && assignments.isEmpty(),
                    "unassigned scan retained");
            pending = roundTrip(first.snapshotState(0L));
        }
        registered.add(0);
        try (SourceSplitEnumerator<ChargeDeskSplit, ChargeDeskSplit> restored =
                source.restoreEnumerator(coordinator, pending)) {
            restored.open();
            restored.run();
            restored.registerReader(0);
            require(
                    assignments.size() == 1 && restored.currentUnassignedSplitSize() == 0,
                    "restored coordinator assigns once");
        }
    }

    private static ChargeDeskSplit roundTrip(ChargeDeskSplit split) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(serialize(split)))) {
            return (ChargeDeskSplit) input.readObject();
        }
    }

    private static void await(ChargeDeskReader reader, Rows rows, Context context)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!context.done && System.nanoTime() < deadline) {
            reader.pollNext(rows);
            Thread.sleep(5);
        }
        require(context.done, "bounded reader must finish");
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<ChargeDeskSplit> enumeratorContext(
            Set<Integer> readers, List<ChargeDeskSplit> assignments) {
        return (SourceSplitEnumerator.Context<ChargeDeskSplit>)
                Proxy.newProxyInstance(
                        SourceSplitEnumerator.Context.class.getClassLoader(),
                        new Class<?>[] {SourceSplitEnumerator.Context.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "currentParallelism":
                                    return 1;
                                case "registeredReaders":
                                    return readers;
                                case "assignSplit":
                                    assignments.add((ChargeDeskSplit) args[1]);
                                    return null;
                                case "signalNoMoreSplits":
                                    return null;
                                default:
                                    throw new AssertionError(
                                            "Unexpected coordinator operation " + method.getName());
                            }
                        });
    }

    private static void terminal(ChargeDeskConfig config, ChargeDeskSplit split) throws Exception {
        Context done = new Context();
        try (ChargeDeskReader reader = opened(config, done);
                ObjectInputStream in =
                        new ObjectInputStream(new ByteArrayInputStream(serialize(split)))) {
            reader.addSplits(List.of((ChargeDeskSplit) in.readObject()));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(done.done && rows.values.isEmpty(), "terminal saved state needs no request");
        }
    }

    private static void rejected(ChargeDeskConfig config, ChargeDeskSplit split) {
        try (ChargeDeskReader reader = opened(config, new Context())) {
            reader.addSplits(List.of(split));
        } catch (ChargeDeskResource.Failure ex) {
            require(
                    ex.getMessage().contains("checkpoint"),
                    "checkpoint error must explain its scope");
            return;
        }
        throw new AssertionError("invalid position was accepted");
    }

    private static ChargeDeskReader opened(ChargeDeskConfig config, Context context) {
        ChargeDeskReader reader = new ChargeDeskReader(config, context);
        reader.open();
        return reader;
    }

    private static byte[] serialize(ChargeDeskSplit split) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(split);
        }
        return bytes.toByteArray();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * Collects actual connector rows while providing the same checkpoint lock as an engine task.
     */
    private static final class Rows implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<SeaTunnelRow> values = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            values.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }

    /**
     * Records bounded completion from the real packaged reader without mocking its state.
     *
     * <p>The probe supplies no metrics service or enumerator-side behavior.
     */
    private static final class Context implements SourceReader.Context {
        private boolean done;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            done = true;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}

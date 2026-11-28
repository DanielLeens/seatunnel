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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Verifies serialized page replay and parent resource lifecycle using the packaged Java connector.
 *
 * <p>These supplemental container modes share the independent peer with actual Zeta savepoints.
 */
public final class Cin7RecoveryProbe {
    private Cin7RecoveryProbe() {}

    /**
     * Restores a published prefix, changed raw pages and scoped lifecycle positions.
     *
     * <p>No checkpoint contains either provider credential or the original provider payload.
     */
    public static void main(String[] args) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("token", "00000000-0000-4000-8000-000000000002");
        options.put("resources", List.of("brands"));
        options.put("tls_trust_certificate", Files.readString(Path.of(args[0])));
        options.put("max_retries", 0);
        options.put("request_interval_ms", 0);
        options.put("page_size", 50);
        options.put("access_key", "00000000-0000-4000-8000-000000000001");
        Cin7Config config = new Cin7Config(ReadonlyConfig.fromMap(options));
        if (args[1].equals("zero")) {
            zero(config, options);
            return;
        }
        Rows before = new Rows();
        Cin7Split saved;
        try (Cin7Reader reader = opened(config, new Context())) {
            require(reader.snapshotState(0L).isEmpty(), "unassigned state must remain empty");
            reader.addSplits(List.of(config.newSplit()));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.values.size() < 32 && System.nanoTime() < deadline) {
                reader.pollNext(before);
                Thread.sleep(5);
            }
            require(before.values.size() == 32, "exactly32 selected rows must precede checkpoint");
            saved = roundTrip(reader.snapshotState(1L).get(0));
            require(
                    saved.row == 32
                            && saved.total == 33
                            && !saved.detailPending
                            && saved.page == 1
                            && saved.digest != null,
                    "flat row advances atomically with collection");
        }
        String encoded = new String(serialize(saved), StandardCharsets.ISO_8859_1);
        require(
                !encoded.contains("00000000-0000-4000-8000-000000000002")
                        && !encoded.contains("00000000-0000-4000-8000-000000000001")
                        && !encoded.contains("synthetic-response-record"),
                "state must omit credentials and provider bodies");
        Rows after = new Rows();
        Context done = new Context();
        boolean rejected = false;
        try (Cin7Reader reader = opened(config, done)) {
            reader.addSplits(List.of(saved));
            await(reader, after, done);
            require(
                    args[1].equals("resume") && after.values.size() == 1,
                    "only one selected suffix may remain");
            require(
                    "B32".equals(after.values.get(0).getField(1))
                            && after.values.get(0).getField(2) == null,
                    "exact suffix identity and time must survive");
            terminal(config, reader.snapshotState(2L).get(0));
        } catch (Cin7Resource.Failure ex) {
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

    private static void zero(Cin7Config config, Map<String, Object> options) throws Exception {
        Cin7Split finished = config.newSplit();
        finished.resourceIndex = config.collections.size();
        terminal(config, finished);
        Context idle = new Context();
        try (Cin7Reader reader = opened(config, idle)) {
            reader.handleNoMoreSplits();
            reader.pollNext(new Rows());
            require(
                    idle.done && reader.snapshotState(0L).isEmpty(),
                    "unassigned EOF must stay quiet");
        }
        try (Cin7Reader reader = new Cin7Reader(config, new Context())) {
            reader.cancel();
            reader.open();
            reader.addSplits(List.of(config.newSplit()));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(rows.values.isEmpty(), "cancel before open remains sticky");
        }
        Map<String, Object> changedValues = new HashMap<>();
        changedValues.put("token", "different-secret");
        changedValues.put("access_key", "different-app");
        changedValues.put("resources", List.of("customers"));
        changedValues.put("page_size", 1);
        for (Map.Entry<String, Object> field : changedValues.entrySet()) {
            Map<String, Object> changed = new HashMap<>(options);
            changed.put(field.getKey(), field.getValue());
            rejected(new Cin7Config(ReadonlyConfig.fromMap(changed)), config.newSplit());
        }
        Cin7Split invalid = config.newSplit();
        invalid.row = -1;
        rejected(config, invalid);
        Cin7Source source = new Cin7Source(ReadonlyConfig.fromMap(options));
        List<Cin7Split> assignments = new ArrayList<>();
        Set<Integer> registered = new HashSet<>();
        SourceSplitEnumerator.Context<Cin7Split> coordinator =
                enumeratorContext(registered, assignments);
        Cin7Split pending;
        try (SourceSplitEnumerator<Cin7Split, Cin7Split> first =
                source.createEnumerator(coordinator)) {
            first.open();
            first.run();
            require(
                    first.currentUnassignedSplitSize() == 1 && assignments.isEmpty(),
                    "unassigned scan retained");
            pending = roundTrip(first.snapshotState(0L));
        }
        registered.add(0);
        try (SourceSplitEnumerator<Cin7Split, Cin7Split> restored =
                source.restoreEnumerator(coordinator, pending)) {
            restored.open();
            restored.run();
            restored.registerReader(0);
            require(
                    assignments.size() == 1 && restored.currentUnassignedSplitSize() == 0,
                    "restored coordinator assigns once");
        }
    }

    private static Cin7Split roundTrip(Cin7Split split) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(serialize(split)))) {
            return (Cin7Split) input.readObject();
        }
    }

    private static void await(Cin7Reader reader, Rows rows, Context context) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!context.done && System.nanoTime() < deadline) {
            reader.pollNext(rows);
            Thread.sleep(5);
        }
        require(context.done, "bounded reader must finish");
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<Cin7Split> enumeratorContext(
            Set<Integer> readers, List<Cin7Split> assignments) {
        return (SourceSplitEnumerator.Context<Cin7Split>)
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
                                    assignments.add((Cin7Split) args[1]);
                                    return null;
                                case "signalNoMoreSplits":
                                    return null;
                                default:
                                    throw new AssertionError(
                                            "Unexpected coordinator operation " + method.getName());
                            }
                        });
    }

    private static void terminal(Cin7Config config, Cin7Split split) throws Exception {
        Context done = new Context();
        try (Cin7Reader reader = opened(config, done);
                ObjectInputStream in =
                        new ObjectInputStream(new ByteArrayInputStream(serialize(split)))) {
            reader.addSplits(List.of((Cin7Split) in.readObject()));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(done.done && rows.values.isEmpty(), "terminal saved state needs no request");
        }
    }

    private static void rejected(Cin7Config config, Cin7Split split) {
        try (Cin7Reader reader = opened(config, new Context())) {
            reader.addSplits(List.of(split));
        } catch (Cin7Resource.Failure ex) {
            require(
                    ex.getMessage().contains("checkpoint"),
                    "checkpoint error must explain its scope");
            return;
        }
        throw new AssertionError("invalid position was accepted");
    }

    private static Cin7Reader opened(Cin7Config config, Context context) {
        Cin7Reader reader = new Cin7Reader(config, context);
        reader.open();
        return reader;
    }

    private static byte[] serialize(Cin7Split split) throws Exception {
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

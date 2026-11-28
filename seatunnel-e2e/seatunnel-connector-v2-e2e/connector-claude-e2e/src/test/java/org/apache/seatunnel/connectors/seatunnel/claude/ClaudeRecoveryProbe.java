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
package org.apache.seatunnel.connectors.seatunnel.claude;

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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Verifies serialized analytics page replay and frozen date scope using the packaged Java
 * connector.
 *
 * <p>These supplemental container modes share the independent peer with actual Zeta savepoints.
 */
public final class ClaudeRecoveryProbe {
    private ClaudeRecoveryProbe() {}

    /**
     * Restores a published prefix, changed raw pages and scoped lifecycle positions.
     *
     * <p>No checkpoint contains either provider credential or the original provider payload.
     */
    public static void main(String[] args) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("token", "synthetic-claude-secret+/=");
        options.put("resources", List.of("users"));
        options.put("tls_trust_certificate", Files.readString(Path.of(args[0])));
        options.put("max_retries", 0);
        options.put("request_interval_ms", 0);
        options.put("page_size", 50);
        options.put("start_date", args[2]);
        options.put("end_date", args[2]);
        ClaudeConfig config = new ClaudeConfig(ReadonlyConfig.fromMap(options));
        if (args[1].equals("zero")) {
            zero(config, options);
            return;
        }
        Rows before = new Rows();
        ClaudeSplit saved;
        try (ClaudeReader reader = opened(config, new Context())) {
            require(reader.snapshotState(0L).isEmpty(), "unassigned state must remain empty");
            reader.addSplits(List.of(config.newSplit(Instant.now())));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.values.size() < 32 && System.nanoTime() < deadline) {
                reader.pollNext(before);
                Thread.sleep(5);
            }
            require(before.values.size() == 32, "exactly32 selected rows must precede checkpoint");
            saved = roundTrip(reader.snapshotState(1L).get(0));
            require(
                    saved.row == 32
                            && saved.windowStart.equals(LocalDate.parse(args[2]))
                            && saved.page == 1
                            && saved.digest != null,
                    "flat row advances atomically with collection");
        }
        String encoded = new String(serialize(saved), StandardCharsets.ISO_8859_1);
        require(
                !encoded.contains("synthetic-claude-secret")
                        && !encoded.contains("synthetic-response-record"),
                "state must omit credentials and provider bodies");
        Rows after = new Rows();
        Context done = new Context();
        boolean rejected = false;
        try (ClaudeReader reader = opened(config, done)) {
            reader.addSplits(List.of(saved));
            await(reader, after, done);
            require(
                    args[1].equals("resume") && after.values.size() == 1,
                    "only one selected suffix may remain");
            require(
                    "U32".equals(after.values.get(0).getField(1))
                            && after.values.get(0).getArity() == 7
                            && "users".equals(after.values.get(0).getField(0))
                            && after.values.get(0).getField(2) == null
                            && (args[2] + "T00:00:00Z").equals(after.values.get(0).getField(3))
                            && (LocalDate.parse(args[2]).plusDays(1) + "T00:00:00Z")
                                    .equals(after.values.get(0).getField(4))
                            && after.values.get(0).getField(5) == null
                            && "{\"user\":{\"id\":\"U32\",\"email_address\":null},\"description\":\"synthetic-response-record\"}"
                                    .equals(after.values.get(0).getField(6)),
                    "all seven suffix columns and complete raw record must survive");
            terminal(config, reader.snapshotState(2L).get(0));
        } catch (ClaudeResource.Failure ex) {
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

    private static void zero(ClaudeConfig config, Map<String, Object> options) throws Exception {
        ClaudeSplit finished = config.newSplit(Instant.now());
        finished.resourceIndex = config.resources.size();
        terminal(config, finished);
        Context idle = new Context();
        try (ClaudeReader reader = opened(config, idle)) {
            reader.handleNoMoreSplits();
            reader.pollNext(new Rows());
            require(
                    idle.done && reader.snapshotState(0L).isEmpty(),
                    "unassigned EOF must stay quiet");
        }
        try (ClaudeReader reader = new ClaudeReader(config, new Context())) {
            reader.cancel();
            reader.open();
            reader.addSplits(List.of(config.newSplit(Instant.now())));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(rows.values.isEmpty(), "cancel before open remains sticky");
        }
        Map<String, Object> changedValues = new HashMap<>();
        changedValues.put("token", "different-secret");
        changedValues.put("resources", List.of("summaries"));
        changedValues.put("page_size", 1);
        changedValues.put(
                "start_date",
                LocalDate.parse((String) options.get("start_date")).minusDays(1).toString());
        changedValues.put(
                "end_date",
                LocalDate.parse((String) options.get("end_date")).plusDays(1).toString());
        for (Map.Entry<String, Object> field : changedValues.entrySet()) {
            Map<String, Object> changed = new HashMap<>(options);
            changed.put(field.getKey(), field.getValue());
            rejected(
                    new ClaudeConfig(ReadonlyConfig.fromMap(changed)),
                    config.newSplit(Instant.now()));
        }
        Map<String, Object> defaults = new HashMap<>(options);
        defaults.remove("start_date");
        defaults.remove("end_date");
        defaults.put("resources", List.of("users", "user_usage_report"));
        ClaudeConfig automatic = new ClaudeConfig(ReadonlyConfig.fromMap(defaults));
        ClaudeSplit frozen = roundTrip(automatic.newSplit(Instant.parse("2026-11-28T12:00:00Z")));
        require(
                frozen.scanStart.equals(LocalDate.of(2026, 11, 21))
                        && frozen.adoptionEnd.equals(LocalDate.of(2026, 11, 26))
                        && frozen.reportEnd.equals(LocalDate.of(2026, 11, 28)),
                "adoption stops before the last two days while reports include yesterday");
        require(
                frozen.windowEnd(ClaudeResource.USERS).equals(LocalDate.of(2026, 11, 22)),
                "default scan begins with a daily slice");
        try (ClaudeReader reader = opened(automatic, new Context())) {
            reader.addSplits(List.of(frozen));
            ClaudeSplit restored = roundTrip(reader.snapshotState(99L).get(0));
            require(
                    restored.scanStart.equals(frozen.scanStart)
                            && restored.adoptionEnd.equals(frozen.adoptionEnd)
                            && restored.reportEnd.equals(frozen.reportEnd),
                    "restore must retain resolved default windows instead of consulting today's clock");
        }
        Map<String, Object> tooOld = new HashMap<>(options);
        tooOld.put("resources", List.of("user_usage_report"));
        tooOld.put("start_date", "2026-01-01");
        tooOld.put("end_date", "2026-01-02");
        boolean rejectedAge = false;
        try {
            new ClaudeConfig(ReadonlyConfig.fromMap(tooOld))
                    .newSplit(Instant.parse("2027-11-28T12:00:00Z"));
        } catch (ClaudeResource.Failure expected) {
            rejectedAge = expected.getMessage().contains("365 days");
        }
        require(rejectedAge, "reports outside the provider retention period must fail before HTTP");
        ClaudeSplit invalid = config.newSplit(Instant.now());
        invalid.row = -1;
        rejected(config, invalid);
        ClaudeSource source = new ClaudeSource(ReadonlyConfig.fromMap(options));
        List<ClaudeSplit> assignments = new ArrayList<>();
        Set<Integer> registered = new HashSet<>();
        SourceSplitEnumerator.Context<ClaudeSplit> coordinator =
                enumeratorContext(registered, assignments);
        ClaudeSplit pending;
        try (SourceSplitEnumerator<ClaudeSplit, ClaudeSplit> first =
                source.createEnumerator(coordinator)) {
            first.open();
            first.run();
            require(
                    first.currentUnassignedSplitSize() == 1 && assignments.isEmpty(),
                    "unassigned scan retained");
            pending = roundTrip(first.snapshotState(0L));
        }
        registered.add(0);
        try (SourceSplitEnumerator<ClaudeSplit, ClaudeSplit> restored =
                source.restoreEnumerator(coordinator, pending)) {
            restored.open();
            restored.run();
            restored.registerReader(0);
            require(
                    assignments.size() == 1 && restored.currentUnassignedSplitSize() == 0,
                    "restored coordinator assigns once");
        }
    }

    private static ClaudeSplit roundTrip(ClaudeSplit split) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(serialize(split)))) {
            return (ClaudeSplit) input.readObject();
        }
    }

    private static void await(ClaudeReader reader, Rows rows, Context context) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!context.done && System.nanoTime() < deadline) {
            reader.pollNext(rows);
            Thread.sleep(5);
        }
        require(context.done, "bounded reader must finish");
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<ClaudeSplit> enumeratorContext(
            Set<Integer> readers, List<ClaudeSplit> assignments) {
        return (SourceSplitEnumerator.Context<ClaudeSplit>)
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
                                    assignments.add((ClaudeSplit) args[1]);
                                    return null;
                                case "signalNoMoreSplits":
                                    return null;
                                default:
                                    throw new AssertionError(
                                            "Unexpected coordinator operation " + method.getName());
                            }
                        });
    }

    private static void terminal(ClaudeConfig config, ClaudeSplit split) throws Exception {
        Context done = new Context();
        try (ClaudeReader reader = opened(config, done);
                ObjectInputStream in =
                        new ObjectInputStream(new ByteArrayInputStream(serialize(split)))) {
            reader.addSplits(List.of((ClaudeSplit) in.readObject()));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(done.done && rows.values.isEmpty(), "terminal saved state needs no request");
        }
    }

    private static void rejected(ClaudeConfig config, ClaudeSplit split) {
        try (ClaudeReader reader = opened(config, new Context())) {
            reader.addSplits(List.of(split));
        } catch (ClaudeResource.Failure ex) {
            require(
                    ex.getMessage().contains("checkpoint"),
                    "checkpoint error must explain its scope");
            return;
        }
        throw new AssertionError("invalid position was accepted");
    }

    private static ClaudeReader opened(ClaudeConfig config, Context context) {
        ClaudeReader reader = new ClaudeReader(config, context);
        reader.open();
        return reader;
    }

    private static byte[] serialize(ClaudeSplit split) throws Exception {
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

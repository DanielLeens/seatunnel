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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Verifies immutable partial-response checkpoints with the packaged Clazar reader.
 *
 * <p>Real Zeta savepoints exercise page, resource and terminal recovery separately.
 */
public final class ClazarRecoveryProbe {
    private ClazarRecoveryProbe() {}

    /**
     * Restores a 32-row prefix or rejects changed published and filtered provider records.
     *
     * <p>All requests use the independent Java peer and synthetic authentication.
     */
    public static void main(String[] args) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("client_id", "demo-clazar-client");
        options.put("client_secret", "synthetic-clazar-secret+/=");
        options.put("resources", List.of("buyers"));
        options.put("endpoint", "https://api.clazar.io");
        options.put("trust_certificate", args[0]);
        options.put("max_attempts", 1);
        options.put("request_timeout_ms", 20000);
        options.put("start_date", "2024-02-29T00:00:00Z");
        options.put("end_date", "2024-03-01T00:00:00Z");
        ClazarConfig config = new ClazarConfig(ReadonlyConfig.fromMap(options));
        if (args[1].equals("zero")) {
            zero(config, options);
            return;
        }
        Rows before = new Rows();
        ClazarSplit saved;
        try (ClazarReader reader = opened(config, new Context())) {
            require(reader.snapshotState(0L).isEmpty(), "unassigned snapshot remains empty");
            reader.addSplits(List.of(ClazarSplit.first(config)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.values.size() < 32 && System.nanoTime() < deadline) {
                reader.pollNext(before);
                Thread.sleep(5);
            }
            require(
                    before.values.size() == 32,
                    "one collector-lock chunk must contain exactly32 rows");
            saved = roundTrip(reader.snapshotState(1L).get(0));
            require(
                    saved.published == 32 && saved.fingerprint != null && saved.resourceIndex == 0,
                    "partial immutable response position must survive");
        }
        String bytes = new String(serialize(saved), StandardCharsets.ISO_8859_1);
        require(
                !bytes.contains("synthetic-clazar-secret")
                        && !bytes.contains("demo-clazar-bearer")
                        && !bytes.contains("marketplace"),
                "state omits authentication and raw response");
        Rows after = new Rows();
        Context done = new Context();
        boolean rejected = false;
        try (ClazarReader reader = opened(config, done)) {
            reader.addSplits(List.of(saved));
            await(reader, after, done);
            require(
                    args[1].equals("resume") && after.values.size() == 1,
                    "only the selected suffix remains");
            SeaTunnelRow row = after.values.get(0);
            require(
                    "40000000-0000-4000-8000-000000000033".equals(row.getField(1))
                            && "2024-02-29T00:00:00Z".equals(row.getField(2)),
                    "exact suffix identity and time survive");
            terminal(config, reader.snapshotState(2L).get(0));
        } catch (ClazarReadException changed) {
            require(
                    args[1].startsWith("changed")
                            && changed.getMessage().contains("changed since checkpoint"),
                    "changed raw page fails before publication");
            require(after.values.isEmpty(), "no suffix from a changed response");
            rejected = true;
        }
        require(rejected == args[1].startsWith("changed"), "requested replay disposition occurs");
    }

    private static void zero(ClazarConfig config, Map<String, Object> supplied) throws Exception {
        Context idle = new Context();
        try (ClazarReader reader = opened(config, idle)) {
            reader.handleNoMoreSplits();
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(
                    idle.done && rows.values.isEmpty() && reader.snapshotState(0L).isEmpty(),
                    "unassigned terminal signal performs no request");
        }
        try (ClazarReader reader = new ClazarReader(new Context(), config, "records")) {
            reader.cancel();
            reader.open();
            reader.addSplits(List.of(ClazarSplit.first(config)));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(rows.values.isEmpty(), "cancel before assignment is sticky");
        }
        Map<String, Object> changedValues = new HashMap<>();
        changedValues.put("client_secret", "different-secret");
        changedValues.put("client_id", "different-client");
        changedValues.put("page_size", 99);
        changedValues.put("resources", List.of("listings"));
        changedValues.put("start_date", "2024-02-29T00:00:00.000000001Z");
        for (Map.Entry<String, Object> changed : changedValues.entrySet()) {
            Map<String, Object> options = new HashMap<>(supplied);
            options.put(changed.getKey(), changed.getValue());
            rejected(new ClazarConfig(ReadonlyConfig.fromMap(options)), ClazarSplit.first(config));
        }
        rejected(config, ClazarSplit.first(config).partial(null, 1));
        Map<String, Object> moving = new HashMap<>(supplied);
        moving.remove("end_date");
        ClazarConfig openEnd = new ClazarConfig(ReadonlyConfig.fromMap(moving));
        ClazarSplit frozen = ClazarSplit.first(openEnd);
        ClazarSplit restored = roundTrip(frozen);
        restored.validate(openEnd);
        require(
                frozen.cutoff != null
                        && frozen.cutoff.equals(restored.cutoff)
                        && frozen.cutoff.isAfter(Instant.parse("2024-02-29T00:00:00Z")),
                "start-only cutoff is frozen through serialization");
    }

    private static void rejected(ClazarConfig config, ClazarSplit split) throws Exception {
        try (ClazarReader reader = opened(config, new Context())) {
            reader.addSplits(List.of(roundTrip(split)));
        } catch (IllegalArgumentException invalid) {
            require(
                    invalid.getMessage().contains("checkpoint"),
                    "scope error identifies checkpoint");
            return;
        }
        throw new AssertionError("incompatible checkpoint accepted");
    }

    private static void terminal(ClazarConfig config, ClazarSplit split) throws Exception {
        Context context = new Context();
        Rows rows = new Rows();
        try (ClazarReader reader = opened(config, context)) {
            reader.addSplits(List.of(roundTrip(split)));
            reader.pollNext(rows);
            require(
                    context.done && rows.values.isEmpty(),
                    "terminal state restores without another provider request");
        }
    }

    private static ClazarReader opened(ClazarConfig config, Context context) throws Exception {
        ClazarReader reader = new ClazarReader(context, config, "records");
        reader.open();
        return reader;
    }

    private static void await(ClazarReader reader, Rows rows, Context context) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!context.done && System.nanoTime() < deadline) {
            reader.pollNext(rows);
            Thread.sleep(5);
        }
        require(context.done, "bounded reader completes");
    }

    private static ClazarSplit roundTrip(ClazarSplit split) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(serialize(split)))) {
            return (ClazarSplit) input.readObject();
        }
    }

    private static byte[] serialize(ClazarSplit split) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        return bytes.toByteArray();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * Collects actual rows only while their supplied checkpoint lock is held.
     *
     * <p>The probe neither reimplements pagination nor alters the reader's checkpoint state.
     */
    private static final class Rows implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<SeaTunnelRow> values = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            require(Thread.holdsLock(lock), "rows must be atomic with checkpoint collection");
            values.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }

    /**
     * Records EOF callbacks from the actual packaged reader.
     *
     * <p>No metrics, source events or assignments are simulated by the completion flag.
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

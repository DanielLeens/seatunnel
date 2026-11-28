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
package org.apache.seatunnel.connectors.seatunnel.carequalitycommission;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

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
 * Exercises exact nested CQC response recovery from the packaged connector.
 *
 * <p>Actual provider calls use the same independent HTTPS peer as the Zeta suite.
 */
public final class CareQualityCommissionRecoveryProbe {
    private static final ObjectMapper JSON = new ObjectMapper();

    private CareQualityCommissionRecoveryProbe() {}

    /**
     * Captures an emitted row, serializes the checkpoint and verifies its precise replay position.
     *
     * <p>The zero mode checks unassigned, cancelled, invalid and terminal positions.
     */
    public static void main(String[] args) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("token", "synthetic-cqc-subscription-key+/=");
        options.put("resources", List.of("provider_locations"));
        options.put("tls_trust_certificate", Files.readString(Path.of(args[0])));
        options.put("max_retries", 0);
        options.put("page_size", 2);
        options.put("request_interval_ms", 0);
        CareQualityCommissionConfig config =
                new CareQualityCommissionConfig(ReadonlyConfig.fromMap(options));
        if ("zero".equals(args[1])) {
            zero(config, options);
            return;
        }
        Rows before = new Rows();
        CareQualityCommissionSplit saved;
        try (CareQualityCommissionReader reader = opened(config, new Context())) {
            require(reader.snapshotState(0L).isEmpty(), "unassigned state must be empty");
            reader.addSplits(List.of(config.newSplit()));
            long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.values.size() < 32 && System.nanoTime() < expires) {
                reader.pollNext(before);
                Thread.sleep(5);
            }
            require(before.values.size() == 32, "32 rows must precede the serialized checkpoint");
            require(
                    "L0".equals(before.values.get(0).getField(1)),
                    "first association must retain native identity");
            reader.pollNext(before);
            require(
                    before.values.size() == 32,
                    "row advancement must not publish the final suffix");
            saved = reader.snapshotState(1L).get(0);
        }
        require(
                saved.frames.size() == 2
                        && saved.frames.get(0).digest != null
                        && saved.frames.get(1).row == 32
                        && saved.frames.get(1).digest != null
                        && saved.frames.get(1).page == 1,
                "checkpoint retains the exact provider and association row");
        byte[] bytes = serialize(saved);
        String raw = new String(bytes, StandardCharsets.ISO_8859_1);
        require(
                !raw.contains("synthetic-cqc-") && !raw.contains("synthetic-provider-record"),
                "checkpoint must exclude credentials and provider records");
        CareQualityCommissionSplit restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            restored = (CareQualityCommissionSplit) in.readObject();
        }
        Rows after = new Rows();
        Context context = new Context();
        boolean failed = false;
        try (CareQualityCommissionReader reader = opened(config, context)) {
            reader.addSplits(List.of(restored));
            long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!context.done && System.nanoTime() < expires) {
                reader.pollNext(after);
                Thread.sleep(5);
            }
            require("resume".equals(args[1]), "a modified page must fail");
            require(
                    context.done && after.values.size() == 1,
                    "only one row remains after restoration");
            SeaTunnelRow row = after.values.get(0);
            require(
                    "L32".equals(row.getField(1))
                            && "RX1".equals(row.getField(2))
                            && "L32".equals(row.getField(3))
                            && "synthetic-provider-record"
                                    .equals(
                                            JSON.readTree((String) row.getField(4))
                                                    .path("name")
                                                    .asText()),
                    "the exact final association must survive");
            terminal(config, reader.snapshotState(2L).get(0));
        } catch (CareQualityCommissionResource.Failure ex) {
            require(
                    args[1].startsWith("changed")
                            && ex.getMessage().contains("changed before replay"),
                    "changed page must fail for its actual saved digest");
            require(after.values.isEmpty(), "rejected replay publishes no suffix");
            failed = true;
        }
        require(
                failed == args[1].startsWith("changed"),
                "probe must observe the expected replay outcome");
    }

    private static void zero(CareQualityCommissionConfig config, Map<String, Object> options)
            throws Exception {
        CareQualityCommissionSplit finished = config.newSplit();
        finished.frames.clear();
        finished.rootIndex = config.roots.size();
        terminal(config, finished);
        Context idle = new Context();
        try (CareQualityCommissionReader reader = opened(config, idle)) {
            reader.handleNoMoreSplits();
            reader.pollNext(new Rows());
            require(
                    idle.done && reader.snapshotState(0L).isEmpty(),
                    "unassigned reader must finish");
        }
        try (CareQualityCommissionReader reader =
                new CareQualityCommissionReader(config, new Context())) {
            reader.cancel();
            reader.open();
            reader.addSplits(List.of(config.newSplit()));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(rows.values.isEmpty(), "cancellation before open is sticky");
        }
        Map<String, Object> changed = new HashMap<>(options);
        changed.put("token", "synthetic-carequalitycommission-rotated-key");
        rejected(
                new CareQualityCommissionConfig(ReadonlyConfig.fromMap(changed)),
                config.newSplit());
        changed = new HashMap<>(options);
        changed.put("resources", List.of("providers"));
        rejected(
                new CareQualityCommissionConfig(ReadonlyConfig.fromMap(changed)),
                config.newSplit());
        changed = new HashMap<>(options);
        changed.put("page_size", 1);
        rejected(
                new CareQualityCommissionConfig(ReadonlyConfig.fromMap(changed)),
                config.newSplit());
        CareQualityCommissionSplit invalid = config.newSplit();
        invalid.frames.get(0).row = -1;
        rejected(config, invalid);
        CareQualityCommissionSource source =
                new CareQualityCommissionSource(ReadonlyConfig.fromMap(options));
        List<CareQualityCommissionSplit> assignments = new ArrayList<>();
        Set<Integer> registered = new HashSet<>();
        SourceSplitEnumerator.Context<CareQualityCommissionSplit> coordinator =
                enumeratorContext(registered, assignments);
        CareQualityCommissionSplit pending;
        try (SourceSplitEnumerator<CareQualityCommissionSplit, CareQualityCommissionSplit> first =
                source.createEnumerator(coordinator)) {
            first.open();
            first.run();
            require(
                    first.currentUnassignedSplitSize() == 1 && assignments.isEmpty(),
                    "unassigned scan must survive coordinator snapshot");
            try (ObjectInputStream input =
                    new ObjectInputStream(
                            new ByteArrayInputStream(serialize(first.snapshotState(0L))))) {
                pending = (CareQualityCommissionSplit) input.readObject();
            }
        }
        registered.add(0);
        try (SourceSplitEnumerator<CareQualityCommissionSplit, CareQualityCommissionSplit>
                restored = source.restoreEnumerator(coordinator, pending)) {
            restored.open();
            restored.run();
            restored.registerReader(0);
            require(
                    assignments.size() == 1 && restored.currentUnassignedSplitSize() == 0,
                    "restored coordinator may assign only once");
        }
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<CareQualityCommissionSplit> enumeratorContext(
            Set<Integer> readers, List<CareQualityCommissionSplit> assignments) {
        return (SourceSplitEnumerator.Context<CareQualityCommissionSplit>)
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
                                    assignments.add((CareQualityCommissionSplit) args[1]);
                                    return null;
                                case "signalNoMoreSplits":
                                    return null;
                                default:
                                    throw new AssertionError(
                                            "Unexpected coordinator operation " + method.getName());
                            }
                        });
    }

    private static void terminal(
            CareQualityCommissionConfig config, CareQualityCommissionSplit split) throws Exception {
        Context done = new Context();
        try (CareQualityCommissionReader reader = opened(config, done);
                ObjectInputStream in =
                        new ObjectInputStream(new ByteArrayInputStream(serialize(split)))) {
            reader.addSplits(List.of((CareQualityCommissionSplit) in.readObject()));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(done.done && rows.values.isEmpty(), "terminal saved state needs no request");
        }
    }

    private static void rejected(
            CareQualityCommissionConfig config, CareQualityCommissionSplit split) {
        try (CareQualityCommissionReader reader = opened(config, new Context())) {
            reader.addSplits(List.of(split));
        } catch (CareQualityCommissionResource.Failure ex) {
            require(
                    ex.getMessage().contains("checkpoint"),
                    "checkpoint error must explain its scope");
            return;
        }
        throw new AssertionError("invalid position was accepted");
    }

    private static CareQualityCommissionReader opened(
            CareQualityCommissionConfig config, Context context) {
        CareQualityCommissionReader reader = new CareQualityCommissionReader(config, context);
        reader.open();
        return reader;
    }

    private static byte[] serialize(CareQualityCommissionSplit split) throws Exception {
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

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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.cleanup.PipelineCleanupRecord;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.operation.ReportMetricsOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.map.IMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

class CoordinatorServicePipelineCleanupTest extends AbstractSeaTunnelServerTest {

    @Test
    void testCleanupRemovesMetricsAndRecordWhenNoTaskGroups() {
        CoordinatorService coordinatorService = awaitActiveCoordinatorService();

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        PipelineLocation otherPipelineLocation = new PipelineLocation(jobId + 1, 1);

        upsertMetricsForPipeline(pipelineLocation);
        upsertMetricsForPipeline(otherPipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.FINISHED);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                pipelineLocation,
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.FINISHED,
                        false,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        Assertions.assertFalse(hasMetricsForPipeline(pipelineLocation));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipelineLocation));
        Assertions.assertFalse(pendingCleanupIMap.containsKey(pipelineLocation));
    }

    @Test
    void testCleanupRemovesMetricsAndRecordForFailedPipeline() {
        CoordinatorService coordinatorService = awaitActiveCoordinatorService();

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        PipelineLocation otherPipelineLocation = new PipelineLocation(jobId + 1, 1);

        upsertMetricsForPipeline(pipelineLocation);
        upsertMetricsForPipeline(otherPipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.FAILED);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                pipelineLocation,
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.FAILED,
                        false,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        Assertions.assertFalse(hasMetricsForPipeline(pipelineLocation));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipelineLocation));
        Assertions.assertFalse(pendingCleanupIMap.containsKey(pipelineLocation));
    }

    @Test
    void testSkipCleanupWhenPipelineNotEndState() {
        CoordinatorService coordinatorService = awaitActiveCoordinatorService();

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);

        upsertMetricsForPipeline(pipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.RUNNING);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        PipelineCleanupRecord record =
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.FINISHED,
                        false,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0);
        pendingCleanupIMap.put(pipelineLocation, record);

        coordinatorService.runPendingPipelineCleanupOnce();

        PipelineCleanupRecord after = pendingCleanupIMap.get(pipelineLocation);
        Assertions.assertNotNull(after);
        Assertions.assertEquals(0, after.getAttemptCount());
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));
    }

    @Test
    void testRemoveRecordWhenShouldCleanupIsFalse() {
        CoordinatorService coordinatorService = awaitActiveCoordinatorService();

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        upsertMetricsForPipeline(pipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.FINISHED);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                pipelineLocation,
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.FINISHED,
                        true,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        Assertions.assertFalse(pendingCleanupIMap.containsKey(pipelineLocation));
        Assertions.assertTrue(
                hasMetricsForPipeline(pipelineLocation),
                "Should not clean metrics when record is removed due to shouldCleanup=false");
    }

    /**
     * Verifies that savepoint-start invalidates stale failed cleanup records from the previous
     * round.
     */
    @Test
    void testRestoreStartRemovesStaleFailedCleanupRecordForSameJob() {
        CoordinatorService coordinatorService = awaitActiveCoordinatorService();

        long jobId = System.currentTimeMillis();
        PipelineLocation restoredPipelineLocation = new PipelineLocation(jobId, 1);
        PipelineLocation otherPipelineLocation = new PipelineLocation(jobId + 1, 1);

        upsertMetricsForPipeline(restoredPipelineLocation);
        upsertMetricsForPipeline(otherPipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(restoredPipelineLocation));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(restoredPipelineLocation, PipelineStatus.FINISHED);
        runningJobStateIMap.put(otherPipelineLocation, PipelineStatus.FAILED);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                restoredPipelineLocation,
                newPipelineCleanupRecord(restoredPipelineLocation, PipelineStatus.FAILED, false));
        pendingCleanupIMap.put(
                otherPipelineLocation,
                newPipelineCleanupRecord(otherPipelineLocation, PipelineStatus.FAILED, false));

        coordinatorService.cleanupPendingPipelineCleanupForRestore(jobId);

        Assertions.assertFalse(pendingCleanupIMap.containsKey(restoredPipelineLocation));
        Assertions.assertTrue(pendingCleanupIMap.containsKey(otherPipelineLocation));

        coordinatorService.runPendingPipelineCleanupOnce();

        Assertions.assertTrue(
                hasMetricsForPipeline(restoredPipelineLocation),
                "Stale failed cleanup record must not delete savepoint-end metrics");
        Assertions.assertFalse(hasMetricsForPipeline(otherPipelineLocation));
        Assertions.assertFalse(pendingCleanupIMap.containsKey(otherPipelineLocation));
    }

    @Test
    void testRestoreInvalidationPreventsCapturedCleanupFromDeletingNewRoundMetrics()
            throws Exception {
        CoordinatorService coordinatorService = awaitActiveCoordinatorService();

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        PipelineCleanupRecord staleRecord =
                newPipelineCleanupRecord(pipelineLocation, PipelineStatus.FAILED, false);

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.FAILED);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(pipelineLocation, staleRecord);
        upsertMetricsForPipeline(pipelineLocation);

        CountDownLatch cleanupStarted = new CountDownLatch(1);
        pendingCleanupIMap.lock(pipelineLocation);
        CompletableFuture<Void> capturedCleanup;
        try {
            capturedCleanup =
                    CompletableFuture.runAsync(
                            () -> {
                                cleanupStarted.countDown();
                                coordinatorService.processPendingPipelineCleanup(
                                        pipelineLocation, staleRecord);
                            });
            Assertions.assertTrue(cleanupStarted.await(10, TimeUnit.SECONDS));
            Assertions.assertFalse(capturedCleanup.isDone());

            coordinatorService.cleanupPendingPipelineCleanupForRestore(jobId);
            Assertions.assertFalse(pendingCleanupIMap.containsKey(pipelineLocation));

            // Simulate metrics published by the restored execution before the captured cleanup
            // gets CPU time again.
            upsertMetricsForPipeline(pipelineLocation);
            runningJobStateIMap.put(pipelineLocation, PipelineStatus.FINISHED);
        } finally {
            pendingCleanupIMap.unlock(pipelineLocation);
        }

        capturedCleanup.get(10, TimeUnit.SECONDS);
        Assertions.assertTrue(
                hasMetricsForPipeline(pipelineLocation),
                "Captured cleanup from the old round must not delete restored-round metrics");
    }

    @Test
    void testCleanupUpdatesRecordAndKeepsItWhenTaskGroupCannotBeCleaned() {
        CoordinatorService coordinatorService = awaitActiveCoordinatorService();

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        upsertMetricsForPipeline(pipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.CANCELED);

        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        Map<TaskGroupLocation, Address> taskGroups = new HashMap<>();
        taskGroups.put(taskGroupLocation, null);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                pipelineLocation,
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.CANCELED,
                        false,
                        taskGroups,
                        new HashSet<>(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        PipelineCleanupRecord updated = pendingCleanupIMap.get(pipelineLocation);
        Assertions.assertNotNull(updated);
        Assertions.assertEquals(1, updated.getAttemptCount());
        Assertions.assertTrue(updated.isMetricsImapCleaned());
        Assertions.assertFalse(updated.isCleaned());
        Assertions.assertFalse(updated.getCleanedTaskGroups().contains(taskGroupLocation));
        Assertions.assertFalse(hasMetricsForPipeline(pipelineLocation));
    }

    @Test
    void testCleanupRemovesRecordWhenAllTaskGroupsCleaned() {
        CoordinatorService coordinatorService = awaitActiveCoordinatorService();

        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        upsertMetricsForPipeline(pipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateIMap.put(pipelineLocation, PipelineStatus.CANCELED);

        Address localAddress = instance.getCluster().getLocalMember().getAddress();
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        Map<TaskGroupLocation, Address> taskGroups = new HashMap<>();
        taskGroups.put(taskGroupLocation, localAddress);

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        pendingCleanupIMap.put(
                pipelineLocation,
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.CANCELED,
                        false,
                        taskGroups,
                        new HashSet<>(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0));

        coordinatorService.runPendingPipelineCleanupOnce();

        Assertions.assertFalse(hasMetricsForPipeline(pipelineLocation));
        Assertions.assertFalse(pendingCleanupIMap.containsKey(pipelineLocation));
    }

    @Test
    void testFailedPipelinePurgesMetricsAgainAfterWorkerCleanup() {
        CoordinatorService coordinatorService = awaitActiveCoordinatorService();
        long jobId = instance.getFlakeIdGenerator("test").newId();
        PipelineLocation pipeline = new PipelineLocation(jobId, 1);
        PipelineLocation otherPipeline = new PipelineLocation(jobId, 2);
        TaskGroupLocation taskGroup = new TaskGroupLocation(jobId, 1, 1L);
        Address localWorker = instance.getCluster().getLocalMember().getAddress();
        IMap<PipelineLocation, PipelineCleanupRecord> pending =
                instance.getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);

        PipelineCleanupRecord record =
                newPipelineCleanupRecord(pipeline, PipelineStatus.FAILED, false);
        record.setTaskGroups(Collections.singletonMap(taskGroup, localWorker));
        // An earlier cleanup pass purged the store while this worker was still unacknowledged.
        record.setMetricsImapCleaned(true);
        pending.put(pipeline, record);
        upsertMetricsForPipeline(pipeline);
        upsertMetricsForPipeline(otherPipeline);

        coordinatorService.processPendingPipelineCleanup(pipeline, record);

        Assertions.assertFalse(pending.containsKey(pipeline));
        Assertions.assertFalse(hasMetricsForPipeline(pipeline));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipeline));
    }

    @Test
    void testLateWorkerReportCannotRestoreFailedPipelineMetrics() throws Exception {
        long jobId = instance.getFlakeIdGenerator("test").newId();
        PipelineLocation failed = new PipelineLocation(jobId, 1);
        PipelineLocation running = new PipelineLocation(jobId, 2);
        IMap<Object, Object> runningStates = instance.getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningStates.put(failed, PipelineStatus.FAILED);
        runningStates.put(running, PipelineStatus.RUNNING);

        try {
            Map<TaskLocation, SeaTunnelMetricsContext> snapshot = new HashMap<>();
            snapshot.put(
                    new TaskLocation(new TaskGroupLocation(jobId, 1, 1L), 0, 0),
                    new SeaTunnelMetricsContext());
            snapshot.put(
                    new TaskLocation(new TaskGroupLocation(jobId, 2, 1L), 0, 0),
                    new SeaTunnelMetricsContext());
            Address localWorker = instance.getCluster().getLocalMember().getAddress();

            NodeEngineUtil.sendOperationToMemberNode(
                            nodeEngine, new ReportMetricsOperation(snapshot), localWorker)
                    .get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(hasMetricsForPipeline(failed));
            Assertions.assertTrue(hasMetricsForPipeline(running));

            // A late report can also arrive after runtime state has been cleaned.
            runningStates.remove(failed);
            NodeEngineUtil.sendOperationToMemberNode(
                            nodeEngine, new ReportMetricsOperation(snapshot), localWorker)
                    .get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(hasMetricsForPipeline(failed));
            Assertions.assertTrue(hasMetricsForPipeline(running));
        } finally {
            runningStates.remove(failed);
            runningStates.remove(running);
            server.removeMetrics(running);
        }
    }

    private void upsertMetricsForPipeline(PipelineLocation pipelineLocation) {
        TaskGroupLocation taskGroupLocation =
                new TaskGroupLocation(
                        pipelineLocation.getJobId(), pipelineLocation.getPipelineId(), 1L);
        TaskLocation taskLocation = new TaskLocation(taskGroupLocation, 0, 0);

        Map<TaskLocation, SeaTunnelMetricsContext> local = new HashMap<>();
        local.put(taskLocation, new SeaTunnelMetricsContext());
        server.updateMetrics(local);
    }

    /**
     * Creates a focused pending cleanup record without task-group cleanup requirements.
     *
     * <p>The cleanup tests use this helper to keep assertions focused on metric cleanup behavior.
     */
    private PipelineCleanupRecord newPipelineCleanupRecord(
            PipelineLocation pipelineLocation,
            PipelineStatus pipelineStatus,
            boolean savepointEnd) {
        return new PipelineCleanupRecord(
                pipelineLocation,
                pipelineStatus,
                savepointEnd,
                Collections.emptyMap(),
                Collections.emptySet(),
                false,
                System.currentTimeMillis(),
                0L,
                0);
    }

    private boolean hasMetricsForPipeline(PipelineLocation pipelineLocation) {
        MetricsSnapshotStateStore metricsStore =
                server.getEngineContext().getStateStores().metricsSnapshotStore();
        return metricsStore.containsPipeline(pipelineLocation);
    }

    /**
     * Waits until the test server exposes an active coordinator service.
     *
     * <p>{@link SeaTunnelServer#getCoordinatorService()} throws before active-master initialization
     * finishes, so tests must wait around the lookup itself instead of checking activity after the
     * lookup succeeds.
     */
    private CoordinatorService awaitActiveCoordinatorService() {
        AtomicReference<CoordinatorService> coordinatorServiceRef = new AtomicReference<>();
        await().ignoreExceptions()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            CoordinatorService coordinatorService = server.getCoordinatorService();
                            Assertions.assertTrue(coordinatorService.isCoordinatorActive());
                            coordinatorServiceRef.set(coordinatorService);
                        });
        return coordinatorServiceRef.get();
    }
}

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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.chargedesk.ChargeDeskApi.Page;
import org.apache.seatunnel.connectors.seatunnel.chargedesk.ChargeDeskApi.Row;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Scans each selected ChargeDesk list in offset order.
 *
 * <p>Network work stays off the poll thread. Accepted pages and emitted rows advance under the
 * collector checkpoint lock.
 */
final class ChargeDeskReader implements SourceReader<SeaTunnelRow, ChargeDeskSplit> {
    private final ChargeDeskConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private Page page;

    private volatile boolean closed;
    private boolean noMoreSplits;
    private boolean signaled;
    private ChargeDeskSplit progress;

    // One worker owns the socket and never modifies the traversal stack.
    private ChargeDeskHttp http;
    private ChargeDeskApi api;
    private ExecutorService executor;
    private Future<Page> pending;
    private long expires;

    ChargeDeskReader(ChargeDeskConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Prepares the HTTPS worker without contacting ChargeDesk.
     *
     * <p>Only the active subtask owns transport state.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new ChargeDeskHttp(config);
                api = new ChargeDeskApi(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "chargedesk-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Loads a page, emits one row or advances the active collection position.
     *
     * <p>A restored reader checks the raw saved page before resuming its row index.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || signaled) {
                return;
            }
            if (progress == null ? noMoreSplits : progress.finished()) {
                signaled = true;
                context.signalNoMoreElement();
                return;
            }
            if (progress == null) {
                return;
            }
        }
        ChargeDeskResource resource = config.resources.get(progress.resourceIndex);
        if (page == null) {
            fetch(output, resource);
            return;
        }
        if (progress.row == page.rows.size()) {
            commit(output, () -> advancePage(resource));
            return;
        }
        Row row = page.rows.get(progress.row);
        commit(
                output,
                () -> {
                    if (!resource.dated()
                            || !progress.hasWindow(config)
                            || config.inWindow(row.seconds, progress.windowEnd)) {
                        emit(output, resource, row);
                    }
                    ++progress.row;
                });
    }

    private void fetch(Collector<SeaTunnelRow> output, ChargeDeskResource resource)
            throws Exception {
        if (progress.digest == null && progress.acceptedPages == config.scanPages) {
            throw resource.failure("scan exceeds max_pages_per_scan");
        }
        Page reply = awaitPage(resource);
        if (reply == null) {
            return;
        }
        if (progress.digest != null
                && (!progress.digest.equals(reply.digest) || progress.row > reply.rows.size())) {
            throw resource.failure("checkpoint page changed before replay");
        }
        commit(
                output,
                () -> {
                    if (progress.digest == null) {
                        ++progress.acceptedPages;
                    }
                    progress.digest = reply.digest;
                    page = reply;
                });
    }

    private void advancePage(ChargeDeskResource resource) {
        if (page.next >= 0) {
            if (progress.page == config.maxPages) {
                throw resource.failure("collection exceeds max_pages_per_collection");
            }
            progress.nextPage(page.next, page.history);
        } else {
            progress.nextResource(config);
        }
        page = null;
    }

    private static void emit(Collector<SeaTunnelRow> output, ChargeDeskResource resource, Row row) {
        String time = row.seconds == null ? null : Instant.ofEpochSecond(row.seconds).toString();
        SeaTunnelRow emitted =
                new SeaTunnelRow(new Object[] {resource.name, row.id, time, row.record});
        emitted.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(emitted);
    }

    private Page awaitPage(ChargeDeskResource resource) throws Exception {
        Future<Page> task;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                expires = http.deadline();
                long deadline = expires;
                pending = executor.submit(() -> api.list(resource, progress, deadline));
            }
            task = pending;
        }
        try {
            Page reply = task.get(5, TimeUnit.MILLISECONDS);
            synchronized (lifecycle) {
                pending = null;
                return closed ? null : reply;
            }
        } catch (TimeoutException ex) {
            if (!closed && expires - System.nanoTime() <= 0) {
                synchronized (lifecycle) {
                    abortWorker();
                }
                throw resource.failure("HTTPS operation deadline exceeded");
            }
            return null;
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.failure("provider read was canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof ChargeDeskResource.Failure) {
                throw (ChargeDeskResource.Failure) ex.getCause();
            }
            throw resource.failure("provider read failed");
        }
    }

    private void commit(Collector<SeaTunnelRow> output, Runnable action) {
        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (!closed) {
                    action.run();
                }
            }
        }
    }

    /**
     * Captures the active traversal without saving response bytes or the token.
     *
     * <p>A restore verifies the pinned page before continuing.
     */
    @Override
    public List<ChargeDeskSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return progress == null ? List.of() : List.of(progress.copy());
        }
    }

    /**
     * Validates the recovered scope before accepting a split.
     *
     * <p>Only subtask zero receives the bounded scan split.
     */
    @Override
    public void addSplits(List<ChargeDeskSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new ChargeDeskResource.Failure("checkpoint", "invalid split assignment");
            }
            ChargeDeskSplit split = splits.get(0);
            split.validate(config);
            progress = split.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (checkpoint) {
            noMoreSplits = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Cancels the network worker and closes the active socket outside the checkpoint lock.
     *
     * <p>Any in-flight reply becomes ineligible for collection.
     */
    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
            abortWorker();
        }
    }

    private void abortWorker() {
        if (http != null) {
            http.close();
        }
        if (pending != null) {
            pending.cancel(true);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public void close() {
        cancel();
    }
}

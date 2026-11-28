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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cin7.Cin7Api.Page;
import org.apache.seatunnel.connectors.seatunnel.cin7.Cin7Api.Row;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Scans Cin7 Core collection pages and collects the selected purchase and sale details.
 *
 * <p>Network work stays off the poll thread. Accepted pages and emitted rows advance under the
 * collector checkpoint lock.
 */
final class Cin7Reader implements SourceReader<SeaTunnelRow, Cin7Split> {
    private final Cin7Config config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private Page page;

    private volatile boolean closed;
    private boolean noMoreSplits;
    private boolean signaled;
    private Cin7Split progress;

    // One worker owns the socket and never modifies the traversal position.
    private Cin7Http http;
    private Cin7Api api;
    private ExecutorService executor;
    private Future<Page> pending;
    private long expires;

    Cin7Reader(Cin7Config config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Prepares the HTTPS worker without contacting Cin7 Core.
     *
     * <p>Only the active subtask owns transport state.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new Cin7Http(config);
                api = new Cin7Api(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "cin7-source");
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
        Cin7Resource resource = config.collections.get(progress.resourceIndex);
        if (page == null) {
            fetch(output, resource);
            return;
        }
        if (progress.row == page.rows.size()) {
            commit(output, () -> advancePage(resource));
            return;
        }
        Row parent = page.rows.get(progress.row);
        Cin7Resource detail = config.selectedDetail(resource);
        if (progress.detailPending) {
            readDetail(output, detail, parent.id);
            return;
        }
        commit(
                output,
                () -> {
                    if (config.resources.contains(resource)) {
                        emit(output, resource, parent, null);
                    }
                    if (detail == null) {
                        ++progress.row;
                    } else {
                        progress.detailPending = true;
                    }
                });
    }

    private void fetch(Collector<SeaTunnelRow> output, Cin7Resource resource) throws Exception {
        if (progress.digest == null) {
            checkBudget(resource);
        }
        Page reply = awaitRead(resource, () -> api.list(resource, progress, expires));
        if (reply == null) {
            return;
        }
        if (progress.digest != null
                && (!progress.digest.equals(reply.digest)
                        || progress.row > reply.rows.size()
                        || progress.detailPending && progress.row == reply.rows.size())) {
            throw resource.failure("checkpoint page changed before replay");
        }
        commit(
                output,
                () -> {
                    if (progress.digest == null) {
                        ++progress.acceptedPages;
                    }
                    progress.digest = reply.digest;
                    progress.total = reply.total;
                    page = reply;
                });
    }

    private void readDetail(Collector<SeaTunnelRow> output, Cin7Resource resource, String parent)
            throws Exception {
        checkBudget(resource);
        Page reply = awaitRead(resource, () -> api.detail(resource, parent, expires));
        if (reply == null) {
            return;
        }
        commit(
                output,
                () -> {
                    emit(output, resource, reply.rows.get(0), parent);
                    ++progress.acceptedPages;
                    progress.detailPending = false;
                    ++progress.row;
                });
    }

    private void checkBudget(Cin7Resource resource) {
        if (progress.acceptedPages == config.scanPages) {
            throw resource.failure("scan exceeds max_pages_per_scan");
        }
    }

    private void advancePage(Cin7Resource resource) {
        if (page.last) {
            progress.nextResource();
        } else {
            if (progress.page == config.maxPages) {
                throw resource.failure("collection exceeds max_pages_per_collection");
            }
            progress.nextPage(page.pageIdentity);
        }
        page = null;
    }

    private static void emit(
            Collector<SeaTunnelRow> output, Cin7Resource resource, Row row, String parent) {
        SeaTunnelRow emitted =
                new SeaTunnelRow(new Object[] {resource.name, row.id, parent, row.record});
        emitted.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(emitted);
    }

    /**
     * Lets the engine poll and checkpoint while one bounded provider request is in flight.
     *
     * <p>The reader re-enters the same stage until its request has returned or been canceled.
     */
    private Page awaitRead(Cin7Resource resource, Callable<Page> request) throws Exception {
        Future<Page> task;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                expires = http.deadline();
                pending = executor.submit(request);
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
            if (ex.getCause() instanceof Cin7Resource.Failure) {
                throw (Cin7Resource.Failure) ex.getCause();
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
     * Captures the active traversal without saving response bytes or credentials.
     *
     * <p>A restore verifies the pinned page before continuing.
     */
    @Override
    public List<Cin7Split> snapshotState(long checkpointId) {
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
    public void addSplits(List<Cin7Split> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new Cin7Resource.Failure("checkpoint", "invalid split assignment");
            }
            Cin7Split split = splits.get(0);
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

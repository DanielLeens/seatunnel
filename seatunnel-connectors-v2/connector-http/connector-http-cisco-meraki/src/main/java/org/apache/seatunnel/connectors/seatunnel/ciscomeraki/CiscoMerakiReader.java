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
package org.apache.seatunnel.connectors.seatunnel.ciscomeraki;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiApi.Page;
import org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiApi.Row;
import org.apache.seatunnel.connectors.seatunnel.ciscomeraki.CiscoMerakiSplit.Position;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Traverses Cisco Meraki organizations, networks, and their selected child operations.
 *
 * <p>Network work stays off the poll thread. Accepted pages and emitted rows advance under the
 * collector checkpoint lock.
 */
final class CiscoMerakiReader implements SourceReader<SeaTunnelRow, CiscoMerakiSplit> {
    private final CiscoMerakiConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private Page organizations;
    private Page networks;
    private Page children;

    private volatile boolean closed;
    private boolean noMoreSplits;
    private boolean signaled;
    private CiscoMerakiSplit progress;

    // One worker owns the socket and never modifies the traversal position.
    private CiscoMerakiHttp http;
    private CiscoMerakiApi api;
    private ExecutorService executor;
    private Future<Page> pending;
    private long expires;

    CiscoMerakiReader(CiscoMerakiConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Prepares the HTTPS worker without contacting Cisco Meraki.
     *
     * <p>Only the active subtask owns transport state.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new CiscoMerakiHttp(config);
                api = new CiscoMerakiApi(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "ciscomeraki-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Loads an operation page, emits one row or advances the nested scan position.
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
        if (organizations == null) {
            fetch(
                    output,
                    CiscoMerakiResource.ORGANIZATIONS,
                    progress.org,
                    null,
                    null,
                    value -> organizations = value);
            return;
        }
        if (progress.org.row == organizations.rows.size()) {
            if (progress.organizationEmitted) {
                throw CiscoMerakiResource.ORGANIZATIONS.failure(
                        "checkpoint has no active organization");
            }
            commit(
                    output,
                    () -> {
                        advance(
                                CiscoMerakiResource.ORGANIZATIONS,
                                progress.org,
                                organizations,
                                () -> progress.complete = true);
                        organizations = null;
                    });
            return;
        }
        Row organization = organizations.rows.get(progress.org.row);
        if (!progress.organizationEmitted) {
            commit(
                    output,
                    () -> {
                        if (config.resources.contains(CiscoMerakiResource.ORGANIZATIONS)) {
                            emit(
                                    output,
                                    CiscoMerakiResource.ORGANIZATIONS,
                                    organization,
                                    organization.id,
                                    null);
                        }
                        if (config.children.isEmpty()) {
                            ++progress.org.row;
                        } else {
                            progress.organizationEmitted = true;
                        }
                    });
            return;
        }
        if (progress.child == config.children.size()) {
            commit(
                    output,
                    () -> {
                        progress.child = 0;
                        progress.organizationEmitted = false;
                        ++progress.org.row;
                    });
            return;
        }
        CiscoMerakiResource child = config.children.get(progress.child);
        if (child == CiscoMerakiResource.NETWORKS) {
            readNetworks(output, organization.id);
        } else {
            readChild(output, child, organization.id, null, () -> ++progress.child);
        }
    }

    private void readNetworks(Collector<SeaTunnelRow> output, String organization)
            throws Exception {
        CiscoMerakiResource resource = CiscoMerakiResource.NETWORKS;
        if (networks == null) {
            fetch(
                    output,
                    resource,
                    progress.network,
                    organization,
                    null,
                    value -> networks = value);
            return;
        }
        if (progress.network.row == networks.rows.size()) {
            if (progress.networkEmitted) {
                throw resource.failure("checkpoint has no active network");
            }
            commit(
                    output,
                    () -> {
                        advance(resource, progress.network, networks, () -> ++progress.child);
                        networks = null;
                    });
            return;
        }
        Row network = networks.rows.get(progress.network.row);
        if (progress.networkEmitted) {
            readChild(
                    output,
                    CiscoMerakiResource.SETTINGS,
                    organization,
                    network.id,
                    () -> {
                        progress.networkEmitted = false;
                        ++progress.network.row;
                    });
            return;
        }
        commit(
                output,
                () -> {
                    if (config.resources.contains(resource)) {
                        emit(output, resource, network, organization, network.id);
                    }
                    if (config.resources.contains(CiscoMerakiResource.SETTINGS)) {
                        progress.networkEmitted = true;
                    } else {
                        ++progress.network.row;
                    }
                });
    }

    private void readChild(
            Collector<SeaTunnelRow> output,
            CiscoMerakiResource resource,
            String organization,
            String network,
            Runnable finished)
            throws Exception {
        Position position = progress.leaf;
        if (children == null) {
            fetch(output, resource, position, organization, network, value -> children = value);
            return;
        }
        if (position.row == children.rows.size()) {
            commit(
                    output,
                    () -> {
                        advance(resource, position, children, finished);
                        children = null;
                    });
            return;
        }
        Row row = children.rows.get(position.row);
        commit(
                output,
                () -> {
                    emit(output, resource, row, organization, network);
                    ++position.row;
                });
    }

    /**
     * Fetches a bounded provider page and verifies its serialized position before publishing.
     *
     * <p>Revalidating a saved page does not count as a new accepted response.
     */
    private void fetch(
            Collector<SeaTunnelRow> output,
            CiscoMerakiResource resource,
            Position position,
            String organization,
            String network,
            Consumer<Page> cache)
            throws Exception {
        if (position.digest == null && progress.acceptedPages == config.scanPages) {
            throw resource.failure("scan exceeds max_pages_per_scan");
        }
        Page reply =
                awaitRead(
                        resource,
                        () ->
                                api.read(
                                        resource,
                                        organization,
                                        network,
                                        progress,
                                        position,
                                        expires));
        if (reply == null) {
            return;
        }
        if (position.digest != null
                && (!position.digest.equals(reply.digest) || position.row > reply.rows.size())) {
            throw resource.failure("checkpoint page changed before replay");
        }
        commit(
                output,
                () -> {
                    if (position.digest == null) {
                        ++progress.acceptedPages;
                    }
                    position.digest = reply.digest;
                    cache.accept(reply);
                });
    }

    private void advance(
            CiscoMerakiResource resource, Position position, Page page, Runnable finished) {
        if (page.next == null) {
            position.reset();
            finished.run();
            return;
        }
        if (position.page == config.maxPages) {
            throw resource.failure("collection exceeds max_pages_per_collection");
        }
        position.next(page.target, page.next);
    }

    private static void emit(
            Collector<SeaTunnelRow> output,
            CiscoMerakiResource resource,
            Row row,
            String organization,
            String network) {
        SeaTunnelRow emitted =
                new SeaTunnelRow(
                        new Object[] {resource.name, row.id, organization, network, row.record});
        emitted.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(emitted);
    }

    /**
     * Lets the engine poll and checkpoint while one bounded provider request is in flight.
     *
     * <p>The reader re-enters the same stage until its request has returned or been canceled.
     */
    private Page awaitRead(CiscoMerakiResource resource, Callable<Page> request) throws Exception {
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
            if (ex.getCause() instanceof CiscoMerakiResource.Failure) {
                throw (CiscoMerakiResource.Failure) ex.getCause();
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
    public List<CiscoMerakiSplit> snapshotState(long checkpointId) {
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
    public void addSplits(List<CiscoMerakiSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new CiscoMerakiResource.Failure("checkpoint", "invalid split assignment");
            }
            CiscoMerakiSplit split = splits.get(0);
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

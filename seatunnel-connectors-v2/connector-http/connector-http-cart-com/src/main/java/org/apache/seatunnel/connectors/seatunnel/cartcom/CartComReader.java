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

package org.apache.seatunnel.connectors.seatunnel.cartcom;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cartcom.CartComJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.cartcom.CartComJson.Page;
import org.apache.seatunnel.connectors.seatunnel.cartcom.CartComJson.Record;
import org.apache.seatunnel.connectors.seatunnel.cartcom.CartComSource.Split;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Fetches one numbered resource traversal without blocking the engine checkpoint thread.
 *
 * <p>The reader stops a restored partial page if raw records, order or its continuation changed. A
 * complete numbered page advances the cursor even when it contains no selected rows.
 */
final class CartComReader implements SourceReader<SeaTunnelRow, Split> {
    private static final int RAW_ROWS_PER_POLL = 32;
    private static final Pattern DIGEST = Pattern.compile("[0-9a-f]{64}");

    private final CartComConfig config;
    private final Context context;
    private final String table;
    private final CartComHttp http;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-cartcom-retrieval");
                        thread.setDaemon(true);
                        return thread;
                    });

    private boolean assigned;
    private volatile boolean noMoreSplits;
    private boolean finished;
    private boolean eof;
    private int page;
    private int offset;
    private String digest;
    private Instant ceiling;
    private Page current;
    private volatile Future<Page> pending;

    CartComReader(CartComConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        http = new CartComHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Publishes ready rows and their raw position under the engine checkpoint lock.
     *
     * @param collector the engine's row output and checkpoint lock
     * @throws IOException if a provider response or saved page cannot be consumed
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws IOException {
        synchronized (collector.getCheckpointLock()) {
            synchronized (progress) {
                if (closed.get() || eof) {
                    return;
                }
                if (finished || !assigned && noMoreSplits) {
                    eof = true;
                    context.signalNoMoreElement();
                    return;
                }
                if (!assigned) {
                    return;
                }
                if (ceiling == null) {
                    ceiling =
                            config.end == null
                                    ? Instant.now().truncatedTo(ChronoUnit.MICROS)
                                    : config.end;
                }
                if (config.resource.incremental && !config.lower.isBefore(ceiling)) {
                    finish();
                    return;
                }
                try {
                    if (current != null || loadPage()) {
                        emit(collector);
                    }
                } catch (Failure e) {
                    if (!closed.get()) {
                        throw new IOException(
                                "CartCom resource " + config.resource.id + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Fetches a page on the retrieval worker and uses only completed results in the poll loop.
     *
     * <p>The task uses immutable request arguments. Snapshotting can proceed while a TLS request is
     * throttled, canceled, or waiting for a response body.
     */
    private boolean loadPage() throws Failure {
        Future<Page> task = pending;
        if (task == null) {
            if (page > config.maxPages) {
                throw CartComJson.failure("pagination reached max_pages before completion");
            }
            int position = page;
            Instant fixedCeiling = ceiling;
            String target = config.resource.target(config, position, fixedCeiling);
            try {
                pending =
                        worker.submit(
                                () ->
                                        CartComJson.page(
                                                http.fetch(target),
                                                config,
                                                position,
                                                fixedCeiling));
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw CartComJson.failure("retrieval worker is unavailable");
                }
            }
            return false;
        }
        if (!task.isDone()) {
            return false;
        }
        try {
            Page loaded = task.get();
            pending = null;
            if (closed.get()) {
                return false;
            }
            if (digest != null && (offset >= loaded.rows.size() || !digest.equals(loaded.digest))) {
                throw CartComJson.failure("saved Cart.com page changed since its checkpoint");
            }
            current = loaded;
            return true;
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            throw CartComJson.failure("retrieval interrupted");
        } catch (CancellationException ignored) {
            throw CartComJson.failure("retrieval canceled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Failure) {
                throw (Failure) e.getCause();
            }
            throw CartComJson.failure("retrieval worker failed");
        }
    }

    private void emit(Collector<SeaTunnelRow> collector) {
        int limit = Math.min(current.rows.size(), offset + RAW_ROWS_PER_POLL);
        while (offset < limit && !closed.get()) {
            Record value = current.rows.get(offset);
            if (value != null) {
                SeaTunnelRow row =
                        new SeaTunnelRow(new Object[] {value.id, value.timestamp, value.json});
                row.setTableId(table);
                collector.collect(row);
            }
            offset++;
            digest = current.digest;
        }
        if (offset == current.rows.size()) {
            page = current.next;
            offset = 0;
            digest = null;
            current = null;
            if (page == 0) {
                finish();
            }
        }
    }

    private void finish() {
        page = 1;
        finished = true;
    }

    /**
     * Saves the assigned split without fetching remote state.
     *
     * @param checkpointId the engine's checkpoint identifier
     * @return active progress, or no split before assignment
     */
    @Override
    public List<Split> snapshotState(long checkpointId) {
        synchronized (progress) {
            return assigned
                    ? List.of(new Split(config.scope, page, offset, digest, ceiling, finished))
                    : List.of();
        }
    }

    /**
     * Checks scope and reuses the same query bounds on a restored traversal.
     *
     * @param splits one validated source split
     */
    @Override
    public void addSplits(List<Split> splits) {
        synchronized (progress) {
            if (assigned || eof || splits.size() != 1) {
                throw CartComConfig.invalid("expected one assigned CartCom traversal");
            }
            Split saved = splits.get(0);
            validate(config, saved);
            page = saved.page;
            offset = saved.offset;
            digest = saved.digest;
            ceiling = saved.ceiling;
            finished = saved.finished;
            assigned = true;
        }
    }

    /**
     * Rejects incompatible checkpoint state before constructing a signed request.
     *
     * @param config expected resource and query scope
     * @param saved snapshot supplied by the engine
     */
    static void validate(CartComConfig config, Split saved) {
        if (saved == null || !config.scope.equals(saved.scope)) {
            throw CartComConfig.invalid(
                    "merchant, authentication, resource, or read bounds changed during restore");
        }
        if (saved.page < 1
                || saved.page > config.maxPages + 1
                || saved.offset < 0
                || saved.offset >= config.pageSize
                || (saved.offset == 0) != (saved.digest == null)
                || saved.digest != null && !DIGEST.matcher(saved.digest).matches()
                || saved.finished && (saved.page != 1 || saved.offset != 0)
                || saved.ceiling == null && (saved.page != 1 || saved.offset != 0)
                || saved.ceiling != null
                        && config.end != null
                        && !config.end.equals(saved.ceiling)) {
            throw CartComConfig.invalid("invalid saved Cart.com page or offset");
        }
    }

    /**
     * Records coordinator exhaustion for handling on the reader's polling thread.
     *
     * <p>An unassigned restored reader ends without signing or requesting a page.
     */
    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    /**
     * Interrupts a socket read or retry delay without acquiring the checkpoint lock.
     *
     * <p>The cancellation flag stays set when a split arrives after cancellation.
     */
    @Override
    public void cancel() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<Page> task = pending;
            if (task != null) {
                task.cancel(true);
            }
            worker.shutdownNow();
        }
    }

    /**
     * Releases the same local resources as cancellation.
     *
     * <p>The engine may call close after it has already canceled the task.
     */
    @Override
    public void close() {
        cancel();
    }
}

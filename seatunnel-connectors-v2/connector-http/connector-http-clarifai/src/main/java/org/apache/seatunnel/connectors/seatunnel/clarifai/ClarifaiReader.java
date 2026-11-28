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

package org.apache.seatunnel.connectors.seatunnel.clarifai;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.clarifai.ClarifaiJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.clarifai.ClarifaiJson.Page;
import org.apache.seatunnel.connectors.seatunnel.clarifai.ClarifaiJson.Record;
import org.apache.seatunnel.connectors.seatunnel.clarifai.ClarifaiSource.Position;
import org.apache.seatunnel.connectors.seatunnel.clarifai.ClarifaiSource.Split;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Traverses one resource using bounded pages and a checkpointed raw row position.
 *
 * <p>Child pages are read depth first, retaining only the current app and model pages. Validation
 * and HTTPS execute outside the engine checkpoint lock.
 */
final class ClarifaiReader implements SourceReader<SeaTunnelRow, Split> {
    private static final int ROWS_PER_POLL = 16;
    private final ClarifaiConfig config;
    private final Context context;
    private final String table;
    private final ClarifaiHttp http;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    action -> {
                        Thread thread = new Thread(action, "seatunnel-clarifai-retrieval");
                        thread.setDaemon(true);
                        return thread;
                    });

    private volatile Future<LoadedPage> pending;
    private volatile boolean noMoreSplits;
    private boolean assigned;
    private boolean finished;
    private boolean eof;
    private int pages;
    private Instant ceiling;
    private Frame app = new Frame(new Position());
    private Frame model = new Frame(new Position());
    private Frame leaf = new Frame(new Position());

    ClarifaiReader(ClarifaiConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        http = new ClarifaiHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Publishes a bounded chunk and its matching raw offset atomically with respect to checkpoints.
     *
     * <p>A restored terminal split or exhausted coordinator reaches end of input without I/O.
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
                if (assigned) {
                    try {
                        poll(collector);
                    } catch (Failure error) {
                        if (!closed.get()) {
                            throw new IOException(
                                    "Clarifai resource "
                                            + config.resource.id
                                            + ": "
                                            + error.getMessage());
                        }
                    }
                }
            }
        }
    }

    private void poll(Collector<SeaTunnelRow> collector) throws Failure {
        ClarifaiResource resource = config.resource;
        if (config.lower != null && ceiling == null) {
            ceiling = config.end == null ? Instant.now() : config.end;
            if (ceiling.isBefore(config.lower)) {
                finish();
                return;
            }
        }
        String appId = null;
        String modelId = null;
        if (resource.depth > 0) {
            if (!load(app, ClarifaiResource.APPLICATIONS, null, null, true)) {
                return;
            }
            if (app.exhausted()) {
                if (app.loaded.page.next) {
                    app.advance();
                } else {
                    finish();
                }
                return;
            }
            appId = ClarifaiJson.identifier(app.loaded.page.rows.get(app.offset));
        }
        if (resource.depth == 2) {
            if (!load(model, ClarifaiResource.MODELS, appId, null, true)) {
                return;
            }
            if (model.exhausted()) {
                if (model.loaded.page.next) {
                    model.advance();
                } else {
                    app.offset++;
                    model = new Frame(new Position());
                }
                return;
            }
            modelId = ClarifaiJson.identifier(model.loaded.page.rows.get(model.offset));
        }
        if (!load(leaf, resource, appId, modelId, false) || !emit(collector)) {
            return;
        }
        if (leaf.loaded.page.next) {
            leaf.advance();
        } else if (resource.depth > 0) {
            Frame parent = resource.depth == 2 ? model : app;
            parent.offset++;
            leaf = new Frame(new Position());
        } else {
            finish();
        }
    }

    /**
     * Starts one request or adopts a completed page. Refetching a counted checkpoint page does not
     * charge it twice against the job budget.
     */
    private boolean load(
            Frame frame, ClarifaiResource resource, String appId, String modelId, boolean inventory)
            throws Failure {
        if (frame.loaded != null) {
            return true;
        }
        if (pending == null) {
            if (frame.digest == null && pages >= config.maxPages) {
                throw ClarifaiJson.failure("max_pages exhausted before reaching end of input");
            }
            int number = frame.page;
            Instant upper = ceiling;
            try {
                pending =
                        worker.submit(
                                () -> fetch(resource, appId, modelId, inventory, number, upper));
            } catch (RejectedExecutionException ignored) {
                throw ClarifaiJson.failure("retrieval canceled");
            }
        }
        if (!pending.isDone()) {
            return false;
        }
        LoadedPage loaded = result(pending);
        pending = null;
        if (frame.offset > loaded.page.rows.size()
                || frame.digest != null && !frame.digest.equals(loaded.page.digest)) {
            throw ClarifaiJson.failure("checkpointed page contents changed before replay");
        }
        if (!loaded.page.rows.isEmpty() && loaded.page.content.equals(frame.previous)) {
            throw ClarifaiJson.failure("provider repeated the preceding page");
        }
        if (frame.digest == null) {
            frame.digest = loaded.page.digest;
            pages++;
        }
        frame.loaded = loaded;
        return true;
    }

    private LoadedPage fetch(
            ClarifaiResource resource,
            String appId,
            String modelId,
            boolean inventory,
            int number,
            Instant upper)
            throws IOException {
        int limit = limit(config, resource);
        JsonNode response = http.fetch(resource.target(config.user, appId, modelId, number, limit));
        Page page = ClarifaiJson.page(response, resource, limit);
        List<Record> projected = new ArrayList<>(inventory ? 0 : page.rows.size());
        if (!inventory) {
            for (JsonNode row : page.rows) {
                projected.add(
                        ClarifaiJson.project(row, resource, appId, modelId, config.lower, upper));
            }
        }
        return new LoadedPage(page, projected);
    }

    private static int limit(ClarifaiConfig config, ClarifaiResource resource) {
        return config.pageSize > 0 ? config.pageSize : resource.defaultPageSize;
    }

    private static LoadedPage result(Future<LoadedPage> task) throws Failure {
        try {
            return task.get();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            throw ClarifaiJson.failure("retrieval canceled");
        } catch (CancellationException ignored) {
            throw ClarifaiJson.failure("retrieval canceled");
        } catch (ExecutionException error) {
            if (error.getCause() instanceof Failure) {
                throw (Failure) error.getCause();
            }
            throw ClarifaiJson.failure("retrieval worker failed");
        }
    }

    private boolean emit(Collector<SeaTunnelRow> collector) {
        List<Record> rows = leaf.loaded.projected;
        int end = Math.min(rows.size(), leaf.offset + ROWS_PER_POLL);
        while (leaf.offset < end && !closed.get()) {
            Record item = rows.get(leaf.offset);
            if (item != null) {
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {
                                    item.id,
                                    config.user,
                                    item.app,
                                    item.model,
                                    item.cursor,
                                    item.data
                                });
                row.setTableId(table);
                collector.collect(row);
            }
            leaf.offset++;
        }
        return leaf.offset == rows.size();
    }

    private void finish() {
        app = new Frame(new Position());
        model = new Frame(new Position());
        leaf = new Frame(new Position());
        pages = 0;
        ceiling = null;
        finished = true;
    }

    /**
     * Captures counted page digests and raw offsets at every traversal depth.
     *
     * <p>When work is in flight, the checkpoint retains the position before that request.
     */
    @Override
    public List<Split> snapshotState(long checkpointId) {
        synchronized (progress) {
            if (!assigned) {
                return List.of();
            }
            return List.of(
                    new Split(
                            config.scope,
                            ceiling,
                            pages,
                            app.snapshot(),
                            model.snapshot(),
                            leaf.snapshot(),
                            finished));
        }
    }

    /**
     * Replays checkpoint positions only within the same connector scope.
     *
     * <p>Page contents are subsequently checked before any saved offsets can be skipped.
     */
    @Override
    public void addSplits(List<Split> splits) {
        if (splits.isEmpty()) {
            return;
        }
        synchronized (progress) {
            if (assigned || splits.size() != 1) {
                throw new IllegalArgumentException("Clarifai source: expected one traversal split");
            }
            Split split = splits.get(0);
            validate(config, split);
            ceiling = split.ceiling;
            pages = split.pages;
            app = new Frame(split.app);
            model = new Frame(split.model);
            leaf = new Frame(split.leaf);
            finished = split.finished;
            assigned = true;
        }
    }

    static void validate(ClarifaiConfig config, Split split) {
        int depth = config.resource.depth;
        if (split == null
                || !config.scope.equals(split.scope)
                || split.pages < 0
                || split.pages > config.maxPages
                || !valid(split.app, limit(config, ClarifaiResource.APPLICATIONS))
                || !valid(split.model, limit(config, ClarifaiResource.MODELS))
                || !valid(split.leaf, limit(config, config.resource))
                || depth == 0 && !split.app.empty()
                || depth < 2 && !split.model.empty()
                || !split.model.empty() && split.app.digest == null
                || !split.leaf.empty()
                        && (depth == 1 && split.app.digest == null
                                || depth == 2 && split.model.digest == null)
                || counted(split.app) + counted(split.model) + counted(split.leaf) > split.pages
                || split.ceiling != null
                        && (config.lower == null
                                || split.ceiling.isBefore(config.lower)
                                || config.end != null && !config.end.equals(split.ceiling))
                || config.lower != null && split.pages > 0 && split.ceiling == null
                || split.finished
                        && (!split.app.empty()
                                || !split.model.empty()
                                || !split.leaf.empty()
                                || split.pages != 0
                                || split.ceiling != null)) {
            throw new IllegalArgumentException(
                    "Clarifai source: checkpoint scope or page position is invalid");
        }
    }

    private static int counted(Position position) {
        return position.digest == null ? 0 : 1;
    }

    private static boolean valid(Position position, int limit) {
        return position != null
                && position.page >= 1
                && position.page <= 100001
                && position.offset >= 0
                && position.offset <= limit
                && (position.digest == null ? position.offset == 0 : digest(position.digest))
                && (position.page == 1 ? position.previous == null : digest(position.previous));
    }

    private static boolean digest(String value) {
        return value != null && value.matches("[0-9a-f]{64}");
    }

    /**
     * Records coordinator exhaustion for the reader polling thread.
     *
     * <p>The assigned split, when present, continues until it reaches its own terminal position.
     */
    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    /**
     * Stops future transport and cancels an outstanding request independently of the progress lock.
     *
     * <p>Cancellation prevents subsequent page publication.
     */
    @Override
    public void cancel() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<LoadedPage> task = pending;
            if (task != null) {
                task.cancel(true);
            }
            worker.shutdownNow();
        }
    }

    @Override
    public void close() {
        cancel();
    }

    /**
     * Holds a validated response and its aligned row projections.
     *
     * <p>Filtered records remain null slots so checkpoint offsets still address the raw page.
     */
    private static final class LoadedPage {
        final Page page;
        final List<Record> projected;

        LoadedPage(Page page, List<Record> projected) {
            this.page = page;
            this.projected = projected;
        }
    }

    /**
     * Tracks one current collection page and its consumed raw offset.
     *
     * <p>Page data stays in memory only. A finalized snapshot contains digests and indices.
     */
    private static final class Frame {
        int page;
        int offset;
        String digest;
        String previous;
        LoadedPage loaded;

        Frame(Position position) {
            page = position.page;
            offset = position.offset;
            digest = position.digest;
            previous = position.previous;
        }

        boolean exhausted() {
            return offset == loaded.page.rows.size();
        }

        void advance() {
            page++;
            offset = 0;
            digest = null;
            previous = loaded.page.content;
            loaded = null;
        }

        Position snapshot() {
            return new Position(page, offset, digest, previous);
        }
    }
}

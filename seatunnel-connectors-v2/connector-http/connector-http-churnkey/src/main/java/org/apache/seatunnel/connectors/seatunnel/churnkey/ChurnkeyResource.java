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
package org.apache.seatunnel.connectors.seatunnel.churnkey;

/**
 * Declares Churnkey's two read-only data routes and their distinct list behavior.
 *
 * <p>Session offset paging is independent of the complete grouped count response.
 */
enum ChurnkeyResource {
    SESSIONS("sessions", "/v1/data/sessions", true),
    SESSION_AGGREGATION("session-aggregation", "/v1/data/session-aggregation", false);

    final String name;
    final String path;
    final boolean paged;

    ChurnkeyResource(String name, String path, boolean paged) {
        this.name = name;
        this.path = path;
        this.paged = paged;
    }

    int limit(ChurnkeyConfig config) {
        return paged ? config.pageSize : config.maxItems;
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Reports a scoped failure without credentials or original response content.
     *
     * <p>The collection name is safe to present in engine diagnostics.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String resource, String reason) {
            super("Churnkey " + resource + ": " + reason);
        }
    }
}

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
package org.apache.seatunnel.connectors.seatunnel.chargify;

import org.apache.seatunnel.api.source.SourceSplit;

/**
 * Immutable progress for a single regional site scan.
 *
 * <p>The preceding completed page is represented only by an identity digest. In-progress page
 * offsets bind the exact response content before a suffix is emitted again.
 */
final class ChargifySplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final int resourceIndex;
    final int page;
    final int requests;
    final String previousIdentities;
    final String fingerprint;
    final int published;

    ChargifySplit(ChargifyConfig config) {
        this(config.scope, 0, 1, 0, null, null, 0);
    }

    private ChargifySplit(
            String scope,
            int resourceIndex,
            int page,
            int requests,
            String previousIdentities,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.resourceIndex = resourceIndex;
        this.page = page;
        this.requests = requests;
        this.previousIdentities = previousIdentities;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    ChargifySplit partial(String digest, int offset) {
        return new ChargifySplit(
                scope, resourceIndex, page, requests, previousIdentities, digest, offset);
    }

    ChargifySplit advance(String identities) {
        return identities == null
                ? new ChargifySplit(scope, resourceIndex + 1, 1, requests + 1, null, null, 0)
                : new ChargifySplit(
                        scope, resourceIndex, page + 1, requests + 1, identities, null, 0);
    }

    @Override
    public String splitId() {
        return "chargify-site";
    }
}

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
package org.apache.seatunnel.connectors.seatunnel.cimis;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.LocalDate;

/**
 * Immutable cursor through the selected CIMIS resources and calendar slices.
 *
 * <p>Each range owns one target and interval. Width changes only before beginning to emit that
 * range, and partial state binds exact contents and row offset.
 */
final class CimisSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final int resourceIndex;
    final int targetIndex;
    final int modeIndex;
    final LocalDate from;
    final int width;
    final int requests;
    final String fingerprint;
    final int published;

    CimisSplit(CimisConfig config) {
        this(config.scope, 0, 0, 0, config.start, config.sliceDays, 0, null, 0);
    }

    private CimisSplit(
            String scope,
            int resourceIndex,
            int targetIndex,
            int modeIndex,
            LocalDate from,
            int width,
            int requests,
            String fingerprint,
            int published) {
        this.scope = scope;
        this.resourceIndex = resourceIndex;
        this.targetIndex = targetIndex;
        this.modeIndex = modeIndex;
        this.from = from;
        this.width = width;
        this.requests = requests;
        this.fingerprint = fingerprint;
        this.published = published;
    }

    LocalDate through(CimisConfig config) {
        LocalDate candidate = from.plusDays(width - 1L);
        return candidate.isAfter(config.end) ? config.end : candidate;
    }

    CimisSplit partial(String digest, int offset) {
        return new CimisSplit(
                scope,
                resourceIndex,
                targetIndex,
                modeIndex,
                from,
                width,
                requests,
                digest,
                offset);
    }

    CimisSplit subdivide(int narrowerWidth) {
        return new CimisSplit(
                scope,
                resourceIndex,
                targetIndex,
                modeIndex,
                from,
                narrowerWidth,
                requests + 1,
                null,
                0);
    }

    CimisSplit advance(CimisConfig config, CimisResource resource) {
        if (resource == CimisResource.DATA) {
            LocalDate next = through(config).plusDays(1L);
            if (!next.isAfter(config.end)) {
                return new CimisSplit(
                        scope,
                        resourceIndex,
                        targetIndex,
                        modeIndex,
                        next,
                        width,
                        requests + 1,
                        null,
                        0);
            }
            if (modeIndex + 1 < config.hourlyModes.size()) {
                return new CimisSplit(
                        scope,
                        resourceIndex,
                        targetIndex,
                        modeIndex + 1,
                        config.start,
                        config.sliceDays,
                        requests + 1,
                        null,
                        0);
            }
            if (targetIndex + 1 < config.targets.size()) {
                return new CimisSplit(
                        scope,
                        resourceIndex,
                        targetIndex + 1,
                        0,
                        config.start,
                        config.sliceDays,
                        requests + 1,
                        null,
                        0);
            }
        }
        return new CimisSplit(
                scope,
                resourceIndex + 1,
                0,
                0,
                config.start,
                config.sliceDays,
                requests + 1,
                null,
                0);
    }

    @Override
    public String splitId() {
        return "cimis-sliced-scan";
    }
}

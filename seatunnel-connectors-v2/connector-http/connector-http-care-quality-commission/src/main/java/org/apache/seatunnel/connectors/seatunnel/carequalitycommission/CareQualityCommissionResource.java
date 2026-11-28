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

import java.util.List;
import java.util.regex.Pattern;

/**
 * Defines the six CQC Syndication reads and the identifiers used to link them.
 *
 * <p>Detail paths are built from validated parent identifiers.
 */
enum CareQualityCommissionResource {
    INSPECTION_AREAS(
            "inspection_areas",
            "/inspection-areas",
            "inspectionAreas",
            "inspectionAreaId",
            Shape.COMPLETE,
            false),
    LOCATIONS("locations", "/locations", "locations", "locationId", Shape.PAGED, false),
    PROVIDERS("providers", "/providers", "providers", "providerId", Shape.PAGED, false),
    PROVIDER_LOCATIONS(
            "provider_locations",
            "/providers/%s/locations",
            "locations",
            "organisationId",
            Shape.COMPLETE,
            true),
    LOCATIONS_DETAILED(
            "locations_detailed", "/locations/%s", null, "locationId", Shape.OBJECT, true),
    PROVIDERS_DETAILED(
            "providers_detailed", "/providers/%s", null, "providerId", Shape.OBJECT, true);

    private static final Pattern PATH_ID = Pattern.compile("[A-Za-z0-9_-]{1,256}");

    final String name;
    final String path;
    final String items;
    final String identity;
    final Shape shape;
    final boolean scoped;

    CareQualityCommissionResource(
            String name, String path, String items, String identity, Shape shape, boolean scoped) {
        this.name = name;
        this.path = path;
        this.items = items;
        this.identity = identity;
        this.shape = shape;
        this.scoped = scoped;
    }

    List<CareQualityCommissionResource> children() {
        switch (this) {
            case LOCATIONS:
                return List.of(LOCATIONS_DETAILED);
            case PROVIDERS:
                return List.of(PROVIDER_LOCATIONS, PROVIDERS_DETAILED);
            default:
                return List.of();
        }
    }

    static boolean validPathId(String id) {
        return PATH_ID.matcher(id).matches();
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Distinguishes native collection pages, complete lists and resource details.
     *
     * <p>Each shape has its own response limit and continuation rules.
     */
    enum Shape {
        PAGED,
        COMPLETE,
        OBJECT
    }

    /**
     * Reports a safe resource-specific failure without response bodies or identifiers.
     *
     * <p>The resource name and a bounded diagnostic can be shown in engine logs.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String resource, String reason) {
            super("CareQualityCommission " + resource + ": " + reason);
        }
    }
}

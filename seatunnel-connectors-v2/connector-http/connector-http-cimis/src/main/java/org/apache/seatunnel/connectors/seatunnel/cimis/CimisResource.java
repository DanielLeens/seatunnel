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

import java.util.Locale;

/**
 * The four current CIMIS resources and their document envelopes.
 *
 * <p>Reference data is read once per selected resource. Weather observations use fixed routes
 * derived from the selected target family.
 */
enum CimisResource {
    STATIONS("/StationWeb/GetAllStations", "Stations"),
    SPATIAL_ZIPCODES("/SpatialWeb/GetAllSpatialZipCodes", "ZipCodes"),
    STATION_ZIPCODES("/StationWeb/GetAllStationsZipCodes", "ZipCodes"),
    DATA(null, null);

    final String id = name().toLowerCase(Locale.ROOT);
    final String route;
    final String envelope;

    CimisResource(String route, String envelope) {
        this.route = route;
        this.envelope = envelope;
    }

    static CimisResource from(String value) {
        for (CimisResource resource : values()) {
            if (resource.id.equals(value)) {
                return resource;
            }
        }
        throw CimisConfig.invalid("resources", "unsupported resource name");
    }

    CimisReadException failure(String description) {
        return new CimisReadException(id, description);
    }
}

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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Metric names accepted by the current CIMIS request contract.
 *
 * <p>Legacy calculation names are kept distinct from current ASCE quantities.
 */
final class CimisDataItems {
    private static final Set<String> DAILY =
            Set.of(
                    "day-air-tmp-avg",
                    "day-air-tmp-max",
                    "day-air-tmp-min",
                    "day-dew-pnt",
                    "day-asce-eto",
                    "day-asce-etr",
                    "day-precip",
                    "day-rel-hum-avg",
                    "day-rel-hum-max",
                    "day-rel-hum-min",
                    "day-soil-tmp-avg",
                    "day-soil-tmp-max",
                    "day-soil-tmp-min",
                    "day-sol-rad-avg",
                    "day-sol-rad-net",
                    "day-vap-pres-min",
                    "day-vap-pres-max",
                    "day-vap-pres-avg",
                    "day-wind-ene",
                    "day-wind-ese",
                    "day-wind-nne",
                    "day-wind-nnw",
                    "day-wind-run",
                    "day-wind-spd-avg",
                    "day-wind-sse",
                    "day-wind-ssw",
                    "day-wind-wnw",
                    "day-wind-wsw");
    private static final Set<String> HOURLY =
            Set.of(
                    "hly-air-tmp",
                    "hly-dew-pnt",
                    "hly-net-rad",
                    "hly-asce-eto",
                    "hly-asce-etr",
                    "hly-precip",
                    "hly-rel-hum",
                    "hly-res-wind",
                    "hly-soil-tmp",
                    "hly-sol-rad",
                    "hly-vap-pres",
                    "hly-wind-dir",
                    "hly-wind-spd");
    private static final Set<String> SPATIAL = Set.of("day-asce-eto", "day-sol-rad-avg");

    private CimisDataItems() {}

    static List<String> checked(List<String> configured, boolean hourly, boolean spatial) {
        if (configured == null || configured.isEmpty()) {
            return List.of();
        }
        Set<String> allowed = hourly ? HOURLY : spatial ? SPATIAL : DAILY;
        Set<String> seen = new HashSet<>();
        for (String name : configured) {
            if (name == null || !allowed.contains(name) || !seen.add(name)) {
                throw CimisConfig.invalid(
                        hourly ? "hourly_data_items" : "daily_data_items",
                        "choose unique supported CIMIS 3.0 metrics for the target and interval");
            }
        }
        return List.copyOf(configured);
    }
}

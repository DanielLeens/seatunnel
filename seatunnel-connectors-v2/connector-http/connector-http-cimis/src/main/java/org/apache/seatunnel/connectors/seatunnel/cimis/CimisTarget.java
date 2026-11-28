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

import java.math.BigDecimal;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A validated CIMIS 3.0 weather target family.
 *
 * <p>Each target is sent as a single query value. Validating separators prevents a configured item
 * from silently adding more targets to one slice.
 */
enum CimisTarget {
    STATION_NUMBERS("/StationWeb/GetDataByStationNumber", "stationNbrs", false),
    ZIP_CODES("/GeoStationWeb/GetDataByGeoStationZipCodes", "zipCodes", false),
    COORDINATES("/SpatialWeb/GetDataBySpatialCoordinates", "coordinates", true),
    ADDRESSES("/SpatialWeb/GetDataByAddresses", "addresses", true);

    private static final Pattern LAT_LONG =
            Pattern.compile(
                    "lat=([+-]?[0-9]{1,3}(?:\\.[0-9]{1,24})?),lng=([+-]?[0-9]{1,3}(?:\\.[0-9]{1,24})?)");
    private static final Pattern ADDRESS = Pattern.compile("addr-name=([^,;]{1,128}),addr=(.+)");

    final String id = name().toLowerCase(Locale.ROOT);
    final String route;
    final String parameter;
    final boolean spatialOnly;

    CimisTarget(String route, String parameter, boolean spatialOnly) {
        this.route = route;
        this.parameter = parameter;
        this.spatialOnly = spatialOnly;
    }

    static CimisTarget from(String value) {
        for (CimisTarget candidate : values()) {
            if (candidate.id.equals(value)) {
                return candidate;
            }
        }
        throw CimisConfig.invalid(
                "target_type", "expected station_numbers, zip_codes, coordinates or addresses");
    }

    String validated(String value) {
        if (value == null
                || value.length() > 512
                || value.indexOf(';') >= 0
                || value.codePoints()
                        .anyMatch(
                                character ->
                                        Character.isISOControl(character)
                                                || character >= Character.MIN_SURROGATE
                                                        && character <= Character.MAX_SURROGATE)) {
            throw invalid();
        }
        switch (this) {
            case STATION_NUMBERS:
                if (value.matches("[0-9]{1,9}") && Integer.parseInt(value) > 0) {
                    return Integer.toString(Integer.parseInt(value));
                }
                break;
            case ZIP_CODES:
                if (value.matches("[0-9]{5}")) {
                    return value;
                }
                break;
            case COORDINATES:
                Matcher coordinates = LAT_LONG.matcher(value);
                if (coordinates.matches()) {
                    BigDecimal latitude = new BigDecimal(coordinates.group(1));
                    BigDecimal longitude = new BigDecimal(coordinates.group(2));
                    if (latitude.abs().compareTo(BigDecimal.valueOf(90)) <= 0
                            && longitude.abs().compareTo(BigDecimal.valueOf(180)) <= 0) {
                        return "lat="
                                + latitude.stripTrailingZeros().toPlainString()
                                + ",lng="
                                + longitude.stripTrailingZeros().toPlainString();
                    }
                }
                break;
            case ADDRESSES:
                Matcher address = ADDRESS.matcher(value);
                if (address.matches()
                        && !address.group(1).isBlank()
                        && !address.group(2).isBlank()
                        && !address.group(2).contains(",addr=")
                        && !address.group(2).contains(",addr-name=")) {
                    return value;
                }
                break;
            default:
                throw new IllegalStateException("invalid target family");
        }
        throw invalid();
    }

    private IllegalArgumentException invalid() {
        return CimisConfig.invalid("targets", "invalid " + id + " target");
    }
}

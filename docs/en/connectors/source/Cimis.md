<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Cimis

## Description

The Cimis source reads weather observations and reference data through the California Department of Water Resources' CIMIS Web API 3.0. Each HTTPS request sends the subscription key in `Ocp-Apim-Subscription-Key`. CIMIS retired its former API on November 16, 2026.

## Resources

| Resource | Path | Data |
| --- | --- | --- |
| `stations` | `/StationWeb/GetAllStations` | Station reference records, including inactive stations |
| `spatial_zipcodes` | `/SpatialWeb/GetAllSpatialZipCodes` | Spatial ZIP coverage and its status |
| `station_zipcodes` | `/StationWeb/GetAllStationsZipCodes` | Station-to-ZIP mappings |
| `data` | The route for the configured target type | Daily or hourly weather observations |

Set one target type for the weather data and list one or more target values of that type.

| `target_type` | Weather route | Target value |
| --- | --- | --- |
| `station_numbers` | `/StationWeb/GetDataByStationNumber` | Positive decimal station number, such as `17`. |
| `zip_codes` | `/GeoStationWeb/GetDataByGeoStationZipCodes` | Five-digit ZIP string. Daily requests use the configured provider preference. Hourly requests use the station network. |
| `coordinates` | `/SpatialWeb/GetDataBySpatialCoordinates` | Decimal `lat=...,lng=...`. Daily spatial data. |
| `addresses` | `/SpatialWeb/GetDataByAddresses` | `addr-name=...,addr=...`, keeping commas in the street address. Daily spatial data. |

## Options

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `access_key` | string | Yes | | Current CIMIS API subscription key. |
| `resources` | list of strings | Yes | | One or more unique resource names from the table. |
| `target_type` | string | For `data` | | Weather target family. |
| `targets` | list of strings | For `data` | | Between 1 and 100 unique values of the same type. |
| `start_date` | string | For `data` | | First CIMIS calendar date, inclusive, in `yyyy-MM-dd` form. |
| `end_date` | string | For `data` | | Last CIMIS calendar date, inclusive, in `yyyy-MM-dd` form. |
| `unit_of_measure` | string | No | `E` | `E` for English units or `M` for metric units. |
| `zip_preference` | string | No | `SCS` | Preferred source for daily ZIP data, `SCS` or `WSN`. |
| `daily_data_items` | list of strings | No | | Supported CIMIS 3.0 daily metrics. Spatial coordinate and address requests accept `day-asce-eto` and `day-sol-rad-avg`. |
| `hourly_data_items` | list of strings | No | | Supported CIMIS 3.0 hourly station metrics. |
| `slice_days` | integer | No | `31` | Starting date slice width per target, range 1–31. |
| `max_requests` | integer | No | `100000` | Maximum completed responses and capacity-driven subdivisions per scan, range 1–1000000. |
| `max_attempts` | integer | No | `4` | Maximum attempts per request, including the initial GET, range 1–10. |
| `request_timeout_ms` | integer | No | `30000` | Total budget per request including retries, range 100–300000. |
| `max_response_bytes` | integer | No | `8388608` | Maximum decoded bytes per response, range 1024–67108864. |
| `endpoint` | string | No | `https://et.water.ca.gov` | Custom bare HTTPS origin. Requires `trust_certificate`. |
| `trust_certificate` | string | No | | PEM trust certificate path for a custom origin, readable on the worker. Hostname validation still applies. |

With neither metric list supplied, the source requests the provider's default daily data. When both are supplied, the connector makes separate daily and hourly requests. It accepts measurement names documented for 3.0. The legacy `day-eto` and `hly-eto` option names are unavailable in the current metric table. Choose the calculation that matches your use of the data.

The source reads one target at a time and includes both endpoint dates in each slice. It narrows a rejected slice when a documented HTTP 400 response starts with the `ERR2112` data-volume error. An affected slice that has already emitted rows or contains only one day fails. HTTP 429 retries respect a valid `Retry-After` within the request budget.

## Output and recovery

| Column | Type | Description |
| --- | --- | --- |
| `resource` | string | Selected resource. |
| `record_id` | string | Station number, ZIP, station-and-ZIP pair, or a digest of weather observation identity. |
| `target` | nullable string | The normalized request target for a weather observation. |
| `date` | nullable string | CIMIS date in the response. |
| `hour` | nullable string | CIMIS hour in the response when present, including `2400`. |
| `provider_name` | nullable string | Weather record provider name. |
| `provider_type` | nullable string | Station or spatial weather provider type. |
| `provider_owner` | nullable string | Weather provider owner. |
| `payload` | string | Complete record JSON, including original values, missing values, quality flags and units. |

CIMIS controls the calendar date and hour. The source preserves these fields as supplied, with no time-zone or day rollover adjustment. A checkpoint retains the current target, calendar range, slice width and emitted offset. Recovery checks the active slice before outputting its remaining rows. The service can revise historical observations. This source does not provide a globally consistent snapshot or a change stream.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  Cimis {
    access_key = ${CIMIS_ACCESS_KEY}
    resources = ["stations", "data"]
    target_type = "station_numbers"
    targets = ["17"]
    start_date = "2026-10-01"
    end_date = "2026-10-07"
    unit_of_measure = "M"
    daily_data_items = ["day-asce-eto", "day-sol-rad-avg"]
    plugin_output = "weather"
  }
}

sink {
  Console {
    plugin_input = "weather"
  }
}
```

See the [CIMIS Web API overview](https://cimis.water.ca.gov/web-api/overview) and [current REST reference](https://cimis.water.ca.gov/web-api/rest-api/latest).

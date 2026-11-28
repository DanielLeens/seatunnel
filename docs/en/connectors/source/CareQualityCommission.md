<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

import ChangeLog from "../changelog/connector-http-care-quality-commission.md";

# Care Quality Commission

The `CareQualityCommission` source reads the public Syndication API of England's Care Quality Commission (CQC) in `BATCH` mode. Create a subscription in the CQC Developer Portal and configure its primary subscription key as `token`. The connector sends it in the `Ocp-Apim-Subscription-Key` header over HTTPS to `api.service.cqc.org.uk`. [Developer Portal](https://api-portal.service.cqc.org.uk/) and [CQC data guidance](https://www.cqc.org.uk/about-us/transparency/using-cqc-data).

| Resource | GET path under `/public/v1` | Record ID |
| --- | --- | --- |
| `inspection_areas` | `/inspection-areas` | `inspectionAreaId` |
| `locations` | `/locations` | `locationId` |
| `providers` | `/providers` | `providerId` |
| `provider_locations` | `/providers/{providerId}/locations` | `organisationId` or `locationId` |
| `locations_detailed` | `/locations/{locationId}` | `locationId` |
| `providers_detailed` | `/providers/{providerId}` | `providerId` |

Locations and providers are paged with `page` and `perPage`, starting at page 1. The connector discovers the necessary parents automatically when a child resource is selected. Select a parent explicitly to include its summary rows, or select the children alone to emit only their records. The API includes active and inactive records and is updated daily. Absent and unpublished ratings remain as returned by the API.

## Output

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | Resource name |
| `id` | STRING | Native record ID |
| `provider_id` | STRING | Owning provider ID when available. Provider rows use their own ID |
| `location_id` | STRING | Location ID for each location record |
| `record` | STRING | Native JSON record with nested information and explicit nulls |

Use `resource` and `id` to distinguish kinds of records. Use `provider_id` as part of the key when writing provider-to-location associations.

CQC makes this information available under the Open Government Licence. Acknowledge CQC when republishing or presenting these data. [CQC attribution guidance](https://www.cqc.org.uk/about-us/transparency/using-cqc-data).

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | | CQC Syndication primary subscription key |
| `resources` | List of strings | No | `[]` | Resources from the table. An empty list selects all six |
| `tls_trust_certificate` | String | No | | Source-local PEM or base64 DER X.509 trust anchor. The hostname is always verified over TLS 1.2 or newer |
| `page_size` | Integer | No | `1000` | Requested records per page for location and provider lists, from 1 to 1000 |
| `max_pages_per_collection` | Integer | No | `10000` | Maximum pages per paginated collection, from 1 to 100000 |
| `max_pages_per_scan` | Integer | No | `500000` | Maximum accepted responses across the scan, from 1 to 1000000 |
| `max_unpaginated_items` | Integer | No | `10000` | Maximum rows in a complete list, from 1 to 100000 |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connect timeout, from 100 to 300000 ms |
| `request_timeout_ms` | Integer | No | `30000` | Deadline per HTTPS attempt, from 100 to 300000 ms |
| `operation_timeout_ms` | Integer | No | `120000` | Deadline across retries and waits for one read. At least `request_timeout_ms` and at most 600000 ms |
| `request_interval_ms` | Integer | No | `0` | Minimum time between HTTPS attempts, from 0 to 60000 ms |
| `max_retries` | Integer | No | `3` | Additional attempts after eligible transient failures, from 0 to 8 |
| `max_retry_delay_ms` | Integer | No | `60000` | Maximum accepted retry delay, from 250 to 600000 ms |
| `max_response_bytes` | Integer | No | `8388608` | Maximum wire and decompressed response bytes, from 256 to 16777216 |

## Recovery

One reader advances through the selected resources and keeps the current parent position for detail requests. A checkpoint saves confirmed page digests and progress. Resume validates the subscription key scope, selected collections and page size before reading, and verifies any fetched page needed for replay. A resource missing from the API returns an explicit HTTP 404 diagnostic, including when a listed record disappears before its detail is fetched. A checkpoint can resume a compatible scan while the API changes its own data independently.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  CareQualityCommission {
    token = "${CQC_SUBSCRIPTION_KEY}"
    resources = ["providers", "provider_locations", "providers_detailed"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />

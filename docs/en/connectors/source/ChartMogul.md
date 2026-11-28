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

import ChangeLog from "../changelog/connector-http-chartmogul.md";

# ChartMogul

The `ChartMogul` source reads customers, subscription activities and four customer count series from the ChartMogul API in `BATCH` mode. Use an API key as the HTTPS Basic username with an empty password. [Authentication](https://dev.chartmogul.com/docs/authentication/).

| Resource | GET path | Record identifier |
| --- | --- | --- |
| `customers` | `/v1/customers` | Internal numeric `id` |
| `activities` | `/v1/activities` | Activity `uuid` |
| `customer_daily_count` | `/v1/metrics/customer-count`, `interval=day` | Period end date |
| `customer_weekly_count` | `/v1/metrics/customer-count`, `interval=week` | Period end date |
| `customer_monthly_count` | `/v1/metrics/customer-count`, `interval=month` | Period end date |
| `customer_quarterly_count` | `/v1/metrics/customer-count`, `interval=quarter` | Period end date |

Customers and activities use the API's `has_more` and `cursor` fields. Short or empty pages continue when `has_more` is true. Customer count requests return a complete series of metric entries for the chosen calendar interval. [Customers](https://dev.chartmogul.com/reference/customers/list/), [activities](https://dev.chartmogul.com/reference/activities/list/) and [customer count](https://dev.chartmogul.com/reference/metrics/customer-count/).

## Output

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | Selected resource name |
| `id` | STRING | Record identifier from the resource table. Customer IDs retain their full numeric precision |
| `uuid` | STRING | Native UUID when available on customers and activities |
| `event_time` | STRING | Customer `customer-since` or activity `date` normalized to UTC, or the native `YYYY-MM-DD` metric period end |
| `record` | STRING | Native JSON object, including custom attributes, source identifiers and nulls. Activity amounts keep their original integer-cent units |

The date range applies to activities and customer counts. Set `start_date` when selecting one of these resources. Activity bounds are inclusive. Customer counts use the UTC calendar dates corresponding to the bounds. The source preserves the period end dates ChartMogul returns, including variable-length months and quarters. Customers are a full read.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | | ChartMogul API key |
| `resources` | List of strings | No | `[]` | Resource names from the table. An empty list selects all six |
| `start_date` | String | For activities or customer counts | | Inclusive ISO 8601 time with an explicit offset |
| `end_date` | String | No | | Inclusive ISO 8601 time with an explicit offset. If omitted, the source freezes the upper bound at scan creation |
| `tls_trust_certificate` | String | No | | Optional source-local PEM or base64 DER X.509 trust anchor. HTTPS hostname verification remains enabled |
| `page_size` | Integer | No | `200` | Requested customer or activity rows per page, from 1 to 200 |
| `max_pages_per_collection` | Integer | No | `10000` | Maximum accepted pages per collection, from 1 to 100000 |
| `max_pages_per_scan` | Integer | No | `100000` | Maximum accepted responses across all resources, from 1 to 1000000 |
| `max_unpaginated_items` | Integer | No | `10000` | Maximum entries in a customer count response, from 1 to 100000 |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connect timeout, from 100 to 300000 ms |
| `request_timeout_ms` | Integer | No | `30000` | Deadline per HTTPS attempt, from 100 to 300000 ms |
| `operation_timeout_ms` | Integer | No | `120000` | Deadline across retries and waits for one read. At least `request_timeout_ms` and at most 600000 ms |
| `request_interval_ms` | Integer | No | `30` | Minimum time between HTTPS attempts, from 0 to 60000 ms |
| `max_retries` | Integer | No | `3` | Additional attempts for eligible transient failures, from 0 to 8 |
| `max_retry_delay_ms` | Integer | No | `60000` | Maximum accepted retry wait, from 250 to 600000 ms |
| `max_response_bytes` | Integer | No | `8388608` | Maximum wire and decompressed response bytes, from 256 to 16777216 |

One active reader enforces the configured request interval. The account or server may also apply compute and concurrency limits. The connector uses bounded backoff and honors `Retry-After` when supplied. HTTP 500 responses receive at most two retries. [API rate limits](https://dev.chartmogul.com/docs/rate-limits/).

## Recovery

The source checkpoint stores the active resource, cursor, page fingerprint, row index, and frozen time bound. A resumed job verifies a saved page before emitting the rows it still needs. Reuse the same token, resource selection, page size and dates. The checkpoint excludes credentials and record JSON. ChartMogul data may change during a scan, so a source checkpoint cannot represent an account-wide snapshot.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  ChartMogul {
    token = "${CHARTMOGUL_API_KEY}"
    resources = ["customers", "activities", "customer_monthly_count"]
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-03-31T23:59:59Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />

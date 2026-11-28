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

import ChangeLog from "../changelog/connector-http-churnkey.md";

# Churnkey

The `Churnkey` source reads cancellation flow sessions and grouped session counts from the Churnkey Data API in `BATCH` mode. Get your Data API key and App ID in Churnkey account settings. Set the Data API key in `token` and the App ID in `access_key`. They are sent as `x-ck-api-key` and `x-ck-app` over HTTPS to `api.churnkey.co`. [Churnkey Data API](https://docs.churnkey.co/data-integrations/data-api/).

| Resource | GET path | Response |
| --- | --- | --- |
| `sessions` | `/v1/data/sessions` | Session array with `limit` and `skip` paging |
| `session-aggregation` | `/v1/data/session-aggregation` | Complete grouped count array |

Session paging advances by the number of records returned. Short pages continue and an empty page ends the list. The `bounced` option passes the corresponding provider filter to both resources. The default `false` excludes sessions that Churnkey marked as bounced after a more relevant session within 24 hours.

Use `breakdown` to group the aggregate response into any of these dimensions, in the order supplied. `month`, `trial`, `billingInterval`, `planId`, `aborted`, `canceled`, `offerType`, `saveType`, `couponId`, `pauseDuration`, `blueprintId`, `abtest`, `surveyResponse`.

## Output

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | Resource name |
| `id` | STRING | Native `_id` for sessions. Null for grouped count rows |
| `event_time` | STRING | Session `createdAt` normalized to UTC or the returned `YYYY-MM` group month, when available |
| `record` | STRING | Native JSON object with nested session information or the grouped count and dimension values |

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | | Churnkey Data API key |
| `access_key` | String | Yes | | Churnkey App ID |
| `resources` | List of strings | No | `[]` | Resource names from the table. An empty list selects both |
| `bounced` | Boolean | No | `false` | Native bounced session filter |
| `breakdown` | List of strings | No | `[]` | Ordered aggregation dimensions. Duplicate names are invalid |
| `tls_trust_certificate` | String | No | | Optional source-local PEM or base64 DER X.509 trust anchor. HTTPS hostname verification remains enabled |
| `page_size` | Integer | No | `100` | Session records requested per page, from 1 to 10000 |
| `max_pages_per_collection` | Integer | No | `10000` | Maximum accepted session pages, from 1 to 100000 |
| `max_pages_per_scan` | Integer | No | `100000` | Maximum accepted responses across both resources, from 1 to 1000000 |
| `max_unpaginated_items` | Integer | No | `10000` | Maximum groups in the aggregation response, from 1 to 100000 |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connect timeout, from 100 to 300000 ms |
| `request_timeout_ms` | Integer | No | `30000` | Deadline per HTTPS attempt, from 100 to 300000 ms |
| `operation_timeout_ms` | Integer | No | `120000` | Deadline across retries and waits in one read. At least `request_timeout_ms` and at most 600000 ms |
| `request_interval_ms` | Integer | No | `0` | Minimum time between HTTPS attempts, from 0 to 60000 ms |
| `max_retries` | Integer | No | `3` | Additional attempts for eligible transient failures, from 0 to 8 |
| `max_retry_delay_ms` | Integer | No | `60000` | Maximum accepted retry wait, from 250 to 600000 ms |
| `max_response_bytes` | Integer | No | `8388608` | Maximum wire and decompressed response bytes, from 256 to 16777216 |

## Recovery

One reader processes both selected resources under the same app scope. A checkpoint retains the resource, offset, row position and response fingerprint. The source verifies a saved response before continuing from its row index. Use the same app, key, resources, page size and filter or grouping settings when restoring. The checkpoint excludes credentials and JSON records. Session lists can change between calls, so a checkpoint provides a scan position rather than an account-wide snapshot.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Churnkey {
    token = "${CHURNKEY_DATA_API_KEY}"
    access_key = "${CHURNKEY_APP_ID}"
    resources = ["sessions", "session-aggregation"]
    breakdown = ["month", "surveyResponse", "billingInterval"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />

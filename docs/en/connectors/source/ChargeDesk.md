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

import ChangeLog from "../changelog/connector-http-chargedesk.md";

# ChargeDesk

The `ChargeDesk` source reads four collections from `https://api.chargedesk.com/v1`. Enable API access for your ChargeDesk company and use its secret key. The connector sends the key as the HTTPS Basic authentication username with an empty password. [ChargeDesk API documentation](https://blue.chargedesk.com/api-docs).

| Resource | Record identifier | Event time | Maximum offset |
| --- | --- | --- | --- |
| `charges` | `charge_id` | `occurred` | 50000 |
| `customers` | `customer_id` | `first_seen` | 50000 |
| `subscriptions` | `subscription_id` | `first_seen` | 10000 |
| `products` | `product_id` | `first_seen` | 10000 |

The charge record retains the payment gateway's `transaction_id` separately inside its JSON. Each list advances by the number of records it actually returns, even if a page is short. An empty page ends the collection. The source fails if the last allowed offset still returns data and the API cannot verify another page.

## Output

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | Resource name |
| `id` | STRING | ChargeDesk identifier from the resource table |
| `event_time` | STRING | UTC time from the native Unix seconds field, or null when unavailable |
| `record` | STRING | The native JSON object, including amount strings, nested metadata and nulls |

## Date ranges

`start_date` and `end_date` apply to charges, customers, and subscriptions. The start is inclusive and the end is exclusive. The connector uses the native `occurred` and `first_seen` query parameters for charges and customers, then checks each returned timestamp against the exact range. Subscriptions use local `first_seen` filtering because the API does not define its subscription `created` filter in terms of `first_seen`. Products always use a full read. A dated record with a missing timestamp fails when a date filter applies.

An offset-limit failure indicates the API has not provided a complete scan. A narrower date range can reduce the native offset span for charges and customers. A group with many records at the same timestamp can still reach the offset limit.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | | Company API secret key |
| `resources` | List of strings | No | `[]` | Resources from the table. An empty list selects all four |
| `start_date` | Long | No | | Inclusive Unix seconds for the dated collections |
| `end_date` | Long | No | | Exclusive Unix seconds. With a start and no end, the source freezes the upper bound at scan creation |
| `tls_trust_certificate` | String | No | | Optional source-local PEM or base64 DER X.509 trust anchor. HTTPS hostname verification remains enabled |
| `page_size` | Integer | No | `500` | Requested records per page, from 1 to 500 |
| `max_pages_per_collection` | Integer | No | `10000` | Maximum accepted responses in one collection, from 1 to 100000 |
| `max_pages_per_scan` | Integer | No | `100000` | Maximum accepted responses across all collections, from 1 to 1000000 |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connect timeout, from 100 to 300000 ms |
| `request_timeout_ms` | Integer | No | `30000` | Deadline per HTTPS attempt, from 100 to 300000 ms |
| `operation_timeout_ms` | Integer | No | `120000` | Deadline across retries and waits for one read. At least `request_timeout_ms` and at most 600000 ms |
| `request_interval_ms` | Integer | No | `1050` | Minimum time between HTTPS attempts, from 0 to 60000 ms |
| `max_retries` | Integer | No | `3` | Additional attempts for eligible transient failures, from 0 to 8 |
| `max_retry_delay_ms` | Integer | No | `60000` | Maximum accepted retry delay, from 250 to 600000 ms |
| `max_response_bytes` | Integer | No | `8388608` | Maximum wire and decompressed response bytes, from 256 to 16777216 |

The default request interval accounts for ChargeDesk's limit of 60 requests per minute for a single reader. HTTP 429 retries honor the server's `Retry-After` header within the configured deadlines. Other jobs sharing the same secret may use the same API quota.

## Recovery

Use `BATCH` mode. One active reader saves the collection position, row index, accepted page digest, and any frozen date bound. It verifies the saved page before emitting remaining rows. Resume with the same secret, resources, page size and date bounds. The API can change between requests, so a resumed scan does not represent a company-wide snapshot.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  ChargeDesk {
    token = "${CHARGEDESK_API_SECRET}"
    resources = ["charges", "customers"]
    start_date = 1775001600
    end_date = 1777593600
  }
}
sink {
  Console {}
}
```

<ChangeLog />

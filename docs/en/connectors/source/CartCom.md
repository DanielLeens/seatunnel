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

import ChangeLog from '../changelog/connector-http-cart-com.md';

# CartCom

Install `connector-http-cart-com` to read the Cart.com Online Store REST API backed by AmeriCommerce. Use one resource per source block in batch mode at source parallelism one. [Online Store API](https://github.com/AmeriCommerce/rest-api/blob/master/docs/README.md)

## Resources

| Resource | Response collection | Records |
| --- | --- | --- |
| `customers` | customers | Customers |
| `orders` | orders | Orders |
| `order_payments` | order_payments / payments | Order payments |
| `order_items` | order_items / items | Order items |
| `order_statuses` | order_statuses | Order statuses, full read |
| `products` | products | Products |
| `addresses` | addresses | Addresses |

The two order endpoints accept the current collection names and their shorter forms. If a response supplies both forms, their arrays must agree.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `resource` | string | Required | One listed resource. |
| `store_host` | string | Single store | Merchant DNS name without scheme, port or path. |
| `token` | string | Single store | Cart.com access token issued for the selected store. |
| `site_id` | string | Central router | Site ID provisioned for central requests. |
| `username` | string | Central router | Provisioned user name. |
| `secret_key` | string | Central router | Signing secret issued with the provisioning credentials. |
| `start_date` | string | `1970-01-01T00:00:00Z` | Inclusive lower bound for dated resources. |
| `end_date` | string | First read time | Exclusive upper bound for dated resources. Saved across restore. |
| `incremental_start` | string | Unset | Optional previous high-water timestamp for a new batch. |
| `lookback_seconds` | int | `0` | Seconds subtracted from incremental_start before clamping to start_date, 0–31536000. |
| `page_size` | int | `1000` | Requested page size, from 1 to 1000. |
| `max_pages` | int | `2000` | Numbered pages including empty pages, from 1 to 20000. |
| `connect_timeout_ms` | int | `10000` | TCP connect timeout, from 1 to 120000. |
| `read_timeout_ms` | int | `30000` | Socket read timeout, from 1 to 120000. |
| `request_deadline_ms` | int | `60000` | Total budget for a request and retries, from 1 to 300000. |
| `max_attempts` | int | `4` | Maximum GET attempts including the first, from 1 to 10. |
| `max_response_bytes` | int | `8388608` | Maximum encoded and decoded response size, from 1024 to 33554432. |
| `tls_trust_certificate` | string | JVM trust store | Absolute path to a worker-local PEM or DER trust anchor file. Hostname verification always applies. |

## Authentication

For single-store access, set `store_host` and `token`. The worker sends `X-AC-Auth-Token` only to that HTTPS hostname. Generate the token with access to the selected resources. The token can come from the admin console or a separately configured OAuth application. [Making API requests](https://support.americommerce.com/hc/en-us/articles/202836750-Making-API-Requests)

For provisioned central router access, set `site_id`, `username` and `secret_key`. The worker uses the fixed HTTPS authority `public.americommerce.com` and signs each request path and encoded query using HMAC-SHA256. Each source block selects one authentication method.

## Paging and output

Six resources read an inclusive `start_date` and exclusive `end_date` window on `updated_at`. A new batch may set `incremental_start` to the previous high-water mark. The source subtracts `lookback_seconds` and clamps the result to `start_date`. It requests `updated_at,id` order, so rows with the same timestamp stay in the read window. Replay can intentionally emit rows again across batches. `order_statuses` scans all pages. A missing or null `updated_at` is emitted as null if the service includes that record. ISO 8601 timestamps without a zone are interpreted as UTC. [Query syntax](https://github.com/AmeriCommerce/rest-api/blob/master/docs/query_syntax.md) [Date format](https://github.com/AmeriCommerce/rest-api/blob/master/docs/datetimes.md)

| String column | Value |
| --- | --- |
| `id` | The exact identifier rendered as a string. |
| `updated_at` | Normalized UTC timestamp, or null. |
| `data` | Full response record as JSON, including nested fields, nulls and precise numeric values. |

Only a sequential page number is extracted from the provider's `next_page` URL. Every page retains the configured host, resource, sort, size, and date bounds. The worker verifies the TLS hostname, rejects redirects and keeps retry delays within the request budget. A checkpoint retains the page, raw offset, date bounds, and page fingerprint. A changed partial page fails on restore before more rows are emitted. A terminal split performs no new requests. The provider can change between pages, so a batch can see concurrent updates. [Pagination](https://support.americommerce.com/hc/en-us/articles/202524814-API-Quick-Start)

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  CartCom {
    resource = "orders"
    store_host = "store.example.com"
    token = ${CART_STORE_TOKEN}
    start_date = "2025-01-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />

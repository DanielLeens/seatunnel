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

import ChangeLog from '../changelog/connector-http-chargebee.md';

# Chargebee

Install `connector-http-chargebee` and configure one API v2 resource in each batch source block with source parallelism `1`. `site` supplies the DNS label for `https://<site>.chargebee.com/api/v2`. The source authenticates with a site API key as the Basic username and an empty password. Provide a key with access to the selected resource and any parent collections. [Chargebee API](https://apidocs.chargebee.com/docs/api/getting-started)

## Resources

| Resource | Catalog | Parent | Cursor | Read |
| --- | --- | --- | --- | --- |
| `addon` | 1.0 | — | `updated_at` | UTC calendar-month windows |
| `attached_item` | 2.0 | `item` | `updated_at` | One time window for each item |
| `customer` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `coupon` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `contact` | 1.0 and 2.0 | `customer` | — | Full read |
| `credit_note` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `event` | 1.0 and 2.0 | — | `occurred_at` | UTC calendar-month windows |
| `gift` | 1.0 and 2.0 | — | `updated_at` | List scan with local date filtering |
| `order` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `hosted_page` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `invoice` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `item` | 2.0 | — | `updated_at` | UTC calendar-month windows |
| `item_price` | 2.0 | — | `updated_at` | UTC calendar-month windows |
| `plan` | 1.0 | — | `updated_at` | UTC calendar-month windows |
| `payment_source` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `promotional_credit` | 1.0 and 2.0 | — | `created_at` | UTC calendar-month windows |
| `subscription` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `subscription_with_scheduled_changes` | 1.0 and 2.0 | `subscription` | — | Full read |
| `transaction` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `unbilled_charge` | 1.0 and 2.0 | — | `updated_at` | List scan with local date filtering |
| `virtual_bank_account` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `quote` | 1.0 and 2.0 | — | `updated_at` | UTC calendar-month windows |
| `quote_line_group` | 1.0 and 2.0 | `quote` | — | Full read |
| `site_migration_detail` | 1.0 and 2.0 | — | `migrated_at` | List scan with local date filtering |
| `comment` | 1.0 and 2.0 | — | `created_at` | UTC calendar-month windows |
| `item_family` | 2.0 | — | `updated_at` | UTC calendar-month windows |
| `differential_price` | 2.0 | — | `updated_at` | List scan with local date filtering |

`product_catalog` selects the site's catalog version within API v2. Selections unavailable for the configured version fail during configuration. Some shared resources still require the corresponding feature or permission on the site. [Product Catalog and API versions](https://apidocs.chargebee.com/docs/api/versioning)

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `site` | string | Required | Chargebee site DNS label before .chargebee.com. |
| `token` | string | Required | Chargebee API key for the selected site. |
| `resource` | string | Required | One supported Chargebee API v2 resource. |
| `product_catalog` | string | `2.0` | Product Catalog version of the site, 1.0 or 2.0. |
| `start_date` | string | Required | Initial inclusive ISO 8601 timestamp, at whole-second precision. |
| `end_date` | string | First read time | Inclusive upper timestamp bound. Defaults to the first read time. |
| `incremental_start` | string | Unset | Previous cursor timestamp for a later batch. |
| `lookback_seconds` | int | `300` | Seconds of read overlap before the previous cursor. |
| `page_size` | int | `100` | Number of rows requested per list response, from 1 to 100. |
| `max_pages` | int | `10000` | Total accepted pages across lists, parent discovery and date windows. |
| `connect_timeout_ms` | int | `10000` | Maximum TCP connect time in milliseconds. |
| `read_timeout_ms` | int | `30000` | Maximum socket read time in milliseconds. |
| `request_deadline_ms` | int | `60000` | Total HTTPS request and retry budget in milliseconds. |
| `max_attempts` | int | `4` | Maximum attempts for a retryable GET, including the first. |
| `max_response_bytes` | int | `8388608` | Maximum wire or decompressed response size in bytes. |
| `tls_trust_certificate` | string | JVM trust store | Absolute path to engine-local PEM or DER trust anchors. |

The maximum is 20000 for `max_pages`, 31536000 seconds for `lookback_seconds`, 11 for `max_attempts`, and 33554432 bytes for `max_response_bytes`. Connect and socket timeout values range from 1 to 120000 milliseconds. The total request budget ranges from 1 to 300000 milliseconds. A configured trust anchor still requires a valid TLS hostname.

## Date windows, pagination, and recovery

Cursor resources use inclusive UTC bounds in seconds. The upper bound stays fixed once the first read starts. Monthly windows advance by one calendar month from the current lower bound. Each next window starts one second after the preceding upper bound. Attached items use the entire configured interval under each item. Resource types without a documented date-filter option scan their lists and apply the date window locally. A returned record with a null cursor stays in the result with a null timestamp.

A subsequent batch can pass its previous high-water mark as `incremental_start`. The source replays the configured overlap, with an initial floor at `start_date`. Duplicate records across batches are possible. Each resource uses its documented sorting and deleted-record options. Resource versions and deletion flags supplied by the API are preserved. Lists are eventually consistent, and the event resource can export events still available to the site. [List operations](https://apidocs.chargebee.com/docs/api/list-ops)

The reader takes `next_offset` as an opaque value even when it looks like JSON. It fully traverses a child list before moving to the next parent. Parent discovery uses no leaf date filter. Scheduled subscription details use a single-object lookup for inventory with scheduled changes. A disappearing subscription or a response indicating that no changes remain can end that individual lookup.

Checkpoints retain the current UTC window, parent and child offsets, raw positions, and page fingerprints. The active page and parent inventory must match on replay before any following rows can publish. Terminal restore performs no HTTPS calls. Retryable HTTP responses use finite attempts, honoring `Retry-After` within the request budget. The source verifies HTTPS hostnames and rejects redirects.

## Output

| String column | Value |
| --- | --- |
| `id` | Resource identifier. Migration details use `entity_id`, and scheduled subscription details use the initiating subscription ID. |
| `parent_id` | Parent identifier for a nested output, otherwise null. |
| `cursor_timestamp` | UTC time from the selected resource's cursor field, or null. |
| `data` | JSON record with exact numeric values, nested objects and nulls. Top-level `cf_*` fields become `custom_fields` entries containing `name` and `value`. |

Contact, quote line group, and scheduled subscription rows also carry `customer_id`, `quote_id`, or `subscription_id` in `data` based on their lookup. Existing `custom_fields` entries persist. Nested fields starting with `cf_` retain their original names.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Chargebee {
    site = "my-subscriptions"
    token = ${CHARGEBEE_API_KEY}
    resource = "invoice"
    product_catalog = "2.0"
    start_date = "2025-01-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />

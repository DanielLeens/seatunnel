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

# Clazar

The Clazar source reads entity collections and AWS, Azure, and Google Cloud marketplace analytics in bounded batch mode. Use source parallelism 1. The worker exchanges the configured client credentials for a bearer token and renews it during a long scan.

## Resources

| Resource | GET route |
| --- | --- |
| `buyers` | `/buyers/` |
| `listings` | `/listings/` |
| `contracts` | `/contracts/` |
| `opportunities` | `/opportunities/` |
| `private_offers` | `/private_offers/` |
| `analytics_aws_cosell_opportunities` | `/analytics/datasets/aws_cosell_opportunities` |
| `analytics_aws_marketplace_disbursements` | `/analytics/datasets/aws_marketplace_disbursements` |
| `analytics_aws_marketplace_revenue` | `/analytics/datasets/aws_marketplace_revenue` |
| `analytics_azure_cosell_opportunities` | `/analytics/datasets/azure_cosell_opportunities` |
| `analytics_azure_marketplace_customers` | `/analytics/datasets/azure_marketplace_customers` |
| `analytics_azure_marketplace_metered_usage` | `/analytics/datasets/azure_marketplace_metered_usage` |
| `analytics_azure_marketplace_orders` | `/analytics/datasets/azure_marketplace_orders` |
| `analytics_azure_marketplace_revenue` | `/analytics/datasets/azure_marketplace_revenue` |
| `analytics_gcp_marketplace_charges_and_usage` | `/analytics/datasets/gcp_marketplace_charges_and_usage` |
| `analytics_gcp_marketplace_daily_insights` | `/analytics/datasets/gcp_marketplace_daily_insights` |
| `analytics_gcp_marketplace_disbursements` | `/analytics/datasets/gcp_marketplace_disbursements` |
| `analytics_gcp_marketplace_disbursements_summary` | `/analytics/datasets/gcp_marketplace_disbursements_summary` |
| `analytics_gcp_marketplace_incremental_daily_insights` | `/analytics/datasets/gcp_marketplace_incremental_daily_insights` |
| `analytics_gcp_marketplace_incremental_monthly_insights` | `/analytics/datasets/gcp_marketplace_incremental_monthly_insights` |
| `analytics_gcp_marketplace_monthly_insights` | `/analytics/datasets/gcp_marketplace_monthly_insights` |

Clazar marks the first five entity endpoints as deprecated and documents that they still work. This source reads those collections directly. Clazar Query Platform is a separate query interface with different entity and projection semantics. Marketplaces control which datasets and reporting periods each account can access.

## Options

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `client_id` | string | Yes | | Clazar API client ID. |
| `client_secret` | string | Yes | | Clazar API client secret. |
| `resources` | list of strings | Yes | | Unique collection and dataset names from the table. |
| `start_date` | string | No | | Inclusive entity `last_modified_at` bound as an ISO 8601 timestamp with offset. |
| `end_date` | string | No | | Exclusive entity `last_modified_at` bound. With only `start_date`, the source captures its cutoff when creating the split. |
| `page_size` | integer | No | `100` | Requested rows per page, range 1–5000. |
| `max_requests` | integer | No | `10000` | Maximum committed pages per scan, range 1–20000. |
| `max_attempts` | integer | No | `4` | Maximum attempts per data page including the initial GET and any retry after authentication, range 1–6. |
| `request_timeout_ms` | integer | No | `30000` | Page deadline including token exchange, transfer, retries, and decoding, range 100–300000. |
| `max_response_bytes` | integer | No | `8388608` | Maximum decoded response bytes, range 1024–33554432. Token responses also have a 65536-byte ceiling. |
| `endpoint` | string | No | `https://api.clazar.io` | Custom bare HTTPS origin. Requires a trust file. |
| `trust_certificate` | string | For a custom origin | | Path to a worker-local X.509 PEM file. The endpoint hostname is verified. |

Date bounds require an entity resource. The worker applies them locally to retain exact timestamp boundaries and reads all selected analytics rows. Page-number traversal validates the native continuation and total count before advancing. Changing counts or repeated entity IDs across adjacent pages cause a read failure. Analytics rows preserve multiplicity, including equal rows.

The worker sends `client_id` and `client_secret` as JSON to `/authenticate/`. It interprets the positive `expires_in` value as seconds and may renew once after a data GET returns 401. An unsuccessful token exchange ends that read attempt. HTTP 429 data retries respect the service's advertised delay within the page deadline.

## Output and recovery

| Column | Type | Description |
| --- | --- | --- |
| `resource` | string | Collection or dataset name. |
| `record_id` | nullable string | Clazar entity UUID. Null for analytics rows. |
| `record_time` | nullable string | Entity modification time normalized to UTC. Null for analytics rows. |
| `payload` | string | Complete provider JSON object with precise numbers, strings, nulls, and nested data. |

Checkpoints retain the resource, page, cutoff and emitted offset. A restored partial page must match its fingerprint before the worker emits the remaining rows. Clazar may change or revise data across pages and reporting periods. The source provides a bounded extract rather than a provider snapshot or change feed.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Clazar {
    client_id = ${CLAZAR_CLIENT_ID}
    client_secret = ${CLAZAR_CLIENT_SECRET}
    resources = ["contracts", "analytics_aws_marketplace_revenue"]
    start_date = "2026-10-01T00:00:00Z"
    end_date = "2026-11-01T00:00:00Z"
    plugin_output = "marketplace_records"
  }
}

sink {
  Console {
    plugin_input = "marketplace_records"
  }
}
```

[Clazar API index](https://developers.clazar.io/llms.txt), [authentication reference](https://developers.clazar.io/reference/retrieve-an-access-token.md).

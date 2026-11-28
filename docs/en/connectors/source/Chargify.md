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

# Chargify

## Description

The Chargify source reads customers, subscriptions, invoices, coupons and legacy transactions from Maxio Advanced Billing. It uses the API key for one regional billing site and sends HTTPS GET requests.

## Resources

| Resource | Path | Record identifier |
| --- | --- | --- |
| `customers` | `/customers.json` | Customer `id` |
| `subscriptions` | `/subscriptions.json` | Subscription `id` |
| `invoices` | `/invoices.json` | Invoice `uid` |
| `coupons` | `/coupons.json` | Coupon `id` |
| `transactions` | `/transactions.json` | Legacy transaction `id` |

Invoice output uses the provider's standard index fields, including totals. Coupon and invoice visibility follow the selected site's API defaults. Select `transactions` only for subscriptions using Legacy Statement Billing. The separate invoice-billing payment model is outside this resource. API errors for unavailable Legacy Statement Billing requests fail the job with the resource and status code.

## Options

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `access_key` | string | Yes | | Advanced Billing site API key. The source uses it as the HTTP Basic username with `x` as password. |
| `site` | string | Yes | | DNS site label, without the domain suffix. Converted to lowercase. |
| `resources` | list of strings | Yes | | One or more distinct values from the resource table. |
| `region` | string | No | `us` | `us` uses `<site>.chargify.com`, `eu` uses `<site>.ebilling.maxio.com`. |
| `page_size` | integer | No | `200` | Number of rows requested per logical page. Range 1–200. |
| `max_requests` | integer | No | `100000` | Maximum successful pages per scan. Includes empty terminal pages. Range 1–1000000. |
| `max_attempts` | integer | No | `4` | Maximum attempts for a page, including the initial GET. Range 1–10. |
| `request_timeout_ms` | integer | No | `240000` | Total page budget including retries. Range 100–600000. One HTTP attempt is also capped at 120 seconds. |
| `max_response_bytes` | integer | No | `8388608` | Maximum response size after decompression. Range 1024–67108864. |
| `endpoint` | string | No | Regional HTTPS origin | Optional bare HTTPS origin. Requires `trust_certificate`. |
| `trust_certificate` | string | No | | Worker-local PEM trust certificate file for a custom origin. Hostname verification still applies. |

The source reads one numbered page at a time and advances until it receives an empty collection. On HTTP 429 it waits at least as long as the server's `Retry-After` value when provided. The fallback is two minutes. If that wait cannot fit in the page budget, the job fails before another request.

## Output and recovery

| Column | Type | Description |
| --- | --- | --- |
| `resource` | string | Selected resource name. |
| `record_id` | string | Exact provider identifier, without numeric rounding. |
| `site` | string | Normalized billing site label. |
| `region` | string | US or EU code from the configuration. |
| `payload` | string | Complete JSON for the returned record, with nested fields, nulls, and numbers preserved. |

A checkpoint stores page progress and a fingerprint plus offset for any partially emitted page. An altered provider page fails recovery before more of its records are emitted. An adjacent page repeating the same set of identifiers also fails. Start a fresh scan after changing the read scope. The API pages can change independently, so a scan provides no transactional snapshot or deletion events.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  Chargify {
    access_key = ${CHARGIFY_ACCESS_KEY}
    site = "billing-sandbox"
    region = "us"
    resources = ["customers", "subscriptions", "invoices", "coupons"]
    plugin_output = "billing"
  }
}

sink {
  Console {
    plugin_input = "billing"
  }
}
```

See [regional setup and authentication](https://developers.maxio.com/http/getting-started/how-to-get-started), [invoice index](https://developers.maxio.com/http/advanced-billing-api/api-endpoints/invoices/list-invoices), [Legacy Statement Billing transactions](https://developers.maxio.com/legacy/http/api-endpoints/legacy-transactions/list-transactions), and [rate limits](https://developers.maxio.com/http/getting-started/about-the-api/error-handling-rate-limiting).

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

import ChangeLog from "../changelog/connector-http-cin7.md";

# Cin7 Core

The `Cin7` source reads stock, accounting, customer, and transaction records from the [Cin7 Core V2 API](https://coreapi.cin7.com/) in `BATCH` mode. Use the Cin7 Core API Application key in `token` and its Account ID in `access_key`. The source sends both values in the provider's headers to `https://inventory.dearsystems.com/ExternalApi/v2/`.

## Resources

| Resource | GET path | Identity |
| --- | --- | --- |
| `bank_accounts` | `ref/accountBank` | `AccountID` |
| `attribute_sets` | `ref/attributeset` | `ID` |
| `accounts` | `ref/account` | `Code` |
| `brands` | `ref/brand` | `ID` |
| `carriers` | `ref/carrier` | `CarrierID` |
| `customers` | `customer` | `ID` |
| `deals` | `reference/deals` | `ID` |
| `locations` | `ref/location` | `ID` |
| `products` | `product` | `ID` |
| `purchase_list` | `purchaseList` | `ID` |
| `purchases` | `advanced-purchase` | `ID` |
| `suppliers` | `supplier` | `ID` |
| `opportunities` | `crm/opportunity` | `ID` |
| `sale_list` | `saleList` | `SaleID` |
| `sales` | `sale` | `ID` |
| `taxes` | `ref/tax` | `ID` |
| `product_categories` | `ref/category` | `ID` |
| `product_family` | `productFamily` | `ID` |
| `leads` | `crm/lead` | `ID` |
| `product_availability` | `ref/productavailability` | `[ID, Location, Bin, Batch, ExpiryDate]` |

Collection pages use the reported record total and page number. Deals pages use a maximum of 25 records and continue until an empty page. Purchases and sales read their corresponding summaries to discover detail IDs. Selecting details alone reads the parent collection internally and emits only details. When both are selected, the source reads each summary page once.

Customer and supplier reads include deprecated records. Product reads also include deprecated records, BOMs, movements, suppliers, attachments, custom prices and reorder levels. The source preserves attachment URLs as fields. It combines purchase and sale additional charges and includes sale transactions. Product availability uses the API's default stock filter.

## Output

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | Resource name |
| `id` | STRING | Native text identifier or the serialized stock identity array |
| `parent_id` | STRING | The parent summary ID on purchase and sale details. Null on collection rows |
| `record` | STRING | Native JSON object, including numeric precision, nulls, nested fields and source dates |

Missing stock identity components appear as null in the array. Empty strings keep their original value.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | | Application key sent in `api-auth-applicationkey` |
| `access_key` | String | Yes | | Account ID sent in `api-auth-accountid` |
| `resources` | List of strings | No | `[]` | Resource names from the table. An empty list reads all 20 |
| `tls_trust_certificate` | String | No | | Optional source-local PEM or base64 DER X.509 trust anchor. HTTPS hostname verification remains enabled |
| `page_size` | Integer | No | `100` | Requested records per page, from 1 to 1000. Deals use at most 25 |
| `max_pages_per_collection` | Integer | No | `10000` | Maximum accepted pages in a collection, from 1 to 100000 |
| `max_pages_per_scan` | Integer | No | `100000` | Maximum accepted collection and detail responses, from 1 to 1000000 |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connect timeout, from 100 to 300000 ms |
| `request_timeout_ms` | Integer | No | `30000` | Deadline per HTTPS attempt, from 100 to 300000 ms |
| `operation_timeout_ms` | Integer | No | `120000` | Deadline per read across retries and waits. At least `request_timeout_ms` and at most 600000 ms |
| `request_interval_ms` | Integer | No | `1000` | Minimum time between HTTPS attempts from this reader, from 0 to 60000 ms |
| `max_retries` | Integer | No | `3` | Additional eligible transient read attempts, from 0 to 8 |
| `max_retry_delay_ms` | Integer | No | `60000` | Maximum accepted retry delay, from 250 to 600000 ms |
| `max_response_bytes` | Integer | No | `8388608` | Maximum wire and decompressed response bytes, from 256 to 16777216 |

The reader paces requests to account for the application quota. Other jobs and external applications using the same key share that quota. The source bounds retries for HTTP 429 and transient server errors and honors a valid `Retry-After` within the configured limits.

## Recovery

A single reader traverses the selected resources. A checkpoint records the collection, page, row and outstanding detail, with a response fingerprint. The source verifies the saved page before resuming its row or fetching an outstanding detail. Restore with the same Account ID, application key, resource selection and page size. Checkpoints exclude raw credentials and records.

This is a bounded collection scan. Cin7 Core can change records between requests. If page metadata or the saved response conflicts with the scan position, the source fails with a resource-specific error.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Cin7 {
    token = "${CIN7_APPLICATION_KEY}"
    access_key = "${CIN7_ACCOUNT_ID}"
    resources = ["products", "product_availability", "sale_list", "sales"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />

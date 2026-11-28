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

import ChangeLog from "../changelog/connector-http-chameleon.md";

# Chameleon

The `Chameleon` source reads ten resources from the Chameleon v3 API in `BATCH` mode. Use the account secret from the Chameleon dashboard. The connector sends it in the `X-Account-Secret` header over HTTPS to `api.chameleon.io`. [Authentication](https://developers.chameleon.io/concepts/authentication).

| Resource | GET path under `/v3` | Output time |
| --- | --- | --- |
| `helpbars` | `/edit/searches` | `updated_at` |
| `segments` | `/edit/segments` | `updated_at` |
| `urls` | `/edit/urls` | `updated_at` |
| `url_groups` | `/edit/url_groups` | `updated_at` |
| `changes` | `/edit/changes` | `created_at` |
| `launchers` | `/edit/launchers` | `updated_at` |
| `tooltips` | `/edit/tooltips` | `updated_at` |
| `tours` | `/edit/tours` | `updated_at` |
| `surveys` | `/edit/surveys` | `updated_at` |
| `survey_responses` | `/analyze/responses` | `updated_at` |

The HelpBar API returns one complete list per account. Other resources follow the native `cursor.before` continuation even when a page is short or empty. The source reads Microsurvey responses for every discovered survey and requests full profile and company expansions. Select `surveys` to also emit survey records. [Pagination](https://developers.chameleon.io/concepts/pagination) and [Microsurvey responses](https://developers.chameleon.io/apis/survey-responses).

## Output

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | Selected resource name |
| `id` | STRING | Native record ID |
| `survey_id` | STRING | Survey ID for survey and response records. Null for other resources |
| `event_time` | STRING | Output time from the resource table, normalized to UTC. May be null in an unfiltered scan |
| `record` | STRING | Native JSON object, including nested data and explicit nulls |

To filter by time, set either `start_date` or `end_date`. The connector applies the range to each output record after reading it. This finds records that were created earlier and updated during the configured window. All surveys remain available for response discovery regardless of their own output time. A selected row with no applicable timestamp fails if a time range is set. Rows may contain user profile details and free-text survey answers.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | | Chameleon account secret |
| `resources` | List of strings | No | `[]` | Names from the resource table. An empty list selects all ten |
| `start_date` | String | No | | Inclusive ISO 8601 timestamp with an explicit offset |
| `end_date` | String | No | | Exclusive ISO 8601 timestamp with an explicit offset. With a start and no end, the scan uses its start time as the upper bound |
| `tls_trust_certificate` | String | No | | Source-local PEM or base64 DER X.509 trust anchor. HTTPS hostname verification remains enabled |
| `page_size` | Integer | No | `50` | Requested rows per cursor page, from 1 to 500 |
| `max_pages_per_collection` | Integer | No | `10000` | Maximum pages in a collection or one survey's responses, from 1 to 100000 |
| `max_pages_per_scan` | Integer | No | `100000` | Maximum accepted responses across the scan, from 1 to 1000000 |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connect timeout, from 100 to 300000 ms |
| `request_timeout_ms` | Integer | No | `30000` | Deadline for an HTTPS attempt, from 100 to 300000 ms |
| `operation_timeout_ms` | Integer | No | `120000` | Deadline across attempts and waits for one read. At least `request_timeout_ms` and at most 600000 ms |
| `request_interval_ms` | Integer | No | `1050` | Minimum time between HTTPS attempts, from 0 to 60000 ms |
| `max_retries` | Integer | No | `3` | Additional attempts for eligible transient failures, from 0 to 8 |
| `max_retry_delay_ms` | Integer | No | `60000` | Maximum allowed retry delay, from 250 to 600000 ms |
| `max_response_bytes` | Integer | No | `8388608` | Maximum wire and decompressed response bytes, from 256 to 16777216 |

On 429 responses, the source honors Chameleon `X-Retry-After` and `X-Ratelimit-Wait` alongside standard `Retry-After`. If the headers specify no positive delay, it can use the JSON response's `wait` value. [Rate limits](https://developers.chameleon.io/concepts/rate-limiting).

## Recovery

One reader enumerates resources and reads the selected survey responses. A checkpoint keeps continuation values, page fingerprints, the active row and any fixed upper time bound. The connector verifies saved pages before resuming. Source credentials and JSON rows stay outside its checkpoint. A resumed scan uses the same token, selected resources, page size and window configuration. Chameleon records can change between API calls, so this process does not provide an account-wide snapshot.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Chameleon {
    token = "${CHAMELEON_ACCOUNT_SECRET}"
    resources = ["surveys", "survey_responses", "changes"]
    start_date = "2026-05-01T00:00:00Z"
    end_date = "2026-06-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
